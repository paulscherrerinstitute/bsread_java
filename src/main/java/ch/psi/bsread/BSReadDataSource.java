package ch.psi.bsread;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.cassandra.utils.MurmurHash;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.zeromq.ZMQ;

import ch.psi.daq.data.config.ConfigConstants;
import ch.psi.daq.data.db.converters.impl.LongArrayByteConverter;
import ch.psi.daq.data.utils.Pair;
import ch.psi.daq.storm.config.DAQConfig;
import ch.psi.daq.storm.config.InputConfig;
import ch.psi.daq.storm.config.InputSourceConfig;
import ch.psi.daq.storm.config.OutputConfig;
import ch.psi.daq.storm.config.OutputSourceConfig;
import ch.psi.daq.storm.config.TimeToLiveConfig;
import ch.psi.daq.storm.data.in.source.DataSink;
import ch.psi.daq.storm.data.in.source.DataToValuesConverter;
import ch.psi.daq.storm.data.in.source.SplitableDataSource;
import ch.psi.daq.storm.data.in.source.bsread.config.BSReadChannelConfig;
import ch.psi.daq.storm.data.in.source.bsread.config.BSReadConfig;
import ch.psi.daq.storm.data.in.source.bsread.config.BSReadDataHeader;
import ch.psi.daq.storm.data.in.source.bsread.config.BSReadHelper;
import ch.psi.daq.storm.data.in.source.bsread.config.BSReadMainHeader;
import ch.psi.daq.storm.data.in.source.bsread.config.GlobalTimestamp;
import ch.psi.daq.storm.data.out.AckStrategy;

public class BSReadDataSource implements SplitableDataSource {
	private static Logger LOGGER = Logger.getLogger(BSReadDataSource.class.getName());
	// two unsigned int -> see DBRDecoder
	private static int IOC_TIME_BYTES = Long.BYTES * 2;
	// private static UIntegerArrayByteConverter IOC_TIME_CONVERTER = new
	// UIntegerArrayByteConverter();
	private static LongArrayByteConverter IOC_TIME_CONVERTER = new LongArrayByteConverter();

	private DataSink sink;
	private DAQConfig daqConf;
	private transient ExecutorService executorService;
	private transient ArrayList<Pair<ZMQReceiver, Future<?>>> receivers;

	@Override
	public void init(DAQConfig daqConf, DataSink sink) {
		LOGGER.log(Level.FINE, () -> String.format("Init '%s'", this.getClass().getName()));

		this.sink = sink;
		this.daqConf = daqConf;

		this.executorService = Executors.newCachedThreadPool();
	}

	@Override
	public void close() {
		LOGGER.log(Level.FINE, () -> String.format("Close '%s'", this.getClass().getName()));

		this.deactivate();

		this.executorService.shutdown();
		try {
			this.executorService.awaitTermination(2000, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			LOGGER.log(Level.WARNING, e, () -> "Interrupted while waiting for ZMQReceiver to stop.");
		}
	}

	@Override
	public void activate() {
		if (this.receivers == null) {
			LOGGER.log(Level.FINE, () -> String.format("Activate '%s'", this.getClass().getName()));

			int readParallelism = this.daqConf.getInputConfig().getReadParallelism();
			this.receivers = new ArrayList<>(readParallelism);
			ZMQReceiver zmqReceiver;
			for (int i = 0; i < readParallelism; ++i) {
				zmqReceiver = new ZMQReceiver(this.sink, this.daqConf);
				zmqReceiver.activate();
				this.receivers.add(Pair.of(zmqReceiver, this.executorService.submit(zmqReceiver)));
			}
		}
	}

	@Override
	public void deactivate() {
		if (this.receivers != null) {
			LOGGER.log(Level.FINE, () -> String.format("Deactivate '%s'", this.getClass().getName()));

			// let existing Receivers complete their task and finish
			for (Pair<ZMQReceiver, Future<?>> pair : this.receivers) {
				pair.getFirst().deactivate();
			}
			// interrupt them if they were waiting on the stream.
			for (Pair<ZMQReceiver, Future<?>> pair : this.receivers) {
				pair.getSecond().cancel(true);
			}

			this.receivers.clear();
			this.receivers = null;
		}
	}

	@Override
	public Collection<InputSourceConfig> extractInputSourceConfigs(DAQConfig daqConfig) {
		return BSReadHelper.extractInputSourceConfigs(daqConfig);
	}

	public static void extractIOCTime(ByteOrder endianness, long[] globalTime, long[] iocTime, ByteBuffer iocTimeByte) {
		// see DBRDecoder line 508 (caj-1.1.12)

		if (iocTimeByte.remaining() == IOC_TIME_BYTES) {
			IOC_TIME_CONVERTER.convert(iocTimeByte, endianness, iocTime);

			// IOC_TIME_CONVERTER.convert(iocTimeByte,
			// config.getByteValueEndianness(), iocTime);
			// // seconds since 0000 Jan 1, 1990 and nanoseconds within
			// second
			// // -> convert into java style time
			// iocTime[0] = TimeHelper.getTimeMillis(iocTime[0],
			// iocTime[1]);
			// iocTime[1] = TimeHelper.getTimeNanoOffset(iocTime[1]);
		} else {
			LOGGER.log(Level.INFO, () -> String.format("IOC time has incorrect format. Expecting '%d' but got '%d' bytes! Use global time.", IOC_TIME_BYTES, iocTimeByte.remaining()));
			iocTime[0] = globalTime[0];
			iocTime[1] = globalTime[1];
		}
	}

	private class ZMQReceiver implements Runnable {
		private DataSink sink;
		private AckStrategy ackStrategy;
		private Long streamSplitIndex;
		private String zmqStreamSource;
		private AtomicBoolean isRunning;
		private ZMQ.Context context;
		private ZMQ.Socket socket;
		private Long daqConfigHash;
		private String daqConfigJson;
		private DAQConfig daqConfig;

		public ZMQReceiver(DataSink sink, DAQConfig daqConf) {
			this.sink = sink;
			this.ackStrategy = daqConf.getAckStrategy();
			this.zmqStreamSource = BSReadHelper.getZMQStreamSource(daqConf);
			this.streamSplitIndex = Long.valueOf(daqConf.getStreamSplitIndex());

			// TODO: load specific output config from OutputWriters

			this.isRunning = new AtomicBoolean(false);

			this.daqConfig = DAQConfig.cloneDAQConfig(daqConf);
			this.daqConfig.setInputConfig(new InputConfig());
			this.daqConfig.setOutputConfig(new OutputConfig());

			// this.daqConfigJson = DAQConfig.getDAQConfigJson(daqConf);
			// ByteBuffer buf =
			// ByteBuffer.wrap(this.daqConfigJson.getBytes(StandardCharsets.UTF_8));
			// this.daqConfigHash = MurmurHash.hash2_64(buf, buf.position(),
			// buf.remaining(), 0);
			this.daqConfigJson = null;
			this.daqConfigHash = null;
		}

		public void activate() {
			if (isRunning.compareAndSet(false, true)) {
				this.reconnectZMQ();
			}
		}

		public void deactivate() {
			if (isRunning.compareAndSet(true, false)) {
				this.closeZMQ();
			}
		}

		private void reconnectZMQ() {
			this.closeZMQ();

			this.context = ZMQ.context(1);
			this.socket = this.context.socket(ZMQ.PULL);
			this.socket.setRcvHWM(BSReadConfig.HIGH_WATER_MARK);
			this.socket.connect(this.zmqStreamSource);
		}

		private void closeZMQ() {
			if (this.socket != null) {
				this.socket.close();
				this.socket = null;
			}
			if (this.context != null) {
				this.context.close();
				this.context = null;
			}
		}

		@Override
		public void run() {
			String mainHeaderStr = null;
			String dataHeaderStr = null;
			ObjectMapper mapper = new ObjectMapper(new JsonFactory());
			BSReadMainHeader mainHeaderConf;
			BSReadDataHeader dataHeaderConf = null;
			List<BSReadChannelConfig> channelConfigs = new ArrayList<>();

			ByteOrder endianness = null;
			ByteBuffer valueByte = null;
			ByteBuffer iocTimeByte = null;

			long currentTimeLoc;
			long[] globalTime = new long[GlobalTimestamp.NR_OF_ELEMENTS];
			long[] iocTime = new long[2];
			long pulseId;
			String activeHashVal = null;
			String hashVal = null;
			DataSink sink = this.sink;
			DataToValuesConverter converter = sink.getConverter();
			Map<String, InputSourceConfig> oldInputSourceConfigs;
			boolean inputSourceChanged;
			Map<String, OutputSourceConfig> newOutputSourceConfs;

			while (this.isRunning.get()) {
				try {
					// # main header #
					// ###############
					mainHeaderStr = this.socket.recvStr();
					currentTimeLoc = System.currentTimeMillis();
					mainHeaderConf = mapper.readValue(mainHeaderStr, BSReadMainHeader.class);

					if (!mainHeaderConf.getHtype().startsWith(BSReadConfig.HEADER_MAIN_HTYPE_VALUE)) {
						String hType = mainHeaderConf.getHtype();
						LOGGER.log(
								Level.WARNING,
								() -> String.format("Expect '%s' for '%s' but was '%s'. Skip messge", BSReadConfig.HEADER_MAIN_HTYPE_VALUE, BSReadConfig.HEADER_MAIN_HTYPE_KEY, hType));

						BSReadHelper.drain(this.socket);
						continue;
					}

					mainHeaderConf.getGlobalTimestamp(globalTime, currentTimeLoc);
					pulseId = mainHeaderConf.getPulseId(globalTime[0]);
					hashVal = mainHeaderConf.getHash();

					// # data header #
					// ###############
					inputSourceChanged = false;
					oldInputSourceConfigs = this.daqConfig.getInputConfig().getSourceConfigMap();
					if (!Objects.equals(activeHashVal, hashVal)) {
						// reload config
						dataHeaderStr = this.socket.recvStr();
						dataHeaderConf = mapper.readValue(dataHeaderStr, BSReadDataHeader.class);

						if (!dataHeaderConf.getHtype().startsWith(BSReadConfig.HEADER_DATA_HTYPE_VALUE)) {
							String hType = mainHeaderConf.getHtype();
							LOGGER.log(
									Level.WARNING,
									() -> String.format("Expect '%s' for '%s' but was '%s'. Skip messge", BSReadConfig.HEADER_DATA_HTYPE_VALUE, BSReadConfig.HEADER_DATA_HTYPE_KEY, hType));

							BSReadHelper.drain(this.socket);
							continue;
						}

						inputSourceChanged = true;
						this.daqConfig.getInputConfig().setSourceConfigs(extractInputSourceConfigs(dataHeaderConf));

						channelConfigs = dataHeaderConf.getChannels();
						activeHashVal = hashVal;
					} else {
						// skip data header
						if (this.socket.hasReceiveMore()) {
							this.socket.recv();
						} else {
							LOGGER.log(Level.WARNING, () -> "There is no data header. Skip message.");

							BSReadHelper.drain(this.socket);
							continue;
						}
					}

					// TODO: manage stream config from distributed map (e.g.
					// using listener and change stream config when changes are
					// applied (and combine it with config comming from IOC)
					newOutputSourceConfs = this.getNewOutputSourceConfigs(inputSourceChanged, oldInputSourceConfigs, this.daqConfig.getInputConfig().getSourceConfigMap(), this.daqConfig
							.getOutputConfig().getSourceConfigMap());
					if (newOutputSourceConfs != null && !newOutputSourceConfs.isEmpty()) {
						this.daqConfig.getOutputConfig().setSourceConfigMap(newOutputSourceConfs);

						this.daqConfigJson = DAQConfig.getDAQConfigJson(daqConf);
						ByteBuffer buf = ByteBuffer.wrap(this.daqConfigJson.getBytes(StandardCharsets.UTF_8));
						this.daqConfigHash = MurmurHash.hash2_64(buf, buf.position(), buf.remaining(), 0);
					}

					endianness = dataHeaderConf.getByteValueEndianness();
					// # read data #
					// #############
					for (int i = 0; i < channelConfigs.size() && this.socket.hasReceiveMore(); ++i) {
						BSReadChannelConfig currentConfig = channelConfigs.get(i);

						// # read data blob #
						// ##################
						if (!this.socket.hasReceiveMore()) {
							LOGGER.log(Level.WARNING, () -> String.format("There is no data for channel '%s'. Skip complete message.", currentConfig.getName()));

							BSReadHelper.drain(this.socket);
							continue;
						}
						valueByte = ByteBuffer.wrap(this.socket.recv()).order(endianness);

						// # read ioc timestamp blob #
						// ###########################
						if (!this.socket.hasReceiveMore()) {
							LOGGER.log(Level.WARNING, () -> String.format("There is no ioc timestamp for channel '%s'. Skip complete message.", currentConfig.getName()));

							BSReadHelper.drain(this.socket);
							continue;
						}
						iocTimeByte = ByteBuffer.wrap(this.socket.recv()).order(endianness);

						// There might be channels with different frequencies
						// (100Hz and 10Hz) that do not always fire together.
						// By convention, channels with no values will have an
						// empty message
						if (valueByte.remaining() > 0) {
							BSReadDataSource.extractIOCTime(endianness, globalTime, iocTime, iocTimeByte);

							sink.publish(
									converter.convert(
											this.streamSplitIndex,
											currentConfig.getName(),
											iocTime[0],
											iocTime[1],
											pulseId,
											globalTime[0],
											globalTime[1],
											ConfigConstants.EVENT_TYPE_DEFAULT,
											ConfigConstants.EVENT_META_DATA_DEFAULT,
											currentConfig.getValueForm(),
											currentConfig.getHeader(),
											valueByte,
											this.ackStrategy,
											this.daqConfigHash,
											this.daqConfigJson
											)
									);
						}
					}

					if (this.socket.hasReceiveMore()) {
						LOGGER.log(Level.INFO, () -> "The zmq socket was not empty after reading all channels!");
						BSReadHelper.drain(this.socket);
					}
				} catch (Throwable e) {
					if (this.isRunning.get()) {
						LOGGER.log(Level.WARNING, e, () -> String.format("Error while reading from zmq stream '%s'", this.zmqStreamSource));
						try {
							Thread.sleep(BSReadConfig.RECONNECT_SLEEP);
						} catch (InterruptedException e1) {
							LOGGER.log(Level.INFO, e, () ->
									"Problem while sleeping for reconnection");
							// Restore the interrupted status
							Thread.currentThread().interrupt();
						}

						this.reconnectZMQ();
					}
				}
			}

			LOGGER.log(Level.INFO, () -> String.format("Stop recording '%s' for source '%s'.", this.getClass().getName(), this.zmqStreamSource));
		}

		private Collection<InputSourceConfig> extractInputSourceConfigs(BSReadDataHeader dataHeaderConf) {
			return BSReadHelper.convertToInputSourceConfigs(dataHeaderConf);
		}

		private Map<String, OutputSourceConfig> getNewOutputSourceConfigs(boolean inputSourceChanged,
				Map<String, InputSourceConfig> newInputSourceConfigs, Map<String, OutputSourceConfig> oldOutputSourceConfigs) {

			if (inputSourceChanged) {
				Map<String, OutputSourceConfig> outSConfs = new HashMap<>(newInputSourceConfigs.size());

				OutputSourceConfig outSConf;
				for (InputSourceConfig inSConf : newInputSourceConfigs.values()) {
					outSConf = oldOutputSourceConfigs.get(inSConf.getSourceId());
					// check if it is a new config
					if (outSConf == null) {
						outSConfs.put(inSConf.getSourceId(), new OutputSourceConfig(inSConf.getSourceId(), inSConf.getType(), inSConf.getShape(), TimeToLiveConfig.TIME_TO_LIVE_CONFIG_DEFAULT));
					}else {
						// there is already an OutputSourceConfig for the InputSourceConfig
						// check if they are equivalent
						if(outSConf.isEquivalent(inSConf)){
							outSConfs.put(outSConf.getSourceId(), outSConf);
						}else{
							outSConfs.put(inSConf.getSourceId(), new OutputSourceConfig(inSConf.getSourceId(), inSConf.getType(), inSConf.getShape(), outSConf.getTimeToLiveConfigs());
						}
					}

				}
			} else {
				return null;
			}
		}
	}

	/**
	 * Extends the DAQConfig with the ZMQ host and the ZMQ port.
	 * 
	 * @param daqConfig
	 *            The DAQConfig
	 * @param zmqHost
	 *            The ZMQ host
	 */
	public static void extendDAQConfig(DAQConfig daqConfig, String zmqHost) {
		BSReadHelper.extendDAQConfig(daqConfig, zmqHost);
	}

	/**
	 * Extends the DAQConfig with the ZMQ host and the ZMQ port.
	 * 
	 * @param daqConfig
	 *            The DAQConfig
	 * @param zmqHost
	 *            The ZMQ host
	 * @param port
	 *            The ZMQ port
	 */
	public static void extendDAQConfig(DAQConfig daqConfig, String zmqHost, int port) {
		BSReadHelper.extendDAQConfig(daqConfig, zmqHost, port);
	}

	/**
	 * Extends the DAQConfig with the ZMQ host and the ZMQ port.
	 * 
	 * @param daqConfig
	 *            The DAQConfig
	 * @param zmqHost
	 *            The ZMQ host
	 * @param protocol
	 *            The protocol (e.g. tcp)
	 * @param port
	 *            The ZMQ port
	 * @param configChannelName
	 *            The name of the config channel
	 */
	public static void extendDAQConfig(DAQConfig daqConfig, String zmqHost, String protocol, int port) {
		BSReadHelper.extendDAQConfig(daqConfig, zmqHost, protocol, port);
	}
}
