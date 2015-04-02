package ch.psi.bsread;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.codec.digest.DigestUtils;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.zeromq.ZMQ;

import backtype.storm.tuple.Values;
import ch.psi.daq.data.DBDataHelper;
import ch.psi.daq.data.db.converters.impl.LongArrayByteConverter;
import ch.psi.daq.storm.config.DAQConfig;
import ch.psi.daq.storm.config.InputConfig;
import ch.psi.daq.storm.config.InputSourceConfig;
import ch.psi.daq.storm.config.OutputConfig;
import ch.psi.daq.storm.config.OutputSourceConfig;
import ch.psi.daq.storm.data.generate.value.LocalDataSource;
import ch.psi.daq.storm.data.generate.value.ScalarValueGenerator;
import ch.psi.daq.storm.data.in.source.DataSink;
import ch.psi.daq.storm.data.in.source.DataSource;
import ch.psi.daq.storm.data.in.source.DataToValuesConverter;
import ch.psi.daq.storm.data.in.source.bsread.BSReadDataSource;
import ch.psi.daq.storm.data.in.source.bsread.config.BSReadChannelConfig;
import ch.psi.daq.storm.data.in.source.bsread.config.BSReadConfig;
import ch.psi.daq.storm.data.in.source.bsread.config.BSReadDataHeader;
import ch.psi.daq.storm.data.in.source.bsread.config.BSReadHelper;
import ch.psi.daq.storm.data.in.source.bsread.config.BSReadMainHeader;
import ch.psi.daq.storm.data.in.source.bsread.config.GlobalTimestamp;
import ch.psi.daq.storm.data.in.source.emitting.EventEmitter;
import ch.psi.daq.storm.data.in.source.emitting.ValuesExtractor;
import ch.psi.daq.storm.loader.InstanceCreator;

public class BSReadDataGenerator implements DataSource, DataSink {
	private static Logger LOGGER = Logger.getLogger(BSReadDataGenerator.class.getName());
	public static final String GLOBAL_SOURCE_ID = "GlobalSource";
	public static final LongArrayByteConverter IOC_TIME_CONVERTER = new LongArrayByteConverter();
	// public static final UIntegerArrayByteConverter IOC_TIME_CONVERTER = new
	// UIntegerArrayByteConverter();
	public static final String BYTE_ORDER = ScalarValueGenerator.BYTE_ORDER;

	private LocalDataSource localDataSource;
	private BSReadDataSource bsReadDataSource;

	private String zmqStreamSource;
	private long[] iocTime = new long[2];
	private MainHeader mainHeader;
	private DataHeader dataHeader;
	private String dataHeaderStr;
	private DataSink valuesQueuingSpout;
	private List<ValuesExtractor> valueExtractors;
	private ByteOrder byteOrderEncoding = DBDataHelper.BYTE_ORDER_DEFAULT;

	private ZMQ.Context context;
	private ZMQ.Socket socket;

	private ObjectMapper mapper = new ObjectMapper(new JsonFactory());

	@Override
	public void init(DAQConfig daqConf, DataSink sink) {
		this.zmqStreamSource = BSReadHelper.getZMQStreamSource(daqConf);

		this.valuesQueuingSpout = sink;
		// for the Config of the underlying EventEmitter
		boolean useSystemTimeForUpdate = true;

		InputConfig inConf = daqConf.getInputConfig();
		this.byteOrderEncoding = inConf.getNonStandardParameter(BYTE_ORDER, DBDataHelper.BYTE_ORDER_DEFAULT);
		Collection<InputSourceConfig> inSConfs = inConf.getSourceConfigs();
		this.valueExtractors = new ArrayList<>(inSConfs.size());
		ValuesExtractor valueExtractor;
		Class<?> valueClazz;
		String type;
		int[] shape;
		this.dataHeader = new DataHeader();
		this.dataHeader.setHtype("bsr_d-1.0");
		this.dataHeader.setEncoding(ChannelConfig.getByteOrderEncoding(this.byteOrderEncoding));

		for (InputSourceConfig inSConf : inSConfs) {
			valueClazz = inSConf.getValueClass().getComponentType();
			if (valueClazz == null) {
				// it was not an array
				valueClazz = inSConf.getValueClass();
			}
			type = valueClazz.getSimpleName().toLowerCase();
			shape = new int[] {inSConf.getArrayLength()};
			this.dataHeader.addChannel(new ChannelConfig(inSConf.getSourceId(), type, shape, inSConf.getUpdateFrequency(), BSReadConfig.HEADER_DATA_CHANNELS_OFFSET_DEFAULT));

			valueExtractor = InstanceCreator.createInstance(ValuesExtractor.class, inSConf.getNonStandardParameters(), ValuesExtractor.VALUE_EXTRACTOR_CLASS_NAME, true);
			valueExtractor.init(daqConf, inConf, inSConf, this.getConverter());
			this.valueExtractors.add(valueExtractor);

			if (!inSConf.getNonStandardParameter(EventEmitter.USE_SYSTEM_TIME_FOR_UPDATE, useSystemTimeForUpdate)) {
				useSystemTimeForUpdate = false;
			}
		}
		try {
			this.dataHeaderStr = this.mapper.writeValueAsString(this.dataHeader);
		} catch (IOException e) {
			String message = "Could not convert data header into json!";
			LOGGER.log(Level.WARNING, message, e);
			throw new RuntimeException(message, e);
		}

		this.mainHeader = new MainHeader("bsr_m-1.0", 0L, new Timestamp(), DigestUtils.md5Hex(this.dataHeaderStr));

		// # Config for underlying DefaultEventEmitter #
		// #############################################
		InputSourceConfig emmitInSConf = new InputSourceConfig(GLOBAL_SOURCE_ID, Integer.class);
		// set to 100Hz
		emmitInSConf.setUpdateRate(InputSourceConfig.UPDATE_RATE_DEFAULT);
		emmitInSConf.setNonStandardParameter(ValuesExtractor.VALUE_EXTRACTOR_CLASS_NAME, ScalarValueGenerator.class.getName());
		emmitInSConf.setNonStandardParameter(EventEmitter.USE_SYSTEM_TIME_FOR_UPDATE, useSystemTimeForUpdate);
		InputConfig emmitInConf = new InputConfig( emmitInSConf);
		
		OutputSourceConfig emmitOutSConf = new OutputSourceConfig(GLOBAL_SOURCE_ID);
		OutputConfig emmitOutConf = new OutputConfig(emmitOutSConf);

		DAQConfig emmitDAQConf = new DAQConfig("BSRead dummy data generator", this.getClass());
		emmitDAQConf.setInputConfig(emmitInConf);
		emmitDAQConf.setOutputConfig(emmitOutConf);
		emmitDAQConf.setReadStart(daqConf.getReadStart());
		emmitDAQConf.setReadEnd(daqConf.getReadEnd());

		this.localDataSource = new LocalDataSource();
		this.localDataSource.init(emmitDAQConf, this);

		this.bsReadDataSource = new BSReadDataSource();
		this.bsReadDataSource.init(daqConf, sink);
	}

	@Override
	public void close() {
		this.deactivate();

		if (this.localDataSource != null) {
			this.localDataSource.close();
		}
		this.localDataSource = null;

		if (this.bsReadDataSource != null) {
			this.bsReadDataSource.close();
		}
		this.bsReadDataSource = null;

		for (ValuesExtractor valueExtractor : valueExtractors) {
			valueExtractor.close();
		}
		this.valueExtractors.clear();

		this.valuesQueuingSpout = null;
	}

	@Override
	public void activate() {
		this.reconnectZMQ();

		for (ValuesExtractor valueExtractor : valueExtractors) {
			valueExtractor.activate();
		}

		if (this.bsReadDataSource != null) {
			this.bsReadDataSource.activate();
		}

		if (this.localDataSource != null) {
			this.localDataSource.activate();
		}
	}

	@Override
	public void deactivate() {
		if (this.localDataSource != null) {
			this.localDataSource.deactivate();
		}

		if (this.bsReadDataSource != null) {
			this.bsReadDataSource.deactivate();
		}

		for (ValuesExtractor valueExtractor : valueExtractors) {
			valueExtractor.deactivate();
		}

		this.closeZMQ();
	}

	protected void reconnectZMQ() {
		this.closeZMQ();

		this.context = ZMQ.context(1);
		this.socket = this.context.socket(ZMQ.PUSH);
		this.socket.setRcvHWM(BSReadConfig.HIGH_WATER_MARK);
		this.socket.bind(this.zmqStreamSource);
		// this.socket.bind("tcp://*:9999");
	}

	protected void closeZMQ() {
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
	public DataToValuesConverter getConverter() {
		if (this.valuesQueuingSpout != null) {
			return this.valuesQueuingSpout.getConverter();
		}
		else {
			return null;
		}
	}

	@Override
	public void publish(Values emitterValues) {
		try {
			// # Important:
			// Be aware that this code cannot be called concurrent (due to
			// global variables)
			DataToValuesConverter converter = this.getConverter();
			long timeMillis = converter.getTimestampMillis(emitterValues);
			long timeNano = converter.getTimestampNanoOffset(emitterValues);
			long pulseId = converter.getPulseId(emitterValues);

			this.iocTime[0] = timeMillis;
			this.iocTime[1] = timeNano;
			// iocTime[0] = TimeHelper.getEpicsSec(timeMillis);
			// iocTime[1] = TimeHelper.getEpicsNanoSec(timeMillis, timeNano);
			ByteBuffer iocTimeByte = IOC_TIME_CONVERTER.convert(iocTime, this.byteOrderEncoding);
			long delay100HZ;

			this.mainHeader.setPulseId(pulseId);
			Timestamp globalTimestamp = this.mainHeader.getGlobalTimestamp();
			globalTimestamp.setEpoch(timeMillis);
			globalTimestamp.setNs(timeNano);

			this.socket.send(this.mapper.writeValueAsString(this.mainHeader), ZMQ.SNDMORE);
			this.socket.send(this.dataHeaderStr, ZMQ.SNDMORE);
			int flag;
			ValuesExtractor extractor;
			List<ChannelConfig> channelConfigs = this.dataHeader.getChannels();
			ChannelConfig channelConfig;
			boolean sendData;

			for (int i = 0; i < this.valueExtractors.size() && i < channelConfigs.size(); ++i) {
				flag = ZMQ.SNDMORE;
				extractor = this.valueExtractors.get(i);
				channelConfig = channelConfigs.get(i);
				delay100HZ = (long) ((1.0 / channelConfig.getFrequency()) * 100L);

				// check if this channel sends data in this iteration
				sendData = ((pulseId + channelConfig.getOffset()) % delay100HZ) == 0;
				if (sendData) {
					Values channelValues = extractor.next(pulseId, timeMillis, timeNano);
					ByteBuffer valueBytes = converter.getValue(channelValues);
					this.socket.sendByteBuffer(valueBytes, flag);
				} else {
					// send an empty data blob to make sure sequence is correct
					// at client side.
					this.socket.send((byte[]) null, flag);
				}

				flag = (i + 1) < this.valueExtractors.size() ? ZMQ.SNDMORE : 0;
				if (sendData) {
					this.socket.sendByteBuffer(iocTimeByte, flag);
				} else {
					// send an empty ioc time blob to make sure sequence is
					// correct at client side.
					this.socket.send((byte[]) null, flag);
				}
			}
		} catch (Throwable t) {
			LOGGER.log(Level.WARNING, "Could not send BSRead messages!", t);
		}
	}

	@Override
	public void extractInputSourceConfigs(DAQConfig daqConfig) {
		throw new UnsupportedOperationException("This is for data generation. No need to support it.");
	}
}
