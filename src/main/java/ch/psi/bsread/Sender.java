package ch.psi.bsread;

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import ch.psi.bsread.converter.ByteConverter;
import ch.psi.bsread.converter.MatlabByteConverter;
import ch.psi.bsread.impl.StandardPulseIdProvider;
import ch.psi.bsread.impl.StandardTimeProvider;
import ch.psi.bsread.message.DataHeader;
import ch.psi.bsread.message.MainHeader;
import ch.psi.bsread.message.Timestamp;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Sender {

	public static final int HIGH_WATER_MARK = 100;

	private Context context;
	private Socket socket;

	private ObjectMapper mapper = new ObjectMapper();

	private MainHeader mainHeader = new MainHeader();
	private byte[] dataHeaderBytes;
	private String dataHeaderMD5 = "";

	private final PulseIdProvider pulseIdProvider;
	private final TimeProvider globalTimeProvider;
	private final ByteConverter byteConverter;

	private List<DataChannel<?>> channels = new ArrayList<>();

	public Sender() {
		this(new StandardPulseIdProvider(), new StandardTimeProvider(), new MatlabByteConverter());
	}

	public Sender(PulseIdProvider pulseIdProvider, TimeProvider globalTimeProvider, ByteConverter byteConverter) {
		this.pulseIdProvider = pulseIdProvider;
		this.globalTimeProvider = globalTimeProvider;
		this.byteConverter = byteConverter;
	}

	public void bind() {
		bind("tcp://*:9999");
	}

	public void bind(String address) {
		this.bind(address, HIGH_WATER_MARK);
	}

	public void bind(String address, int highWaterMark) {
		this.context = ZMQ.context(1);
		this.socket = this.context.socket(ZMQ.PUSH);
		this.socket.setSndHWM(highWaterMark);
		this.socket.bind(address);
	}

	public void close() {
		socket.close();
		context.close();
	}

	public void send() {
		long pulseId = pulseIdProvider.getNextPulseId();
		boolean isSendNeeded = false;
		DataChannel<?> channel;
		ByteOrder byteOrder;
		// check if it is realy necessary to send something (e.g. if there is
		// only only a 10Hz it should send only every 10th call)
		for (int i = 0; i < channels.size() && !isSendNeeded; ++i) {
			isSendNeeded = isSendNeeded(pulseId, channels.get(i));
		}

		if (isSendNeeded) {
			mainHeader.setPulseId(pulseId);
			mainHeader.setGlobalTimestamp(this.globalTimeProvider.getTime(pulseId));
			mainHeader.setHash(dataHeaderMD5);

			try {
				// Send header
				socket.send(mapper.writeValueAsBytes(mainHeader), ZMQ.NOBLOCK | ZMQ.SNDMORE);

				// Send data header
				socket.send(dataHeaderBytes, ZMQ.NOBLOCK | ZMQ.SNDMORE);
				// Send data

				int lastSendMore;
				for (int i = 0; i < channels.size(); ++i) {
					channel = channels.get(i);
					byteOrder = channel.getConfig().getByteOrder();
					lastSendMore = ((i + 1) < channels.size() ? ZMQ.NOBLOCK | ZMQ.SNDMORE : ZMQ.NOBLOCK);
					isSendNeeded = isSendNeeded(pulseId, channel);

					if (isSendNeeded) {
						final Object value = channel.getValue(pulseId);

						socket.sendByteBuffer(this.byteConverter.getBytes(channel.getConfig().getType().getKey(), value, byteOrder), ZMQ.NOBLOCK | ZMQ.SNDMORE);

						Timestamp timestamp = channel.getTime(pulseId);
						// c-implementation uses a unsigned long (Json::UInt64,
						// uint64_t) for time -> decided to ignore this here
						socket.sendByteBuffer(this.byteConverter.getBytes(timestamp.getAsLongArray(), byteOrder), lastSendMore);
					}
					else {
						// Send placeholder
						socket.send((byte[]) null, ZMQ.NOBLOCK | ZMQ.SNDMORE);
						socket.send((byte[]) null, lastSendMore);
					}
				}
			} catch (JsonProcessingException e) {
				throw new IllegalStateException("Unable to serialize message", e);
			}
		}
	}

	private boolean isSendNeeded(long pulseId, DataChannel<?> channel) {
		// Check if this channel sends data for given pulseId
		return ((pulseId + channel.getConfig().getOffset()) % channel.getConfig().getModulo()) == 0;
	}

	/**
	 * (Re)Generate the data header based on the configured data channels
	 */
	private void generateDataHeader() {
		DataHeader dataHeader = new DataHeader();

		for (DataChannel<?> channel : channels) {
			dataHeader.getChannels().add(channel.getConfig());
		}

		try {
			dataHeaderBytes = mapper.writeValueAsBytes(dataHeader);
			dataHeaderMD5 = Utils.computeMD5(dataHeaderBytes);
		} catch (JsonProcessingException e) {
			throw new RuntimeException("Unable to generate data header", e);
		}
	}

	public void addSource(DataChannel<?> channel) {
		channels.add(channel);
		generateDataHeader();
	}

	public void removeSource(DataChannel<?> channel) {
		channels.remove(channel);
		generateDataHeader();
	}

	/**
	 * Returns the currently configured data channels as an unmodifiable list
	 * 
	 * @return Unmodifiable list of data channels
	 */
	public List<DataChannel<?>> getChannels() {
		return Collections.unmodifiableList(channels);
	}
}
