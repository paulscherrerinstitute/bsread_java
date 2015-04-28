package ch.psi.bsread;

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

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
	private String dataHeaderString = "";
	private String dataHeaderMD5 = "";

	private final PulseIdProvider pulseIdProvider;

	private List<DataChannel<?>> channels = new ArrayList<>();
	private ByteOrder byteOrder = ByteOrder.BIG_ENDIAN;

	public Sender(){
		this.pulseIdProvider = new StandardPulseIdProvider();
	}
	
	public Sender(PulseIdProvider provider){
		this.pulseIdProvider = provider;
	}
	
	public void bind() {
		bind("tcp://*:9999");
	}

	public void bind(String address) {
		this.context = ZMQ.context(1);
		this.socket = this.context.socket(ZMQ.PUSH);
		this.socket.setSndHWM(HIGH_WATER_MARK);
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
		// check if it is realy necessary to send something (e.g. if there is
		// only only a 10Hz it should send only every 10th call)
		for (int i = 0; i < channels.size() && !isSendNeeded; ++i) {
			isSendNeeded = isSendNeeded(pulseId, channels.get(i));
		}

		if (isSendNeeded) {
			mainHeader.setPulseId(pulseId);
			mainHeader.setGlobalTimestamp(new Timestamp(System.currentTimeMillis(), 0L));
			mainHeader.setHash(dataHeaderMD5);

			try {
				// Send header
				socket.sendMore(mapper.writeValueAsString(mainHeader));

				// Send data header
				socket.sendMore(dataHeaderString);
				// Send data

				int lastSendMore;
				for (int i = 0; i < channels.size(); ++i) {
					channel = channels.get(i);
					lastSendMore = ((i + 1) < channels.size() ? ZMQ.SNDMORE : 0);
					isSendNeeded = isSendNeeded(pulseId, channel);

					if (isSendNeeded) {
						Object value = channel.getValue(pulseId);

						socket.sendByteBuffer(Converter.getBytes(value, byteOrder), ZMQ.SNDMORE);

						// TODO: Use same time for all channels (performance -
						// same ByteBuffer for all)?
						Timestamp timestamp = new Timestamp(System.currentTimeMillis(), 0L);
						socket.sendByteBuffer(Converter.getBytes(timestamp.getAsLongArray(), byteOrder), lastSendMore);
					}
					else {
						// Send placeholder
						socket.send((byte[]) null, ZMQ.SNDMORE);
						socket.send((byte[]) null, lastSendMore);
					}
				}
			} catch (JsonProcessingException e) {
				throw new IllegalStateException("Unable to serialize message", e);
			}
		}
	}

	private boolean isSendNeeded(long pulseId, DataChannel<?> channel) {
		// 100L represents the highest supported frequency. 
		// (??? -> TODO 100 needs to be configurable)
		// This calculation also supports frequencies < 1Hz
		long numberOfPulsesBetweenSends = (long) ((1.0 / channel.getConfig().getFrequency()) * 100L);
		// check if this channel sends data for given pulseId
		return ((pulseId + channel.getConfig().getOffset()) % numberOfPulsesBetweenSends) == 0;
	}

	/**
	 * (Re)Generate the data header based on the configured data channels
	 */
	private void generateDataHeader() {
		DataHeader dataHeader = new DataHeader();
		dataHeader.setByteOrder(byteOrder);

		for (DataChannel<?> channel : channels) {
			dataHeader.getChannels().add(channel.getConfig());
		}

		try {
			dataHeaderString = mapper.writeValueAsString(dataHeader);
			dataHeaderMD5 = Utils.computeMD5(dataHeaderString);
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

	public void setByteOrder(ByteOrder byteOrder) {
		this.byteOrder = byteOrder;
	}

	public ByteOrder getByteOrder() {
		return byteOrder;
	}
}
