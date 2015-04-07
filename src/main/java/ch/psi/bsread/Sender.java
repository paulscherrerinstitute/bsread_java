package ch.psi.bsread;

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import ch.psi.bsread.message.DataHeader;
import ch.psi.bsread.message.MainHeader;
import ch.psi.bsread.message.Timestamp;
import ch.psi.daq.data.db.converters.ByteConverter;
import ch.psi.daq.data.db.converters.impl.LongArrayByteConverter;

public class Sender {
	
	public static final int HIGH_WATER_MARK = 100;
	
	private Context context;
	private Socket socket;
	
	private ObjectMapper mapper = new ObjectMapper();
	
	
	private MainHeader mainHeader = new MainHeader();
	private String dataHeaderString = "";
	private String dataHeaderMD5 = "";
	
	private long pulseId = 0;
	
	private List<DataChannel<?>> channels = new ArrayList<>();
	private List<ByteConverter<?,?,?>> converters = new ArrayList<>();
	private final LongArrayByteConverter timestampConverter = new LongArrayByteConverter();
	private ByteOrder byteOrder = ByteOrder.BIG_ENDIAN;
	
	public void bind(){
		bind("tcp://*:9999");
	}
	
	public void bind(String address){
		this.context = ZMQ.context(1);
		this.socket = this.context.socket(ZMQ.PUSH);
		this.socket.setSndHWM(HIGH_WATER_MARK);
		this.socket.bind(address);
	}
	
	public void close(){
		socket.close();
		context.close();
	}

	public void send() {
		
		mainHeader.setPulseId(pulseId);
		mainHeader.setGlobalTimestamp(new Timestamp(System.currentTimeMillis(), 0L));
		mainHeader.setHash(dataHeaderMD5);
		
		try {
			// Send header
			socket.sendMore(""+mapper.writeValueAsString(mainHeader));
			
			// Send data header
			socket.sendMore(dataHeaderString);
			// Send data
			
			Iterator<ByteConverter<?,?,?>> iconverters = converters.iterator();
			for(DataChannel<?> channel: channels){
				Object value = channel.getValue(pulseId);
				socket.sendByteBuffer(iconverters.next().convertObject(value, byteOrder), ZMQ.SNDMORE);
				
				Timestamp timestamp = new Timestamp(System.currentTimeMillis(), 0L);
				socket.sendByteBuffer(timestampConverter.convert(timestamp.getAsLongArray(), byteOrder), ZMQ.SNDMORE);
			}
			
			socket.send("");
		} catch (JsonProcessingException e) {
			throw new RuntimeException("Unable to serialize message", e);
		}
		
		pulseId++;
	}
	
	
	/**
	 * (Re)Generate the data header based on the configured data channels
	 */
	private void generateDataHeader(){
		DataHeader dataHeader = new DataHeader();
		dataHeader.setByteOrder(byteOrder);
		
		for(DataChannel<?> channel: channels){
			dataHeader.getChannels().add(channel.getConfig());
			
			// Register required converters for performance reasons
			converters.add(channel.getConfig().getType().getConverter());
		}
		
		try {
			dataHeaderString = mapper.writeValueAsString(dataHeader);
			dataHeaderMD5 = Utils.computeMD5(dataHeaderString);
		} catch (JsonProcessingException e) {
			throw new RuntimeException("Unable to generate data header", e);
		}
	}
	
	public void addSource(DataChannel<?> channel){
		channels.add(channel);
		generateDataHeader();
	}
	
	public void removeSource(DataChannel<?> channel){
		channels.remove(channel);
		generateDataHeader();
	}
	
	/**
	 * Returns the currently configured data channels as an unmodifiable list
	 * @return	Unmodifiable list of data channels
	 */
	public List<DataChannel<?>> getChannels(){
		return Collections.unmodifiableList(channels);
	}

	public void setByteOrder(ByteOrder byteOrder) {
		this.byteOrder = byteOrder;
	}

	public ByteOrder getByteOrder() {
		return byteOrder;
	}
}
