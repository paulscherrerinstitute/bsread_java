package ch.psi.bsread;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import com.fasterxml.jackson.databind.ObjectMapper;

import ch.psi.bsread.message.DataHeader;
import ch.psi.bsread.message.MainHeader;
import ch.psi.bsread.message.Message;
import ch.psi.bsread.message.Timestamp;
import ch.psi.bsread.message.Value;

public class Receiver {
	
	public static final int HIGH_WATER_MARK = 100;
	
	private Context context;
	private Socket socket;
	
	private ObjectMapper mapper = new ObjectMapper();
	
	private List<Consumer<MainHeader>> mainHeaderHandlers = new ArrayList<>();
	private List<Consumer<DataHeader>> dataHeaderHandlers = new ArrayList<>();
	private List<Consumer<List<Value>>> valueHandlers = new ArrayList<>();
	
	private String dataHeaderHash = "";
	private int numberOfChannels;
	private ByteOrder endianess = null;
	
	
	public void connect(){
		connect("tcp://localhost:9999");
	}
	
	public void connect(String address){
		this.context = ZMQ.context(1);
		this.socket = this.context.socket(ZMQ.PULL);
		this.socket.setRcvHWM(HIGH_WATER_MARK);
		this.socket.connect(address);
	}
	
	public void close(){
		socket.close();
		context.close();
		socket = null;
		context = null;
	}

	public Message receive() {
		try {
			Message message = new Message();
			MainHeader mainHeader = null;
			DataHeader dataHeader = null;

			// Receive main header
			mainHeader = mapper.readValue(socket.recv(), MainHeader.class);
			
			for(Consumer<MainHeader> handler: mainHeaderHandlers){
				handler.accept(mainHeader);
			}
			
			// Receive data header
			if(socket.hasReceiveMore()){
				if(mainHeader.getHash().equals(dataHeaderHash)){
					// The data header did not change so no interpretation of the header ...
					socket.recv();
				}
				else{
					dataHeaderHash = mainHeader.getHash();
					dataHeader = mapper.readValue(socket.recv(), DataHeader.class);
					endianess = dataHeader.getByteOrder();
					numberOfChannels = dataHeader.getChannels().size();
					
					for(Consumer<DataHeader> handler: dataHeaderHandlers){
						handler.accept(dataHeader);
					}
				}
			}
			else{
				throw new RuntimeException();
			}
			
			// Receiver data
			List<Value> values = new ArrayList<>();
			while(socket.hasReceiveMore()){
				
				byte[] valueBytes = socket.recv(); // value
				if(! socket.hasReceiveMore()){
					// Ignore the last terminating message ...
					break; 
				}
				
				byte[] timestampBytes = socket.recv();

				// Create value object
				if(valueBytes.length>0){
					Value value = new Value();
					value.setValue(ByteBuffer.wrap(valueBytes).order(endianess));
					ByteBuffer tsByteBuffer = ByteBuffer.wrap(timestampBytes).order(endianess);
					value.setTimestamp(new Timestamp(tsByteBuffer.getLong(), tsByteBuffer.getLong()));
					values.add(value);
				}
				else{
					values.add(null);
				}
			}

			// Sanity check of value list 
			if(values.size()!=numberOfChannels){
				throw new RuntimeException("Number of received values does not match number of channels");
			}
			
			for(Consumer<List<Value>> handler: valueHandlers){
				handler.accept(values);
			}
			
			message.setMainHeader(mainHeader);
			message.setDataHeader(dataHeader);
			message.setValues(values);
			return message;
		
		} catch (IOException e) {
			throw new RuntimeException("Unable to serialize message", e);
		}
	}
	
	public void addValueHandler(Consumer<List<Value>> handler){
		valueHandlers.add(handler);
	}
	
	public void removeValueHandler(Consumer<List<Value>> handler){
		valueHandlers.remove(handler);
	}
	
	public void addMainHeaderHandler(Consumer<MainHeader> handler){
		mainHeaderHandlers.add(handler);
	}
	
	public void removeMainHeaderHandler(Consumer<MainHeader> handler){
		mainHeaderHandlers.remove(handler);
	}
	
	public void addDataHeaderHandler(Consumer<DataHeader> handler){
		dataHeaderHandlers.add(handler);
	}
	
	public void removeDataHeaderHandler(Consumer<DataHeader> handler){
		dataHeaderHandlers.remove(handler);
	}

}
