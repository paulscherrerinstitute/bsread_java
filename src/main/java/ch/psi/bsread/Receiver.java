package ch.psi.bsread;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import com.fasterxml.jackson.databind.ObjectMapper;

import ch.psi.bsread.message.DataHeader;
import ch.psi.bsread.message.MainHeader;
import ch.psi.bsread.message.Timestamp;
import ch.psi.bsread.message.Value;

public class Receiver implements Callable<Boolean> {
	
	public static final int HIGH_WATER_MARK = 100;
	
	private Context context;
	private Socket socket;
	
	private ObjectMapper mapper = new ObjectMapper();
	
	private List<Consumer<MainHeader>> mainHeaderHandlers = new ArrayList<>();
	private List<Consumer<DataHeader>> dataHeaderHandlers = new ArrayList<>();
	private List<Consumer<List<Value>>> valueHandlers = new ArrayList<>();
	
	private String dataHeaderHash = "";
	private ByteOrder endianess = null;
	
	private String address;
	
	public Receiver(){
		this.address = "tcp://localhost:9999";
	}
	
	
	public Receiver(String address){
		this.address = address;
	}
	
	public void connect(){
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

	public void receive() {
		try {

			MainHeader mainHeader;
			DataHeader dataHeader;

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
				
				byte[] val = socket.recv(); // value
				if(! socket.hasReceiveMore()){
					// Ignore the last terminating message ...
					break; 
				}
				
				byte[] tsbytes = socket.recv();

				// Create value object
				Value value = new Value();
				value.setValue(ByteBuffer.wrap(val).order(endianess));
				ByteBuffer tsByteBuffer = ByteBuffer.wrap(tsbytes).order(endianess);
				value.setTimestamp(new Timestamp(tsByteBuffer.getLong(), tsByteBuffer.getLong()));
				values.add(value);
			}
			
			for(Consumer<List<Value>> handler: valueHandlers){
				handler.accept(values);
			}
		
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

	public void setAddress(String address){
		// Check whether there is currently no connection open.
		if(socket==null || context==null){
			throw new IllegalStateException("Receiver currently has a connection open ...");
		}
		this.address = address;
	}
	
	
	@Override
	public Boolean call() throws Exception {
		connect();
		while(!Thread.currentThread().isInterrupted()){
			receive();
		}
		close();
		return true;
	}
}
