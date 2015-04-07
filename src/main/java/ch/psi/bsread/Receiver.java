package ch.psi.bsread;

import java.io.IOException;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import com.fasterxml.jackson.databind.ObjectMapper;

import ch.psi.bsread.message.DataHeader;
import ch.psi.bsread.message.MainHeader;

public class Receiver {
	
	public static final int HIGH_WATER_MARK = 100;
	
	private Context context;
	private Socket socket;
	
	private ObjectMapper mapper = new ObjectMapper();
	
	private String dataHeaderHash = "";
	
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
	}

	public void receive() {
		try {
			MainHeader mainHeader;
			DataHeader dataHeader;

			// Receive main header
			mainHeader = mapper.readValue(socket.recv(), MainHeader.class);
			System.out.println(mapper.writeValueAsString(mainHeader)); // TODO REMOVE
			
			// Receive data header
			if(socket.hasReceiveMore()){
				if(mainHeader.getHash().equals(dataHeaderHash)){
					// The data header did not change so no interpretation of the header ...
					socket.recv();
				}
				else{
					dataHeaderHash = mainHeader.getHash();
					dataHeader = mapper.readValue(socket.recv(), DataHeader.class);
					System.out.println(mapper.writeValueAsString(dataHeader)); // TODO REMOVE
					// TODO trigger some callback to handle reconfiguration of receiving
				}
			}
			else{
				throw new RuntimeException();
			}
			
			// TODO: Receiver data
			while(socket.hasReceiveMore()){
				socket.recv();
			}
			
			// TODO: Drop/ignore the last termination message
		
		} catch (IOException e) {
			throw new RuntimeException("Unable to serialize message", e);
		}
	}
}
