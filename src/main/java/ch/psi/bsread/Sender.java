package ch.psi.bsread;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import ch.psi.bsread.message.GlobalTimestamp;
import ch.psi.bsread.message.MainHeader;

public class Sender {
	
	public static final int HIGH_WATER_MARK = 10;
	
	private Context context;
	private Socket socket;
	
	private MainHeader mainHeader = new MainHeader();
	private long pulseId = 0;
	
	public void bind(){
		bind("tcp://*:9999");
	}
	
	public void bind(String address){
		this.context = ZMQ.context(1);
		this.socket = this.context.socket(ZMQ.PULL);
		this.socket.setRcvHWM(HIGH_WATER_MARK);
		this.socket.bind(address);
	}
	
	public void close(){
		socket.close();
		context.close();
	}

	public void send() {
		mainHeader.setPulseId(pulseId);
		mainHeader.setGlobalTimestamp(new GlobalTimestamp(System.currentTimeMillis(), 0L));
		mainHeader.setHash("");
		// Send header
		// Send data header
		// Send data
		
		pulseId++;
	}
	
	

}
