package ch.psi.bsread;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import ch.psi.bsread.message.DataHeader;
import ch.psi.bsread.message.MainHeader;
import ch.psi.bsread.message.Message;

public class ReceiverTest {
	private ObjectMapper mapper = new ObjectMapper();
	
	@Test
	public void test() {
		
		
		Receiver receiver = new Receiver();
		
		// Optional - register callbacks
		receiver.addMainHeaderHandler(header -> printHeader(header));
		receiver.addDataHeaderHandler(header -> printDataHeader(header));
		receiver.addValueHandler(values -> System.out.println(values));
	
		receiver.connect();

		// Receive data
		Message message = null;
		while(!Thread.currentThread().isInterrupted()){
			message = receiver.receive();
			System.out.println(message.getMainHeader().getPulseId());
		}
		

		receiver.close();
	}

	public void printHeader(MainHeader header){
		try {
			System.out.println(mapper.writeValueAsString(header));
		} catch (JsonProcessingException e) {
		}
	}
	
	public void printDataHeader(DataHeader header){
		try {
			System.out.println(mapper.writeValueAsString(header));
		} catch (JsonProcessingException e) {
		}
	}
	
	
}
