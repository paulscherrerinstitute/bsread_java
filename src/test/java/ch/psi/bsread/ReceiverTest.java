package ch.psi.bsread;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import ch.psi.bsread.message.DataHeader;
import ch.psi.bsread.message.MainHeader;

public class ReceiverTest {
	private ObjectMapper mapper = new ObjectMapper();
	
	@Test
	public void test() {
		
		
		Receiver receiver = new Receiver();
		
		receiver.addMainHeaderHandler(header -> printHeader(header));
		receiver.addDataHeaderHandler(header -> printDataHeader(header));
		receiver.addValueHandler(values -> System.out.println(values));
		
		Future<Boolean> future = Executors.newSingleThreadExecutor().submit(receiver);
		try {
			future.get();
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}
		
//		try {
//			receiver.call();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
	
		
		
//		receiver.connect();
//
//		// Receive data
//		while(!Thread.currentThread().isInterrupted()){
//		while(true){
//			receiver.receive();
//		}

//		receiver.close();
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
