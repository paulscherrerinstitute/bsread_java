package ch.psi.bsread;

import org.junit.Test;

public class ReceiverTest {

	@Test
	public void test() {
		Receiver receiver = new Receiver();

		receiver.connect();

		// Receive data
//		while(Thread.currentThread().isInterrupted()){
		while(true){
			receiver.receive();
		}

//		receiver.close();
	}

}
