package ch.psi.bsread;

import ch.psi.bsread.converter.MatlabByteConverter;
import ch.psi.bsread.impl.StandardMessageExtractor;
import ch.psi.bsread.message.Message;

public class ReceiverExample {

	public static void main(String[] args) {
		Receiver<Object> receiver = new Receiver<Object>(new StandardMessageExtractor<Object>(new MatlabByteConverter()));
		
		receiver.connect("tcp://localhost:9000");

		// Its also possible to register callbacks for certain message parts.
		// These callbacks are triggered within the receive() function 
		// (within the same thread) it is guaranteed that the sequence is ordered
		// main header, data header, values
//		receiver.addDataHeaderHandler(header -> System.out.println(header));
//		receiver.addMainHeaderHandler(header -> System.out.println(header) );
//		receiver.addValueHandler(data -> System.out.println(data));
		
		while(!Thread.currentThread().isInterrupted()){
			Message<Object> message = receiver.receive();
			
			System.out.println(message.getMainHeader());
			System.out.println(message.getDataHeader());
			System.out.println(message.getValues());
		}
		
		receiver.close();
	}

}
