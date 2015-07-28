package ch.psi.bsread;

import static org.junit.Assert.*;

import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.junit.Test;

import ch.psi.bsread.basic.BasicMessage;
import ch.psi.bsread.basic.BasicReceiver;
import ch.psi.bsread.message.ChannelConfig;
import ch.psi.bsread.message.Type;

public class SenderTest {

	private static final Logger logger = Logger.getLogger(SenderTest.class.getName());
	
	private final String testChannel = "ABC";
	
	@Test
	public void test() {
		Sender sender = new Sender();
		
		// Register data sources ...
		sender.addSource(new DataChannel<Double>(new ChannelConfig(testChannel, Type.Double, 1, 0)) {
			@Override
			public Double getValue(long pulseId) {
				return (double)pulseId;
			}
		});
		
		sender.bind();
		
		
		
		BasicReceiver receiver = new BasicReceiver();
		receiver.connect();
		
		// Waiting some time to ensure the connection is established
		try {
			TimeUnit.MILLISECONDS.sleep(100);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		// Send data
		for(int pulse=0;pulse<11;pulse++){
			logger.info("Sending for pulse "+pulse);
			sender.send();
			BasicMessage message = receiver.receive();
			assertEquals((double) pulse, (Double) message.getValues().get(testChannel).getValue(), 0.001);
		}
		
		System.out.println("done");
		
		receiver.close();
		sender.close();
		
	}
	
	// TODO Test whether expected messages are created
	// TODO Test different modulo sources
	// TODO Test different offset sources

}
