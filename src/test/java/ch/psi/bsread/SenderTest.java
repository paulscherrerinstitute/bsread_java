package ch.psi.bsread;

import static org.junit.Assert.*;

import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.psi.bsread.basic.BasicReceiver;
import ch.psi.bsread.message.ChannelConfig;
import ch.psi.bsread.message.Message;
import ch.psi.bsread.message.Type;

public class SenderTest {

	private static final Logger LOGGER = LoggerFactory.getLogger(SenderTest.class.getName());

	private final String testChannel = "ABC";

	@Test
	public void test() {
		Sender sender = new Sender();

		// Register data sources ...
		sender.addSource(new DataChannel<Double>(new ChannelConfig(testChannel, Type.Float64, 1, 0)) {
			@Override
			public Double getValue(long pulseId) {
				return (double) pulseId;
			}
		});

		BasicReceiver receiver = new BasicReceiver();
		try {
			sender.connect();

			receiver.connect();

			// Waiting some time to ensure the connection is established
			TimeUnit.MILLISECONDS.sleep(100);

			// Send data
			for (int pulse = 0; pulse < 11; pulse++) {
				LOGGER.info("Sending for pulse '{}'.", pulse);
				sender.send();
				Message<Object> message = receiver.receive();
				assertEquals((double) pulse, message.getValues().get(testChannel).getValue(Number.class).doubleValue(), 0.001);
			}
		} catch(Exception e){
			e.printStackTrace();
		} finally {
			receiver.close();
			sender.close();
		}
	}

	// TODO Test whether expected messages are created
	// TODO Test different modulo sources
	// TODO Test different offset sources

}
