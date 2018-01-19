package ch.psi.bsread.basic;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import ch.psi.bsread.DataChannel;
import ch.psi.bsread.ScheduledSender;
import ch.psi.bsread.message.ChannelConfig;
import ch.psi.bsread.message.Type;

public class BasicReceiverTest {

	private final String testChannel = "ABC";
	private long initialDelay = 200;
	private long period = 1;

	@Test
	public void test() {
		ScheduledSender sender = new ScheduledSender();

		// Register data sources ...
		sender.addSource(new DataChannel<Double>(new ChannelConfig(testChannel, Type.Float64, 1, 0)) {
			@Override
			public Double getValue(long pulseId) {
				return (double) pulseId;
			}
		});

		BasicReceiver receiver = new BasicReceiver();

		try {
			receiver.connect();
			// We schedule faster as we want to have the testcase execute faster
			sender.connect();
			sender.sendAtFixedRate(initialDelay, period, TimeUnit.MILLISECONDS);

			for (double i = 0; i < 50; i++) {
				Double value = (Double) receiver.receive().getValues().get(testChannel).getValue();
				assertEquals(i, value, 0.001);
			}
		} finally {
			receiver.close();
			sender.close();
		}
	}
}
