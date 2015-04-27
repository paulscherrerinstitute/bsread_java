package ch.psi.bsread.basic;

import static org.junit.Assert.*;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import ch.psi.bsread.DataChannel;
import ch.psi.bsread.Sender;
import ch.psi.bsread.message.ChannelConfig;
import ch.psi.bsread.message.Type;

public class BasicReceiverTest {

	private final String testChannel = "ABC";
	
	@Test
	public void test() {
		Sender sender = new Sender();

		// Register data sources ...
		sender.addSource(new DataChannel<Double>(new ChannelConfig(testChannel, Type.Double, 100, 0)) {
			@Override
			public Double getValue(long pulseId) {
				return (double) pulseId;
			}
		});

		sender.bind();
		// We schedule faster than 100 HZ as we want to have the testcase execute faster
		ScheduledFuture<?> sendFuture = Executors.newScheduledThreadPool(1).scheduleAtFixedRate(() -> sender.send(), 0, 10, TimeUnit.MILLISECONDS);
		
		BasicReceiver receiver = new BasicReceiver();
		receiver.connect();
		
		for(double i=0;i<50;i++){
			Double value = (Double) receiver.receive().getValues().get(testChannel).getValue();
			assertEquals(i, value, 0.001);
		}

		sendFuture.cancel(true);
		
		receiver.close();
		sender.close();
	}

}
