package ch.psi.bsread;

import org.junit.Test;

import ch.psi.bsread.message.ChannelConfig;
import ch.psi.bsread.message.Type;

public class SenderTest {

	@Test
	public void test() {
		Sender sender = new Sender();
		
		// Register data sources ...
		sender.addSource(new DataChannel<Double>(new ChannelConfig("ABC", Type.Double, 10, 0)) {
			@Override
			public Double getValue(long pulseId) {
				return (double)pulseId;
			}
		});
		
		sender.bind();
		
		// Send data
		for(int pulse=0;pulse<11;pulse++){
			sender.send();
		}
		
		sender.close();
		
	}
	
	// TODO Test whether expected messages are created
	// TODO Test different frequency sources
	// TODO Test different offset sources

}
