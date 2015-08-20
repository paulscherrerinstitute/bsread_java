package ch.psi.bsread;

import ch.psi.bsread.impl.StandardMessageExtractor;

public class ReceiverDirectBufferTest extends ReceiverTest {

	protected Receiver getReceiver() {
		return new Receiver(false, new StandardMessageExtractor(0));
	}
}
