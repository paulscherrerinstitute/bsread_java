package ch.psi.bsread;

import java.nio.ByteBuffer;

import ch.psi.bsread.impl.DirectByteBufferValueConverter;
import ch.psi.bsread.impl.StandardMessageExtractor;

public class ReceiverDirectBufferTest extends ReceiverTest {

	protected Receiver<ByteBuffer> getReceiver() {
		return new Receiver<ByteBuffer>(false, new StandardMessageExtractor<ByteBuffer>(new DirectByteBufferValueConverter(0)));
	}
}
