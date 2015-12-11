package ch.psi.bsread.basic;

import ch.psi.bsread.Receiver;
import ch.psi.bsread.ReceiverConfig;
import ch.psi.bsread.converter.MatlabByteConverter;
import ch.psi.bsread.impl.StandardMessageExtractor;

/**
 * A simplified receiver delivering values as real values and not byte blobs.
 */
public class BasicReceiver extends Receiver<Object> {

	public BasicReceiver() {
		this(new ReceiverConfig<Object>(new StandardMessageExtractor<Object>(new MatlabByteConverter())));
	}

	public BasicReceiver(ReceiverConfig<Object> receiverConfig) {
		super(receiverConfig);
	}
}
