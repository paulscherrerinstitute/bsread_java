package ch.psi.bsread.basic;

import ch.psi.bsread.Receiver;
import ch.psi.bsread.converter.MatlabByteConverter;
import ch.psi.bsread.converter.ValueConverter;
import ch.psi.bsread.impl.StandardMessageExtractor;

/**
 * A simplified receiver delivering values as real values and not byte blobs.
 */
public class BasicReceiver extends Receiver<Object> {

	public BasicReceiver() {
		this(new MatlabByteConverter());
	}

	public BasicReceiver(ValueConverter valueConverter) {
		super(new StandardMessageExtractor<Object>(valueConverter));
	}
}
