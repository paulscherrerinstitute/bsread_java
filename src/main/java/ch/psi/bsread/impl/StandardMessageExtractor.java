package ch.psi.bsread.impl;

import ch.psi.bsread.converter.ValueConverter;

/**
 * A MessageExtractor that allows to use DirectBuffers to store data blobs that
 * are bigger than a predefined threshold. This helps to overcome
 * OutOfMemoryError when Messages are buffered since the JAVA heap space will
 * not be the limiting factor.
 */
public class StandardMessageExtractor<V> extends AbstractMessageExtractor<V> {

	public StandardMessageExtractor() {
	   this(new DirectByteBufferValueConverter());
	}

	/**
	 * Constructor
	 * 
	 * @param valueConverter
	 *            The value converter 
	 */
	public StandardMessageExtractor(ValueConverter valueConverter) {
		super(valueConverter);
	}
}
