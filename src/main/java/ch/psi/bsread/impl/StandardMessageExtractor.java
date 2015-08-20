package ch.psi.bsread.impl;

/**
 * A MessageExtractor that allows to use DirectBuffers to store data blobs that
 * are bigger than a predefined threshold. This helps to overcome
 * OutOfMemoryError when Messages are buffered since the JAVA heap space will
 * not be the limiting factor.
 */
public class StandardMessageExtractor extends AbstractMessageExtractor {

	public StandardMessageExtractor() {
	}

	/**
	 * Constructor
	 * 
	 * @param directThreshold
	 *            The number of byte threshold defining when direct ByteBuffers
	 *            should be used
	 */
	public StandardMessageExtractor(int directThreshold) {
		super(directThreshold);
	}
}
