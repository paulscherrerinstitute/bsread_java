package ch.psi.bsread.impl;

import java.nio.ByteBuffer;

import ch.psi.bsread.converter.ValueConverter;

public class DirectByteBufferValueConverter implements ValueConverter {
	private int directThreshold;

	public DirectByteBufferValueConverter() {
		this(Integer.MAX_VALUE);
	}

	public DirectByteBufferValueConverter(int directThreshold) {
		this.directThreshold = directThreshold;
	}

	@SuppressWarnings("unchecked")
	@Override
	public ByteBuffer getValue(ByteBuffer byteValue, String type, int[] shape) {
		if (byteValue.remaining() <= directThreshold) {
			return byteValue;
		} else if (byteValue.isDirect()) {
			return byteValue;
		} else {
			ByteBuffer direct = ByteBuffer.allocateDirect(byteValue.remaining());

			direct.order(byteValue.order());
			direct.put(byteValue.duplicate());
			direct.flip();
			return direct;
		}
	}
}
