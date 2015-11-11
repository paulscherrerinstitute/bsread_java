package ch.psi.bsread.impl;

import java.nio.ByteBuffer;
import java.util.function.IntFunction;

public class ByteBufferAllocator implements IntFunction<ByteBuffer> {
	private int directThreshold;

	public ByteBufferAllocator() {
		this(Integer.MAX_VALUE);
	}

	public ByteBufferAllocator(int directThreshold) {
		this.directThreshold = directThreshold;
	}

	@Override
	public ByteBuffer apply(int nBytes) {
		if (nBytes < directThreshold) {
			return ByteBuffer.allocate(nBytes);
		} else {
			return ByteBuffer.allocateDirect(nBytes);
		}
	}
}
