package ch.psi.bsread.copy.common.allocator;

import java.nio.ByteBuffer;
import java.util.function.IntFunction;

/**
 * Copy of ch.psi.daq.common.allocator.ByteBufferAllocator
 */
public class ByteBufferAllocator implements IntFunction<ByteBuffer> {
	public static final int DEFAULT_DIRECT_THRESHOLD = 1024;
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
