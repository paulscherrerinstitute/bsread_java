package ch.psi.bsread.impl;

import java.nio.ByteBuffer;
import java.util.function.IntFunction;

public class ReuseByteBufferAllocator implements IntFunction<ByteBuffer> {
	private IntFunction<ByteBuffer> allocator;
	private ByteBuffer buffer;

	public ReuseByteBufferAllocator(IntFunction<ByteBuffer> allocator) {
		this.allocator = allocator;
	}

	@Override
	public ByteBuffer apply(int nBytes) {
		if (buffer == null || buffer.capacity() < nBytes) {
			buffer = allocator.apply(nBytes);
		}
		
		buffer.position(0);
		buffer.limit(buffer.capacity());
		
		return buffer;
	}
}
