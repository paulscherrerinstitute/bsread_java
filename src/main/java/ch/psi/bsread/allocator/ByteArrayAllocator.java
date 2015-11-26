package ch.psi.bsread.allocator;

import java.util.function.IntFunction;

/**
 * Copy of ch.psi.daq.common.allocator.ByteArrayAllocator
 */
public class ByteArrayAllocator implements IntFunction<byte[]> {

	public ByteArrayAllocator() {
	}

	@Override
	public byte[] apply(int nBytes) {
		return new byte[nBytes];
	}
}
