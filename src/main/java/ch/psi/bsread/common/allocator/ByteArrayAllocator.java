package ch.psi.bsread.common.allocator;

import java.util.function.IntFunction;

public class ByteArrayAllocator implements IntFunction<byte[]> {

	public ByteArrayAllocator() {
	}

	@Override
	public byte[] apply(int nBytes) {
		return new byte[nBytes];
	}
}
