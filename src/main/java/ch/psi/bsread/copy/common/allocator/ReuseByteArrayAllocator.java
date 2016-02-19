package ch.psi.bsread.copy.common.allocator;

import java.util.function.IntFunction;

/**
 * Copy of ch.psi.daq.common.allocator.ReuseByteArrayAllocator
 */
//TODO: Good idea? Could lead to huge blocks of non-freeable memory
public class ReuseByteArrayAllocator implements IntFunction<byte[]> {
	private IntFunction<byte[]> allocator;
	private byte[] array;

	public ReuseByteArrayAllocator(IntFunction<byte[]> allocator) {
		this.allocator = allocator;
	}

	@Override
	public byte[] apply(int nBytes) {
		if (array == null || array.length < nBytes) {
			array = allocator.apply(nBytes);
		}

		return array;
	}
}
