package ch.psi.bsread.copy.common.allocator;

import java.util.function.IntFunction;

/**
 * Copy of ch.psi.daq.common.allocator.ThreadLocalByteArrayAllocator
 */
//TODO: Good idea? Could lead to huge blocks of non-freeable memory
public class ThreadLocalByteArrayAllocator implements IntFunction<byte[]> {
	private static final ThreadLocal<IntFunction<byte[]>> BYTEBUFFER_ALLOCATOR =
			ThreadLocal.<IntFunction<byte[]>> withInitial
					(() -> new ReuseByteArrayAllocator(new ByteArrayAllocator()));

	@Override
	public byte[] apply(int nBytes) {
		return BYTEBUFFER_ALLOCATOR.get().apply(nBytes);
	}
}
