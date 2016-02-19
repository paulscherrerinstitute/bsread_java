package ch.psi.bsread.copy.common.allocator;

import java.nio.ByteBuffer;
import java.util.function.IntFunction;

/**
 * Copy of ch.psi.daq.common.allocator.ThreadLocalByteBufferAllocator
 */
// TODO: Good idea? Could lead to huge blocks of non-freeable memory
public class ThreadLocalByteBufferAllocator implements IntFunction<ByteBuffer> {
	private static final ThreadLocal<IntFunction<ByteBuffer>> BYTEBUFFER_ALLOCATOR =
			ThreadLocal.<IntFunction<ByteBuffer>> withInitial
					(() -> new ReuseByteBufferAllocator(
							new ByteBufferAllocator(ByteBufferAllocator.DEFAULT_DIRECT_THRESHOLD)));

	@Override
	public ByteBuffer apply(int nBytes) {
		return BYTEBUFFER_ALLOCATOR.get().apply(nBytes);
	}
}
