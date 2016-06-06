package ch.psi.bsread.common.allocator;

import java.nio.ByteBuffer;
import java.util.function.IntFunction;

// TODO: Good idea? Could lead to huge blocks of non-freeable memory
public class ThreadLocalByteBufferAllocator implements IntFunction<ByteBuffer> {
	private static final ThreadLocal<IntFunction<ByteBuffer>> BYTEBUFFER_ALLOCATOR =
			ThreadLocal.<IntFunction<ByteBuffer>> withInitial
					(() -> new ReuseByteBufferAllocator(
							ByteBufferAllocator.DEFAULT_ALLOCATOR));

	@Override
	public ByteBuffer apply(int nBytes) {
		return BYTEBUFFER_ALLOCATOR.get().apply(nBytes);
	}
}
