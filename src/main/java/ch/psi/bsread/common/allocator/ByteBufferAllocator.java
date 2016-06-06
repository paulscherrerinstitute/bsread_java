package ch.psi.bsread.common.allocator;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ByteBufferAllocator implements IntFunction<ByteBuffer> {
	public static final int DEFAULT_DIRECT_THRESHOLD = Integer.MAX_VALUE; // 1024;
																			// //
																			// 1K
	public static final long DEFAULT_DIRECT_CLEAN_THRESHOLD = 8L * 1024L * 1024L * 1024L; // 8G
	private static final DirectBufferCleaner DIRECT_BUFFER_CLEANER = new DirectBufferCleaner(DEFAULT_DIRECT_CLEAN_THRESHOLD);
	public static final ByteBufferAllocator DEFAULT_ALLOCATOR = new ByteBufferAllocator(ByteBufferAllocator.DEFAULT_DIRECT_THRESHOLD);

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
			DIRECT_BUFFER_CLEANER.allocateBytes(nBytes);
			return ByteBuffer.allocateDirect(nBytes);
		}
	}

	// it happened that DirectBuffer memory was not reclaimed. The cause was was
	// not enough gc pressure as there were not enough Object created on the jvm
	// heap and thus gc (which indirectly reclaims DirectByteBuffer's memory)
	// was not executed enought.
	private static class DirectBufferCleaner {
		private static final Logger LOGGER = LoggerFactory.getLogger(DirectBufferCleaner.class);
		private final long gcThreshold;
		private AtomicLong allocatedBytes = new AtomicLong();

		public DirectBufferCleaner(long gcThreshold) {
			this.gcThreshold = gcThreshold;
		}

		public void allocateBytes(int nBytes) {
			long totalDirectBytes = allocatedBytes.addAndGet(nBytes);

			// see
			// https://docs.oracle.com/javase/8/docs/api/java/lang/management/GarbageCollectorMXBean.html
			// and
			// https://docs.oracle.com/javase/8/docs/api/java/lang/management/MemoryPoolMXBean.html
			// for more info on GC
			if (totalDirectBytes > gcThreshold && allocatedBytes.compareAndSet(totalDirectBytes, 0)) {
				LOGGER.debug("Perform gc with '{}' direct bytes.", totalDirectBytes);

				System.gc();
			}
		}
	}
}
