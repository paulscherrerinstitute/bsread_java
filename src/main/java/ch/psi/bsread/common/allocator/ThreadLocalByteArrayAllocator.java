package ch.psi.bsread.common.allocator;

import java.util.function.IntFunction;

// TODO: Good idea? Could lead to huge blocks of non-freeable memory
public class ThreadLocalByteArrayAllocator implements IntFunction<byte[]> {
   private static final ThreadLocal<IntFunction<byte[]>> BYTEBUFFER_ALLOCATOR =
         ThreadLocal.<IntFunction<byte[]>>withInitial(() -> new ReuseByteArrayAllocator(new ByteArrayAllocator()));
   public static final ThreadLocalByteArrayAllocator DEFAULT_ALLOCATOR = new ThreadLocalByteArrayAllocator();

   @Override
   public byte[] apply(int nBytes) {
      return BYTEBUFFER_ALLOCATOR.get().apply(nBytes);
   }
}
