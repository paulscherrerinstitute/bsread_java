package ch.psi.bsread.allocator;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;

import org.junit.Test;

import ch.psi.bsread.common.allocator.ByteBufferAllocator;

public class AllocatorTest {

   @Test
   public void testAllocator() {
      final int allocate = 4;
      final long preAllocated = ByteBufferAllocator.getDirectMemoryUsage();
      // keep ref in order to prevent GC
      final ByteBuffer buf = ByteBufferAllocator.DEFAULT_ALLOCATOR.allocateDirect(allocate);
      final long postAllocated = ByteBufferAllocator.getDirectMemoryUsage();
      assertEquals(preAllocated + allocate, postAllocated);
   }
}
