package ch.psi.bsread.common.allocator;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.psi.bsread.common.concurrent.executor.CommonExecutors;
import ch.psi.bsread.common.concurrent.singleton.Deferred;

import sun.misc.JavaNioAccess.BufferPool;
import sun.misc.VM;

public class ByteBufferAllocator implements IntFunction<ByteBuffer> {
   private static final String DEFAULT_DIRECT_THRESHOLD_PARAM = "DirectMemoryAllocationThreshold";
   public static final Logger LOGGER = LoggerFactory.getLogger(ByteBufferAllocator.class);

   static {
      long threshold = Integer.MAX_VALUE; // 64 * 1024; // 64KB
      String thresholdStr = System.getProperty(DEFAULT_DIRECT_THRESHOLD_PARAM);

      long multiplier = 1; // for the byte case.
      if (thresholdStr != null) {
         thresholdStr = thresholdStr.toLowerCase().trim();

         if (thresholdStr.contains("k")) {
            multiplier = 1024;
         } else if (thresholdStr.contains("m")) {
            multiplier = 1048576;
         } else if (thresholdStr.contains("g")) {
            multiplier = 1073741824;
         } else if (thresholdStr.contains("t")) {
            multiplier = 1073741824 * 1024;
         }
         thresholdStr = thresholdStr.replaceAll("[^\\d]", "");

         try {
            threshold = Long.parseLong(thresholdStr) * multiplier;
         } catch (Exception e) {
            LOGGER.warn("Could not parse '{}' containing '{}' as bytes.", DEFAULT_DIRECT_THRESHOLD_PARAM, thresholdStr,
                  e);
         }
      }

      DEFAULT_DIRECT_THRESHOLD = threshold;
   }
   public static final long DEFAULT_DIRECT_THRESHOLD;
   public static final ByteBufferAllocator DEFAULT_ALLOCATOR = new ByteBufferAllocator(
         ByteBufferAllocator.DEFAULT_DIRECT_THRESHOLD);

   public static final long DEFAULT_DIRECT_CLEAN_THRESHOLD = (long) (0.9 * VM.maxDirectMemory());
   private static final DirectBufferCleaner DIRECT_BUFFER_CLEANER = new DirectBufferCleaner(
         DEFAULT_DIRECT_CLEAN_THRESHOLD);
   private static BufferPool DIRECT_BUFFER_POOL = sun.misc.SharedSecrets.getJavaNioAccess().getDirectBufferPool();

   private long directThreshold;

   public ByteBufferAllocator() {
      this(Integer.MAX_VALUE);
   }

   public ByteBufferAllocator(long directThreshold) {
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
      private final long gcThreshold;
      private AtomicReference<Object> syncRef = new AtomicReference<>();
      private Object syncObj = new Object();
      private Runnable gcRunnable = ()->{
        System.gc();
        syncRef.compareAndSet(syncObj, null);
      };
      private Deferred<ExecutorService> gcService = new Deferred<>(
            () -> CommonExecutors.newSingleThreadExecutor("DirectBufferCleaner"));

      public DirectBufferCleaner(long gcThreshold) {
         this.gcThreshold = gcThreshold;
      }

      public void allocateBytes(int nBytes) {
         // long totalDirectBytes = allocatedBytes.addAndGet(nBytes);

         // see
         // https://docs.oracle.com/javase/8/docs/api/java/lang/management/GarbageCollectorMXBean.html
         // and
         // https://docs.oracle.com/javase/8/docs/api/java/lang/management/MemoryPoolMXBean.html
         // for more info on GC
         if (DIRECT_BUFFER_POOL.getMemoryUsed() > gcThreshold && syncRef.compareAndSet(null, syncObj)) {
            LOGGER.info("Perform gc with '{}' direct bytes and '{}' threshold.", DIRECT_BUFFER_POOL.getMemoryUsed(), gcThreshold);

            // see also:
            // https://github.com/apache/hbase/blob/master/hbase-server/src/main/java/org/apache/hadoop/hbase/util/DirectMemoryUtils.java
            // -> destroyDirectByteBuffer()
            // or:
            // https://apache.googlesource.com/flume/+/trunk/flume-ng-core/src/main/java/org/apache/flume/tools/DirectMemoryUtils.java
            // -> clean()
            gcService.get().execute(gcRunnable);
         }
      }
   }
}
