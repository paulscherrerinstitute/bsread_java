package ch.psi.bsread.common.allocator;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntFunction;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.psi.bsread.common.concurrent.executor.CommonExecutors;
import ch.psi.bsread.common.concurrent.singleton.Deferred;

public class ByteBufferAllocator implements IntFunction<ByteBuffer> {
   private static final String DEFAULT_DIRECT_ALLOCATION_THRESHOLD_PARAM = "DirectMemoryAllocationThreshold";
   private static final String DEFAULT_DIRECT_CLEANER_THRESHOLD_PARAM = "DirectMemoryCleanerThreshold";
   public static final Logger LOGGER = LoggerFactory.getLogger(ByteBufferAllocator.class);

   private static final String MEMORY_USED = "MemoryUsed";
   private static final MBeanServer BEAN_SERVER;
   private static final ObjectName NIO_DIRECT_POOL;
   private static final boolean HAS_MEMORY_USED_ATTRIBUTE;

   static {
      long directAllocationThreshold = Integer.MAX_VALUE; // 64 * 1024; // 64KB
      String thresholdStr = System.getProperty(DEFAULT_DIRECT_ALLOCATION_THRESHOLD_PARAM);

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
            directAllocationThreshold = Long.parseLong(thresholdStr) * multiplier;
         } catch (Exception e) {
            LOGGER.warn("Could not parse '{}' containing '{}' as bytes.", DEFAULT_DIRECT_ALLOCATION_THRESHOLD_PARAM,
                  thresholdStr, e);
         }
      }

      DIRECT_ALLOCATION_THRESHOLD = directAllocationThreshold;
      LOGGER.info("Allocate direct memory if junks get bigger than '{}' bytes.", directAllocationThreshold);

      double directMemoryCleanerThreshold = 0.9;
      thresholdStr = System.getProperty(DEFAULT_DIRECT_CLEANER_THRESHOLD_PARAM);
      if (thresholdStr != null) {
         try {
            directMemoryCleanerThreshold = Double.parseDouble(thresholdStr);
         } catch (Exception e) {
            LOGGER.warn("Could not parse '{}' containing '{}' as double.", DEFAULT_DIRECT_CLEANER_THRESHOLD_PARAM,
                  thresholdStr, e);
         }
      }
      if (directMemoryCleanerThreshold > 0.9) {
         LOGGER.warn("'{}' being '{}' is bigger than '0.9'. Redefine to that value.",
               DEFAULT_DIRECT_CLEANER_THRESHOLD_PARAM, thresholdStr);
         directMemoryCleanerThreshold = 0.9;
      } else if (directMemoryCleanerThreshold < 0.25) {
         LOGGER.warn("'{}' being '{}' is smaller than '0.25'. Redefine to that value.",
               DEFAULT_DIRECT_CLEANER_THRESHOLD_PARAM, thresholdStr);
         directMemoryCleanerThreshold = 0.25;
      }
      final long maxDirectMemory = getDirectMemorySize();
      DIRECT_CLEANER_THRESHOLD = (long) (directMemoryCleanerThreshold * maxDirectMemory);
      LOGGER.info(
            "Run explicit GC if allocated direct memory is bigger than '{}%' of the max direct memory of '{}' bytes.",
            (int) (directMemoryCleanerThreshold * 100), maxDirectMemory);

      ObjectName n = null;
      MBeanServer s = null;
      Object a = null;
      try {
         n = new ObjectName("java.nio:type=BufferPool,name=direct");
      } catch (MalformedObjectNameException e) {
         LOGGER.warn("Unable to initialize ObjectName for DirectByteBuffer allocations.", e);
      } finally {
         NIO_DIRECT_POOL = n;
      }
      if (NIO_DIRECT_POOL != null) {
         s = ManagementFactory.getPlatformMBeanServer();
      }
      BEAN_SERVER = s;
      if (BEAN_SERVER != null) {
         try {
            a = BEAN_SERVER.getAttribute(NIO_DIRECT_POOL, MEMORY_USED);
         } catch (final JMException e) {
            LOGGER.debug("Failed to retrieve nio.BufferPool direct MemoryUsed attribute: " + e);
         }
      }
      HAS_MEMORY_USED_ATTRIBUTE = a != null;

      if (!HAS_MEMORY_USED_ATTRIBUTE) {
         LOGGER.error("Cannot retrieve direct memory usage.");
      }
   }

   public static final long DIRECT_ALLOCATION_THRESHOLD;
   public static final ByteBufferAllocator DEFAULT_ALLOCATOR = new ByteBufferAllocator(
         ByteBufferAllocator.DIRECT_ALLOCATION_THRESHOLD);

   public static final long DIRECT_CLEANER_THRESHOLD;
   private static final DirectBufferCleaner DIRECT_BUFFER_CLEANER = new DirectBufferCleaner(DIRECT_CLEANER_THRESHOLD);

   private long directThreshold;

   public ByteBufferAllocator() {
      this(Integer.MAX_VALUE);
   }

   public ByteBufferAllocator(long directThreshold) {
      this.directThreshold = directThreshold;
   }

   @Override
   public ByteBuffer apply(int nBytes) {
      return allocate(nBytes);
   }

   public ByteBuffer allocate(int nBytes) {
      if (nBytes < directThreshold) {
         return allocateHeap(nBytes);
      } else {
         return allocateDirect(nBytes);
      }
   }

   public ByteBuffer allocateHeap(int nBytes) {
      return ByteBuffer.allocate(nBytes);
   }

   public ByteBuffer allocateDirect(int nBytes) {
      DIRECT_BUFFER_CLEANER.allocateBytes(nBytes);
      return ByteBuffer.allocateDirect(nBytes);
   }

   public static long getDirectMemoryUsage() {
      if (BEAN_SERVER == null || NIO_DIRECT_POOL == null || !HAS_MEMORY_USED_ATTRIBUTE) {
         return 0;
      }
      try {
         final Long value = (Long) BEAN_SERVER.getAttribute(NIO_DIRECT_POOL, MEMORY_USED);
         return value == null ? 0 : value;
      } catch (JMException e) {
         // should print further diagnostic information?
         return 0;
      }
   }

   private static long getDirectMemorySize() {
      final RuntimeMXBean runtimemxBean = ManagementFactory.getRuntimeMXBean();
      final List<String> arguments = runtimemxBean.getInputArguments();
      for (final String s : arguments) {
         if (s.contains("-XX:MaxDirectMemorySize=")) {
            String memSize = s.toLowerCase(Locale.ROOT).replace("-xx:maxdirectmemorysize=", "").trim();
            final long multiplier = getMultiplier(memSize);
            memSize = memSize.replaceAll("[^\\d]", "");

            try {
               return Long.parseLong(memSize) * multiplier;
            } catch (final Exception e) {
               LOGGER.warn("Could not parse '{}'.", memSize, e);
            }
         }
      }

      try {
         final Class<?> VM = Class.forName("sun.misc.VM");
         final Method maxDirectMemory = VM.getMethod("maxDirectMemory");
         final Object result = maxDirectMemory.invoke(null, (Object[]) null);
         if (result != null && result instanceof Long) {
            return (Long) result;
         }
      } catch (Exception e) {
         LOGGER.info("Unable to get maxDirectMemory from VM due to '{}'.", e.getMessage());
      }
      // default according to VM.maxDirectMemory()
      return Runtime.getRuntime().maxMemory();
   }

   private static long getMultiplier(final String byteStr) {
      // for the byte case
      int multiplier = 1;

      if (byteStr.contains("k")) {
         multiplier = 1024;
      } else if (byteStr.contains("m")) {
         multiplier = 1048576;
      } else if (byteStr.contains("g")) {
         multiplier = 1073741824;
      } else if (byteStr.contains("t")) {
         multiplier = 1073741824 * 1024;
      }

      return multiplier;
   }

   // it happened that DirectBuffer memory was not reclaimed. The cause was was
   // not enough gc pressure as there were not enough Object created on the jvm
   // heap and thus gc (which indirectly reclaims DirectByteBuffer's memory)
   // was not executed enought.
   private static class DirectBufferCleaner {
      private final static long DELTA_TIME = TimeUnit.SECONDS.toMillis(2);
      private final long gcThreshold;
      private final AtomicLong earliestNextTime = new AtomicLong(System.currentTimeMillis());
      private final AtomicReference<Object> syncRef = new AtomicReference<>();
      private final Object syncObj = new Object();
      private final Runnable gcRunnable = () -> {
         earliestNextTime.set(System.currentTimeMillis() + DELTA_TIME);

         try {
            System.gc();
            LOGGER.info("Explicit GC finished.");
         } catch (final Exception e) {
            LOGGER.warn("Issues with explicit GC.", e);
         } finally {
            // inform that gc finished
            syncRef.set(null);
         }
      };
      private final Deferred<ExecutorService> gcService = new Deferred<>(
            () -> CommonExecutors.newSingleThreadExecutor(CommonExecutors.QUEUE_SIZE_UNBOUNDED, "DirectBufferCleaner",
                  CommonExecutors.DEFAULT_IS_MONITORING, Thread.MAX_PRIORITY));

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
         if (System.currentTimeMillis() > earliestNextTime.get()
               && getDirectMemoryUsage() + nBytes > gcThreshold
               && syncRef.compareAndSet(null, syncObj)) {
            LOGGER.info("Perform explicit GC.");

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
