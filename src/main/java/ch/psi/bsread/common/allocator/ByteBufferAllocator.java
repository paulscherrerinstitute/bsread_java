package ch.psi.bsread.common.allocator;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.psi.bsread.common.concurrent.executor.CommonExecutors;
import ch.psi.bsread.common.concurrent.singleton.Deferred;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ByteBufferAllocator implements IntFunction<ByteBuffer> {

    private static final String DEFAULT_DIRECT_ALLOCATION_THRESHOLD_PARAM = "DirectMemoryAllocationThreshold";
    private static final String DEFAULT_DIRECT_CLEANER_THRESHOLD_PARAM = "DirectMemoryCleanerThreshold";
    public static final Logger LOGGER = LoggerFactory.getLogger(ByteBufferAllocator.class);
    private static final BufferPoolMXBean POOL_DIRECT;


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
        final long maxDirectMemory = maxDirectMemory0(); //VM.maxDirectMemory();
        DIRECT_CLEANER_THRESHOLD = (long) (directMemoryCleanerThreshold * maxDirectMemory);
        LOGGER.info(
                "Run explicit GC if allocated direct memory is bigger than '{}%' of the max direct memory of '{}' bytes.",
                (int) (directMemoryCleanerThreshold * 100), maxDirectMemory);

        BufferPoolMXBean poolDirect = null;
        List<BufferPoolMXBean> pools = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);
        for (BufferPoolMXBean pool : pools) {
            if (pool.getName().equals("direct")) {
                poolDirect = pool;
                break;
            }
        }
        POOL_DIRECT = poolDirect;
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
                    &&  getDirectMemoryUsage() + nBytes > gcThreshold
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

    public static long getDirectMemoryUsage() {
        return POOL_DIRECT.getMemoryUsed();
    }

    //From https://github.com/Jire/Acelta/blob/master/src/main/java/io/netty/util/internal/PlatformDependent.java
    private static final Pattern MAX_DIRECT_MEMORY_SIZE_ARG_PATTERN = Pattern.compile(
            "\\s*-XX:MaxDirectMemorySize\\s*=\\s*([0-9]+)\\s*([kKmMgG]?)\\s*$");

    private static long maxDirectMemory0() {
        long maxDirectMemory = 0;

        ClassLoader systemClassLoader = getSystemClassLoader();

        try {
            // Now try to get the JVM option (-XX:MaxDirectMemorySize) and parse it.
            // Note that we are using reflection because Android doesn't have these classes.
            Class<?> mgmtFactoryClass = Class.forName(
                    "java.lang.management.ManagementFactory", true, systemClassLoader);
            Class<?> runtimeClass = Class.forName(
                    "java.lang.management.RuntimeMXBean", true, systemClassLoader);

            Object runtime = mgmtFactoryClass.getDeclaredMethod("getRuntimeMXBean").invoke(null);

            @SuppressWarnings("unchecked")
            List<String> vmArgs = (List<String>) runtimeClass.getDeclaredMethod("getInputArguments").invoke(runtime);
            for (int i = vmArgs.size() - 1; i >= 0; i --) {
                Matcher m = MAX_DIRECT_MEMORY_SIZE_ARG_PATTERN.matcher(vmArgs.get(i));
                if (!m.matches()) {
                    continue;
                }

                maxDirectMemory = Long.parseLong(m.group(1));
                switch (m.group(2).charAt(0)) {
                    case 'k': case 'K':
                        maxDirectMemory *= 1024;
                        break;
                    case 'm': case 'M':
                        maxDirectMemory *= 1024 * 1024;
                        break;
                    case 'g': case 'G':
                        maxDirectMemory *= 1024 * 1024 * 1024;
                        break;
                }
                break;
            }
        } catch (Throwable ignored) {
            // Ignore
        }

        if (maxDirectMemory <= 0) {
            maxDirectMemory = Runtime.getRuntime().maxMemory();
            LOGGER.debug("maxDirectMemory: {} bytes (maybe)", maxDirectMemory);
        } else {
            LOGGER.debug("maxDirectMemory: {} bytes", maxDirectMemory);
        }

        return maxDirectMemory;
    }

    static ClassLoader getSystemClassLoader() {
        if (System.getSecurityManager() == null) {
            return ClassLoader.getSystemClassLoader();
        } else {
            return AccessController.doPrivileged(new PrivilegedAction<ClassLoader>() {
                @Override
                public ClassLoader run() {
                    return ClassLoader.getSystemClassLoader();
                }
            });
        }
    }
    
}
