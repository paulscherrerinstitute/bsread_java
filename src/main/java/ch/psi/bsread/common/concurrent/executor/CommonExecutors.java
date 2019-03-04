package ch.psi.bsread.common.concurrent.executor;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;

public class CommonExecutors {
   public static final boolean DEFAULT_IS_MONITORING = false;
   public static final int QUEUE_SIZE_UNBOUNDED = -1;
   public static final int DEFAULT_CORE_POOL_SIZE = Math.max(4, Runtime.getRuntime().availableProcessors());
   public static final int DEFAULT_MAX_POOL_SIZE = Integer.MAX_VALUE; // 10 * DEFAULT_CORE_POOL_SIZE
   public static final long DEFAULT_TTL_SECONDS = TimeUnit.MINUTES.toSeconds(5);
   public static final RejectedExecutionHandler DEFAULT_HANDLER = new AbortPolicy();

   public static ExecutorService newSingleThreadExecutor(String poolName) {
      return newSingleThreadExecutor(QUEUE_SIZE_UNBOUNDED, poolName);
   }

   public static ExecutorService newSingleThreadExecutor(int queueSize, String poolName) {
      return newFixedThreadPool(1, queueSize, poolName, DEFAULT_IS_MONITORING, Thread.NORM_PRIORITY);
   }

   public static ExecutorService newSingleThreadExecutor(int queueSize, String poolName, boolean monitoring,
         int threadPriority) {
      return newFixedThreadPool(1, queueSize, poolName, monitoring, threadPriority);
   }

   public static ExecutorService newSingleThreadExecutor(int queueSize, String poolName, boolean monitoring) {
      return newFixedThreadPool(1, queueSize, poolName, monitoring, Thread.NORM_PRIORITY);
   }

   public static ExecutorService newFixedThreadPool(int nThreads, String poolName) {
      return newFixedThreadPool(nThreads, QUEUE_SIZE_UNBOUNDED, poolName, DEFAULT_IS_MONITORING, Thread.NORM_PRIORITY);
   }

   public static ExecutorService newFixedThreadPool(int nThreads, int queueSize, String poolName,
         boolean monitoring) {
      return newFixedThreadPool(nThreads, queueSize, poolName, monitoring, Thread.NORM_PRIORITY);
   }

   public static ExecutorService newFixedThreadPool(int nThreads, int queueSize, String poolName,
         boolean monitoring, int threadPriority) {
      ThreadFactory threadFactory =
            new BasicThreadFactory.Builder()
                  .namingPattern(poolName + "-%d")
                  .priority(threadPriority)
                  .build();

      if (monitoring) {
         threadFactory = new ExceptionCatchingThreadFactory(threadFactory);
      }

      BlockingQueue<Runnable> workQueue;
      if (queueSize > 0) {
         workQueue = new LinkedBlockingQueue<>(queueSize);
      } else {
         workQueue = new LinkedBlockingQueue<>();
      }

      RejectedExecutionHandler rejectedExecutionHandler = DEFAULT_HANDLER;

      // always monitor rejections
      rejectedExecutionHandler = new MonitoringRejectedExecutionHandler(rejectedExecutionHandler, poolName);

      ThreadPoolExecutor executor =
            new ThreadPoolExecutor(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS, workQueue, threadFactory,
                  rejectedExecutionHandler);

      if (monitoring) {
         return new MonitoringExecutorService(executor, () -> executor.getQueue().size(), 0);
      } else {
         return executor;
      }
   }

   public static ExecutorService newCachedThreadPool(int corePoolSize, int maximumPoolSize, String poolName) {
      return newCachedThreadPool(corePoolSize, maximumPoolSize, QUEUE_SIZE_UNBOUNDED, poolName, DEFAULT_IS_MONITORING,
            Thread.NORM_PRIORITY);
   }

   public static ExecutorService newCachedThreadPool(int corePoolSize, int maximumPoolSize, int queueSize,
         String poolName, boolean monitoring) {
      return newCachedThreadPool(corePoolSize, maximumPoolSize, queueSize, poolName, monitoring,
            Thread.NORM_PRIORITY);
   }

   public static ExecutorService newCachedThreadPool(int corePoolSize, int maximumPoolSize, int queueSize,
         String poolName, boolean monitoring, int threadPriority) {
      ThreadFactory threadFactory =
            new BasicThreadFactory.Builder()
                  .namingPattern(poolName + "-%d")
                  .priority(threadPriority)
                  .build();

      if (monitoring) {
         threadFactory = new ExceptionCatchingThreadFactory(threadFactory);
      }

      BlockingQueue<Runnable> workQueue = new SynchronousQueue<Runnable>();
      // if (maximumPoolSize == Integer.MAX_VALUE) {
      // workQueue = new SynchronousQueue<Runnable>();
      // } else {
      // if (queueSize > 0) {
      // workQueue = new LinkedBlockingQueue<>(queueSize);
      // } else {
      // workQueue = new LinkedBlockingQueue<>();
      // }
      // }

      RejectedExecutionHandler rejectedExecutionHandler = DEFAULT_HANDLER;

      // always monitor rejections
      rejectedExecutionHandler = new MonitoringRejectedExecutionHandler(rejectedExecutionHandler, poolName);

      ThreadPoolExecutor executor =
            new ThreadPoolExecutor(
                  corePoolSize, maximumPoolSize,
                  60L, TimeUnit.SECONDS,
                  workQueue,
                  threadFactory,
                  rejectedExecutionHandler);

      if (monitoring) {
         return new MonitoringExecutorService(executor, () -> executor.getQueue().size(), 0);
      } else {
         return executor;
      }
   }

   public static ScheduledExecutorService newSingleThreadScheduledExecutor(String poolName) {
      return newScheduledThreadPool(1, poolName);
   }

   public static ScheduledExecutorService newScheduledThreadPool(int nThreads, String poolName) {
      return newScheduledThreadPool(nThreads, poolName, DEFAULT_IS_MONITORING, Thread.NORM_PRIORITY);
   }

   public static ScheduledExecutorService newScheduledThreadPool(int nThreads, String poolName, boolean monitoring) {
      return newScheduledThreadPool(nThreads, poolName, monitoring, Thread.NORM_PRIORITY);
   }

   public static ScheduledExecutorService newScheduledThreadPool(int nThreads, String poolName, boolean monitoring,
         int threadPriority) {
      final ThreadFactory threadFactory =
            new BasicThreadFactory.Builder()
                  .namingPattern(poolName + "-%d")
                  .priority(threadPriority)
                  .build();

      RejectedExecutionHandler rejectedExecutionHandler = DEFAULT_HANDLER;

      // always monitor rejections
      rejectedExecutionHandler = new MonitoringRejectedExecutionHandler(rejectedExecutionHandler, poolName);

      ScheduledThreadPoolExecutor executor =
            new ScheduledThreadPoolExecutor(nThreads, threadFactory, rejectedExecutionHandler);

      if (monitoring) {
         return new MonitoringScheduledExecutorService(executor, () -> executor.getQueue().size(), 0);
      } else {
         return executor;
      }
   }

   public static ExecutorService newElasticThreadPool(int corePoolSize, int maximumPoolSize, String poolName) {
      return newElasticThreadPool(corePoolSize, maximumPoolSize, DEFAULT_TTL_SECONDS, poolName, DEFAULT_IS_MONITORING,
            Thread.NORM_PRIORITY);
   }

   public static ExecutorService newElasticThreadPool(int corePoolSize, int maximumPoolSize, long ttlSeconds,
         String poolName, boolean monitoring) {
      return newElasticThreadPool(corePoolSize, maximumPoolSize, ttlSeconds, poolName, monitoring,
            Thread.NORM_PRIORITY);
   }

   public static ExecutorService newElasticThreadPool(int corePoolSize, int maximumPoolSize, long ttlSeconds,
         String poolName, boolean monitoring, int threadPriority) {
      ThreadFactory threadFactory =
            new BasicThreadFactory.Builder()
                  .namingPattern(poolName + "-%d")
                  .priority(threadPriority)
                  .build();

      if (monitoring) {
         threadFactory = new ExceptionCatchingThreadFactory(threadFactory);
      }

      RejectedExecutionHandler rejectedExecutionHandler = DEFAULT_HANDLER;

      // always monitor rejections
      rejectedExecutionHandler = new MonitoringRejectedExecutionHandler(rejectedExecutionHandler, poolName);

      ElasticExecutorService executor =
            new ElasticExecutorService(
                  corePoolSize, maximumPoolSize,
                  poolName,
                  threadFactory,
                  ttlSeconds,
                  rejectedExecutionHandler);

      if (monitoring) {
         return new MonitoringExecutorService(executor, () -> executor.getQueue().size(), 0);
      } else {
         return executor;
      }
   }

   /**
    * A handler for rejected tasks that throws a {@code RejectedExecutionException}.
    */
   public static class AbortPolicy implements RejectedExecutionHandler {
      /**
       * Creates an {@code AbortPolicy}.
       */
      public AbortPolicy() {}

      /**
       * Always throws RejectedExecutionException.
       *
       * @param r the runnable task requested to be executed
       * @param e the executor attempting to execute this task
       * @throws RejectedExecutionException always
       */
      public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
         throw new RejectedExecutionException("Task " + r.toString() +
               " rejected from " +
               e.toString());
      }
   }

   private static class ExceptionCatchingThreadFactory implements ThreadFactory {
      private final ThreadFactory delegate;

      private ExceptionCatchingThreadFactory(ThreadFactory delegate) {
         this.delegate = delegate;
      }

      public Thread newThread(final Runnable r) {
         Thread t = delegate.newThread(r);
         t.setUncaughtExceptionHandler((trd, ex) -> {
            ex.printStackTrace(); // replace with your handling logic.
         });
         return t;
      }
   }
}
