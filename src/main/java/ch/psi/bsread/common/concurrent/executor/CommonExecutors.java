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
   public static final int DEFAULT_CORE_POOL_SIZE = Math.max(2, Runtime.getRuntime().availableProcessors());
   public static final int DEFAULT_MAX_POOL_SIZE = Integer.MAX_VALUE; // 10 * DEFAULT_CORE_POOL_SIZE
   private static final RejectedExecutionHandler DEFAULT_HANDLER = new AbortPolicy();

   public static ExecutorService newSingleThreadExecutor(String poolName) {
      return newSingleThreadExecutor(QUEUE_SIZE_UNBOUNDED, poolName);
   }

   public static ExecutorService newSingleThreadExecutor(int queueSize, String poolName) {
      return newFixedThreadPool(1, queueSize, poolName, DEFAULT_IS_MONITORING);
   }

   public static ExecutorService newFixedThreadPool(int nThreads, String poolName) {
      return newFixedThreadPool(nThreads, QUEUE_SIZE_UNBOUNDED, poolName, DEFAULT_IS_MONITORING);
   }

   public static ExecutorService newFixedThreadPool(int nThreads, int queueSize, String poolName,
         boolean monitoring) {
      ThreadFactory threadFactory =
            new BasicThreadFactory.Builder()
            .namingPattern(poolName + "-%d")
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
         return new MonitoringExecutorService(executor, 0);
      } else {
         return executor;
      }
   }

   public static ExecutorService newCachedThreadPool(int corePoolSize, int maximumPoolSize, String poolName) {
      return newCachedThreadPool(corePoolSize, maximumPoolSize, QUEUE_SIZE_UNBOUNDED, poolName, DEFAULT_IS_MONITORING);
   }

   public static ExecutorService newCachedThreadPool(int corePoolSize, int maximumPoolSize, int queueSize,
         String poolName, boolean monitoring) {
      ThreadFactory threadFactory =
            new BasicThreadFactory.Builder().namingPattern(poolName + "-%d").build();

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
         return new MonitoringExecutorService(executor, 0);
      } else {
         return executor;
      }
   }

   public static ScheduledExecutorService newSingleThreadScheduledExecutor(String poolName) {
      return newScheduledThreadPool(1, poolName);
   }

   public static ScheduledExecutorService newScheduledThreadPool(int nThreads, String poolName) {
      return newScheduledThreadPool(nThreads, poolName, DEFAULT_IS_MONITORING);
   }

   public static ScheduledExecutorService newScheduledThreadPool(int nThreads, String poolName, boolean monitoring) {
      final ThreadFactory threadFactory =
            new BasicThreadFactory.Builder().namingPattern(poolName + "-%d").build();

      RejectedExecutionHandler rejectedExecutionHandler = DEFAULT_HANDLER;

      // always monitor rejections
      rejectedExecutionHandler = new MonitoringRejectedExecutionHandler(rejectedExecutionHandler, poolName);

      ScheduledThreadPoolExecutor executor =
            new ScheduledThreadPoolExecutor(nThreads, threadFactory, rejectedExecutionHandler);

      if (monitoring) {
         return new MonitoringScheduledExecutorService(executor, 0);
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
