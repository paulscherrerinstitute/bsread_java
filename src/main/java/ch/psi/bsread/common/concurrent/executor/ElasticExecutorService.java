package ch.psi.bsread.common.concurrent.executor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In contrast to other ExecutorService this has the advantages: 
 * - Threads are dynamically created up to a predefined upper limit and destroyed if they are not used anymore (e.g. situation with high initial load) 
 * - If the max number of threads are created, further tasks are cached and executed when threads become available (no exception is thrown as in other ExecutorServices)
 */
// Inspired by reactor.core.scheduler.ElasticScheduler
public class ElasticExecutorService extends AbstractExecutorService implements Supplier<ExecutorService> { // ScheduledExecutorService
   private static final Logger LOGGER = LoggerFactory.getLogger(ElasticExecutorService.class); // {
   private static final AtomicLong COUNTER = new AtomicLong();

   static final ThreadFactory EVICTOR_FACTORY = r -> {
      Thread t = new Thread(r, "ElasticExecutorEvictor-" + COUNTER.incrementAndGet());
      t.setDaemon(true);
      return t;
   };

   public static final int DEFAULT_TTL_SECONDS = 60;

   private final Queue<CachedFutureTask<?>> tasksQueue;
   private final ThreadFactory factory;
   private final long ttlMillis;
   private final RejectedExecutionHandler rejectedExecutionHandler;
   private final AtomicLong serviceCounter;
   private final Deque<CachedService> expiryPool;
   private final Queue<CachedService> allPool;
   // use counter because allPool.size() is O(n) instead of O(1)
   private final AtomicInteger allPoolSize;
   private final int corePoolSize;
   private final int maxPoolSize;
   private final List<ExecutorService> awaitTermination;
   private final ScheduledExecutorService evictor;
   private final AtomicBoolean shutdown;
   private final AtomicBoolean terminated;

   public ElasticExecutorService(
         final int corePoolSize,
         final int maxPoolSize,
         final String poolName,
         final long ttlSeconds) {
      this(
            corePoolSize,
            maxPoolSize,
            new BasicThreadFactory.Builder()
                  .namingPattern(poolName + "-%d")
                  .priority(Thread.NORM_PRIORITY)
                  .build(),
            ttlSeconds);
   }

   public ElasticExecutorService(
         final int corePoolSize,
         final int maxPoolSize,
         final ThreadFactory factory,
         final long ttlSeconds) {
      this(corePoolSize,
            maxPoolSize,
            factory,
            ttlSeconds,
            CommonExecutors.DEFAULT_HANDLER);
   }

   public ElasticExecutorService(
         final int corePoolSize,
         final int maxPoolSize,
         final ThreadFactory factory,
         final long ttlSeconds,
         final RejectedExecutionHandler rejectedExecutionHandler) {
      if (ttlSeconds < 0) {
         throw new IllegalArgumentException("ttlSeconds must be positive, was: " + ttlSeconds);
      }
      this.tasksQueue = new ConcurrentLinkedQueue<>();
      this.ttlMillis = ttlSeconds * 1000L;
      this.rejectedExecutionHandler = rejectedExecutionHandler;
      this.factory = factory;
      this.serviceCounter = new AtomicLong();
      this.expiryPool = new ConcurrentLinkedDeque<>();
      this.allPool = new ConcurrentLinkedQueue<>();
      this.allPoolSize = new AtomicInteger();
      this.corePoolSize = corePoolSize;
      this.maxPoolSize = maxPoolSize;
      this.shutdown = new AtomicBoolean();
      this.terminated = new AtomicBoolean();
      this.awaitTermination = new ArrayList<>();
      this.evictor = Executors.newScheduledThreadPool(1, EVICTOR_FACTORY);
      this.evictor.scheduleAtFixedRate(this::eviction,
            ttlSeconds,
            ttlSeconds,
            TimeUnit.SECONDS);
   }

   @SuppressWarnings("rawtypes")
   @Override
   public void execute(Runnable command) {
      if (!shutdown.get()) {
         final CachedFutureTask task;
         if (command instanceof CachedFutureTask) {
            task = (CachedFutureTask) command;
         } else {
            task = newTaskFor(command, null);
         }

         final CachedService cached = pick();
         if (cached != null) {
            task.setCachedService(cached);
            cached.exec.execute(task);
         } else {
            tasksQueue.offer(task);
         }
      } else {
         LOGGER.warn("Shutting down and cannot execute '{}'.", command);
      }
   }

   @Override
   protected <T> CachedFutureTask<T> newTaskFor(Runnable runnable, T value) {
      return new CachedFutureTask<T>(runnable, value);
   }

   @Override
   protected <T> CachedFutureTask<T> newTaskFor(Callable<T> callable) {
      return new CachedFutureTask<T>(callable);
   }

   public Queue<CachedFutureTask<?>> getQueue() {
      return tasksQueue;
   }

   protected CachedService pick() {
      if (shutdown.get()) {
         return null;
      }

      // get from the beginning -> busy Threads are kept at the beginning of the Deque
      CachedService result = expiryPool.pollFirst();
      if (result != null) {
         return result;
      }

      // result = new CachedService(serviceCounter.getAndIncrement(), this);
      // allPool.add(result);
      if (allPoolSize.incrementAndGet() > maxPoolSize || shutdown.get()) {
         // allPool.remove(result);
         allPoolSize.decrementAndGet();
         // result.exec.shutdownNow();
         return null;
      } else {
         result = new CachedService(serviceCounter.getAndIncrement(), this);
         allPool.add(result);
         return result;
      }
   }

   protected void eviction() {
      try {
         // atomic access -> called from evictor
         final long now = System.currentTimeMillis();

         final List<CachedService> list = new ArrayList<>(expiryPool);
         // try to expire CachedService which are not busy first (are at end of Deque)
         Collections.reverse(list);
         for (final CachedService e : list) {
            if (e.expireMillis < now) {
               if (allPoolSize.get() > corePoolSize && expiryPool.remove(e)) {
                  allPool.remove(e);
                  allPoolSize.decrementAndGet();
                  e.exec.shutdownNow();
               }
            }
         }

         // LOGGER.info("Left '{}' expirig '{}'.", allPool, expiryPool);
         // LOGGER.info("After eviction pool size counted '{}' vs. actuall '{}' expiring '{}'
         // waiting
         // tasks '{}'.", allPoolSize.get(), allPool.size(), expiryPool.size(), tasksQueue.size());
      } catch (final Exception e) {
         LOGGER.warn("Issues with evicting old CachedServices.", e);
      }
   }

   @Override
   public ExecutorService get() {
      return new ThreadPoolExecutor(
            1,
            1,
            ttlMillis, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(),
            factory,
            rejectedExecutionHandler);
   }

   @Override
   public void shutdown() {
      if (shutdown.compareAndSet(false, true)) {
         evictor.shutdown();
         awaitTermination.add(evictor);

         expiryPool.clear();

         CachedService cached;
         while ((cached = allPool.poll()) != null) {
            allPoolSize.decrementAndGet();
            cached.exec.shutdown();
            awaitTermination.add(cached.exec);
         }

         tasksQueue.clear();

         terminated.set(true);
      }
   }

   @Override
   public List<Runnable> shutdownNow() {
      if (shutdown.compareAndSet(false, true)) {
         final List<Runnable> tasks = new ArrayList<>();
         tasks.addAll(evictor.shutdownNow());
         awaitTermination.add(evictor);

         expiryPool.clear();

         CachedService cached;
         while ((cached = allPool.poll()) != null) {
            allPoolSize.decrementAndGet();
            tasks.addAll(cached.exec.shutdownNow());
            awaitTermination.add(cached.exec);
         }
         tasks.addAll(tasksQueue);
         tasksQueue.clear();

         terminated.set(true);

         return tasks;
      } else {
         return Collections.emptyList();
      }
   }

   @Override
   public boolean isShutdown() {
      return shutdown.get();
   }

   @Override
   public boolean isTerminated() {
      return terminated.get();
   }

   @Override
   public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
      final long endTime = System.nanoTime() + unit.toNanos(timeout);

      for (final ExecutorService executor : awaitTermination) {
         final long calcTimeout = endTime - System.nanoTime();
         if (calcTimeout > 0) {
            executor.awaitTermination(calcTimeout, TimeUnit.NANOSECONDS);
         }
      }

      return endTime > System.nanoTime();
   }

   static final class CachedService implements Comparable<CachedService> {
      private final long id;
      private final ElasticExecutorService parent;
      private ExecutorService exec;
      private long expireMillis;

      CachedService(final long id, final ElasticExecutorService parent) {
         this.id = id;
         this.parent = parent;
         if (parent != null) {
            this.exec = parent.get();
         }
      }

      public void dispose() {
         if (exec != null) {
            if (!parent.shutdown.get()) {

               final CachedFutureTask<?> task = parent.tasksQueue.poll();
               if (task != null) {
                  task.setCachedService(this);
                  exec.execute(task);
               } else {
                  expireMillis = System.currentTimeMillis() + parent.ttlMillis;
                  parent.expiryPool.addFirst(this);
                  if (parent.shutdown.get()) {
                     if (parent.expiryPool.remove(this)) {
                        if (parent.allPool.remove(this)) {
                           parent.allPoolSize.decrementAndGet();
                           exec.shutdownNow();
                        }
                     }
                  }
               }
            }
         }
      }

      @Override
      public int compareTo(CachedService other) {
         return Long.compare(id, other.id);
      }

      @Override
      public int hashCode() {
         final int prime = 31;
         int result = 1;
         result = prime * result + (int) (id ^ (id >>> 32));
         return result;
      }

      @Override
      public boolean equals(Object obj) {
         if (this == obj)
            return true;
         if (obj == null)
            return false;
         if (getClass() != obj.getClass())
            return false;
         CachedService other = (CachedService) obj;
         if (id != other.id)
            return false;
         return true;
      }

      @Override
      public String toString() {
         return Long.toString(id); // + " expires in " + expireMillis;
      }
   }

   public class CachedFutureTask<V> extends FutureTask<V> {
      private CachedService cached;

      public CachedFutureTask(Callable<V> callable) {
         super(callable);
      }

      public CachedFutureTask(Runnable runnable, V result) {
         super(runnable, result);
      }

      public void setCachedService(CachedService cached) {
         this.cached = cached;
      }

      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
         boolean ret = false;
         try {
            ret = super.cancel(mayInterruptIfRunning);
         } catch (Throwable t) {
            handleError(t);
         } finally {
            if (cached != null) {
               cached.dispose();
            }
         }

         return ret;
      }

      @Override
      public void run() {
         try {
            super.run();
         } catch (Throwable t) {
            handleError(t);
         } finally {
            if (cached != null) {
               cached.dispose();
            }
         }
      }

      protected void handleError(Throwable ex) {
         Thread thread = Thread.currentThread();
         Thread.UncaughtExceptionHandler x = thread.getUncaughtExceptionHandler();
         if (x != null) {
            x.uncaughtException(thread, ex);
         } else {
            LOGGER.error("ElasticExecutorService failed with an uncaught exception", ex);
         }
      }
   }
}
