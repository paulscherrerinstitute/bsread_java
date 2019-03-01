package ch.psi.bsread.common.concurrent.executor;

import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;

public class MonitoringScheduledExecutorService extends MonitoringExecutorService implements ScheduledExecutorService {
   private final ScheduledExecutorService target;

   public MonitoringScheduledExecutorService(ScheduledExecutorService target, IntSupplier queueSizeProvider,
         int logMessageAtQueueSize) {
      super(target, queueSizeProvider, logMessageAtQueueSize);
      this.target = target;
   }

   @Override
   public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
      return target.schedule(wrap(command), delay, unit);
   }

   @Override
   public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
      return target.schedule(wrap(callable), delay, unit);
   }

   @Override
   public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
      return target.scheduleAtFixedRate(wrap(command), initialDelay, period, unit);
   }

   @Override
   public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
      return target.scheduleWithFixedDelay(wrap(command), initialDelay, delay, unit);
   }
}
