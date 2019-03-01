package ch.psi.bsread.common.concurrent.executor;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.function.IntSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MonitoringExecutorService extends AbstractMonitoringExecutorService {
   private static Logger LOGGER = LoggerFactory.getLogger(MonitoringExecutorService.class);

   private final int logMessageAtQueueSize;

   public MonitoringExecutorService(ExecutorService target, IntSupplier queueSizeProvider, int logMessageAtQueueSize) {
      super(target, queueSizeProvider);
      this.logMessageAtQueueSize = logMessageAtQueueSize;
   }

   @Override
   protected <T> Callable<T> wrap(final Callable<T> task) {
      final Exception clientStack = clientTrace();
      final String clientThreadName = Thread.currentThread().getName();
      final long startTime = System.nanoTime();

      int submitSize = getQueueSize();
      if (submitSize >= logMessageAtQueueSize) {
         LOGGER.info("Submit task '{}' at queue size '{}'.", task, submitSize, clientStack);
      }
      return () -> {
         if (submitSize >= logMessageAtQueueSize) {
            LOGGER.info("Task '{}' spent {}ns in the queue having size of '{}' and submit size of '{}'.", task,
                  (System.nanoTime() - startTime), getQueueSize(), submitSize);
         }

         try {
            return task.call();
         } catch (Exception e) {
            LOGGER.error("Exception '{}' in task submitted from thread '{}' here:", e, clientThreadName, clientStack);
            throw e;
         }
      };
   }

   @Override
   protected Runnable wrap(final Runnable run) {
      final Exception clientStack = clientTrace();
      final String clientThreadName = Thread.currentThread().getName();
      final long startTime = System.nanoTime();

      int submitSize = getQueueSize();
      if (submitSize >= logMessageAtQueueSize) {
         LOGGER.info("Submit runnable '{}' at queue size '{}'.", run, submitSize, clientStack);
      }
      return () -> {
         if (submitSize >= logMessageAtQueueSize) {
            LOGGER.info("Runnable '{}' spent {}ns in the queue having size of '{}' and submit size of '{}'.", run,
                  (System.nanoTime() - startTime), getQueueSize(), submitSize);
         }

         try {
            run.run();
         } catch (Exception e) {
            LOGGER.error("Exception '{}' in task submitted from thread '{}' here:", e, clientThreadName, clientStack);
            throw e;
         }
      };
   }
}
