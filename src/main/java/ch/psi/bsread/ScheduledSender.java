package ch.psi.bsread;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.psi.bsread.command.Command;

public class ScheduledSender {
   private static final Logger LOGGER = LoggerFactory.getLogger(ScheduledSender.class.getName());

   // make sure everything is executed from the same Thread
   private ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
   private ScheduledFuture<?> fixedRateSender;
   private long bindCloseTimeoutMillis = 0;

   private Sender sender;

   public ScheduledSender() {
      this(new SenderConfig());
   }

   public ScheduledSender(SenderConfig senderConfig) {
      this.sender = new Sender(senderConfig);
   }

   public void connect() {
      Future<?> future = executor.submit(() -> {
         try {
            sender.connect();
            // it seems that the socket does sometimes not bind in a timely
            // manner.
            TimeUnit.MILLISECONDS.sleep(bindCloseTimeoutMillis);
         } catch (Exception e) {
            LOGGER.error("Error while binding to '{}'.", sender.getSenderConfig().getAddress(), e);
            throw new RuntimeException(e);
         }
      });
      try {
         future.get(1000, TimeUnit.MILLISECONDS);
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
         LOGGER.warn("Could not wait for successful bind of '{}'", sender.getSenderConfig().getAddress(), e);
         throw new RuntimeException(e);
      }
   }

   public void close() {
      if (fixedRateSender != null) {
         if (!fixedRateSender.cancel(false)) {
            LOGGER.warn("Could not terminate fixed rate sender of '{}' in timely manner.",
                  sender.getSenderConfig().getAddress());
         }
         fixedRateSender = null;
      }

      if (executor != null) {
         executor.execute(() -> {
            try {
               sender.close();
               // it seems that the socket does sometimes not get closed in
               // a
               // timely manner.
               TimeUnit.MILLISECONDS.sleep(bindCloseTimeoutMillis);
            } catch (Exception e) {
               LOGGER.error("Error while closing '{}'.", sender.getSenderConfig().getAddress(), e);
               throw new RuntimeException(e);
            }
         });

         executor.shutdown();
         try {
            executor.awaitTermination(1000, TimeUnit.MILLISECONDS);
         } catch (InterruptedException e) {
            LOGGER.warn("Could not wait for successful termination of '{}'.", sender.getSenderConfig().getAddress(), e);
         }
         executor = null;
      }
   }

   public void send() {
      Future<?> future = executor.submit(() -> {
         try {
            sender.send();
         } catch (Exception e) {
            LOGGER.error("Error while sending to '{}'.", sender.getSenderConfig().getAddress(), e);
            throw e;
         }
      });
      try {
         future.get(1000, TimeUnit.MILLISECONDS);
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
         LOGGER.warn("Could not wait for successful send of '{}'", sender.getSenderConfig().getAddress(), e);
         throw new RuntimeException(e);
      }
   }

   public void sendDirect() {
      sender.send();
   }

   public ScheduledFuture<?> send(long delay, TimeUnit unit) {
      return executor.schedule(() -> {
         try {
            sender.send();
         } catch (Exception e) {
            LOGGER.error("Error while sending to '{}'.", sender.getSenderConfig().getAddress(), e);
            throw e;
         }
      }, delay, unit);
   }

   public ScheduledFuture<?> sendAtFixedRate(long initialDelay, long period, TimeUnit unit) {
      return sendAtFixedRate(initialDelay, period, unit, new AtomicLong());
   }

   public ScheduledFuture<?> sendAtFixedRate(long initialDelay, long period, TimeUnit unit, AtomicLong count) {
      return this.sendAtFixedRate(() -> {
         try {
            sender.send();
            count.incrementAndGet();
         } catch (Exception e) {
            LOGGER.error("Error while sending to '{}'.", sender.getSenderConfig().getAddress(), e);
            throw e;
         }
      }, initialDelay, period, unit);
   }

   public ScheduledFuture<?> sendAtFixedRate(Runnable runnable, long initialDelay, long period, TimeUnit unit) {
      if (fixedRateSender == null) {
         fixedRateSender = executor.scheduleAtFixedRate(runnable, initialDelay, period, unit);
         return fixedRateSender;
      } else {
         String message = String.format("There is already a fixed rate sender initialized for '%s'!",
               sender.getSenderConfig().getAddress());
         LOGGER.error(message);
         throw new RuntimeException(message);
      }
   }

   public void sendCommand(Command command) {
      Future<?> future = executor.submit(() -> {
         try {
            sender.sendCommand(command);
         } catch (Exception e) {
            LOGGER.error("Error while sending to '{}'.", sender.getSenderConfig().getAddress(), e);
            throw e;
         }
      });
      try {
         future.get(1000, TimeUnit.MILLISECONDS);
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
         LOGGER.warn("Could not wait for successful send of '{}'", sender.getSenderConfig().getAddress(), e);
         throw new RuntimeException(e);
      }
   }

   public void addSource(DataChannel<?> channel) {
      sender.addSource(channel);
   }

   public void removeSource(DataChannel<?> channel) {
      sender.removeSource(channel);
   }

   /**
    * Returns the currently configured data channels as an unmodifiable list
    * 
    * @return Unmodifiable list of data channels
    */
   public List<DataChannel<?>> getChannels() {
      return sender.getChannels();
   }
}
