package ch.psi.bsread.monitors;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ.Socket;

import ch.psi.bsread.common.concurrent.executor.CommonExecutors;
import ch.psi.bsread.message.commands.StopCommand;

import zmq.ZError;
import zmq.ZMQ;
import zmq.ZMQ.Event;

// builds on https://github.com/zeromq/jeromq/blob/master/src/test/java/zmq/TestMonitor.java
public class ConnectionCounterMonitor implements Monitor {
   private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionCounterMonitor.class);
   private Set<IntConsumer> handlers = new LinkedHashSet<>();
   // keep track of restarts
   private AtomicLong runIdProvider = new AtomicLong();
   private final AtomicInteger connectionCounter = new AtomicInteger();
   private MonitorConfig monitorConfig;

   public ConnectionCounterMonitor() {}

   @Override
   public void start(MonitorConfig monitorConfig) {
      // setup connection
      final String address = "inproc://" + monitorConfig.getMonitorItentifier();
      Socket monitorSock = null;
      try {
         monitorConfig.getSocket().base().monitor(address,
               ZMQ.ZMQ_EVENT_ACCEPTED // server bind accepts
                     | ZMQ.ZMQ_EVENT_CONNECTED // client connects
                     | ZMQ.ZMQ_EVENT_DISCONNECTED // server/client disconnect
                     | ZMQ.ZMQ_EVENT_MONITOR_STOPPED);
         monitorSock = monitorConfig.getContext().socket(ZMQ.ZMQ_PAIR);
         // monitorSock.setRcvHWM(ReceiverConfig.DEFAULT_HIGH_WATER_MARK);
         // monitorSock.setLinger(ReceiverConfig.DEFAULT_LINGER);
         monitorSock.connect(address);
      } catch (final Exception e) {
         monitorSock = null;
         LOGGER.warn("Could not establish a connection monitor for identifier '{}'.",
               monitorConfig.getMonitorItentifier());
      }

      if (monitorSock != null) {
         // keep and use loc refs when Monitor gets reused
         // e.g. make sure the old Runnable will not accidentally close the executor service
         final long runId = runIdProvider.incrementAndGet();
         final ExecutorService executor = CommonExecutors.newSingleThreadExecutor(monitorConfig.getMonitorItentifier());
         this.monitorConfig = monitorConfig;
         connectionCounter.set(0);
         // make sure first update happens in thread that setups the connection
         updateHandlers(runId, 0);

         final Socket monitorSockFinal = monitorSock;
         executor.execute(() -> {
            try {
               boolean isRunning = true;
               while (isRunning) {

                  final Event event = Event.read(monitorSockFinal.base());

                  if (event == null || monitorConfig.getSocket().base().errno() == ZError.ETERM) {
                     // stop loop
                     break;
                  }

                  switch (event.event) {
                     case zmq.ZMQ.ZMQ_EVENT_ACCEPTED:
                     case zmq.ZMQ.ZMQ_EVENT_CONNECTED:
                        // server bind accepts new connections
                        // client connects
                        updateHandlers(runId, connectionCounter.incrementAndGet());
                        break;
                     case zmq.ZMQ.ZMQ_EVENT_DISCONNECTED:
                        // server and client disconnect
                        updateHandlers(runId, connectionCounter.decrementAndGet());
                        break;
                     // case ZMQ.ZMQ_EVENT_CLOSED:
                     // connectionCounter.set(0);
                     // updateHandlers();
                     // break;
                     case ZMQ.ZMQ_EVENT_MONITOR_STOPPED:
                        // stop
                        isRunning = false;
                        break;
                     default:
                        LOGGER.info("Unexpected event '{}' received for identifier '{} monitoring '{}'", event.event,
                              monitorConfig.getMonitorItentifier(), event.addr);
                  }
               }
            } catch (Throwable e) {
               LOGGER.warn("Monitoring zmq connections failed for identifier '{}'.",
                     monitorConfig.getMonitorItentifier(),
                     e);
            } finally {
               // Clear interrupted state as this might cause problems with the
               // rest of the remaining code - i.e. closing of the zmq socket
               // Thread.interrupted();

               LOGGER.debug("Stop ConnectionCounter for identifier '{}'.", monitorConfig.getMonitorItentifier());

               if (monitorSockFinal != null) {
                  monitorSockFinal.close();
               }

               // only takes effect if not restarted yet (due to runId)
               updateHandlers(runId, 0);

               executor.shutdown();
            }
         });
      }
   }

   @Override
   public synchronized void stop(final long sentMessages) {
      if (this.monitorConfig != null) {
         sendStopMessage(this.monitorConfig.getSocket(), this, sentMessages, connectionCounter.get());
      }
   }

   @Override
   public void stop() {
      stop(StopCommand.SENT_MESSAGES_UNKNOWN);
   }

   public void disableUpdate() {
      runIdProvider.incrementAndGet();
   }

   public synchronized void addHandler(IntConsumer handler) {
      handlers.add(handler);
   }

   public synchronized void removeHandler(IntConsumer handler) {
      handlers.remove(handler);
   }

   private synchronized void updateHandlers(final long runId, final int connectionCount) {
      if (runId == runIdProvider.get()) {
         for (IntConsumer handler : handlers) {
            handler.accept(connectionCount);
         }
      } else {
         LOGGER.debug("Do not update connection count handlers since there is a new runId.");
      }
   }

   public static void sendStopMessage(final Socket socket, final ConnectionCounterMonitor monitor,
         final long sentMessages, final int connectionCounts) {
      try {
         final MonitorConfig monitorConfig = monitor.monitorConfig;
         if (monitorConfig != null && monitorConfig.isSendStopMessage()) {
            final byte[] stopBytes = StopCommand.getAsBytes(monitorConfig.getObjectMapper(), sentMessages);
            int nrOfStopMsgs = 1;
            if (monitorConfig.getSocketType() == ZMQ.ZMQ_PUSH) {
               nrOfStopMsgs = connectionCounts;
            }

            final int blockingFlag = monitorConfig.isBlockingSend() ? 0 : org.zeromq.ZMQ.NOBLOCK;
            for (int i = 0; i < nrOfStopMsgs; ++i) {
               // Receivers can react on it or not (see
               // ReceiverConfig.keepListeningOnStop)
               socket.send(stopBytes, blockingFlag);
            }
         }
      } catch (Exception e) {
         LOGGER.warn("Could not send stop command.", e);
      }
   }
}
