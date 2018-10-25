package ch.psi.bsread.monitors;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;
import org.zeromq.ZMQ;

import ch.psi.bsread.DataChannel;
import ch.psi.bsread.Receiver;
import ch.psi.bsread.ReceiverConfig;
import ch.psi.bsread.ScheduledSender;
import ch.psi.bsread.SenderConfig;
import ch.psi.bsread.TimeProvider;
import ch.psi.bsread.basic.BasicReceiver;
import ch.psi.bsread.compression.Compression;
import ch.psi.bsread.converter.MatlabByteConverter;
import ch.psi.bsread.impl.StandardMessageExtractor;
import ch.psi.bsread.impl.StandardPulseIdProvider;
import ch.psi.bsread.message.ChannelConfig;
import ch.psi.bsread.message.Timestamp;
import ch.psi.bsread.message.Type;

public class ReceiverConnectionTest {
   private long waitTime = 500;


   // @Test
   // public void testConnectionCounter_Async() throws Exception {
   // // see ATEST-907
   // SenderConfig senderConfig = new SenderConfig(
   // SenderConfig.DEFAULT_ADDRESS,
   // new StandardPulseIdProvider(),
   // new TimeProvider() {
   //
   // @Override
   // public Timestamp getTime(long pulseId) {
   // return new Timestamp(pulseId, 0L);
   // }
   //
   //
   // },
   // new MatlabByteConverter());
   // senderConfig.setSocketType(ZMQ.PUSH);
   //
   // ScheduledSender sender = new ScheduledSender(senderConfig);
   //
   // int size = 2048;
   // Random rand = new Random(0);
   // // Register data sources ...
   // sender.addSource(new DataChannel<double[]>(new ChannelConfig("ABC", Type.Float64, new int[]
   // {size}, 1, 0,
   // ChannelConfig.DEFAULT_ENCODING, Compression.bitshuffle_lz4)) {
   // @Override
   // public double[] getValue(long pulseId) {
   // double[] val = new double[size];
   // for (int i = 0; i < size; ++i) {
   // val[i] = rand.nextDouble();
   // }
   //
   // return val;
   // }
   //
   // @Override
   // public Timestamp getTime(long pulseId) {
   // return new Timestamp(pulseId, 0L);
   // }
   // });
   // sender.addSource(new DataChannel<double[]>(new ChannelConfig("ABB", Type.Float64, new int[]
   // {size}, 1, 0,
   // ChannelConfig.DEFAULT_ENCODING, Compression.bitshuffle_lz4)) {
   // @Override
   // public double[] getValue(long pulseId) {
   // double[] val = new double[size];
   // for (int i = 0; i < size; ++i) {
   // val[i] = rand.nextDouble();
   // }
   //
   // return val;
   // }
   //
   // @Override
   // public Timestamp getTime(long pulseId) {
   // return new Timestamp(pulseId, 0L);
   // }
   // });
   //
   // ReceiverConfig<Object> config1 =
   // new ReceiverConfig<Object>(
   // ReceiverConfig.DEFAULT_ADDRESS,
   // new StandardMessageExtractor<Object>(new MatlabByteConverter()));
   // config1.setSocketType(ZMQ.PULL);
   // ConnectionCounterMonitor connectionMonitor = new ConnectionCounterMonitor();
   // AtomicInteger connectionCounter = new AtomicInteger();
   // connectionMonitor.addHandler((count) -> connectionCounter.set(count));
   // config1.setMonitor(connectionMonitor);
   // final int reconnectTimeoutMS = (int)(5 * waitTime);
   // config1.setInactiveConnectionTimeout(reconnectTimeoutMS);
   // Receiver<Object> receiver1 = new BasicReceiver(config1);
   // AtomicLong mainHeaderCounter1 = new AtomicLong();
   // AtomicLong dataHeaderCounter1 = new AtomicLong();
   // AtomicLong valCounter1 = new AtomicLong();
   // AtomicLong loopCounter1 = new AtomicLong();
   // receiver1.addMainHeaderHandler(header -> mainHeaderCounter1.incrementAndGet());
   // receiver1.addDataHeaderHandler(header -> dataHeaderCounter1.incrementAndGet());
   // receiver1.addValueHandler(values -> valCounter1.incrementAndGet());
   //
   // try {
   // assertEquals(0, connectionCounter.get());
   //
   // receiver1.connect();
   // TimeUnit.MILLISECONDS.sleep(waitTime);
   // assertEquals(0, connectionCounter.get());
   //
   // ExecutorService receiverService = Executors.newFixedThreadPool(2);
   // receiverService.execute(() -> {
   // try {
   // while (receiver1.receive() != null) {
   // loopCounter1.incrementAndGet();
   // }
   // } catch (Throwable t) {
   // System.out.println("Receiver1 executor: " + t.getMessage());
   // t.printStackTrace();
   // }
   // });
   //
   // TimeUnit.SECONDS.sleep(5);
   //
   // sender.connect();
   // TimeUnit.MILLISECONDS.sleep(waitTime);
   // assertEquals(1, connectionCounter.get());
   //
   // // send/receive data
   // int sendCount = 40;
   // for (int i = 0; i < sendCount; ++i) {
   // sender.send();
   // TimeUnit.MILLISECONDS.sleep(1);
   // }
   // TimeUnit.MILLISECONDS.sleep(waitTime);
   //
   // assertEquals(1, dataHeaderCounter1.get());
   // assertEquals(sendCount, mainHeaderCounter1.get());
   // assertEquals(sendCount, valCounter1.get());
   // assertEquals(sendCount, loopCounter1.get());
   //
   // sender.close();
   // TimeUnit.MILLISECONDS.sleep(waitTime);
   // assertEquals(0, connectionCounter.get());
   //
   // sender.connect();
   // TimeUnit.MILLISECONDS.sleep(waitTime);
   // assertEquals(1, connectionCounter.get());
   //
   // receiver1.close();
   // TimeUnit.MILLISECONDS.sleep(waitTime);
   // assertEquals(0, connectionCounter.get());
   //
   // receiver1.connect();
   // TimeUnit.MILLISECONDS.sleep(waitTime);
   // assertEquals(1, connectionCounter.get());
   //
   // TimeUnit.MILLISECONDS.sleep(waitTime);
   // assertEquals(1, connectionCounter.get());
   //
   // receiverService.shutdown();
   //
   // } finally {
   // receiver1.close();
   // sender.close();
   // }
   // }

   @Test
   public void testConnectionCounter_Sync() throws Exception {
      // see ATEST-907
      SenderConfig senderConfig = new SenderConfig(
            SenderConfig.DEFAULT_ADDRESS,
            new StandardPulseIdProvider(),
            new TimeProvider() {

               @Override
               public Timestamp getTime(long pulseId) {
                  return new Timestamp(pulseId, 0L);
               }


            },
            new MatlabByteConverter());
      senderConfig.setSocketType(ZMQ.PUSH);

      ScheduledSender sender = new ScheduledSender(senderConfig);

      int size = 2048;
      Random rand = new Random(0);
      // Register data sources ...
      sender.addSource(new DataChannel<double[]>(new ChannelConfig("ABC", Type.Float64, new int[] {size}, 1, 0,
            ChannelConfig.DEFAULT_ENCODING, Compression.bitshuffle_lz4)) {
         @Override
         public double[] getValue(long pulseId) {
            double[] val = new double[size];
            for (int i = 0; i < size; ++i) {
               val[i] = rand.nextDouble();
            }

            return val;
         }

         @Override
         public Timestamp getTime(long pulseId) {
            return new Timestamp(pulseId, 0L);
         }
      });
      sender.addSource(new DataChannel<double[]>(new ChannelConfig("ABB", Type.Float64, new int[] {size}, 1, 0,
            ChannelConfig.DEFAULT_ENCODING, Compression.bitshuffle_lz4)) {
         @Override
         public double[] getValue(long pulseId) {
            double[] val = new double[size];
            for (int i = 0; i < size; ++i) {
               val[i] = rand.nextDouble();
            }

            return val;
         }

         @Override
         public Timestamp getTime(long pulseId) {
            return new Timestamp(pulseId, 0L);
         }
      });

      ReceiverConfig<Object> config1 =
            new ReceiverConfig<Object>(
                  ReceiverConfig.DEFAULT_ADDRESS,
                  new StandardMessageExtractor<Object>(new MatlabByteConverter()));
      config1.setSocketType(ZMQ.PULL);
      final int reconnectTimeoutMS = (int) (5 * waitTime);
      config1.setInactiveConnectionTimeout(reconnectTimeoutMS);
      Receiver<Object> receiver1 = new BasicReceiver(config1);
      AtomicLong mainHeaderCounter1 = new AtomicLong();
      AtomicLong dataHeaderCounter1 = new AtomicLong();
      AtomicLong valCounter1 = new AtomicLong();
      AtomicLong loopCounter1 = new AtomicLong();
      receiver1.addMainHeaderHandler(header -> mainHeaderCounter1.incrementAndGet());
      receiver1.addDataHeaderHandler(header -> dataHeaderCounter1.incrementAndGet());
      receiver1.addValueHandler(values -> valCounter1.incrementAndGet());
      final List<Integer> connectionCounts = Collections.synchronizedList(new ArrayList<>());
      receiver1.addConnectionCountHandler((count) -> {
         connectionCounts.add(count);
      });
      final List<Integer> expectedConnectionCounts = new ArrayList<>();

      long waitTime = (long) (1.5 * config1.getInactiveConnectionTimeout());
      ExecutorService receiverService = Executors.newFixedThreadPool(2);

      try {
         assertEquals(expectedConnectionCounts, connectionCounts);

         receiver1.connect();
         TimeUnit.MILLISECONDS.sleep(2 * reconnectTimeoutMS);
         // updates are done in receiving thread
         assertEquals(expectedConnectionCounts, connectionCounts);

         receiverService.execute(() -> {
            try {
               while (receiver1.receive() != null) {
                  loopCounter1.incrementAndGet();
               }
            } catch (Throwable t) {
               System.out.println("Receiver1 executor: " + t.getMessage());
               t.printStackTrace();
            }
         });

         TimeUnit.MILLISECONDS.sleep(2 * reconnectTimeoutMS);
         expectedConnectionCounts.add(0);
         assertEquals(expectedConnectionCounts, connectionCounts);

         sender.connect();
         TimeUnit.MILLISECONDS.sleep(waitTime);
         expectedConnectionCounts.add(1);
         assertEquals(expectedConnectionCounts, connectionCounts);

         // send/receive data
         int sendCount = 40;
         for (int i = 0; i < sendCount; ++i) {
            sender.send();
            TimeUnit.MILLISECONDS.sleep(1);
         }
         TimeUnit.MILLISECONDS.sleep(waitTime);

         assertEquals(1, dataHeaderCounter1.get());
         assertEquals(sendCount, mainHeaderCounter1.get());
         assertEquals(sendCount, valCounter1.get());
         assertEquals(sendCount, loopCounter1.get());

         TimeUnit.MILLISECONDS.sleep(2 * reconnectTimeoutMS);
         assertEquals(expectedConnectionCounts, connectionCounts);

         sender.close();
         TimeUnit.MILLISECONDS.sleep(waitTime);
         expectedConnectionCounts.add(0);
         assertEquals(expectedConnectionCounts, connectionCounts);

         TimeUnit.MILLISECONDS.sleep(2 * reconnectTimeoutMS);
         assertEquals(expectedConnectionCounts, connectionCounts);

         sender.connect();
         TimeUnit.MILLISECONDS.sleep(waitTime);
         expectedConnectionCounts.add(1);
         assertEquals(expectedConnectionCounts, connectionCounts);

         TimeUnit.MILLISECONDS.sleep(2 * reconnectTimeoutMS);
         assertEquals(expectedConnectionCounts, connectionCounts);

         receiver1.close();
         TimeUnit.MILLISECONDS.sleep(waitTime);
         expectedConnectionCounts.add(0);
         assertEquals(expectedConnectionCounts, connectionCounts);

         TimeUnit.MILLISECONDS.sleep(2 * reconnectTimeoutMS);
         assertEquals(expectedConnectionCounts, connectionCounts);

         receiver1.connect();
         TimeUnit.MILLISECONDS.sleep(waitTime);
         // updates are done in receiving loop/thread
         assertEquals(expectedConnectionCounts, connectionCounts);

         receiverService.execute(() -> {
            try {
               while (receiver1.receive() != null) {
                  loopCounter1.incrementAndGet();
               }
            } catch (Throwable t) {
               System.out.println("Receiver1 executor: " + t.getMessage());
               t.printStackTrace();
            }
         });

         TimeUnit.MILLISECONDS.sleep(2 * waitTime);
         expectedConnectionCounts.add(1);
         assertEquals(expectedConnectionCounts, connectionCounts);

         TimeUnit.MILLISECONDS.sleep(2 * reconnectTimeoutMS);
         assertEquals(expectedConnectionCounts, connectionCounts);

         receiver1.close();

         TimeUnit.MILLISECONDS.sleep(waitTime);
         expectedConnectionCounts.add(0);
         assertEquals(expectedConnectionCounts, connectionCounts);

         TimeUnit.MILLISECONDS.sleep(2 * reconnectTimeoutMS);
         assertEquals(expectedConnectionCounts, connectionCounts);

         sender.close();

         TimeUnit.MILLISECONDS.sleep(waitTime);
         assertEquals(expectedConnectionCounts, connectionCounts);
      } finally {
         receiver1.close();
         sender.close();

         receiverService.shutdown();
      }
   }

   @Test
   public void testIdleState_Sync() throws Exception {
      SenderConfig senderConfig = new SenderConfig(
            SenderConfig.DEFAULT_ADDRESS,
            new StandardPulseIdProvider(),
            new TimeProvider() {

               @Override
               public Timestamp getTime(long pulseId) {
                  return new Timestamp(pulseId, 0L);
               }


            },
            new MatlabByteConverter());
      senderConfig.setSocketType(ZMQ.PUSH);

      ScheduledSender sender = new ScheduledSender(senderConfig);

      int size = 2048;
      Random rand = new Random(0);
      // Register data sources ...
      sender.addSource(new DataChannel<double[]>(new ChannelConfig("ABC", Type.Float64, new int[] {size}, 1, 0,
            ChannelConfig.DEFAULT_ENCODING, Compression.bitshuffle_lz4)) {
         @Override
         public double[] getValue(long pulseId) {
            double[] val = new double[size];
            for (int i = 0; i < size; ++i) {
               val[i] = rand.nextDouble();
            }

            return val;
         }

         @Override
         public Timestamp getTime(long pulseId) {
            return new Timestamp(pulseId, 0L);
         }
      });
      sender.addSource(new DataChannel<double[]>(new ChannelConfig("ABB", Type.Float64, new int[] {size}, 1, 0,
            ChannelConfig.DEFAULT_ENCODING, Compression.bitshuffle_lz4)) {
         @Override
         public double[] getValue(long pulseId) {
            double[] val = new double[size];
            for (int i = 0; i < size; ++i) {
               val[i] = rand.nextDouble();
            }

            return val;
         }

         @Override
         public Timestamp getTime(long pulseId) {
            return new Timestamp(pulseId, 0L);
         }
      });

      ReceiverConfig<Object> config1 =
            new ReceiverConfig<Object>(
                  ReceiverConfig.DEFAULT_ADDRESS,
                  new StandardMessageExtractor<Object>(new MatlabByteConverter()));
      config1.setSocketType(ZMQ.PULL);
      final int reconnectTimeoutMS = (int) (5 * waitTime);
      config1.setIdleConnectionTimeout(reconnectTimeoutMS);
      Receiver<Object> receiver1 = new BasicReceiver(config1);
      AtomicLong mainHeaderCounter1 = new AtomicLong();
      AtomicLong dataHeaderCounter1 = new AtomicLong();
      AtomicLong valCounter1 = new AtomicLong();
      AtomicLong loopCounter1 = new AtomicLong();
      receiver1.addMainHeaderHandler(header -> mainHeaderCounter1.incrementAndGet());
      receiver1.addDataHeaderHandler(header -> dataHeaderCounter1.incrementAndGet());
      receiver1.addValueHandler(values -> valCounter1.incrementAndGet());
      final List<Boolean> idleStates = Collections.synchronizedList(new ArrayList<>());
      receiver1.addConnectionIdleHandler((state) -> {
         idleStates.add(state);
      });
      final List<Boolean> expectedIdleStates = new ArrayList<>();

      long waitTime = (long) (1.5 * config1.getIdleConnectionTimeout());
      ExecutorService receiverService = Executors.newFixedThreadPool(2);

      try {
         assertEquals(expectedIdleStates, idleStates);

         receiver1.connect();
         TimeUnit.MILLISECONDS.sleep(2 * reconnectTimeoutMS);
         // updates are done in receiving thread
         assertEquals(expectedIdleStates, idleStates);

         receiverService.execute(() -> {
            try {
               while (receiver1.receive() != null) {
                  loopCounter1.incrementAndGet();
               }
            } catch (Throwable t) {
               System.out.println("Receiver1 executor: " + t.getMessage());
               t.printStackTrace();
            }
         });

         TimeUnit.MILLISECONDS.sleep(2 * reconnectTimeoutMS);
         expectedIdleStates.add(Boolean.TRUE);
         assertEquals(expectedIdleStates, idleStates);

         sender.connect();
         TimeUnit.MILLISECONDS.sleep(waitTime);
         assertEquals(expectedIdleStates, idleStates);

         // send/receive data
         int sendCount = 40;
         for (int i = 0; i < sendCount; ++i) {
            sender.send();
            TimeUnit.MILLISECONDS.sleep(1);
         }
         TimeUnit.MILLISECONDS.sleep(waitTime);

         assertEquals(1, dataHeaderCounter1.get());
         assertEquals(sendCount, mainHeaderCounter1.get());
         assertEquals(sendCount, valCounter1.get());
         assertEquals(sendCount, loopCounter1.get());

         TimeUnit.MILLISECONDS.sleep(2 * reconnectTimeoutMS);
         expectedIdleStates.add(Boolean.FALSE);
         expectedIdleStates.add(Boolean.TRUE);
         assertEquals(expectedIdleStates, idleStates);

         TimeUnit.MILLISECONDS.sleep(2 * reconnectTimeoutMS);
         assertEquals(expectedIdleStates, idleStates);

         sender.send();
         TimeUnit.MILLISECONDS.sleep(2 * reconnectTimeoutMS);
         expectedIdleStates.add(Boolean.FALSE);
         expectedIdleStates.add(Boolean.TRUE);
         assertEquals(expectedIdleStates, idleStates);

         sender.send();
         sender.close();
         TimeUnit.MILLISECONDS.sleep(2 * reconnectTimeoutMS);
         expectedIdleStates.add(Boolean.FALSE);
         expectedIdleStates.add(Boolean.TRUE);
         assertEquals(expectedIdleStates, idleStates);

         TimeUnit.MILLISECONDS.sleep(2 * reconnectTimeoutMS);
         assertEquals(expectedIdleStates, idleStates);

         sender.connect();
         TimeUnit.MILLISECONDS.sleep(waitTime);
         assertEquals(expectedIdleStates, idleStates);

         TimeUnit.MILLISECONDS.sleep(2 * reconnectTimeoutMS);
         assertEquals(expectedIdleStates, idleStates);

         sender.send();
         TimeUnit.MILLISECONDS.sleep(waitTime);
         receiver1.close();
         TimeUnit.MILLISECONDS.sleep(2 * reconnectTimeoutMS);
         expectedIdleStates.add(Boolean.FALSE);
         expectedIdleStates.add(Boolean.TRUE);
         assertEquals(expectedIdleStates, idleStates);

         TimeUnit.MILLISECONDS.sleep(2 * reconnectTimeoutMS);
         assertEquals(expectedIdleStates, idleStates);

         receiver1.connect();
         TimeUnit.MILLISECONDS.sleep(waitTime);
         // updates are done in receiving loop/thread
         assertEquals(expectedIdleStates, idleStates);

         receiverService.execute(() -> {
            try {
               while (receiver1.receive() != null) {
                  loopCounter1.incrementAndGet();
               }
            } catch (Throwable t) {
               System.out.println("Receiver1 executor: " + t.getMessage());
               t.printStackTrace();
            }
         });

         TimeUnit.MILLISECONDS.sleep(2 * waitTime);
         assertEquals(expectedIdleStates, idleStates);

         sender.send();

         TimeUnit.MILLISECONDS.sleep(2 * reconnectTimeoutMS);
         expectedIdleStates.add(Boolean.FALSE);
         expectedIdleStates.add(Boolean.TRUE);
         assertEquals(expectedIdleStates, idleStates);

         sender.send();
         TimeUnit.MILLISECONDS.sleep(waitTime);
         receiver1.close();
         TimeUnit.MILLISECONDS.sleep(waitTime);
         expectedIdleStates.add(Boolean.FALSE);
         expectedIdleStates.add(Boolean.TRUE);
         assertEquals(expectedIdleStates, idleStates);

         TimeUnit.MILLISECONDS.sleep(2 * reconnectTimeoutMS);
         assertEquals(expectedIdleStates, idleStates);

         sender.close();

         TimeUnit.MILLISECONDS.sleep(waitTime);
         assertEquals(expectedIdleStates, idleStates);
      } finally {
         receiver1.close();
         sender.close();

         receiverService.shutdown();
      }
   }
   
   @Test
   public void testInactiveState_Sync() throws Exception {
      SenderConfig senderConfig = new SenderConfig(
            SenderConfig.DEFAULT_ADDRESS,
            new StandardPulseIdProvider(),
            new TimeProvider() {

               @Override
               public Timestamp getTime(long pulseId) {
                  return new Timestamp(pulseId, 0L);
               }


            },
            new MatlabByteConverter());
      senderConfig.setSocketType(ZMQ.PUSH);

      ScheduledSender sender = new ScheduledSender(senderConfig);

      int size = 2048;
      Random rand = new Random(0);
      // Register data sources ...
      sender.addSource(new DataChannel<double[]>(new ChannelConfig("ABC", Type.Float64, new int[] {size}, 1, 0,
            ChannelConfig.DEFAULT_ENCODING, Compression.bitshuffle_lz4)) {
         @Override
         public double[] getValue(long pulseId) {
            double[] val = new double[size];
            for (int i = 0; i < size; ++i) {
               val[i] = rand.nextDouble();
            }

            return val;
         }

         @Override
         public Timestamp getTime(long pulseId) {
            return new Timestamp(pulseId, 0L);
         }
      });
      sender.addSource(new DataChannel<double[]>(new ChannelConfig("ABB", Type.Float64, new int[] {size}, 1, 0,
            ChannelConfig.DEFAULT_ENCODING, Compression.bitshuffle_lz4)) {
         @Override
         public double[] getValue(long pulseId) {
            double[] val = new double[size];
            for (int i = 0; i < size; ++i) {
               val[i] = rand.nextDouble();
            }

            return val;
         }

         @Override
         public Timestamp getTime(long pulseId) {
            return new Timestamp(pulseId, 0L);
         }
      });

      ReceiverConfig<Object> config1 =
            new ReceiverConfig<Object>(
                  ReceiverConfig.DEFAULT_ADDRESS,
                  new StandardMessageExtractor<Object>(new MatlabByteConverter()));
      config1.setSocketType(ZMQ.PULL);
      final int reconnectTimeoutMS = (int) (5 * waitTime);
      config1.setInactiveConnectionTimeout(reconnectTimeoutMS);
      Receiver<Object> receiver1 = new BasicReceiver(config1);
      AtomicLong mainHeaderCounter1 = new AtomicLong();
      AtomicLong dataHeaderCounter1 = new AtomicLong();
      AtomicLong valCounter1 = new AtomicLong();
      AtomicLong loopCounter1 = new AtomicLong();
      receiver1.addMainHeaderHandler(header -> mainHeaderCounter1.incrementAndGet());
      receiver1.addDataHeaderHandler(header -> dataHeaderCounter1.incrementAndGet());
      receiver1.addValueHandler(values -> valCounter1.incrementAndGet());
      final List<Boolean> inactiveStates = Collections.synchronizedList(new ArrayList<>());
      receiver1.addConnectionInactiveHandler((state) -> {
         inactiveStates.add(state);
      });
      final List<Boolean> expectedInactiveStates = new ArrayList<>();

      long waitTime = (long) (1.5 * config1.getInactiveConnectionTimeout());
      ExecutorService receiverService = Executors.newFixedThreadPool(2);

      try {
         assertEquals(expectedInactiveStates, inactiveStates);

         receiver1.connect();
         TimeUnit.MILLISECONDS.sleep(2 * reconnectTimeoutMS);
         // updates are done in receiving thread
         assertEquals(expectedInactiveStates, inactiveStates);

         receiverService.execute(() -> {
            try {
               while (receiver1.receive() != null) {
                  loopCounter1.incrementAndGet();
               }
            } catch (Throwable t) {
               System.out.println("Receiver1 executor: " + t.getMessage());
               t.printStackTrace();
            }
         });

         TimeUnit.MILLISECONDS.sleep(2 * reconnectTimeoutMS);
         expectedInactiveStates.add(Boolean.TRUE);
         assertEquals(expectedInactiveStates, inactiveStates);

         sender.connect();
         TimeUnit.MILLISECONDS.sleep(waitTime);
         assertEquals(expectedInactiveStates, inactiveStates);

         // send/receive data
         int sendCount = 40;
         for (int i = 0; i < sendCount; ++i) {
            sender.send();
            TimeUnit.MILLISECONDS.sleep(1);
         }
         TimeUnit.MILLISECONDS.sleep(waitTime);

         assertEquals(1, dataHeaderCounter1.get());
         assertEquals(sendCount, mainHeaderCounter1.get());
         assertEquals(sendCount, valCounter1.get());
         assertEquals(sendCount, loopCounter1.get());

         TimeUnit.MILLISECONDS.sleep(2 * reconnectTimeoutMS);
         expectedInactiveStates.add(Boolean.FALSE);
         expectedInactiveStates.add(Boolean.TRUE);
         assertEquals(expectedInactiveStates, inactiveStates);

         TimeUnit.MILLISECONDS.sleep(2 * reconnectTimeoutMS);
         assertEquals(expectedInactiveStates, inactiveStates);

         sender.send();
         TimeUnit.MILLISECONDS.sleep(2 * reconnectTimeoutMS);
         expectedInactiveStates.add(Boolean.FALSE);
         expectedInactiveStates.add(Boolean.TRUE);
         assertEquals(expectedInactiveStates, inactiveStates);

         sender.send();
         sender.close();
         TimeUnit.MILLISECONDS.sleep(2 * reconnectTimeoutMS);
         expectedInactiveStates.add(Boolean.FALSE);
         expectedInactiveStates.add(Boolean.TRUE);
         assertEquals(expectedInactiveStates, inactiveStates);

         TimeUnit.MILLISECONDS.sleep(2 * reconnectTimeoutMS);
         assertEquals(expectedInactiveStates, inactiveStates);

         sender.connect();
         TimeUnit.MILLISECONDS.sleep(waitTime);
         assertEquals(expectedInactiveStates, inactiveStates);

         TimeUnit.MILLISECONDS.sleep(2 * reconnectTimeoutMS);
         assertEquals(expectedInactiveStates, inactiveStates);

         sender.send();
         TimeUnit.MILLISECONDS.sleep(waitTime);
         receiver1.close();
         TimeUnit.MILLISECONDS.sleep(2 * reconnectTimeoutMS);
         expectedInactiveStates.add(Boolean.FALSE);
         expectedInactiveStates.add(Boolean.TRUE);
         assertEquals(expectedInactiveStates, inactiveStates);

         TimeUnit.MILLISECONDS.sleep(2 * reconnectTimeoutMS);
         assertEquals(expectedInactiveStates, inactiveStates);

         receiver1.connect();
         TimeUnit.MILLISECONDS.sleep(waitTime);
         // updates are done in receiving loop/thread
         assertEquals(expectedInactiveStates, inactiveStates);

         receiverService.execute(() -> {
            try {
               while (receiver1.receive() != null) {
                  loopCounter1.incrementAndGet();
               }
            } catch (Throwable t) {
               System.out.println("Receiver1 executor: " + t.getMessage());
               t.printStackTrace();
            }
         });

         TimeUnit.MILLISECONDS.sleep(2 * waitTime);
         assertEquals(expectedInactiveStates, inactiveStates);

         sender.send();

         TimeUnit.MILLISECONDS.sleep(2 * reconnectTimeoutMS);
         expectedInactiveStates.add(Boolean.FALSE);
         expectedInactiveStates.add(Boolean.TRUE);
         assertEquals(expectedInactiveStates, inactiveStates);

         sender.send();
         TimeUnit.MILLISECONDS.sleep(waitTime);
         receiver1.close();
         TimeUnit.MILLISECONDS.sleep(waitTime);
         expectedInactiveStates.add(Boolean.FALSE);
         expectedInactiveStates.add(Boolean.TRUE);
         assertEquals(expectedInactiveStates, inactiveStates);

         TimeUnit.MILLISECONDS.sleep(2 * reconnectTimeoutMS);
         assertEquals(expectedInactiveStates, inactiveStates);

         sender.close();

         TimeUnit.MILLISECONDS.sleep(waitTime);
         assertEquals(expectedInactiveStates, inactiveStates);
      } finally {
         receiver1.close();
         sender.close();

         receiverService.shutdown();
      }
   }
}
