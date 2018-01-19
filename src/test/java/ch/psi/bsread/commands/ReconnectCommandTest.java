package ch.psi.bsread.commands;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import ch.psi.bsread.DataChannel;
import ch.psi.bsread.Receiver;
import ch.psi.bsread.ReceiverConfig;
import ch.psi.bsread.ScheduledSender;
import ch.psi.bsread.SenderConfig;
import ch.psi.bsread.TimeProvider;
import ch.psi.bsread.converter.MatlabByteConverter;
import ch.psi.bsread.impl.StandardMessageExtractor;
import ch.psi.bsread.impl.StandardPulseIdProvider;
import ch.psi.bsread.message.ChannelConfig;
import ch.psi.bsread.message.DataHeader;
import ch.psi.bsread.message.MainHeader;
import ch.psi.bsread.message.Message;
import ch.psi.bsread.message.Timestamp;
import ch.psi.bsread.message.Type;
import ch.psi.bsread.message.Value;
import ch.psi.bsread.message.commands.ReconnectCommand;
import ch.psi.bsread.message.commands.StopCommand;

public class ReconnectCommandTest {
   private MainHeader hookMainHeader;
   private boolean hookMainHeaderCalled;
   private DataHeader hookDataHeader;
   private boolean hookDataHeaderCalled;
   private Map<String, Value<ByteBuffer>> hookValues;
   private boolean hookValuesCalled;
   private Map<String, ChannelConfig> channelConfigs = new HashMap<>();

   @Test
   public void testReconnect_01() throws Exception {
      String sender_01_Addr = "tcp://*:9999";
      String receiver_01_Addr = "tcp://localhost:9999";
      ScheduledSender sender_01 = new ScheduledSender(
            new SenderConfig(
                  sender_01_Addr,
                  new StandardPulseIdProvider(),
                  new TimeProvider() {

                     @Override
                     public Timestamp getTime(long pulseId) {
                        return new Timestamp(pulseId, 0L);
                     }
                  },
                  new MatlabByteConverter()));
      String sender_02_Addr = "tcp://*:9998";
      String receiver_02_Addr = "tcp://localhost:9998";
      ScheduledSender sender_02 = new ScheduledSender(
            new SenderConfig(
                  sender_02_Addr,
                  new StandardPulseIdProvider(),
                  new TimeProvider() {

                     @Override
                     public Timestamp getTime(long pulseId) {
                        return new Timestamp(pulseId, 0L);
                     }
                  },
                  new MatlabByteConverter()));
      String sender_03_Addr = "tcp://*:9997";
      String receiver_03_Addr = "tcp://localhost:9997";
      ScheduledSender sender_03 = new ScheduledSender(
            new SenderConfig(
                  sender_03_Addr,
                  new StandardPulseIdProvider(),
                  new TimeProvider() {

                     @Override
                     public Timestamp getTime(long pulseId) {
                        return new Timestamp(pulseId, 0L);
                     }
                  },
                  new MatlabByteConverter()));

      // Register data sources ...
      DataChannel<Double> channel_01_02 = new DataChannel<Double>(new ChannelConfig("ABC", Type.Float64, 1, 0)) {
         @Override
         public Double getValue(long pulseId) {
            return (double) pulseId;
         }

         @Override
         public Timestamp getTime(long pulseId) {
            return new Timestamp(pulseId, 0L);
         }
      };
      DataChannel<Double> channel_03 = new DataChannel<Double>(new ChannelConfig("ABCD", Type.Float64, 1, 0)) {
         @Override
         public Double getValue(long pulseId) {
            return (double) pulseId;
         }

         @Override
         public Timestamp getTime(long pulseId) {
            return new Timestamp(pulseId, 0L);
         }
      };
      sender_01.addSource(channel_01_02);
      sender_02.addSource(channel_01_02);
      sender_03.addSource(channel_03);

      ReceiverConfig<ByteBuffer> receiverConfig =
            new ReceiverConfig<ByteBuffer>(receiver_01_Addr, false, true, new StandardMessageExtractor<ByteBuffer>());
      Receiver<ByteBuffer> receiver = new Receiver<ByteBuffer>(receiverConfig);

      // Optional - register callbacks
      receiver.addMainHeaderHandler(header -> setMainHeader(header));
      receiver.addDataHeaderHandler(header -> setDataHeader(header));
      receiver.addValueHandler(values -> setValues(values));

      try {
         sender_01.connect();
         sender_02.connect();
         sender_03.connect();
         receiver.connect();
         // TimeUnit.MILLISECONDS.sleep(500);

         CountDownLatch latch = new CountDownLatch(1);
         ExecutorService executor = Executors.newSingleThreadExecutor();
         executor.execute(() -> {
            try {
               sender_01.send();
               TimeUnit.MILLISECONDS.sleep(10);
               sender_01.send();
               sender_01.sendCommand(new ReconnectCommand(receiver_02_Addr));
               TimeUnit.MILLISECONDS.sleep(50);
               // should not receive this
               sender_01.send();

               sender_02.send();
               TimeUnit.MILLISECONDS.sleep(10);
               sender_02.send();
               sender_02.sendCommand(new ReconnectCommand(receiver_03_Addr));
               TimeUnit.MILLISECONDS.sleep(50);
               // should not receive this
               sender_02.send();

               sender_03.send();
               TimeUnit.MILLISECONDS.sleep(10);
               sender_03.send();
               sender_03.sendCommand(new StopCommand());

               sender_01.close();
               sender_02.close();
               sender_03.close();
            } catch (Exception e) {
               e.printStackTrace();
            } finally {
               latch.countDown();
            }
         });

         hookMainHeaderCalled = false;
         hookDataHeaderCalled = false;
         hookValuesCalled = false;
         Message<ByteBuffer> message = receiver.receive();
         assertTrue("Main header hook should always be called.", hookMainHeaderCalled);
         assertEquals("Data header hook should only be called the first time.", true, hookDataHeaderCalled);
         assertTrue("Value hook should always be called.", hookValuesCalled);
         // should be the same instance
         assertSame(hookMainHeader, message.getMainHeader());
         assertSame(hookDataHeader, message.getDataHeader());
         assertSame(hookValues, message.getValues());
         assertEquals(0, hookMainHeader.getPulseId());

         hookMainHeaderCalled = false;
         hookDataHeaderCalled = false;
         hookValuesCalled = false;
         message = receiver.receive();
         assertTrue("Main header hook should always be called.", hookMainHeaderCalled);
         assertEquals("Data header hook should only be called the first time.", false, hookDataHeaderCalled);
         assertTrue("Value hook should always be called.", hookValuesCalled);
         // should be the same instance
         assertSame(hookMainHeader, message.getMainHeader());
         assertSame(hookDataHeader, message.getDataHeader());
         assertSame(hookValues, message.getValues());
         assertEquals(1, hookMainHeader.getPulseId());

         // change address
         hookMainHeaderCalled = false;
         hookDataHeaderCalled = false;
         hookValuesCalled = false;
         message = receiver.receive();
         assertNotNull(message);
         assertTrue("Main header hook should always be called.", hookMainHeaderCalled);
         assertEquals("Data header hook should be called after reconnect.", true, hookDataHeaderCalled);
         assertTrue("Value hook should always be called.", hookValuesCalled);
         // should be the same instance
         assertSame(hookMainHeader, message.getMainHeader());
         assertSame(hookDataHeader, message.getDataHeader());
         assertSame(hookValues, message.getValues());
         assertEquals(0, hookMainHeader.getPulseId());

         hookMainHeaderCalled = false;
         hookDataHeaderCalled = false;
         hookValuesCalled = false;
         message = receiver.receive();
         assertTrue("Main header hook should always be called.", hookMainHeaderCalled);
         assertEquals("Data header hook should only be called the first time.", false, hookDataHeaderCalled);
         assertTrue("Value hook should always be called.", hookValuesCalled);
         // should be the same instance
         assertSame(hookMainHeader, message.getMainHeader());
         assertSame(hookDataHeader, message.getDataHeader());
         assertSame(hookValues, message.getValues());
         assertEquals(1, hookMainHeader.getPulseId());

         // change address and channel -> new DataHeader
         hookMainHeaderCalled = false;
         hookDataHeaderCalled = false;
         hookValuesCalled = false;
         message = receiver.receive();
         assertNotNull(message);
         assertTrue("Main header hook should always be called.", hookMainHeaderCalled);
         assertEquals("Data header hook should be called after reconnect and channel changes.", true,
               hookDataHeaderCalled);
         assertTrue("Value hook should always be called.", hookValuesCalled);
         // should be the same instance
         assertSame(hookMainHeader, message.getMainHeader());
         assertSame(hookDataHeader, message.getDataHeader());
         assertSame(hookValues, message.getValues());
         assertEquals(0, hookMainHeader.getPulseId());

         hookMainHeaderCalled = false;
         hookDataHeaderCalled = false;
         hookValuesCalled = false;
         message = receiver.receive();
         assertTrue("Main header hook should always be called.", hookMainHeaderCalled);
         assertEquals("Data header hook should only be called the first time.", false, hookDataHeaderCalled);
         assertTrue("Value hook should always be called.", hookValuesCalled);
         // should be the same instance
         assertSame(hookMainHeader, message.getMainHeader());
         assertSame(hookDataHeader, message.getDataHeader());
         assertSame(hookValues, message.getValues());
         assertEquals(1, hookMainHeader.getPulseId());

         hookMainHeaderCalled = false;
         hookDataHeaderCalled = false;
         hookValuesCalled = false;
         // stops on stop
         message = receiver.receive();
         assertNull(message);

         latch.await();
         executor.shutdown();
      } finally {
         receiver.close();
      }
   }

   private void setMainHeader(MainHeader header) {
      this.hookMainHeader = header;
      this.hookMainHeaderCalled = true;
   }

   private void setDataHeader(DataHeader header) {
      this.hookDataHeader = header;
      this.hookDataHeaderCalled = true;

      this.channelConfigs.clear();
      for (ChannelConfig chConf : header.getChannels()) {
         this.channelConfigs.put(chConf.getName(), chConf);
      }
   }

   public void setValues(Map<String, Value<ByteBuffer>> values) {
      this.hookValues = values;
      this.hookValuesCalled = true;
   }
}
