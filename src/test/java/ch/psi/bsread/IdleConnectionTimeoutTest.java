package ch.psi.bsread;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import ch.psi.bsread.ReceiverConfig.IdleConnectionTimeoutBehavior;
import ch.psi.bsread.converter.MatlabByteConverter;
import ch.psi.bsread.impl.StandardPulseIdProvider;
import ch.psi.bsread.message.ChannelConfig;
import ch.psi.bsread.message.DataHeader;
import ch.psi.bsread.message.Message;
import ch.psi.bsread.message.Timestamp;
import ch.psi.bsread.message.Type;

public class IdleConnectionTimeoutTest {

   @Test
   public void testReceiveTimeoutDefaultSettings() {
      // default is block until message available
      ReceiverConfig<ByteBuffer> receiverConfig = new ReceiverConfig<>();
      assertEquals(ReceiverConfig.DEFAULT_RECEIVE_TIMEOUT, receiverConfig.getReceiveTimeout());
      assertEquals(ReceiverConfig.DEFAULT_IDLE_CONNECTION_TIMEOUT, receiverConfig.getIdleConnectionTimeout());
      assertEquals(ReceiverConfig.IdleConnectionTimeoutBehavior.RECONNECT,
            receiverConfig.getIdleConnectionTimeoutBehavior());
   }

   @Test
   public void testSenderReceiverTimeout_Reconnect() {
      ScheduledSender sender = new ScheduledSender(
            new SenderConfig(
                  SenderConfig.DEFAULT_ADDRESS,
                  new StandardPulseIdProvider(),
                  new TimeProvider() {

                     @Override
                     public Timestamp getTime(long pulseId) {
                        return new Timestamp(pulseId, 0L);
                     }
                  },
                  new MatlabByteConverter()));

      // Register data sources ...
      sender.addSource(new DataChannel<Double>(new ChannelConfig("ABC", Type.Float64, 1, 0)) {
         @Override
         public Double getValue(long pulseId) {
            return (double) pulseId;
         }

         @Override
         public Timestamp getTime(long pulseId) {
            return new Timestamp(pulseId, 0L);
         }
      });

      int idleConnectionTimeout = (int) TimeUnit.MILLISECONDS.toMillis(500);
      int receiveTimeout = idleConnectionTimeout / 4;
      ReceiverConfig<ByteBuffer> receiverConfig = new ReceiverConfig<>();
      receiverConfig.setReceiveTimeout(receiveTimeout);
      receiverConfig.setIdleConnectionTimeout(idleConnectionTimeout);
      receiverConfig.setIdleConnectionTimeoutBehavior(IdleConnectionTimeoutBehavior.RECONNECT);
      Receiver<ByteBuffer> receiver = new Receiver<ByteBuffer>(receiverConfig);

      // Send/Receive data
      Message<ByteBuffer> message = null;
      DataHeader lastDataHeader = null;
      try {
         sender.connect();
         receiver.connect();

         sender.send();
         message = receiver.receive();
         assertNotNull(message);
         lastDataHeader = message.getDataHeader();

         sender.send();
         message = receiver.receive();
         assertNotNull(message);
         assertSame(lastDataHeader, message.getDataHeader());
         lastDataHeader = message.getDataHeader();

         sender.send((long) (1.5 * idleConnectionTimeout), TimeUnit.MILLISECONDS);
         // should reconnect and wait for new messages
         message = receiver.receive();
         assertNotNull(message);
         assertNotSame(lastDataHeader, message.getDataHeader());
         lastDataHeader = message.getDataHeader();

         sender.send();
         message = receiver.receive();
         assertNotNull(message);
         assertSame(lastDataHeader, message.getDataHeader());
         lastDataHeader = message.getDataHeader();

         sender.send();
         message = receiver.receive();
         assertNotNull(message);
         assertSame(lastDataHeader, message.getDataHeader());
         lastDataHeader = message.getDataHeader();

         sender.send((long) (2.0 * idleConnectionTimeout), TimeUnit.MILLISECONDS);
         // should reconnect and wait for new messages
         message = receiver.receive();
         assertNotNull(message);
         assertNotSame(lastDataHeader, message.getDataHeader());
         lastDataHeader = message.getDataHeader();

         sender.send();
         message = receiver.receive();
         assertNotNull(message);
         assertSame(lastDataHeader, message.getDataHeader());
         lastDataHeader = message.getDataHeader();

         sender.send();
         message = receiver.receive();
         assertNotNull(message);
         assertSame(lastDataHeader, message.getDataHeader());
         lastDataHeader = message.getDataHeader();

         sender.send((long) (3.0 * idleConnectionTimeout), TimeUnit.MILLISECONDS);
         // should reconnect and wait for new messages
         message = receiver.receive();
         assertNotNull(message);
         assertNotSame(lastDataHeader, message.getDataHeader());
         lastDataHeader = message.getDataHeader();

         sender.send();
         message = receiver.receive();
         assertNotNull(message);
         assertSame(lastDataHeader, message.getDataHeader());
         lastDataHeader = message.getDataHeader();

         sender.send();
         message = receiver.receive();
         assertNotNull(message);
         assertSame(lastDataHeader, message.getDataHeader());
         lastDataHeader = message.getDataHeader();
      } finally {
         receiver.close();
         sender.close();
      }
   }

   @Test
   public void testSenderReceiverTimeout_Return() {
      Sender sender = new Sender(
            new SenderConfig(
                  SenderConfig.DEFAULT_ADDRESS,
                  new StandardPulseIdProvider(),
                  new TimeProvider() {

                     @Override
                     public Timestamp getTime(long pulseId) {
                        return new Timestamp(pulseId, 0L);
                     }
                  },
                  new MatlabByteConverter()));

      // Register data sources ...
      sender.addSource(new DataChannel<Double>(new ChannelConfig("ABC", Type.Float64, 1, 0)) {
         @Override
         public Double getValue(long pulseId) {
            return (double) pulseId;
         }

         @Override
         public Timestamp getTime(long pulseId) {
            return new Timestamp(pulseId, 0L);
         }
      });

      int idleConnectionTimeout = (int) TimeUnit.MILLISECONDS.toMillis(500);
      int receiveTimeout = idleConnectionTimeout / 4;
      ReceiverConfig<ByteBuffer> receiverConfig = new ReceiverConfig<>();
      receiverConfig.setReceiveTimeout(receiveTimeout);
      receiverConfig.setIdleConnectionTimeout(idleConnectionTimeout);
      receiverConfig.setIdleConnectionTimeoutBehavior(IdleConnectionTimeoutBehavior.STOP);
      Receiver<ByteBuffer> receiver = new Receiver<ByteBuffer>(receiverConfig);

      // Send/Receive data
      Message<ByteBuffer> message = null;
      try {
         sender.connect();
         receiver.connect();

         sender.send();
         message = receiver.receive();
         assertNotNull(message);

         sender.send();
         message = receiver.receive();
         assertNotNull(message);

         // should timeout and return null
         message = receiver.receive();
         assertNull(message);
      } finally {
         receiver.close();
         sender.close();
      }
   }
}
