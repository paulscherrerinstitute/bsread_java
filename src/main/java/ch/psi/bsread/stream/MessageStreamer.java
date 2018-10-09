package ch.psi.bsread.stream;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;

import zmq.MsgAllocator;

import ch.psi.bsread.Receiver;
import ch.psi.bsread.ReceiverConfig;
import ch.psi.bsread.common.concurrent.executor.CommonExecutors;
import ch.psi.bsread.configuration.Channel;
import ch.psi.bsread.converter.ValueConverter;
import ch.psi.bsread.impl.DirectByteBufferValueConverter;
import ch.psi.bsread.impl.StandardMessageExtractor;
import ch.psi.bsread.message.DataHeader;
import ch.psi.bsread.message.Message;

public class MessageStreamer<Value, Mapped> implements Closeable {
   private static final Logger LOGGER = LoggerFactory.getLogger(MessageStreamer.class);

   private List<Receiver<Value>> receivers = new ArrayList<>();

   private ExecutorService executor;
   private List<Future<?>> executorFutures = new ArrayList<>();
   private AtomicBoolean isRunning = new AtomicBoolean(true);

   private Stream<StreamSection<Mapped>> stream;
   private AsyncTransferSpliterator<Mapped> spliterator;

   public MessageStreamer(
         int socketType,
         String address,
         Collection<Channel> requestedChannels,
         int intoPastElements,
         int intoFutureElements,
         Function<Message<Value>, Mapped> messageMapper) {
      this(socketType,
            address,
            requestedChannels,
            intoPastElements,
            intoFutureElements,
            new DirectByteBufferValueConverter(),
            messageMapper);
   }

   public MessageStreamer(
         int socketType,
         String address,
         Collection<Channel> requestedChannels,
         int intoPastElements,
         int intoFutureElements,
         ValueConverter valueConverter,
         Function<Message<Value>, Mapped> messageMapper) {
      this(socketType,
            address,
            requestedChannels,
            intoPastElements,
            intoFutureElements,
            valueConverter,
            null,
            messageMapper);
   }

   public MessageStreamer(
         int socketType,
         String address,
         Collection<Channel> requestedChannels,
         int intoPastElements,
         int intoFutureElements,
         ValueConverter valueConverter,
         Function<Message<Value>, Mapped> messageMapper,
         Consumer<DataHeader> dataHeaderHandler) {
      this(socketType,
            address,
            requestedChannels,
            intoPastElements,
            intoFutureElements,
            AsyncTransferSpliterator.DEFAULT_BACKPRESSURE_SIZE,
            valueConverter,
            null,
            messageMapper,
            dataHeaderHandler);
   }


   public MessageStreamer(
         int socketType,
         String address,
         Collection<Channel> requestedChannels,
         int intoPastElements,
         int intoFutureElements,
         ValueConverter valueConverter,
         Function<Message<Value>, Mapped> messageMapper,
         Consumer<DataHeader> dataHeaderHandler,
         Integer receiveBufferSize) {
      this(socketType,
            address,
            requestedChannels,
            intoPastElements,
            intoFutureElements,
            AsyncTransferSpliterator.DEFAULT_BACKPRESSURE_SIZE,
            valueConverter,
            null,
            messageMapper,
            dataHeaderHandler,
            receiveBufferSize);
   }

   public MessageStreamer(
         int socketType,
         String address,
         int streamSplit,
         Collection<Channel> requestedChannels,
         int intoPastElements,
         int intoFutureElements,
         ValueConverter valueConverter,
         Function<Message<Value>, Mapped> messageMapper,
         Consumer<DataHeader> dataHeaderHandler,
         Integer receiveBufferSize) {
      this(socketType,
            address,
            streamSplit,
            requestedChannels,
            intoPastElements,
            intoFutureElements,
            AsyncTransferSpliterator.DEFAULT_BACKPRESSURE_SIZE,
            valueConverter,
            null,
            messageMapper,
            dataHeaderHandler,
            receiveBufferSize);
   }

   public MessageStreamer(
         int socketType,
         String address,
         Collection<Channel> requestedChannels,
         int intoPastElements,
         int intoFutureElements,
         ValueConverter valueConverter,
         MsgAllocator msgAllocator,
         Function<Message<Value>, Mapped> messageMapper) {
      this(socketType,
            address,
            requestedChannels,
            intoPastElements,
            intoFutureElements,
            AsyncTransferSpliterator.DEFAULT_BACKPRESSURE_SIZE,
            valueConverter,
            msgAllocator,
            messageMapper);
   }

   public MessageStreamer(
         int socketType,
         String address,
         Collection<Channel> requestedChannels,
         int intoPastElements,
         int intoFutureElements,
         int backpressure,
         ValueConverter valueConverter,
         Function<Message<Value>, Mapped> messageMapper) {
      this(socketType,
            address,
            requestedChannels,
            intoPastElements,
            intoFutureElements,
            backpressure,
            valueConverter,
            null,
            messageMapper,
            null);
   }

   public MessageStreamer(
         int socketType,
         String address,
         Collection<Channel> requestedChannels,
         int intoPastElements,
         int intoFutureElements,
         int backpressure,
         ValueConverter valueConverter,
         MsgAllocator msgAllocator,
         Function<Message<Value>, Mapped> messageMapper) {
      this(socketType,
            address,
            requestedChannels,
            intoPastElements,
            intoFutureElements,
            backpressure,
            valueConverter,
            msgAllocator,
            messageMapper,
            null);
   }


   public MessageStreamer(
         int socketType,
         String address,
         Collection<Channel> requestedChannels,
         int intoPastElements,
         int intoFutureElements,
         int backpressure,
         ValueConverter valueConverter,
         MsgAllocator msgAllocator,
         Function<Message<Value>, Mapped> messageMapper,
         Consumer<DataHeader> dataHeaderHandler) {
      this(socketType,
            address,
            requestedChannels,
            intoPastElements,
            intoFutureElements,
            backpressure,
            valueConverter,
            msgAllocator,
            messageMapper,
            dataHeaderHandler,
            ReceiverConfig.DEFAULT_RECEIVE_BUFFER_SIZE);
   }

   public MessageStreamer(
         int socketType,
         String address,
         Collection<Channel> requestedChannels,
         int intoPastElements,
         int intoFutureElements,
         int backpressure,
         ValueConverter valueConverter,
         MsgAllocator msgAllocator,
         Function<Message<Value>, Mapped> messageMapper,
         Consumer<DataHeader> dataHeaderHandler,
         final Integer receiveBufferSize) {
      this(socketType,
            address,
            1,
            requestedChannels,
            intoPastElements,
            intoFutureElements,
            backpressure,
            valueConverter,
            msgAllocator,
            messageMapper,
            dataHeaderHandler,
            receiveBufferSize);
   }

   public MessageStreamer(
         int socketType,
         String address,
         int streamSplit,
         Collection<Channel> requestedChannels,
         int intoPastElements,
         int intoFutureElements,
         int backpressure,
         ValueConverter valueConverter,
         MsgAllocator msgAllocator,
         Function<Message<Value>, Mapped> messageMapper,
         Consumer<DataHeader> dataHeaderHandler,
         final Integer receiveBufferSize) {
      if (streamSplit > 1 && socketType != ZMQ.PULL) {
         final String message = String.format(
               "Stream splits bigger than 1 ('%d') without using push/pull ('%d') will result in duplicates.",
               streamSplit, socketType);
         LOGGER.error(message);
         throw new IllegalStateException(message);
      }

      executor = CommonExecutors.newFixedThreadPool(streamSplit, "MessageStreamer for " + address);
      spliterator = new AsyncTransferSpliterator<>(intoPastElements, intoFutureElements, backpressure);

      ReceiverConfig<Value> receiverConfig =
            new ReceiverConfig<Value>(address, false, true, new StandardMessageExtractor<Value>(valueConverter),
                  msgAllocator);
      // receiverConfig.setReceiveBufferSize(16 * 1024 * 1024);
      receiverConfig.setHighWaterMark(ReceiverConfig.CLIENT_HIGH_WATER_MARK);
      receiverConfig.setSocketType(socketType);
      if (requestedChannels != null) {
         receiverConfig.setRequestedChannels(requestedChannels);
      }
      if (receiveBufferSize != null && receiveBufferSize > 0) {
         receiverConfig.setReceiveBufferSize(receiveBufferSize);
      }

      final Consumer<DataHeader> syncDataHeaderHandler;
      if (dataHeaderHandler != null) {
         syncDataHeaderHandler = new SynchronizedDataHeaderConsumer(dataHeaderHandler);
      } else {
         syncDataHeaderHandler = null;
      }

      // streamSplit > 1 -> messages might be processed out of order
      for (int i = 0; i < streamSplit; ++i) {
         final Receiver<Value> receiver = new Receiver<>(receiverConfig);
         receivers.add(receiver);

         if (syncDataHeaderHandler != null) {
            receiver.addDataHeaderHandler(syncDataHeaderHandler);
         }
         receiver.connect();

         executorFutures.add(executor.submit(() -> {
            try {
               Message<Value> message;
               while ((message = receiver.receive()) != null) {
                  spliterator.onAvailable(message, messageMapper);
               }
            } catch (ZMQException e) {
               LOGGER.debug("Close streamer since ZMQ stream closed.", e);
            } catch (Exception e) {
               LOGGER.error("Close streamer since Receiver encountered a problem.", e);
            }

            try {
               close();
            } catch (Exception e) {
               LOGGER.warn("Exception while closing streamer.", e);
            }
         }));
      }
   }

   public Stream<StreamSection<Mapped>> getStream() {
      if (stream == null) {
         // support only sequential processing
         // stream = new
         // ParallelismAwareStream<StreamSection<T>>(StreamSupport.stream(spliterator,
         // false), false);
         stream = StreamSupport.stream(spliterator, false);
         stream.onClose(() -> close());
      }

      return stream;
   }

   @Override
   public void close() {
      if (isRunning.compareAndSet(true, false)) {
         if (receivers != null) {
            for (final Receiver<Value> receiver : receivers) {
               receiver.close();
            }
            receivers = null;
         }

         if (executorFutures != null) {
            for (final Future<?> executorFuture : executorFutures) {
               executorFuture.cancel(true);
            }
            executorFutures = null;
         }
         if (executor != null) {
            executor.shutdown();
            executor = null;
         }

         if (spliterator != null) {
            // release waiting consumers
            spliterator.onClose();
            spliterator = null;
         }

         if (stream != null) {
            stream.close();
            stream = null;
         }
      }
   }

   @Override
   public String toString() {
      return spliterator.toString();
   }
}
