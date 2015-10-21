package ch.psi.bsread.stream;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.psi.bsread.Receiver;
import ch.psi.bsread.converter.ValueConverter;
import ch.psi.bsread.impl.DirectByteBufferValueConverter;
import ch.psi.bsread.impl.StandardMessageExtractor;
import ch.psi.bsread.message.DataHeader;
import ch.psi.bsread.message.Message;

public class MessageStreamer<T, V> implements Closeable {
   private static final Logger LOGGER = LoggerFactory.getLogger(MessageStreamer.class);

   private Receiver<V> receiver;

   private ExecutorService executor;
   private Future<?> executorFuture;

   private Stream<StreamSection<T>> stream;
   private AsyncTransferSpliterator<T> spliterator;

   public MessageStreamer(String address, int intoPastElements, int intoFutureElements,
         Function<Message<V>, T> messageMapper) {
      this(address, intoPastElements, intoFutureElements, messageMapper, new DirectByteBufferValueConverter());
   }

   public MessageStreamer(String address, int intoPastElements, int intoFutureElements,
         Function<Message<V>, T> messageMapper, ValueConverter valueConverter) {
      this(address, intoPastElements, intoFutureElements, AsyncTransferSpliterator.DEFAULT_BACKPRESSURE_SIZE,
            messageMapper, valueConverter, null);
   }

   public MessageStreamer(String address, int intoPastElements, int intoFutureElements, int backpressure,
         Function<Message<V>, T> messageMapper, ValueConverter valueConverter) {
      this(address, intoPastElements, intoFutureElements, backpressure, messageMapper, valueConverter, null);
   }

   public MessageStreamer(String address, int intoPastElements, int intoFutureElements, int backpressure,
         Function<Message<V>, T> messageMapper, ValueConverter valueConverter,
         Consumer<DataHeader> dataHeaderHandler) {
      executor = Executors.newSingleThreadExecutor();
      spliterator = new AsyncTransferSpliterator<>(intoPastElements, intoFutureElements, backpressure);

      receiver = new Receiver<V>(true, new StandardMessageExtractor<V>(valueConverter));
      if (dataHeaderHandler != null) {
         receiver.addDataHeaderHandler(dataHeaderHandler);
      }
      receiver.connect(address);

      executorFuture = executor.submit(() -> {
         try {
            Message<V> message;
            while ((message = receiver.receive()) != null) {
               spliterator.onAvailable(message, messageMapper);
            }
         } catch (Exception e) {
            LOGGER.error("Close streamer since Receiver encountered a problem.", e);
         }

         try {
            close();
         } catch (Exception e) {
            LOGGER.warn("Exception while closing streamer.", e);
         }
      });
   }

   public Stream<StreamSection<T>> getStream() {
      if (stream == null) {
         // support only sequential processing
         // stream = new ParallelismAwareStream<StreamSection<T>>(StreamSupport.stream(spliterator,
         // false), false);
         return StreamSupport.stream(spliterator, false);
      }

      return stream;
   }

   @Override
   public synchronized void close() throws IOException {
      if (receiver != null) {
         receiver.close();
         receiver = null;
      }

      if (executorFuture != null) {
         executorFuture.cancel(true);
         executorFuture = null;
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

   @Override
   public String toString() {
      return spliterator.toString();
   }
}
