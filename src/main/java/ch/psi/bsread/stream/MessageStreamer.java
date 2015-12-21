package ch.psi.bsread.stream;

import java.io.Closeable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQException;

import zmq.MsgAllocator;

import ch.psi.bsread.Receiver;
import ch.psi.bsread.ReceiverConfig;
import ch.psi.bsread.converter.ValueConverter;
import ch.psi.bsread.impl.DirectByteBufferValueConverter;
import ch.psi.bsread.impl.StandardMessageExtractor;
import ch.psi.bsread.message.DataHeader;
import ch.psi.bsread.message.Message;

public class MessageStreamer<Value, Mapped> implements Closeable {
	private static final Logger LOGGER = LoggerFactory.getLogger(MessageStreamer.class);

	private Receiver<Value> receiver;

	private ExecutorService executor;
	private Future<?> executorFuture;
	private AtomicBoolean isRunning = new AtomicBoolean(true);

	private Stream<StreamSection<Mapped>> stream;
	private AsyncTransferSpliterator<Mapped> spliterator;

	public MessageStreamer(String address, int intoPastElements, int intoFutureElements,
			Function<Message<Value>, Mapped> messageMapper) {
		this(address, intoPastElements, intoFutureElements, new DirectByteBufferValueConverter(), messageMapper);
	}

	public MessageStreamer(String address, int intoPastElements, int intoFutureElements,
			ValueConverter valueConverter, Function<Message<Value>, Mapped> messageMapper) {
		this(address, intoPastElements, intoFutureElements, valueConverter, null, messageMapper);
	}

	public MessageStreamer(String address, int intoPastElements, int intoFutureElements,
			ValueConverter valueConverter, MsgAllocator msgAllocator, Function<Message<Value>, Mapped> messageMapper) {
		this(address, intoPastElements, intoFutureElements, AsyncTransferSpliterator.DEFAULT_BACKPRESSURE_SIZE,
				valueConverter, msgAllocator, messageMapper);
	}
	
	public MessageStreamer(String address, int intoPastElements, int intoFutureElements, int backpressure,
			ValueConverter valueConverter, Function<Message<Value>, Mapped> messageMapper) {
		this(address, intoPastElements, intoFutureElements, backpressure, valueConverter, null, messageMapper, null);
	}

	public MessageStreamer(String address, int intoPastElements, int intoFutureElements, int backpressure,
			ValueConverter valueConverter, MsgAllocator msgAllocator, Function<Message<Value>, Mapped> messageMapper) {
		this(address, intoPastElements, intoFutureElements, backpressure, valueConverter, msgAllocator, messageMapper, null);
	}

	public MessageStreamer(String address, int intoPastElements, int intoFutureElements, int backpressure,
			ValueConverter valueConverter, MsgAllocator msgAllocator, Function<Message<Value>, Mapped> messageMapper,
			Consumer<DataHeader> dataHeaderHandler) {
		executor = Executors.newSingleThreadExecutor();
		spliterator = new AsyncTransferSpliterator<>(intoPastElements, intoFutureElements, backpressure);

		receiver = new Receiver<Value>(new ReceiverConfig<Value>(false, true, new StandardMessageExtractor<Value>(valueConverter), msgAllocator));
		if (dataHeaderHandler != null) {
			receiver.addDataHeaderHandler(dataHeaderHandler);
		}
		receiver.connect(address);

		executorFuture = executor.submit(() -> {
			try {
				Message<Value> message;
				while (isRunning.get() && (message = receiver.receive()) != null) {
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
		});
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
	}

	@Override
	public String toString() {
		return spliterator.toString();
	}
}
