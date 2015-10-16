package ch.psi.bsread.stream;

import java.util.Spliterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncTransferSpliterator<T> implements Spliterator<StreamSection<T>> {
	private static final Logger LOGGER = LoggerFactory.getLogger(AsyncTransferSpliterator.class);

	// using a big backpressure ensures that Events get buffered on the client
	// and if the client is not fast enough in processing elements it will
	// result in an OutOfMemoryError on the client.
	public static final int DEFAULT_BACKPRESSURE_SIZE = Integer.MAX_VALUE;
	private static final int CHARACTERISTICS = Spliterator.ORDERED | Spliterator.NONNULL;

	private AtomicBoolean isRunning = new AtomicBoolean(true);
	private ConcurrentSkipListMap<Long, T> values = new ConcurrentSkipListMap<>();
	private int pastElements;
	private int futureElements;
	private long backpressureSize;
	private AtomicLong idGenerator = new AtomicLong();
	private AtomicLong readyIndex;
	private AtomicLong processingIndex;
	// would be better to use ManagedReentrantLock
	private ReentrantLock readyLock = new ReentrantLock(true);
	private Condition readyCondition = readyLock.newCondition();
	private ReentrantLock fullLock = new ReentrantLock(true);
	private Condition fullCondition = fullLock.newCondition();

	private ConcurrentHashMap<Thread, Long> threadToProcessingIndex = new ConcurrentHashMap<>();
	private ConcurrentSkipListSet<Long> deletingQueue = new ConcurrentSkipListSet<>();

	/**
	 * Constructor
	 * 
	 * @param pastElements
	 *            The number of elements a {@link StreamSection} provides into
	 *            the past.
	 * @param futureElements
	 *            The number of elements a {@link StreamSection} provides into
	 *            the future.
	 */
	public AsyncTransferSpliterator(int pastElements, int futureElements) {
		this(pastElements, futureElements, DEFAULT_BACKPRESSURE_SIZE);
	}

	/**
	 * Constructor
	 * 
	 * @param pastElements
	 *            The number of elements a {@link StreamSection} provides into
	 *            the past.
	 * @param futureElements
	 *            The number of elements a {@link StreamSection} provides into
	 *            the future.
	 * @param backpressureSize
	 *            The number of unprocessed events after which the spliterator
	 *            starts to block the producer threads.
	 */
	public AsyncTransferSpliterator(int pastElements, int futureElements, int backpressureSize) {
		this.pastElements = pastElements;
		this.futureElements = futureElements;
		this.backpressureSize = backpressureSize;
		// add this way to ensure long (and not integer overflow)
		this.backpressureSize += pastElements;
		this.backpressureSize += futureElements;

		readyIndex = new AtomicLong(pastElements);
		processingIndex = new AtomicLong(readyIndex.get());

		for (long i = 0; i < pastElements; ++i) {
			deletingQueue.add(i);
		}
	}

	/**
	 * A value got available.
	 * 
	 * @param value
	 *            The value
	 */
	public void onAvailable(T value) {
		onAvailable(value, Function.identity());
	}

	/**
	 * A value got available that should be mapped to another value for later
	 * processing.
	 * 
	 * @param <V>
	 *            The JAVA type
	 * @param origValue
	 *            The original value
	 * @param mapper
	 *            The mapper function
	 */
	public <V> void onAvailable(V origValue, Function<V, T> mapper) {
		// reserve the index as soon as value arrives (to ensure correct
		// sequence in case mapper takes
		// variable time or threads sleep due to backpressure)
		long valueIndex = idGenerator.getAndIncrement();
		T value = mapper.apply(origValue);

		if (isRunning.get()) {
			values.put(valueIndex, value);

			long index = readyIndex.get();
			while (isRunning.get() && values.get(index) != null && valueIndex - index >= futureElements && readyIndex.compareAndSet(index, index + 1)) {
				readyLock.lock();
				try {
					readyCondition.signal();
				} finally {
					readyLock.unlock();
				}

				++index;
				valueIndex = idGenerator.get() - 1;
			}
		}

		// consider backpressureSize
		if (isRunning.get() && processingIndex.get() + backpressureSize < idGenerator.get()) {
			fullLock.lock();
			try {
				fullCondition.await();
			} catch (InterruptedException e) {
				LOGGER.debug("Interrupted while waiting for elements to get ready.", e);
			} finally {
				fullLock.unlock();
			}
		}
	}

	/**
	 * Close the Spliterator and unblock waiting threads.
	 */
	public void onClose() {
		if (isRunning.compareAndSet(true, false)) {
			// release all threads that try provide elements to process
			fullLock.lock();
			try {
				fullCondition.signalAll();
			} finally {
				fullLock.unlock();
			}

			// release all threads that are waiting for new elements to process
			readyLock.lock();
			try {
				readyCondition.signalAll();
			} finally {
				readyLock.unlock();
			}
		}
	}

	@Override
	public boolean tryAdvance(Consumer<? super StreamSection<T>> action) {
		StreamSection<T> section = getNext();
		if (section != null) {
			action.accept(section);
			return true;
		} else {
			return false;
		}
	}

	@Override
	public Spliterator<StreamSection<T>> trySplit() {
		// process one at a time (for now)
		final StreamSection<T> section = getNext();
		if (section != null) {
			return new Spliterator<StreamSection<T>>() {

				@Override
				public boolean tryAdvance(Consumer<? super StreamSection<T>> action) {
					action.accept(section);
					return false;
				}

				@Override
				public Spliterator<StreamSection<T>> trySplit() {
					return null;
				}

				@Override
				public long estimateSize() {
					return 1;
				}

				@Override
				public int characteristics() {
					return CHARACTERISTICS;
				}
			};
		} else {
			return null;
		}
	}

	/**
	 * Get the next StreamSection to process (or block until one is available).
	 * 
	 * @return StreamSection The StreamSection
	 */
	protected StreamSection<T> getNext() {
		StreamSection<T> streamSection = null;

		if (isRunning.get()) {
			Thread currentThread = Thread.currentThread();
			// what happens if a Thread does not return from processing an
			// element?
			Long processedIndex = threadToProcessingIndex.remove(currentThread);
			if (processedIndex != null) {
				// make sure elements are removed in ascending sequence and only
				// after all older
				// elements have been successfully processed (otherwise an
				// element might get removed
				// that an older/existing StreamSection is currently processing)
				deletingQueue.add(processedIndex);

				Long deleteIndex = deletingQueue.first();
				// ensure ascending sequence exists (otherwise we might remove
				// an element a delayed
				// Thread is currently processing and that there are enough
				// elements
				while (Long.valueOf(deleteIndex.longValue() + 1).equals(deletingQueue.higher(deleteIndex)) &&
						processedIndex.longValue() - deleteIndex.longValue() >= pastElements) {
					values.remove(deleteIndex);

					deletingQueue.remove(deleteIndex);
					deleteIndex = deletingQueue.first();
				}
			}

			while (isRunning.get() && processingIndex.get() >= readyIndex.get()) {
				readyLock.lock();
				try {
					readyCondition.await();
				} catch (InterruptedException e) {
					LOGGER.debug("Interrupted while waiting for elements to get ready.", e);
				} finally {
					readyLock.unlock();
				}
			}

			if (isRunning.get()) {
				Long processIdx = processingIndex.getAndIncrement();
				threadToProcessingIndex.put(currentThread, processIdx);

				streamSection = new StreamSection<T>(processIdx, this.values.subMap(processIdx.longValue() - pastElements, true,
						processIdx.longValue() + futureElements, true));

				// inform about free slot
				if (processingIndex.get() + backpressureSize <= idGenerator.get()) {
					fullLock.lock();
					try {
						fullCondition.signal();
					} finally {
						fullLock.unlock();
					}
				}
			}

		}

		return streamSection;
	}

	@Override
	public long estimateSize() {
		return Integer.MAX_VALUE;
	}

	@Override
	public int characteristics() {
		return CHARACTERISTICS;
	}

	// only for testing purposes
	protected int getSize() {
		// IMPORTANT: Not a O(1) operation!!!!
		return values.size();
	}
	
	@Override
	public String toString(){
		return values.keySet().toString();
	}
}
