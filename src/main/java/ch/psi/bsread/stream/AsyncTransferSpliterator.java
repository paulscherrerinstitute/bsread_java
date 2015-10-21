package ch.psi.bsread.stream;

import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Spliterator;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.psi.daq.common.concurrent.singleton.Deferred;

public class AsyncTransferSpliterator<T> implements Spliterator<StreamSection<T>> {
   private static final Logger LOGGER = LoggerFactory.getLogger(AsyncTransferSpliterator.class);

   // using a big backpressure ensures that Events get buffered on the client
   // and if the client is not fast enough in processing elements it will
   // result in an OutOfMemoryError on the client.
   public static final int DEFAULT_BACKPRESSURE_SIZE = Integer.MAX_VALUE;
   private static final int CHARACTERISTICS = Spliterator.ORDERED | Spliterator.NONNULL;
   private static final Deferred<ExecutorService> DEFAULT_MAPPING_SERVICE = new Deferred<>(
         () -> Executors.newWorkStealingPool(2 * Runtime.getRuntime().availableProcessors()));

   private AtomicBoolean isRunning = new AtomicBoolean(true);
   private ConcurrentSkipListMap<Long, CompletableFuture<T>> values = new ConcurrentSkipListMap<>();
   private int pastElements;
   private int futureElements;
   private long backpressureSize;
   private AtomicLong idGenerator = new AtomicLong();
   private AtomicLong readyIndex;
   private AtomicLong processingIndex;
   // might be better to use ManagedReentrantLock
   private ReentrantLock readyLock = new ReentrantLock(true);
   private Condition readyCondition = readyLock.newCondition();
   private ReentrantLock fullLock = new ReentrantLock(true);
   private Condition fullCondition = fullLock.newCondition();
   private ExecutorService mapperService;

   /**
    * Constructor
    * 
    * @param pastElements The number of elements a {@link StreamSection} provides into the past.
    * @param futureElements The number of elements a {@link StreamSection} provides into the future.
    */
   public AsyncTransferSpliterator(int pastElements, int futureElements) {
      this(pastElements, futureElements, DEFAULT_BACKPRESSURE_SIZE, DEFAULT_MAPPING_SERVICE.get());
   }

   /**
    * Constructor
    * 
    * @param pastElements The number of elements a {@link StreamSection} provides into the past.
    * @param futureElements The number of elements a {@link StreamSection} provides into the future.
    * @param mapperService ExecutorService which does the mapping
    */
   public AsyncTransferSpliterator(int pastElements, int futureElements, ExecutorService mapperService) {
      this(pastElements, futureElements, DEFAULT_BACKPRESSURE_SIZE, mapperService);
   }

   /**
    * Constructor
    * 
    * @param pastElements The number of elements a {@link StreamSection} provides into the past.
    * @param futureElements The number of elements a {@link StreamSection} provides into the future.
    * @param backpressureSize The number of unprocessed events after which the spliterator starts to
    *        block the producer threads.
    */
   public AsyncTransferSpliterator(int pastElements, int futureElements, int backpressureSize) {
      this(pastElements, futureElements, backpressureSize, DEFAULT_MAPPING_SERVICE.get());
   }

   /**
    * Constructor
    * 
    * @param pastElements The number of elements a {@link StreamSection} provides into the past.
    * @param futureElements The number of elements a {@link StreamSection} provides into the future.
    * @param backpressureSize The number of unprocessed events after which the spliterator starts to
    *        block the producer threads.
    * @param mapperService ExecutorService which does the mapping
    */
   public AsyncTransferSpliterator(int pastElements, int futureElements, int backpressureSize,
         ExecutorService mapperService) {
      this.pastElements = pastElements;
      this.futureElements = futureElements;
      this.backpressureSize = backpressureSize;
      // add this way to ensure long (and not integer overflow)
      this.backpressureSize += pastElements;
      this.backpressureSize += futureElements;

      readyIndex = new AtomicLong(pastElements);
      processingIndex = new AtomicLong(readyIndex.get());

      this.mapperService = mapperService;
   }

   /**
    * A value got available.
    * 
    * @param value The value
    */
   public void onAvailable(T value) {
      long valueIndex = idGenerator.getAndIncrement();
      CompletableFuture<T> futureValue = CompletableFuture.completedFuture(value);

      this.onAvailable(valueIndex, futureValue);
   }

   /**
    * A value got available that should be mapped to another value for later processing.
    * 
    * @param <V> The JAVA type
    * @param origValue The original value
    * @param mapper The mapper function
    */
   public <V> void onAvailable(V origValue, Function<V, T> mapper) {
      long valueIndex = idGenerator.getAndIncrement();

      // offload mapping work from receiving thread (most likely, mapping will access
      // Values<T>.getValue() which would again onload the value conversion work to the receiver
      // thread (which was initially offloaded in AbstractMessageExtractor))
      CompletableFuture<T> futureValue = CompletableFuture.supplyAsync(() -> mapper.apply(origValue), mapperService);

      this.onAvailable(valueIndex, futureValue);
   }

   protected void onAvailable(long valueIndex, CompletableFuture<T> futureValue) {
      values.put(valueIndex, futureValue);

      ReentrantLock rLock = readyLock;
      Condition rCondition = readyCondition;
      long index = readyIndex.get();
      while (isRunning.get() && values.get(index) != null && valueIndex - index >= futureElements
            && readyIndex.compareAndSet(index, index + 1)) {
         rLock.lock();
         try {
            rCondition.signal();
         } finally {
            rLock.unlock();
         }

         ++index;
         // valueIndex = idGenerator.get() - 1;
      }

      // consider backpressure
      ReentrantLock fLock = fullLock;
      Condition fCondition = fullCondition;
      while (isRunning.get() && processingIndex.get() + backpressureSize <= valueIndex) {
         fLock.lock();
         try {
            fCondition.await();
         } catch (InterruptedException e) {
            LOGGER.debug("Interrupted while waiting for elements to get ready.", e);
         } finally {
            fLock.unlock();
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
      StreamSection<T> section = getNext(false);
      if (section != null) {
         action.accept(section);
         return true;
      } else {
         return false;
      }
   }

   @Override
   public Spliterator<StreamSection<T>> trySplit() {
      throw new UnsupportedOperationException("Not yet supported.");
      // // process one at a time
      // final StreamSection<T> section = getNext(true);
      // if (section != null) {
      // return new Spliterator<StreamSection<T>>() {
      //
      // @Override
      // public boolean tryAdvance(Consumer<? super StreamSection<T>> action) {
      // action.accept(section);
      // return false;
      // }
      //
      // @Override
      // public Spliterator<StreamSection<T>> trySplit() {
      // return null;
      // }
      //
      // @Override
      // public long estimateSize() {
      // return 1;
      // }
      //
      // @Override
      // public int characteristics() {
      // return CHARACTERISTICS;
      // }
      // };
      // } else {
      // return null;
      // }
   }

   /**
    * Get the next StreamSection to process (or block until one is available).
    * 
    * @param doCopy Defines if a copy of the submap should be created
    * @return StreamSection The StreamSection
    */
   protected StreamSection<T> getNext(boolean doCopy) {
      StreamSection<T> streamSection = null;

      ReentrantLock rLock = readyLock;
      Condition rCondition = readyCondition;
      while (isRunning.get() && processingIndex.get() >= readyIndex.get()) {
         rLock.lock();
         try {
            rCondition.await();
         } catch (InterruptedException e) {
            LOGGER.debug("Interrupted while waiting for elements to get ready.", e);
         } finally {
            rLock.unlock();
         }
      }

      if (isRunning.get()) {
         Long processIdx = processingIndex.getAndIncrement();

         NavigableMap<Long, CompletableFuture<T>> subMap =
               this.values.subMap(processIdx.longValue() - pastElements, true,
                     processIdx.longValue() + futureElements, true);
         if (doCopy) {
            subMap = new TreeMap<>(subMap);
         }
         streamSection = new StreamSection<T>(processIdx, subMap);

         // delete elements that are not needed anymore
         Entry<Long, CompletableFuture<T>> oldestEntry = values.firstEntry();
         while (oldestEntry != null && processIdx - oldestEntry.getKey() > pastElements) {
            values.remove(oldestEntry.getKey());

            oldestEntry = values.firstEntry();
         }

         // inform about free slot
         ReentrantLock fLock = fullLock;
         Condition fCondition = fullCondition;
         if (isRunning.get() && processIdx + backpressureSize <= idGenerator.get()) {
            // ++processIdx;
            fLock.lock();
            try {
               fCondition.signal();
            } finally {
               fLock.unlock();
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
   public String toString() {
      return values.keySet().toString();
   }
}
