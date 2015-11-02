package ch.psi.bsread.stream;

import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Spliterator;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.Function;

import ch.psi.bsread.impl.singleton.Deferred;

public class AsyncTransferSpliterator<T> implements Spliterator<StreamSection<T>> {

   // using a big backpressure ensures that Events get buffered on the client
   // and if the client is not fast enough in processing elements it will
   // result in an OutOfMemoryError on the client.
   public static final int DEFAULT_BACKPRESSURE_SIZE = Integer.MAX_VALUE;
   private static final int CHARACTERISTICS = Spliterator.ORDERED | Spliterator.NONNULL;
   private static final Deferred<ExecutorService> DEFAULT_MAPPING_SERVICE = new Deferred<>(
         () -> Executors.newFixedThreadPool(Math.max(2, Runtime.getRuntime().availableProcessors())));

   private AtomicBoolean isRunning = new AtomicBoolean(true);
   private ConcurrentSkipListMap<Long, CompletableFuture<T>> values = new ConcurrentSkipListMap<>();
   private int pastElements;
   private int futureElements;
   private long backpressureSize;
   private AtomicLong idGenerator = new AtomicLong();
   private AtomicLong readyIndex;
   private AtomicLong processingIndex;
   private ConcurrentLinkedQueue<Thread> producers = new ConcurrentLinkedQueue<>();
   private ConcurrentLinkedQueue<Thread> consumers = new ConcurrentLinkedQueue<>();
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

      long index = readyIndex.get();
      while (isRunning.get() && values.get(index) != null && valueIndex - index >= futureElements
            && readyIndex.compareAndSet(index, index + 1) && !consumers.isEmpty()) {
         LockSupport.unpark(consumers.poll());

         ++index;
         // valueIndex = idGenerator.get() - 1;
      }

      // consider backpressure
      while (isRunning.get() && processingIndex.get() + backpressureSize <= valueIndex) {
         producers.add(Thread.currentThread());
         LockSupport.park();
      }
   }

   /**
    * Close the Spliterator and unblock waiting threads.
    */
   public void onClose() {
      if (isRunning.compareAndSet(true, false)) {
         // release all threads that try provide elements to process
         while (!producers.isEmpty()) {
            LockSupport.unpark(producers.poll());
         }

         // release all threads that are waiting for new elements to process
         while (!consumers.isEmpty()) {
            LockSupport.unpark(consumers.poll());
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
      // process one at a time (for now?)
      final StreamSection<T> section = getNext(true);
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
    * @param doCopy Defines if a copy of the submap should be created
    * @return StreamSection The StreamSection
    */
   protected StreamSection<T> getNext(boolean doCopy) {
      StreamSection<T> streamSection = null;

      while (isRunning.get() && processingIndex.get() >= readyIndex.get()) {
         consumers.add(Thread.currentThread());
         LockSupport.park();
      }

      if (isRunning.get()) {
         Long processIdx = processingIndex.getAndIncrement();

         NavigableMap<Long, CompletableFuture<T>> subMap =
               this.values.subMap(processIdx.longValue() - pastElements, true,
                     processIdx.longValue() + futureElements, true);
         if (doCopy) {
            subMap = new TreeMap<>(subMap);
         }
         streamSection = new StreamSectionImpl<T>(processIdx, subMap);

         // delete elements that are not needed anymore
         Entry<Long, CompletableFuture<T>> oldestEntry = values.firstEntry();
         while (oldestEntry != null && processIdx - oldestEntry.getKey() > pastElements) {
            values.remove(oldestEntry.getKey());

            oldestEntry = values.firstEntry();
         }

         // inform about free slot
         if (isRunning.get() && processIdx + backpressureSize <= idGenerator.get() && !producers.isEmpty()) {
            // ++processIdx;
            LockSupport.unpark(producers.poll());
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
