package ch.psi.bsread.stream;

import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.psi.bsread.message.Value;

public class StreamSection<T> {
   private static final Logger LOGGER = LoggerFactory.getLogger(Value.class);
   public static final long DEFAULT_TIMEOUT_IN_MILLIS = Value.DEFAULT_TIMEOUT_IN_MILLIS;

   private Long currentIndex;
   private NavigableMap<Long, CompletableFuture<T>> subMap;

   public StreamSection(Long currentIndex, NavigableMap<Long, CompletableFuture<T>> subMap) {
      this.currentIndex = currentIndex;
      this.subMap = subMap;
   }

   private T extract(CompletableFuture<T> future) {
      try {
         return future.get(DEFAULT_TIMEOUT_IN_MILLIS, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
         // log since exceptions can get lost (e.g.in JAVA Streams)
         LOGGER.error("Could not load value from future.", e);
         throw new RuntimeException(e);
      }
   }

   /**
    * Provides the currently active value.
    * 
    * @return the current value
    */
   public T getCurrent() {
      return this.extract(subMap.get(currentIndex));
   }

   /**
    * Provides the value which will expire/retire in the next iteration.
    * 
    * @return the expiring value
    */
   public T getExpiring() {
      return this.extract(subMap.firstEntry().getValue());
   }

   /**
    * Provides the value which joined the section in the current iteration.
    * 
    * @return the joining value
    */
   public T getJoining() {
      return this.extract(subMap.lastEntry().getValue());
   }

   /**
    * Provides a view on all elements in the section (past, current, and future)
    * 
    * @param ascending <tt>true</tt> orders from oldest to the youngest value, <tt>false</tt> orders
    *        from youngest to the oldest value.
    * @return Collection the values
    */
   public Stream<T> getAll(boolean ascending) {
      Stream<CompletableFuture<T>> stream;
      if (ascending) {
         stream = subMap.values().stream();
      } else {
         stream = subMap.descendingMap().values().stream();
      }

      return stream.map(future -> extract(future));
   }

   /**
    * Provides a view on all elements older than the current value.
    * 
    * @param ascending <tt>true</tt> orders from oldest to the youngest value, <tt>false</tt> orders
    *        from youngest to the oldest value.
    * @return Collection the values
    */
   public Stream<T> getPast(boolean ascending) {
      Stream<CompletableFuture<T>> stream;
      if (ascending) {
         stream = subMap.headMap(currentIndex, false).values().stream();
      } else {
         stream = subMap.headMap(currentIndex, false).descendingMap().values().stream();
      }

      return stream.map(future -> extract(future));
   }

   /**
    * Provides a view on all elements younger than the current value.
    * 
    * @param ascending <tt>true</tt> orders from oldest to the youngest value, <tt>false</tt> orders
    *        from youngest to the oldest value.
    * @return Collection the values
    */
   public Stream<T> getFuture(boolean ascending) {
      Stream<CompletableFuture<T>> stream;
      if (ascending) {
         stream = subMap.tailMap(currentIndex, false).values().stream();
      } else {
         stream = subMap.tailMap(currentIndex, false).descendingMap().values().stream();
      }

      return stream.map(future -> extract(future));
   }
}
