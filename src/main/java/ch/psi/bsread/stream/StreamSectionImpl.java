package ch.psi.bsread.stream;

import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.psi.bsread.message.Value;

public class StreamSectionImpl<T> implements StreamSection<T> {
   private static final Logger LOGGER = LoggerFactory.getLogger(Value.class);
   public static final long DEFAULT_TIMEOUT_IN_MILLIS = Value.DEFAULT_TIMEOUT_IN_MILLIS;

   private Long currentIndex;
   private NavigableMap<Long, CompletableFuture<T>> subMap;

   public StreamSectionImpl(Long currentIndex, NavigableMap<Long, CompletableFuture<T>> subMap) {
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

   @Override
   public T getCurrent() {
      return this.extract(subMap.get(currentIndex));
   }

   @Override
   public T getPreviousCurrent() {
      Entry<Long, CompletableFuture<T>> entry = subMap.lowerEntry(currentIndex);
      if (entry != null) {
         return this.extract(entry.getValue());
      } else {
         return null;
      }
   }

   @Override
   public T getNextCurrent() {
      Entry<Long, CompletableFuture<T>> entry = subMap.higherEntry(currentIndex);
      if (entry != null) {
         return this.extract(entry.getValue());
      } else {
         return null;
      }
   }

   @Override
   public T getExpiring() {
      return this.extract(subMap.firstEntry().getValue());
   }

   @Override
   public T getJoining() {
      return this.extract(subMap.lastEntry().getValue());
   }

   @Override
   public Stream<T> getAll() {
      return getAll(true);
   }

   @Override
   public Stream<T> getAll(boolean ascending) {
      Stream<CompletableFuture<T>> stream;
      if (ascending) {
         stream = subMap.values().stream();
      } else {
         stream = subMap.descendingMap().values().stream();
      }

      return stream.map(future -> extract(future));
   }

   @Override
   public Stream<T> getPast() {
      return getPast(true);
   }

   @Override
   public Stream<T> getPast(boolean ascending) {
      Stream<CompletableFuture<T>> stream;
      if (ascending) {
         stream = subMap.headMap(currentIndex, false).values().stream();
      } else {
         stream = subMap.headMap(currentIndex, false).descendingMap().values().stream();
      }

      return stream.map(future -> extract(future));
   }

   @Override
   public Stream<T> getFuture() {
      return getFuture(true);
   }

   @Override
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
