package ch.psi.bsread.stream;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentNavigableMap;

public class StreamSection<T> {
   private Long currentIndex;
   private ConcurrentNavigableMap<Long, T> subMap;

   public StreamSection(Long currentIndex, ConcurrentNavigableMap<Long, T> subMap) {
      this.currentIndex = currentIndex;
      this.subMap = subMap;
   }

   public T getCurrentValue() {
      return subMap.get(currentIndex);
   }

   public Collection<T> getPastValues(boolean ascending) {
      if (ascending) {
         return Collections.unmodifiableCollection(subMap.headMap(currentIndex, false).values());
      } else {
         return Collections.unmodifiableCollection(subMap.headMap(currentIndex, false).descendingMap().values());
      }
   }

   public Collection<T> getFutureValues(boolean ascending) {
      if (ascending) {
         return Collections.unmodifiableCollection(subMap.tailMap(currentIndex, false).values());
      } else {
         return Collections.unmodifiableCollection(subMap.tailMap(currentIndex, false).descendingMap().values());
      }
   }
}
