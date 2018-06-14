package ch.psi.bsread.sync;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

public abstract class AbstractMessageSynchronizerLockFree<Msg> extends AbstractMessageSynchronizer<Msg> {
   protected final long messageSendTimeoutMillis;

   protected final AtomicBoolean isRunning = new AtomicBoolean(true);
   private final AtomicBoolean isUnparking = new AtomicBoolean(false);
   protected final NavigableMap<Long, Boolean> completePulseIds = new ConcurrentSkipListMap<>();
   // ConcurrentLinkedHashMap would be fairer (load distributed more equally)
   private final ConcurrentMap<Long, Thread> consumers = new ConcurrentHashMap<>(4, 0.75f, 4);

   // map[ pulseId -> map[channel -> value] ]
   protected final ConcurrentSkipListMap<Long, TimedMessages<Msg>> sortedMap = new ConcurrentSkipListMap<>();

   public AbstractMessageSynchronizerLockFree(
         long messageSendTimeoutMillis,
         Collection<? extends SyncChannel> channels) {
      super(channels);
      this.messageSendTimeoutMillis = messageSendTimeoutMillis;
   }

   protected void unparkAll() {
      if (isUnparking.compareAndSet(false, true)) {

         Iterator<Entry<Long, Thread>> iter = consumers.entrySet().iterator();
         while (iter.hasNext()) {
            LockSupport.unpark(iter.next().getValue());
         }

         isUnparking.set(false);
      }
   }

   @Override
   public void close() {
      if (isRunning.compareAndSet(true, false)) {
         // release all threads that are waiting for new elements to process
         unparkAll();
      }
   }

   protected void parkIfNeeded(final Map<String, Msg> msgMap, final boolean reCheck) {
      // there was no message available
      if (isRunning.get() && msgMap == null && !isUnparking.get() && completePulseIds.isEmpty() && !reCheck) {
         // need to add Thread before isUnparking "barrier" to ensure it gets unparked
         final Thread thread = Thread.currentThread();
         consumers.put(thread.getId(), thread);

         // double check (might save some puts into consumer and parks)
         if (isRunning.get() && msgMap == null && !isUnparking.get() && completePulseIds.isEmpty() && !reCheck) {

            long parkNanos = -1;
            if (messageSendTimeoutMillis < Long.MAX_VALUE) {
               final Entry<Long, TimedMessages<Msg>> entry = this.sortedMap.firstEntry();
               if (entry != null) {
                  // in millis
                  parkNanos = entry.getValue().getSubmitTime() + messageSendTimeoutMillis - System.currentTimeMillis();
               } else {
                  // in millis
                  parkNanos = messageSendTimeoutMillis;
               }
               // in nanos
               parkNanos = TimeUnit.MILLISECONDS.toNanos(parkNanos);
            }

            // make sure consumer wakes up periodically to check for timed-out messages (in case
            // no new messages arrive)
            if (parkNanos >= 0) {
               LockSupport.parkNanos(parkNanos);
            } else {
               LockSupport.park();
            }
         }

         consumers.remove(thread.getId());
      }
   }

   protected void clearHead(NavigableMap<Long, ?> map, Long pulseId, boolean inclusive) {
      map.headMap(pulseId, inclusive).clear();

      // Entry<Long, ?> entry = map.firstEntry();
      // while (entry != null
      // && (inclusive && entry.getKey() <= pulseId
      // || !inclusive && entry.getKey() < pulseId)) {
      // map.remove(entry.getKey());
      // entry = map.firstEntry();
      // }
   }

   /**
    * Get size of the current pulseId buffer. This function is mainly for testing purposes.
    * 
    * @return int The buffer size
    */
   @Override
   public int getBufferSize() {
      return sortedMap.size();
   }
}
