package ch.psi.bsread.sync;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.tuple.Pair;

public abstract class AbstractMessageSynchronizer<Msg> implements MessageSynchronizer<Msg> {
   protected static final long INITIAL_LAST_SENT_OR_DELETE_PULSEID = Long.MIN_VALUE;

   protected final AtomicLong smallestEverReceivedPulseId = new AtomicLong(Long.MAX_VALUE);
   protected final AtomicLong lastSentOrDeletedPulseId = new AtomicLong(INITIAL_LAST_SENT_OR_DELETE_PULSEID);

   protected final Map<String, Pair<Long, Long>> channelConfigs;

   public AbstractMessageSynchronizer(Collection<? extends SyncChannel> channels) {
      this.channelConfigs = new HashMap<>(channels.size());
      for (SyncChannel channel : channels) {
         this.channelConfigs.put(channel.getName(), Pair.of((long) channel.getModulo(), (long) channel.getOffset()));
      }
   }

   protected boolean isPulseIdMissing(long nextGroupPulseId) {
      return AbstractMessageSynchronizer.isPulseIdMissing(
            Math.max(this.smallestEverReceivedPulseId.get(), this.lastSentOrDeletedPulseId.get()), nextGroupPulseId,
            channelConfigs.values());
   }

   // make this thing testable from outside
   public static boolean isPulseIdMissing(long lastPulseId, long nextGroupPulseId,
         Collection<Pair<Long, Long>> channelConfigs) {
      // optimization for 100Hz case
      if (nextGroupPulseId - lastPulseId > 1) {
         for (Pair<Long, Long> channelConfig : channelConfigs) {
            final long modulo = channelConfig.getLeft();
            final long offset = channelConfig.getRight();

            if (nextGroupPulseId - lastPulseId > modulo) {
               return true;
            } else {
               // + modulo to overcome case when pulseId - offset results
               // into neg value
               final long lastHops = (lastPulseId + modulo - offset) / modulo;
               final long nextHops = (nextGroupPulseId + modulo - offset) / modulo;
               if (nextHops - lastHops > 1) {
                  return true;
               } else if (nextHops - lastHops == 1) {
                  if ((lastPulseId - offset) % modulo != 0 && (nextGroupPulseId - offset) % modulo != 0) {
                     return true;
                  }
               }
            }
         }
      }

      return false;
   }

   protected void updateLastSentOrDeletedPulseId(long pulseId) {
      long lastPulseId = this.lastSentOrDeletedPulseId.get();
      while (lastPulseId < pulseId && !this.lastSentOrDeletedPulseId.compareAndSet(lastPulseId, pulseId)) {
         lastPulseId = this.lastSentOrDeletedPulseId.get();
      }
   }

   protected void updateSmallestEverReceivedPulseId(long pulseId) {
      long smallestPulseId = this.smallestEverReceivedPulseId.get();
      while (smallestPulseId > pulseId && !this.smallestEverReceivedPulseId.compareAndSet(smallestPulseId, pulseId)) {
         smallestPulseId = this.smallestEverReceivedPulseId.get();
      }
   }

   protected int getNumberOfExpectedChannels(long pulseId) {
      int nrOfChannels = 0;

      for (Pair<Long, Long> channelConfig : channelConfigs.values()) {
         if (this.isRequestedPulseId(pulseId, channelConfig)) {
            ++nrOfChannels;
         }
      }
      return nrOfChannels;
   }

   protected boolean isRequestedPulseId(long pulseId, Pair<Long, Long> channelConfig) {
      return (pulseId - channelConfig.getRight()) % channelConfig.getLeft() == 0;
   }

   /**
    * Get size of the current pulseId buffer. This function is mainly for testing purposes.
    * 
    * @return int The buffer size
    */
   public abstract int getBufferSize();
}
