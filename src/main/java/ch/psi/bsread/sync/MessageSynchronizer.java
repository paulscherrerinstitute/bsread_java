package ch.psi.bsread.sync;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.ToLongFunction;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.psi.bsread.configuration.Channel;

/**
 * MessageBuffer based on a max. allowed size. Accordingly, the time limit messages are kept is
 * given by the frequency and the buffer size (assuming there are constantly messages arriving)
 */
public class MessageSynchronizer<Msg> {
   private static final Logger LOGGER = LoggerFactory.getLogger(MessageSynchronizer.class.getName());

   private final int maxNumberOfMessagesToKeep;
   private final boolean sendIncompleteMessages;

   private final Map<String, Pair<Long, Long>> channelConfigs;
   private final AtomicLong smallestEverReceivedPulseId = new AtomicLong(Long.MAX_VALUE);
   private final AtomicLong lastSentOrDeletedPulseId = new AtomicLong(Long.MIN_VALUE);
   private final Queue<Map<String, Msg>> completeQueue;
   // map[ pulseId -> map[channel -> value] ]
   private final ConcurrentSkipListMap<Long, Map<String, Msg>> sortedMap = new ConcurrentSkipListMap<>();
   private final Function<Msg, String> channelNameProvider;
   private final ToLongFunction<Msg> pulseIdProvider;

   public MessageSynchronizer(Queue<Map<String, Msg>> completeQueue, int maxNumberOfMessagesToKeep,
         boolean sendIncompleteMessages, Collection<Channel> channels, Function<Msg, String> channelNameProvider,
         ToLongFunction<Msg> pulseIdProvider) {
      this.completeQueue = completeQueue;
      this.maxNumberOfMessagesToKeep = maxNumberOfMessagesToKeep;
      this.sendIncompleteMessages = sendIncompleteMessages;
      this.channelNameProvider = channelNameProvider;
      this.pulseIdProvider = pulseIdProvider;

      this.channelConfigs = new HashMap<>(channels.size());
      for (Channel channel : channels) {
         this.channelConfigs.put(channel.getName(), Pair.of((long) channel.getModulo(), (long) channel.getOffset()));
      }
   }

   public void addMessage(Msg msg) {
      final long pulseId = pulseIdProvider.applyAsLong(msg);
      final String channelName = channelNameProvider.apply(msg);
      this.updateSmallestEverReceivedPulseId(pulseId);
      long lastPulseId = this.lastSentOrDeletedPulseId.get();

      if (pulseId > lastPulseId) {
         final Pair<Long, Long> channelConfig = this.channelConfigs.get(channelName);
         if (channelConfig != null) {
            // check if message is in the requested period
            if (this.isRequestedPulseId(pulseId, channelConfig)) {

               // Create ConcurrentHashMap using a functional interface
               // A ConcurrentMap is needed due to later put (addMessage is
               // called concurrently when subscribed to more than one
               // ITopic.
               Map<String, Msg> pulseIdMap =
                     this.sortedMap.computeIfAbsent(pulseId, (k) -> new ConcurrentHashMap<>(this.channelConfigs.size(),
                           0.75f, 1));
               pulseIdMap.put(channelName, msg);
            } else {
               LOGGER.debug(
                     "Drop message of pulse '{}' from channel '{}' that does not match modulo '{}' and offset '{}'",
                     pulseId, channelName, channelConfig.getLeft(), channelConfig.getRight());
            }
         } else {
            LOGGER.info("Received message from channel '{}' but that channel is not part of the configuration.",
                  channelName);
         }
      } else {
         LOGGER.info(
               "Drop message of pulse '{}' from channel '{}' since it is smaller than the last send/deleted pulse '{}'",
               pulseId, channelName, lastPulseId);
      }

      this.checkForCompleteMessages();
   }

   private void checkForCompleteMessages() {

      // handle all messages that exceed the messages to keep
      // TODO: sortedMap.size() is an expensive operation -> consider using ConcurrentHashMap
      // (Collections.setFromMap()) for
      // counting (see: http://stackoverflow.com/a/22996395) or use an AtomicInteger as counter
      // (uncertainty here is that computeIfAbsent does not guarantee atomic execution of creator
      // function)
      int sortedMapSize = this.sortedMap.size();
      while (sortedMapSize > this.maxNumberOfMessagesToKeep) {
         // Remove oldest pulse ID - i.e. first
         Entry<Long, Map<String, Msg>> entry = this.sortedMap.pollFirstEntry();
         --sortedMapSize;

         long numberOfChannels = this.getNumberOfExpectedChannels(entry.getKey());
         // check if message is complete
         if (entry.getValue().size() >= numberOfChannels) {
            // we send complete messages by definition
            LOGGER.debug("Send complete pulse '{}'.", entry.getKey());
            this.handleCompleteMessages(entry.getKey(), entry.getValue());

         } else if (this.sendIncompleteMessages) {
            // the user also wants incomplete messages
            LOGGER.debug("Send incomplete pulse '{}'.", entry.getKey());
            this.handleCompleteMessages(entry.getKey(), entry.getValue());
         } else {
            LOGGER.info("Drop messages for pulse '{}'. Requested number of channels '{}' but got only '{}'.",
                  entry.getKey(), numberOfChannels, entry.getValue().size());
            this.updateLastSentOrDeletedPulseId(entry.getKey());
         }
      }

      // handle all complete messages
      Entry<Long, Map<String, Msg>> entry = this.sortedMap.firstEntry();
      while (entry != null && entry.getValue().size() >= this.getNumberOfExpectedChannels(entry.getKey())) {
         // make sure there is no pulse missing (i.e. there should be a pulse
         // before the currently handled one but we have not yet received a
         // message for this pulse
         final Long pulseId = entry.getKey();
         if (!this.isPulseIdMissing(pulseId)) {
            Map<String, Msg> messages = this.sortedMap.remove(pulseId);
            // in case there was another Thread that was also checking this
            // pulse and was faster
            if (messages != null) {
               LOGGER.debug("Send complete pulse '{}'.", pulseId);
               this.handleCompleteMessages(entry.getKey(), entry.getValue());
            }
            entry = this.sortedMap.firstEntry();
         } else {
            LOGGER.debug("Keep pulse '{}' since there are pulses missing.", pulseId);
            // stop since there are still elements missing
            entry = null;
         }
      }
   }

   private void handleCompleteMessages(long pulseId, Map<String, Msg> messages) {
      if (!this.completeQueue.offer(messages)) {
         LOGGER.warn("Had to drop messages for pulse '{}' because capacity constrained queue seems to be full.",
               pulseId);
      }

      this.updateLastSentOrDeletedPulseId(pulseId);
   }

   private boolean isPulseIdMissing(long nextGroupPulseId) {
      return MessageSynchronizer.isPulseIdMissing(
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
               // + modulo to overcome case when pulseId - offset results into neg value
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

   private void updateLastSentOrDeletedPulseId(long pulseId) {
      long lastPulseId = this.lastSentOrDeletedPulseId.get();
      while (lastPulseId < pulseId && !this.lastSentOrDeletedPulseId.compareAndSet(lastPulseId, pulseId)) {
         lastPulseId = this.lastSentOrDeletedPulseId.get();
      }
   }

   private void updateSmallestEverReceivedPulseId(long pulseId) {
      long smallestPulseId = this.smallestEverReceivedPulseId.get();
      while (smallestPulseId > pulseId && !this.smallestEverReceivedPulseId.compareAndSet(smallestPulseId, pulseId)) {
         smallestPulseId = this.smallestEverReceivedPulseId.get();
      }
   }

   private int getNumberOfExpectedChannels(long pulseId) {
      int nrOfChannels = 0;

      for (Pair<Long, Long> channelConfig : channelConfigs.values()) {
         if (this.isRequestedPulseId(pulseId, channelConfig)) {
            ++nrOfChannels;
         }
      }
      return nrOfChannels;
   }

   private boolean isRequestedPulseId(long pulseId, Pair<Long, Long> channelConfig) {
      return (pulseId - channelConfig.getRight()) % channelConfig.getLeft() == 0;
   }

   /**
    * Retrieves all currently buffered messages.
    * 
    * @return List The buffered messages.
    */
   public List<Msg> retrieveBufferedMessages() {
      List<Msg> remainingMsgs = new LinkedList<>();
      for (Map<String, Msg> map : sortedMap.values()) {
         remainingMsgs.addAll(map.values());
      }

      return remainingMsgs;
   }

   /**
    * Get size of the current pulseId buffer. This function is mainly for testing purposes.
    * 
    * @return int The buffer size
    */
   public int getBufferSize() {
      return sortedMap.size();
   }
}
