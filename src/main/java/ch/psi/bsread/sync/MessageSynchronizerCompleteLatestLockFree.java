package ch.psi.bsread.sync;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Function;
import java.util.function.ToLongFunction;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageSynchronizerCompleteLatestLockFree<Msg> extends AbstractMessageSynchronizerLockFree<Msg> {
   private static final Logger LOGGER = LoggerFactory.getLogger(MessageSynchronizerCompleteLatestLockFree.class.getName());

   private final int maxNumberOfMessagesToKeep;
   private final boolean sendIncompleteMessages;

   private final NavigableMap<Long, Boolean> wakeupPulseIds = new ConcurrentSkipListMap<>();

   private final Function<Msg, String> channelNameProvider;
   private final ToLongFunction<Msg> pulseIdProvider;

   public MessageSynchronizerCompleteLatestLockFree(
         int maxNumberOfMessagesToKeep,
         boolean sendIncompleteMessages,
         boolean sendFirstComplete,
         Collection<? extends SyncChannel> channels,
         Function<Msg, String> channelNameProvider,
         ToLongFunction<Msg> pulseIdProvider) {
      this(maxNumberOfMessagesToKeep,
            Long.MAX_VALUE,
            sendIncompleteMessages,
            sendFirstComplete,
            channels,
            channelNameProvider,
            pulseIdProvider);
   }

   public MessageSynchronizerCompleteLatestLockFree(
         long messageSendTimeoutMillis,
         boolean sendIncompleteMessages,
         boolean sendFirstComplete,
         Collection<? extends SyncChannel> channels,
         Function<Msg, String> channelNameProvider,
         ToLongFunction<Msg> pulseIdProvider) {
      this(Integer.MAX_VALUE,
            messageSendTimeoutMillis,
            sendIncompleteMessages,
            sendFirstComplete,
            channels,
            channelNameProvider,
            pulseIdProvider);
   }

   public MessageSynchronizerCompleteLatestLockFree(
         int maxNumberOfMessagesToKeep,
         long messageSendTimeoutMillis,
         boolean sendIncompleteMessages,
         boolean sendFirstComplete,
         Collection<? extends SyncChannel> channels,
         Function<Msg, String> channelNameProvider,
         ToLongFunction<Msg> pulseIdProvider) {
      super(messageSendTimeoutMillis, channels);
      this.maxNumberOfMessagesToKeep = maxNumberOfMessagesToKeep;
      this.sendIncompleteMessages = sendIncompleteMessages;
      this.channelNameProvider = channelNameProvider;
      this.pulseIdProvider = pulseIdProvider;

      // ignore
      // this.sendFirstComplete = sendFirstComplete;
   }

   @Override
   public void addMessage(Msg msg) {
      if (isRunning.get()) {
         final long pulseId = pulseIdProvider.applyAsLong(msg);
         final String channelName = channelNameProvider.apply(msg);
         this.updateSmallestEverReceivedPulseId(pulseId);
         final long lastPulseId = lastSentOrDeletedPulseId.get();
         final long currentTime = System.currentTimeMillis();

         if (pulseId > lastPulseId) {
            final Pair<Long, Long> channelConfig = this.channelConfigs.get(channelName);
            if (channelConfig != null) {
               // check if message is in the requested period
               if (this.isRequestedPulseId(pulseId, channelConfig)) {

                  // Create ConcurrentHashMap using a functional interface
                  // A ConcurrentMap is needed due to later put (addMessage is
                  // called concurrently when subscribed to more than one
                  // ITopic.
                  final Map<String, Msg> pulseIdMap = this.sortedMap.computeIfAbsent(
                        pulseId,
                        (k) -> new TimedMessages<>(
                              currentTime,
                              channelConfigs.size()))
                        .getMessagesMap();
                  pulseIdMap.put(channelName, msg);
               } else {
                  LOGGER.debug(
                        "Drop message of pulse '{}' from channel '{}' that does not match modulo/offset '{}'",
                        pulseId, channelName, channelConfig);
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

         this.checkForCompleteMessages(pulseId, currentTime);
      } else {
         LOGGER.warn("'{}' stopped running.", this.getClass());
      }
   }

   private void checkForCompleteMessages(final long currentPulseId, final long currentTime) {
      Entry<Long, TimedMessages<Msg>> entry = this.sortedMap.firstEntry();
      TimedMessages<Msg> msg = this.sortedMap.get(currentPulseId);
      if (entry != null && currentPulseId == entry.getKey()) {
         // no need to check this
         entry = null;
      }

      if (msg != null
            && msg.availableChannels() >= this.getNumberOfExpectedChannels(currentPulseId)) {
         onComplete(currentPulseId);
      }

      // Time eviction: Handle all messages that are older than specified
      // timeout
      if (messageSendTimeoutMillis < Long.MAX_VALUE) {
         if (entry != null && currentTime - entry.getValue().getSubmitTime() >= messageSendTimeoutMillis) {
            onWakup(entry.getKey());
            entry = null;
         }
      }

      // Size eviction: Handle all messages that exceed the messages to keep
      if (maxNumberOfMessagesToKeep < Integer.MAX_VALUE) {
         // TODO: sortedMap.size() is an expensive operation -> consider usingan AtomicInteger as
         // counter (see ConcurrentLongHistogram for a possibility to get around map.compute() does
         // not guarantee atomic execution of creator function)
         if (entry != null && this.sortedMap.size() > this.maxNumberOfMessagesToKeep) {
            onWakup(entry.getKey());
            entry = null;
         }
      }

      // handle all complete messages
      if (entry != null
            && entry.getValue().availableChannels() >= this.getNumberOfExpectedChannels(entry.getKey())) {
         onWakup(entry.getKey());
         entry = null;
      }
   }

   protected void onComplete(final Long pulseId) {
      if (completePulseIds.putIfAbsent(pulseId, Boolean.TRUE) == null) {
         onWakup(pulseId);
      }
   }

   private void onWakup(final Long pulseId) {
      if (wakeupPulseIds.putIfAbsent(pulseId, Boolean.TRUE) == null) {
         // give all consumers a chance
         unparkAll();
      }
   }

   @Override
   public Map<String, Msg> nextMessage() {
      Map<String, Msg> msgMap = null;
      Entry<Long, TimedMessages<Msg>> entry;
      Entry<Long, Boolean> completePulseId;

      while (isRunning.get() && msgMap == null) {
         if (!sendIncompleteMessages) {
            completePulseId = completePulseIds.firstEntry();
            if (completePulseId != null) {
               clearHead(sortedMap, completePulseId.getKey(), false);
            }
         }
         final long currentTime = System.currentTimeMillis();
         boolean reCheck = false;
         entry = this.sortedMap.firstEntry();

         if (entry != null) {
            final long pulseId = entry.getKey();
            final int nrOfExpectedChannels = this.getNumberOfExpectedChannels(entry.getKey());

            // check time and size eviction
            if ((messageSendTimeoutMillis < Long.MAX_VALUE
                  && currentTime - entry.getValue().getSubmitTime() >= messageSendTimeoutMillis)
                  || (maxNumberOfMessagesToKeep < Integer.MAX_VALUE
                        && this.sortedMap.size() > this.maxNumberOfMessagesToKeep)) {
               // potentially incomplete message
               //
               this.updateLastSentOrDeletedPulseId(pulseId);
               // Remove current pulse-id (might be accessed by several consumers -> one will
               // win)
               final TimedMessages<Msg> messages = this.sortedMap.remove(entry.getKey());
               // in case there was another consumer Thread that was also checking this
               // pulse and was faster

               if (messages != null) {
                  clearHead(wakeupPulseIds, pulseId, true);
                  clearHead(completePulseIds, pulseId, true);
                  clearHead(sortedMap, pulseId, true);

                  // check if message is complete
                  if (entry.getValue().availableChannels() >= nrOfExpectedChannels) {
                     // we send complete messages by definition
                     LOGGER.debug("Send complete pulse '{}' due to eviction.", entry.getKey());
                     msgMap = entry.getValue().getMessagesMap();
                  } else if (this.sendIncompleteMessages) {
                     // the user also wants incomplete messages
                     LOGGER.debug("Send incomplete pulse '{}' due to eviction.", entry.getKey());
                     msgMap = entry.getValue().getMessagesMap();
                  } else {
                     LOGGER.debug(
                           "Drop messages for pulse '{}' due to eviction. Requested number of channels '{}' but got only '{}'.",
                           entry.getKey(), nrOfExpectedChannels, entry.getValue().getMessagesMap().size());
                     // there might be more messages available ready for send
                     reCheck = true;
                  }
               } else {
                  // LOGGER.debug("Another consumer thread is handling message of pulse '{}'. Let it
                  // do the work.",
                  // pulseId);
               }
            } else if ((completePulseId = completePulseIds.firstEntry()) != null) {
               // potentially complete message
               //
               // Remove current pulse-id (might be accessed by several consumers -> one will
               // win)
               if (pulseId <= completePulseId.getKey()) {
                  this.updateLastSentOrDeletedPulseId(pulseId);
                  final TimedMessages<Msg> messages = this.sortedMap.remove(pulseId);
                  // in case there was another consumer Thread that was also checking this
                  // pulse and was faster
                  if (messages != null) {
                     clearHead(wakeupPulseIds, pulseId, true);
                     clearHead(completePulseIds, pulseId, true);
                     clearHead(sortedMap, pulseId, true);

                     if (entry.getValue().availableChannels() >= nrOfExpectedChannels) {
                        // LOGGER.debug("Send complete pulse '{}'.", pulseId);
                        msgMap = entry.getValue().getMessagesMap();
                     } else if (sendIncompleteMessages) {
                        // the user also wants incomplete messages
                        LOGGER.debug("Send incomplete pulse '{}'.", entry.getKey());
                        msgMap = entry.getValue().getMessagesMap();
                     } else {
                        LOGGER.debug(
                              "Drop messages for pulse '{}'. Requested number of channels '{}' but got only '{}'.",
                              entry.getKey(), nrOfExpectedChannels, entry.getValue().getMessagesMap().size());
                        reCheck = true;
                     }
                  } else {
                     // LOGGER.debug("Another consumer thread is handling message of pulse '{}'. Let
                     // it do the work.",
                     // pulseId);
                  }
               } else {
                  reCheck = true;
               }
            }
         }

         // if there was no message available
         parkIfNeeded(msgMap, reCheck);
      }

      return msgMap;
   }
}
