package ch.psi.bsread.sync;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.function.ToLongFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageSynchronizerCompleteAllLockFree<Msg> extends AbstractMessageSynchronizerLockFree<Msg> {
   private static final Logger LOGGER = LoggerFactory.getLogger(MessageSynchronizerCompleteAllLockFree.class.getName());
   private static final long INITIAL_LAST_SENT_OR_DELETE_PULSEID = Long.MIN_VALUE;

   private final int maxNumberOfMessagesToKeep;
   private final boolean sendIncompleteMessages;

   private final Function<Msg, String> channelNameProvider;
   private final ToLongFunction<Msg> pulseIdProvider;
   private final boolean sendFirstComplete;

   public MessageSynchronizerCompleteAllLockFree(
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

   public MessageSynchronizerCompleteAllLockFree(
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

   public MessageSynchronizerCompleteAllLockFree(
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
      this.sendFirstComplete = sendFirstComplete;
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
            final SyncChannel channelConfig = this.channelConfigs.get(channelName);
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

                  if (lastPulseId == INITIAL_LAST_SENT_OR_DELETE_PULSEID
                        && sendFirstComplete
                        && (pulseIdMap.size() >= this.getNumberOfExpectedChannels(pulseId))
                        || (pulseId <= this.lastSentOrDeletedPulseId.get())) {
                     // several threads might enter this code block but it is
                     // important that they cleanup
                     this.updateLastSentOrDeletedPulseId(pulseId - 1);
                     Entry<Long, TimedMessages<Msg>> entry = this.sortedMap.firstEntry();
                     while (entry != null && entry.getKey() < pulseId) {
                        LOGGER.info("Drop message of pulse '{}' from channel '{}' as there is a later complete start.",
                              entry.getKey(), channelName);
                        this.sortedMap.remove(entry.getKey());
                        entry = this.sortedMap.firstEntry();
                     }
                  }
               } else {
                  LOGGER.debug(
                        "Drop message of pulse '{}' from channel '{}' that does not match modulo/offset '{}'.",
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

         this.checkForCompleteMessages(currentTime);
      } else {
         LOGGER.warn("'{}' stopped running.", this.getClass());
      }
   }

   private void checkForCompleteMessages(final long currentTime) {
      Entry<Long, TimedMessages<Msg>> entry = this.sortedMap.firstEntry();

      // Time eviction: Handle all messages that are older than specified
      // timeout
      if (messageSendTimeoutMillis < Long.MAX_VALUE) {
         if (entry != null && currentTime - entry.getValue().getSubmitTime() >= messageSendTimeoutMillis) {
            onComplete(entry.getKey());
            // no need to check further as consumer will take over
            return;
         }
      }

      // Size eviction: Handle all messages that exceed the messages to keep
      if (maxNumberOfMessagesToKeep < Integer.MAX_VALUE) {
         // TODO: sortedMap.size() is an expensive operation -> consider usingan AtomicInteger as
         // counter (see ConcurrentLongHistogram for a possibility to get around map.compute() does
         // not guarantee atomic execution of creator function)
         if (entry != null && this.sortedMap.size() > this.maxNumberOfMessagesToKeep) {
            onComplete(entry.getKey());
            // no need to check further as consumer will take over
            return;
         }
      }

      // handle all complete messages
      if (entry != null
            && entry.getValue().availableChannels() >= this.getNumberOfExpectedChannels(entry.getKey())) {
         // make sure there is no pulse missing (i.e. there should be a pulse
         // before the currently handled one but we have not yet received a
         // message for this pulse
         final Long pulseId = entry.getKey();
         if (!this.isPulseIdMissing(pulseId)) {
            onComplete(pulseId);
            // no need to check further as consumer will take over
            return;
         }
      }
   }

   private void onComplete(final Long pulseId) {
      if (completePulseIds.putIfAbsent(pulseId, Boolean.TRUE) == null) {
         // give all consumers a chance
         unparkAll();
      }
   }

   @Override
   public Map<String, Msg> nextMessage() {
      Map<String, Msg> msgMap = null;
      Entry<Long, TimedMessages<Msg>> entry;

      while (isRunning.get() && msgMap == null) {
         entry = this.sortedMap.firstEntry();
         final long currentTime = System.currentTimeMillis();
         boolean reCheck = false;

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
                     LOGGER.info(
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
            } else if (entry.getValue().availableChannels() >= nrOfExpectedChannels) {
               // potentially complete message
               //
               if (!this.isPulseIdMissing(pulseId)) {
                  // in start phase, it can happen that a later pulse is complete before the very
                  // first was added
                  if (pulseId == this.sortedMap.firstKey()) {
                     // Remove current pulse-id (might be accessed by several consumers -> one will
                     // win)
                     this.updateLastSentOrDeletedPulseId(pulseId);
                     final TimedMessages<Msg> messages = this.sortedMap.remove(pulseId);
                     // in case there was another consumer Thread that was also checking this
                     // pulse and was faster
                     if (messages != null) {
                        clearHead(completePulseIds, pulseId, true);
                        clearHead(sortedMap, pulseId, true);

                        // LOGGER.debug("Send complete pulse '{}'.", pulseId);
                        msgMap = entry.getValue().getMessagesMap();
                     } else {
                        // LOGGER.debug("Another consumer thread is handling message of pulse '{}'.
                        // Let it do the work.",
                        // pulseId);
                     }
                  } else {
                     reCheck = true;
                  }
               }
            }
         }

         // if there was no message available
         parkIfNeeded(msgMap, reCheck);
      }

      return msgMap;
   }
}
