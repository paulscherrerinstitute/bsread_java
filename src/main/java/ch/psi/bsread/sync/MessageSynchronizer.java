package ch.psi.bsread.sync;

import java.io.Closeable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;
import java.util.function.ToLongFunction;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MessageBuffer based on a max. allowed size. Accordingly, the time limit messages are kept is
 * given by the frequency and the buffer size (assuming there are constantly messages arriving)
 */
public class MessageSynchronizer<Msg> implements Closeable {
   private static final Logger LOGGER = LoggerFactory.getLogger(MessageSynchronizer.class.getName());
   private static final long INITIAL_LAST_SENT_OR_DELETE_PULSEID = Long.MIN_VALUE;

   private final int maxNumberOfMessagesToKeep;
   private final long messageSendTimeoutMillis;
   private final boolean sendIncompleteMessages;

   private final AtomicBoolean isRunning = new AtomicBoolean(true);
   private final AtomicBoolean isUnparking = new AtomicBoolean(false);
   private final NavigableMap<Long, Boolean> checkPulseIds = new ConcurrentSkipListMap<>();
   private final ConcurrentMap<Long, Thread> consumers = new ConcurrentHashMap<>(4, 0.75f, 4);

   private final Map<String, Pair<Long, Long>> channelConfigs;
   private final AtomicLong smallestEverReceivedPulseId = new AtomicLong(Long.MAX_VALUE);
   private final AtomicLong lastSentOrDeletedPulseId = new AtomicLong(INITIAL_LAST_SENT_OR_DELETE_PULSEID);
   // map[ pulseId -> map[channel -> value] ]
   private final ConcurrentSkipListMap<Long, TimedMessages<Msg>> sortedMap = new ConcurrentSkipListMap<>();
   private final Function<Msg, String> channelNameProvider;
   private final ToLongFunction<Msg> pulseIdProvider;
   private final boolean sendFirstComplete;

   public MessageSynchronizer(
         int maxNumberOfMessagesToKeep,
         boolean sendIncompleteMessages,
         boolean sendFirstComplete,
         Collection<? extends SyncChannel> channels,
         Function<Msg, String> channelNameProvider,
         ToLongFunction<Msg> pulseIdProvider) {
      this(maxNumberOfMessagesToKeep, Long.MAX_VALUE, sendIncompleteMessages, sendFirstComplete, channels,
            channelNameProvider, pulseIdProvider);
   }

   public MessageSynchronizer(
         long messageSendTimeoutMillis,
         boolean sendIncompleteMessages,
         boolean sendFirstComplete,
         Collection<? extends SyncChannel> channels,
         Function<Msg, String> channelNameProvider,
         ToLongFunction<Msg> pulseIdProvider) {
      this(Integer.MAX_VALUE, messageSendTimeoutMillis, sendIncompleteMessages, sendFirstComplete, channels,
            channelNameProvider, pulseIdProvider);
   }

   public MessageSynchronizer(
         int maxNumberOfMessagesToKeep,
         long messageSendTimeoutMillis,
         boolean sendIncompleteMessages,
         boolean sendFirstComplete,
         Collection<? extends SyncChannel> channels,
         Function<Msg, String> channelNameProvider,
         ToLongFunction<Msg> pulseIdProvider) {
      this.maxNumberOfMessagesToKeep = maxNumberOfMessagesToKeep;
      this.messageSendTimeoutMillis = messageSendTimeoutMillis;
      this.sendIncompleteMessages = sendIncompleteMessages;
      this.channelNameProvider = channelNameProvider;
      this.pulseIdProvider = pulseIdProvider;
      this.sendFirstComplete = sendFirstComplete;

      this.channelConfigs = new HashMap<>(channels.size());
      for (SyncChannel channel : channels) {
         this.channelConfigs.put(channel.getName(), Pair.of((long) channel.getModulo(), (long) channel.getOffset()));
      }
   }

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
            onAvailable(entry.getKey());
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
            onAvailable(entry.getKey());
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
            onAvailable(pulseId);
            // no need to check further as consumer will take over
            return;
         }
      }
   }

   private void onAvailable(final Long pulseId) {
      if (checkPulseIds.putIfAbsent(pulseId, Boolean.TRUE) == null) {
         // give all consumers a chance
         unparkAll();
      }
   }

   private void unparkAll() {
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

   public Map<String, Msg> nextMessage() {
      Map<String, Msg> msgMap = null;
      Entry<Long, TimedMessages<Msg>> entry;

      while (isRunning.get() && msgMap == null) {
         entry = this.sortedMap.firstEntry();
         final long currentTime = System.currentTimeMillis();
         boolean needToRecheck = false;

         if (entry != null) {
            final Long pulseId = entry.getKey();
            final int nrOfExpectedChannels = this.getNumberOfExpectedChannels(entry.getKey());

            // check time and size eviction
            if ((messageSendTimeoutMillis < Long.MAX_VALUE
                  && currentTime - entry.getValue().getSubmitTime() >= messageSendTimeoutMillis)
                  || (maxNumberOfMessagesToKeep < Integer.MAX_VALUE
                        && this.sortedMap.size() > this.maxNumberOfMessagesToKeep)) {
               // potentially incomplete message
               //
               this.updateLastSentOrDeletedPulseId(pulseId);
               // Remove current pulse-id (might be accessed by several consumers -> one will win)
               final TimedMessages<Msg> messages = this.sortedMap.remove(entry.getKey());
               // in case there was another consumer Thread that was also checking this
               // pulse and was faster
               if (messages != null) {
                  this.checkPulseIds.headMap(pulseId, true).clear();
                  this.sortedMap.headMap(pulseId, true).clear();

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
                     needToRecheck = true;
                  }
               } else {
                  LOGGER.debug("Another consumer thread is handling message of pulse '{}'. Let it do the work.",
                        pulseId);
               }
            } else if (entry.getValue().availableChannels() >= nrOfExpectedChannels) {
               // potentially complete message
               //
               if (!this.isPulseIdMissing(pulseId)) {
                  // Remove current pulse-id (might be accessed by several consumers -> one will
                  // win)
                  this.updateLastSentOrDeletedPulseId(pulseId);
                  final TimedMessages<Msg> messages = this.sortedMap.remove(pulseId);
                  // in case there was another consumer Thread that was also checking this
                  // pulse and was faster
                  if (messages != null) {
                     this.checkPulseIds.headMap(pulseId, true).clear();
                     this.sortedMap.headMap(pulseId, true).clear();

                     LOGGER.debug("Send complete pulse '{}'.", pulseId);
                     msgMap = entry.getValue().getMessagesMap();
                  } else {
                     LOGGER.debug("Another consumer thread is handling message of pulse '{}'. Let it do the work.",
                           pulseId);
                  }
               }
            }
         }

         // there was no message available
         if (isRunning.get() && msgMap == null && !isUnparking.get() && checkPulseIds.isEmpty() && !needToRecheck) {
            long parkNanos = -1;
            if (messageSendTimeoutMillis < Long.MAX_VALUE) {
               entry = this.sortedMap.firstEntry();
               if (entry != null) {
                  // in millis
                  parkNanos = entry.getValue().getSubmitTime() + messageSendTimeoutMillis - currentTime;
               } else {
                  // in millis
                  parkNanos = messageSendTimeoutMillis;
               }
               parkNanos = TimeUnit.MILLISECONDS.toNanos(parkNanos);
            }

            final Thread thread = Thread.currentThread();
            consumers.put(thread.getId(), thread);
            // make sure consumer wakes up periodically to check for timed-out messages (in case no
            // new messages arrive)
            if (parkNanos >= 0) {
               LockSupport.parkNanos(parkNanos);
            } else {
               LockSupport.park();
            }
            consumers.remove(thread.getId());
         }
      }

      return msgMap;
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
      for (TimedMessages<Msg> messages : sortedMap.values()) {
         remainingMsgs.addAll(messages.getMessagesMap().values());
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

   private static class TimedMessages<Msg> {
      private long submitTime;
      private ConcurrentMap<String, Msg> messagesMap;

      public TimedMessages(long submitTime, int nrOfChannels) {
         this.submitTime = submitTime;
         messagesMap = new ConcurrentHashMap<>(nrOfChannels, 0.75f, 4);
      }

      public long getSubmitTime() {
         return submitTime;
      }

      public Map<String, Msg> getMessagesMap() {
         return messagesMap;
      }

      public int availableChannels() {
         return messagesMap.size();
      }
   }
}
