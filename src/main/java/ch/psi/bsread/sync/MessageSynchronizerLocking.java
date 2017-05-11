package ch.psi.bsread.sync;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.ToLongFunction;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MessageBuffer based on a max. allowed size. Accordingly, the time limit messages are kept is
 * given by the frequency and the buffer size (assuming there are constantly messages arriving)
 */
public class MessageSynchronizerLocking<Msg> extends AbstractMessageSynchronizer<Msg> {
   private static final Logger LOGGER = LoggerFactory.getLogger(MessageSynchronizerLocking.class.getName());

   private final int maxNumberOfMessagesToKeep;
   private final long messageSendTimeoutMillis;
   private final boolean sendIncompleteMessages;

   private final AtomicBoolean isRunning = new AtomicBoolean(true);
   private final ReentrantLock lock = new ReentrantLock();
   private final Condition condition = lock.newCondition();
   private final Queue<Map<String, Msg>> queue = new ArrayDeque<>();

   // map[ pulseId -> map[channel -> value] ]
   private final ConcurrentSkipListMap<Long, TimedMessages<Msg>> sortedMap = new ConcurrentSkipListMap<>();
   private final Function<Msg, String> channelNameProvider;
   private final ToLongFunction<Msg> pulseIdProvider;
   private final boolean sendFirstComplete;

   public MessageSynchronizerLocking(
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

   public MessageSynchronizerLocking(
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

   public MessageSynchronizerLocking(
         int maxNumberOfMessagesToKeep,
         long messageSendTimeoutMillis,
         boolean sendIncompleteMessages,
         boolean sendFirstComplete,
         Collection<? extends SyncChannel> channels,
         Function<Msg, String> channelNameProvider,
         ToLongFunction<Msg> pulseIdProvider) {
      super(channels);

      this.maxNumberOfMessagesToKeep = maxNumberOfMessagesToKeep;
      this.messageSendTimeoutMillis = messageSendTimeoutMillis;
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
                     // important that they
                     // cleanup
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

   private void checkForCompleteMessages(long currentTime) {
      Entry<Long, TimedMessages<Msg>> entry;

      // Size eviction: Handle all messages that exceed the messages to keep
      if (maxNumberOfMessagesToKeep < Integer.MAX_VALUE) {
         // TODO: sortedMap.size() is an expensive operation -> consider
         // using
         // an AtomicInteger as counter (see ConcurrentLongHistogram for a
         // possibility to get around map.compute() does not guarantee atomic
         // execution of creator function)
         entry = this.sortedMap.firstEntry();
         while (entry != null && this.sortedMap.size() > this.maxNumberOfMessagesToKeep) {
            if (handleIncompleteMessages(entry)) {
               entry = this.sortedMap.firstEntry();
            } else {
               // stop
               return;
            }
         }
      }

      // Time eviction: Handle all messages that are older than specified
      // timeout
      if (messageSendTimeoutMillis < Long.MAX_VALUE) {
         entry = this.sortedMap.firstEntry();
         final Entry<Long, TimedMessages<Msg>> lastEntry = this.sortedMap.lastEntry();
         while (entry != null && lastEntry != null
               && lastEntry.getValue().getSubmitTime() - entry.getValue().getSubmitTime() >= messageSendTimeoutMillis) {
            if (handleIncompleteMessages(entry)) {
               entry = this.sortedMap.firstEntry();
               // lastEntry = this.sortedMap.lastEntry();
            } else {
               // stop
               return;
            }
         }
      }

      // handle all complete messages
      entry = this.sortedMap.firstEntry();
      while (entry != null
            && entry.getValue().availableChannels() >= this.getNumberOfExpectedChannels(entry.getKey())) {
         // make sure there is no pulse missing (i.e. there should be a pulse
         // before the currently handled one but we have not yet received a
         // message for this pulse
         if (!this.isPulseIdMissing(entry.getKey())) {
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
               final TimedMessages<Msg> messages = this.sortedMap.remove(entry.getKey());
               // in case there was another Thread that was also checking this
               // pulse and was faster
               if (messages != null) {
                  // LOGGER.debug("Send complete pulse '{}'.", entry.getKey());
                  if (!this.queue.offer(messages.getMessagesMap())) {
                     LOGGER.warn(
                           "Had to drop messages for pulse '{}' because capacity constrained queue seems to be full.",
                           entry.getKey());
                  }

                  this.updateLastSentOrDeletedPulseId(entry.getKey());
                  condition.signalAll();
               }
            } finally {
               lock.unlock();
            }
            entry = this.sortedMap.firstEntry();
         } else {
            LOGGER.debug("Keep pulse '{}' since there are pulses missing.", entry.getKey());
            // stop since there are still elements missing
            entry = null;
         }
      }
   }

   private boolean handleIncompleteMessages(final Entry<Long, TimedMessages<Msg>> entry) {
      final ReentrantLock lock = this.lock;
      lock.lock();
      try {
         // Remove oldest pulse ID - i.e. first
         final TimedMessages<Msg> messages = this.sortedMap.remove(entry.getKey());

         if (messages != null) {
            final long numberOfChannels = this.getNumberOfExpectedChannels(entry.getKey());
            // check if message is complete
            if (entry.getValue().availableChannels() >= numberOfChannels) {
               // we send complete messages by definition
               LOGGER.debug("Send complete pulse '{}' due to eviction.", entry.getKey());
               if (!this.queue.offer(messages.getMessagesMap())) {
                  LOGGER.warn(
                        "Had to drop messages for pulse '{}' because capacity constrained queue seems to be full.",
                        entry.getKey());
               }

               this.updateLastSentOrDeletedPulseId(entry.getKey());
               condition.signalAll();

            } else if (this.sendIncompleteMessages) {
               // the user also wants incomplete messages
               LOGGER.debug("Send incomplete pulse '{}' due to eviction.", entry.getKey());
               if (!this.queue.offer(messages.getMessagesMap())) {
                  LOGGER.warn(
                        "Had to drop messages for pulse '{}' because capacity constrained queue seems to be full.",
                        entry.getKey());
               }

               this.updateLastSentOrDeletedPulseId(entry.getKey());
               condition.signalAll();

            } else {
               LOGGER.info(
                     "Drop messages for pulse '{}' due to size eviction. Requested number of channels '{}' but got only '{}'.",
                     entry.getKey(), numberOfChannels, entry.getValue().getMessagesMap().size());
               this.updateLastSentOrDeletedPulseId(entry.getKey());
            }
            // keep going
            return true;
         } else {
            // LOGGER.debug("Another consumer thread is handling message of pulse '{}'. Let it do
            // the work.", entry.getKey());
            // stop here
            return false;
         }
      } finally {
         lock.unlock();
      }
   }

   @Override
   public void close() {
      if (isRunning.compareAndSet(true, false)) {
         final ReentrantLock lock = this.lock;
         lock.lock();
         try {
            condition.signalAll();
         } finally {
            lock.unlock();
         }
      }
   }

   @Override
   public Map<String, Msg> nextMessage() {
      final ReentrantLock lock = this.lock;
      Map<String, Msg> msgMap = null;
      Entry<Long, TimedMessages<Msg>> entry;

      lock.lock();
      try {
         msgMap = queue.poll();
         while (isRunning.get() && msgMap == null) {
            long parkNanos = -1;
            if (messageSendTimeoutMillis < Long.MAX_VALUE) {
               entry = this.sortedMap.firstEntry();
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

            try {
               // make sure consumer wakes up periodically to check for timed-out messages (in case
               // no new messages arrive)
               if (parkNanos >= 0) {
                  condition.await(parkNanos, TimeUnit.NANOSECONDS);
                  checkForCompleteMessages(System.currentTimeMillis());
               } else {
                  condition.await();
               }

               msgMap = queue.poll();
            } catch (InterruptedException e) {
               LOGGER.error("Interrupted while waiting!", e);
            }
         }
      } finally {
         lock.unlock();
      }

      return msgMap;
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
