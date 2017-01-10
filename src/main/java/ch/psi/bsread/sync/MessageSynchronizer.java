package ch.psi.bsread.sync;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.LongUnaryOperator;
import java.util.function.ToLongFunction;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MessageBuffer based on a max. allowed size. Accordingly, the time limit
 * messages are kept is given by the frequency and the buffer size (assuming
 * there are constantly messages arriving)
 */
public class MessageSynchronizer<Msg> {
	private static final Logger LOGGER = LoggerFactory.getLogger(MessageSynchronizer.class.getName());
	private static final long INITIAL_LAST_SENT_OR_DELETE_PULSEID = Long.MIN_VALUE;
	public static final LongUnaryOperator CURRENT_TIME_SUPPLIER = (pulseId) -> System.currentTimeMillis();

	private final int maxNumberOfMessagesToKeep;
	private final long messageSendTimeoutMillis;
	private final boolean sendIncompleteMessages;

	private final Map<String, Pair<Long, Long>> channelConfigs;
	private final AtomicLong smallestEverReceivedPulseId = new AtomicLong(Long.MAX_VALUE);
	private final AtomicLong lastSentOrDeletedPulseId = new AtomicLong(INITIAL_LAST_SENT_OR_DELETE_PULSEID);
	private final Queue<Map<String, Msg>> completeQueue;
	// map[ pulseId -> map[channel -> value] ]
	private final ConcurrentSkipListMap<Long, TimedMessages<Msg>> sortedMap = new ConcurrentSkipListMap<>();
	private final Function<Msg, String> channelNameProvider;
	private final ToLongFunction<Msg> pulseIdProvider;
	private final LongUnaryOperator currentTimeProvider;
	private final boolean sendFirstComplete;

	public MessageSynchronizer(Queue<Map<String, Msg>> completeQueue, int maxNumberOfMessagesToKeep,
			boolean sendIncompleteMessages, boolean sendFirstComplete, Collection<? extends SyncChannel> channels,
			Function<Msg, String> channelNameProvider, ToLongFunction<Msg> pulseIdProvider) {
		this(completeQueue, maxNumberOfMessagesToKeep, Long.MAX_VALUE, sendIncompleteMessages, sendFirstComplete, channels, channelNameProvider, pulseIdProvider, (pulseId) -> 0);
	}

	public MessageSynchronizer(Queue<Map<String, Msg>> completeQueue, long messageSendTimeoutMillis,
			boolean sendIncompleteMessages, boolean sendFirstComplete, Collection<? extends SyncChannel> channels,
			Function<Msg, String> channelNameProvider, ToLongFunction<Msg> pulseIdProvider) {
		this(completeQueue, Integer.MAX_VALUE, messageSendTimeoutMillis, sendIncompleteMessages, sendFirstComplete, channels, channelNameProvider, pulseIdProvider, CURRENT_TIME_SUPPLIER);
	}

	public MessageSynchronizer(Queue<Map<String, Msg>> completeQueue, long messageSendTimeoutMillis,
			boolean sendIncompleteMessages, boolean sendFirstComplete, Collection<? extends SyncChannel> channels,
			Function<Msg, String> channelNameProvider, ToLongFunction<Msg> pulseIdProvider, LongUnaryOperator currentTimeProvider) {
		this(completeQueue, Integer.MAX_VALUE, messageSendTimeoutMillis, sendIncompleteMessages, sendFirstComplete, channels, channelNameProvider, pulseIdProvider, currentTimeProvider);
	}

	public MessageSynchronizer(Queue<Map<String, Msg>> completeQueue, int maxNumberOfMessagesToKeep, long messageSendTimeoutMillis,
			boolean sendIncompleteMessages, boolean sendFirstComplete, Collection<? extends SyncChannel> channels,
			Function<Msg, String> channelNameProvider, ToLongFunction<Msg> pulseIdProvider, LongUnaryOperator currentTimeProvider) {
		this.completeQueue = completeQueue;
		this.maxNumberOfMessagesToKeep = maxNumberOfMessagesToKeep;
		this.messageSendTimeoutMillis = messageSendTimeoutMillis;
		this.sendIncompleteMessages = sendIncompleteMessages;
		this.channelNameProvider = channelNameProvider;
		this.pulseIdProvider = pulseIdProvider;
		this.currentTimeProvider = currentTimeProvider;
		this.sendFirstComplete = sendFirstComplete;

		this.channelConfigs = new HashMap<>(channels.size());
		for (SyncChannel channel : channels) {
			this.channelConfigs.put(channel.getName(), Pair.of((long) channel.getModulo(), (long) channel.getOffset()));
		}
	}

	public void addMessage(Msg msg) {
		final long pulseId = pulseIdProvider.applyAsLong(msg);
		final String channelName = channelNameProvider.apply(msg);
		this.updateSmallestEverReceivedPulseId(pulseId);
		final long lastPulseId = lastSentOrDeletedPulseId.get();
		final long currentTime = currentTimeProvider.applyAsLong(pulseId);

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
			while (entry != null && lastEntry != null && lastEntry.getValue().getSubmitTime() - entry.getValue().getSubmitTime() >= messageSendTimeoutMillis) {
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
		while (entry != null && entry.getValue().getMessagesMap().size() >= this.getNumberOfExpectedChannels(entry.getKey())) {
			// make sure there is no pulse missing (i.e. there should be a pulse
			// before the currently handled one but we have not yet received a
			// message for this pulse
			final Long pulseId = entry.getKey();
			if (!this.isPulseIdMissing(pulseId)) {
				final TimedMessages<Msg> messages = this.sortedMap.remove(pulseId);
				// in case there was another Thread that was also checking this
				// pulse and was faster
				if (messages != null) {
					LOGGER.debug("Send complete pulse '{}'.", pulseId);
					this.handleCompleteMessages(entry.getKey(), entry.getValue().getMessagesMap());
				}
				entry = this.sortedMap.firstEntry();
			} else {
				LOGGER.debug("Keep pulse '{}' since there are pulses missing.", pulseId);
				// stop since there are still elements missing
				entry = null;
			}
		}
	}

	private boolean handleIncompleteMessages(final Entry<Long, TimedMessages<Msg>> entry) {
		// Remove oldest pulse ID - i.e. first
		final TimedMessages<Msg> messages = this.sortedMap.remove(entry.getKey());

		if (messages != null) {
			final long numberOfChannels = this.getNumberOfExpectedChannels(entry.getKey());
			// check if message is complete
			if (entry.getValue().getMessagesMap().size() >= numberOfChannels) {
				// we send complete messages by definition
				LOGGER.debug("Send complete pulse '{}' due to size eviction.", entry.getKey());
				this.handleCompleteMessages(entry.getKey(), entry.getValue().getMessagesMap());

			} else if (this.sendIncompleteMessages) {
				// the user also wants incomplete messages
				LOGGER.debug("Send incomplete pulse '{}' due to size eviction.", entry.getKey());
				this.handleCompleteMessages(entry.getKey(), entry.getValue().getMessagesMap());
			} else {
				LOGGER.info("Drop messages for pulse '{}' due to size eviction. Requested number of channels '{}' but got only '{}'.",
						entry.getKey(), numberOfChannels, entry.getValue().getMessagesMap().size());
				this.updateLastSentOrDeletedPulseId(entry.getKey());
			}
			// keep going
			return true;
		} else {
			LOGGER.debug("Another thread is handling message of pulse '{}'. Let it do the work.", entry.getKey());
			// stop here
			return false;
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
	 * Get size of the current pulseId buffer. This function is mainly for
	 * testing purposes.
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
			messagesMap = new ConcurrentHashMap<>(nrOfChannels, 0.75f, 1);
		}

		public long getSubmitTime() {
			return submitTime;
		}

		public Map<String, Msg> getMessagesMap() {
			return messagesMap;
		}
	}
}