package ch.psi.bsread;

import zmq.MsgAllocator;

import com.fasterxml.jackson.databind.ObjectMapper;

import ch.psi.bsread.command.Command;
import ch.psi.bsread.command.PolymorphicCommandMixIn;
import ch.psi.bsread.impl.StandardMessageExtractor;

public class ReceiverConfig<V> {
	public static final int DEFAULT_HIGH_WATER_MARK = 100;
	public static final int DEFAULT_ALIGNMENT_RETRIES = 20;

	private boolean keepListeningOnStop;
	private boolean parallelProcessing;
	private final int highWaterMark = DEFAULT_HIGH_WATER_MARK;
	private int alignmentRetries = DEFAULT_ALIGNMENT_RETRIES;
	private MessageExtractor<V> messageExtractor;
	private ObjectMapper objectMapper;
	private final MsgAllocator msgAllocator;

	public ReceiverConfig() {
		this(new StandardMessageExtractor<V>());
	}

	public ReceiverConfig(MessageExtractor<V> messageExtractor) {
		this(true, false, messageExtractor);
	}

	public ReceiverConfig(boolean keepListeningOnStop, boolean parallelProcessing, MessageExtractor<V> messageExtractor) {
		this(keepListeningOnStop, parallelProcessing, messageExtractor, null);
	}
	
	public ReceiverConfig(boolean keepListeningOnStop, boolean parallelProcessing, MessageExtractor<V> messageExtractor, MsgAllocator msgAllocator) {
		this.keepListeningOnStop = keepListeningOnStop;
		this.parallelProcessing = parallelProcessing;
		this.messageExtractor = messageExtractor;
		this.msgAllocator = msgAllocator;
		
		this.setObjectMapper(new ObjectMapper());
	}

	public boolean isKeepListeningOnStop() {
		return keepListeningOnStop;
	}

	public void setKeepListeningOnStop(boolean keepListeningOnStop) {
		this.keepListeningOnStop = keepListeningOnStop;
	}

	public boolean isParallelProcessing() {
		return parallelProcessing;
	}

	public void setParallelProcessing(boolean parallelProcessing) {
		this.parallelProcessing = parallelProcessing;
	}

	public int getHighWaterMark() {
		return highWaterMark;
	}

	public int getAlignmentRetries() {
		return alignmentRetries;
	}

	public void setAlignmentRetries(int alignmentRetries) {
		this.alignmentRetries = alignmentRetries;
	}

	public MessageExtractor<V> getMessageExtractor() {
		return messageExtractor;
	}

	public void setMessageExtractor(MessageExtractor<V> messageExtractor) {
		this.messageExtractor = messageExtractor;
	}
	
	public MsgAllocator getMsgAllocator() {
		return msgAllocator;
	}
	
	public ObjectMapper getObjectMapper() {
		return objectMapper;
	}

	public void setObjectMapper(ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
		
		addObjectMapperMixin(objectMapper);
	}
	
	public static void addObjectMapperMixin(ObjectMapper objectMapper){
		objectMapper.addMixIn(Command.class, PolymorphicCommandMixIn.class);
	}
}
