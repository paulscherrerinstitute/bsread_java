package ch.psi.bsread;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.zeromq.ZMQ;

import zmq.MsgAllocator;

import com.fasterxml.jackson.databind.ObjectMapper;

import ch.psi.bsread.command.Command;
import ch.psi.bsread.command.PolymorphicCommandMixIn;
import ch.psi.bsread.copy.common.singleton.Deferred;
import ch.psi.bsread.impl.StandardMessageExtractor;

public class ReceiverConfig<V> {
	public static final int DEFAULT_HIGH_WATER_MARK = 100;
	public static final int DEFAULT_ALIGNMENT_RETRIES = 20;

	private static final Deferred<ExecutorService> DEFAULT_VALUE_CONVERSION_SERVICE = new Deferred<>(
			() -> Executors.newFixedThreadPool(2 * Runtime.getRuntime().availableProcessors()));

	private boolean keepListeningOnStop;
	private boolean parallelHandlerProcessing;
	private final int highWaterMark = DEFAULT_HIGH_WATER_MARK;
	private int alignmentRetries = DEFAULT_ALIGNMENT_RETRIES;
	private MessageExtractor<V> messageExtractor;
	private ObjectMapper objectMapper;
	private final MsgAllocator msgAllocator;
	private int socketType = ZMQ.PULL;
	private String address;
	private ExecutorService valueConversionService;

	public ReceiverConfig() {
		this(new StandardMessageExtractor<V>());
	}

	public ReceiverConfig(MessageExtractor<V> messageExtractor) {
		this(true, false, messageExtractor);
	}

	public ReceiverConfig(boolean keepListeningOnStop, boolean parallelHandlerProcessing, MessageExtractor<V> messageExtractor) {
		this(keepListeningOnStop, parallelHandlerProcessing, messageExtractor, null);
	}

	public ReceiverConfig(boolean keepListeningOnStop, boolean parallelHandlerProcessing, MessageExtractor<V> messageExtractor, MsgAllocator msgAllocator) {
		this.keepListeningOnStop = keepListeningOnStop;
		this.parallelHandlerProcessing = parallelHandlerProcessing;
		this.msgAllocator = msgAllocator;

		this.setMessageExtractor(messageExtractor);
		this.setObjectMapper(new ObjectMapper());
	}

	public boolean isKeepListeningOnStop() {
		return keepListeningOnStop;
	}

	public void setKeepListeningOnStop(boolean keepListeningOnStop) {
		this.keepListeningOnStop = keepListeningOnStop;
	}

	public boolean isParallelHandlerProcessing() {
		return parallelHandlerProcessing;
	}

	public void setParallelHandlerProcessing(boolean parallelHandlerProcessing) {
		this.parallelHandlerProcessing = parallelHandlerProcessing;
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
		messageExtractor.setReceiverConfig(this);
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

	public int getSocketType() {
		return socketType;
	}

	public void setSocketType(int socketType) {
		this.socketType = socketType;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public String getAddress() {
		return address;
	}

	public ExecutorService getValueConversionService() {
		if (valueConversionService == null) {
			valueConversionService = DEFAULT_VALUE_CONVERSION_SERVICE.get();
		}
		return valueConversionService;
	}

	public void setValueConversionService(ExecutorService valueConversionService) {
		this.valueConversionService = valueConversionService;
	}

	public static void addObjectMapperMixin(ObjectMapper objectMapper) {
		objectMapper.addMixIn(Command.class, PolymorphicCommandMixIn.class);
	}
}
