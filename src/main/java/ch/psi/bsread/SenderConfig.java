package ch.psi.bsread;

import java.nio.ByteBuffer;
import java.util.function.IntFunction;
import java.util.function.Supplier;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;

import com.fasterxml.jackson.databind.ObjectMapper;

import ch.psi.bsread.common.allocator.ByteBufferAllocator;
import ch.psi.bsread.compression.Compression;
import ch.psi.bsread.converter.ByteConverter;
import ch.psi.bsread.converter.MatlabByteConverter;
import ch.psi.bsread.impl.StandardPulseIdProvider;
import ch.psi.bsread.impl.StandardTimeProvider;
import ch.psi.bsread.message.MainHeader;
import ch.psi.bsread.monitors.Monitor;

public class SenderConfig {
	public static final String DEFAULT_SENDING_ADDRESS = "tcp://*:9999";
	public static final int DEFAULT_HIGH_WATER_MARK = 100;

	private Context context;
	private final Compression dataHeaderCompression;
	private final IntFunction<ByteBuffer> valueAllocator;
	private final IntFunction<ByteBuffer> compressedValueAllocator;
	// only for testing purposes
	private Supplier<MainHeader> mainHeaderSupplier;

	private final PulseIdProvider pulseIdProvider;
	private final TimeProvider globalTimeProvider;
	private final ByteConverter byteConverter;

	private ObjectMapper objectMapper;
	private int socketType = ZMQ.PUSH;
	private String address = DEFAULT_SENDING_ADDRESS;
	private int highWaterMark = DEFAULT_HIGH_WATER_MARK;
	private Monitor monitor;

	public SenderConfig() {
		this(DEFAULT_SENDING_ADDRESS);
	}

	public SenderConfig(String address) {
		this(address, new StandardPulseIdProvider(), new StandardTimeProvider(), new MatlabByteConverter());
	}

	public SenderConfig(String address, PulseIdProvider pulseIdProvider, TimeProvider globalTimeProvider, ByteConverter byteConverter) {
		this(address, pulseIdProvider, globalTimeProvider, byteConverter, Compression.none);
	}

	public SenderConfig(String address, PulseIdProvider pulseIdProvider, TimeProvider globalTimeProvider, ByteConverter byteConverter,
			Compression dataHeaderCompression) {
		this(address, pulseIdProvider, globalTimeProvider, byteConverter, dataHeaderCompression, ByteBufferAllocator.DEFAULT_ALLOCATOR, ByteBufferAllocator.DEFAULT_ALLOCATOR);
	}

	public SenderConfig(String address, PulseIdProvider pulseIdProvider, TimeProvider globalTimeProvider, ByteConverter byteConverter,
			Compression dataHeaderCompression, IntFunction<ByteBuffer> valueAllocator,
			IntFunction<ByteBuffer> compressedValueAllocator) {
		this.address = address;
		this.pulseIdProvider = pulseIdProvider;
		this.globalTimeProvider = globalTimeProvider;
		this.byteConverter = byteConverter;
		this.dataHeaderCompression = dataHeaderCompression;
		this.valueAllocator = valueAllocator;
		this.compressedValueAllocator = compressedValueAllocator;

		this.setObjectMapper(new ObjectMapper());
	}
	
	public Context getContext() {
		if (this.context != null) {
			return context;
		} else {
			return ReceiverConfig.DEFERRED_CONTEXT.get();
		}
	}

	public void setContext(Context context) {
		this.context = context;
	}

	public int getHighWaterMark() {
		return highWaterMark;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public void setHighWaterMark(int highWaterMark) {
		this.highWaterMark = highWaterMark;
	}

	public Compression getDataHeaderCompression() {
		return dataHeaderCompression;
	}

	public IntFunction<ByteBuffer> getValueAllocator() {
		return valueAllocator;
	}

	public IntFunction<ByteBuffer> getCompressedValueAllocator() {
		return compressedValueAllocator;
	}

	public PulseIdProvider getPulseIdProvider() {
		return pulseIdProvider;
	}

	public TimeProvider getGlobalTimeProvider() {
		return globalTimeProvider;
	}

	public ByteConverter getByteConverter() {
		return byteConverter;
	}

	public ObjectMapper getObjectMapper() {
		return objectMapper;
	}

	public void setObjectMapper(ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
	}

	public int getSocketType() {
		return socketType;
	}

	public void setSocketType(int socketType) {
		this.socketType = socketType;
	}

	public Monitor getMonitor() {
		return monitor;
	}

	public void setMonitor(Monitor monitor) {
		this.monitor = monitor;
	}

	public void setMainHeaderSupplier(Supplier<MainHeader> mainHeaderSupplier) {
		this.mainHeaderSupplier = mainHeaderSupplier;
	}

	public Supplier<MainHeader> getMainHeaderSupplier() {
		return mainHeaderSupplier;
	}
}
