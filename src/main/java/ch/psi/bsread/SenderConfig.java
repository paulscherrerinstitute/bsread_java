package ch.psi.bsread;

import java.nio.ByteBuffer;
import java.util.function.IntFunction;

import org.zeromq.ZMQ;

import com.fasterxml.jackson.databind.ObjectMapper;

import ch.psi.bsread.compression.Compression;
import ch.psi.bsread.converter.ByteConverter;
import ch.psi.bsread.converter.MatlabByteConverter;
import ch.psi.bsread.copy.common.allocator.ByteBufferAllocator;
import ch.psi.bsread.impl.StandardPulseIdProvider;
import ch.psi.bsread.impl.StandardTimeProvider;
import ch.psi.bsread.monitors.Monitor;

public class SenderConfig {
	private final Compression dataHeaderCompression;
	private final IntFunction<ByteBuffer> valueAllocator;
	private final IntFunction<ByteBuffer> compressedValueAllocator;

	private final PulseIdProvider pulseIdProvider;
	private final TimeProvider globalTimeProvider;
	private final ByteConverter byteConverter;

	private ObjectMapper objectMapper;
	private int socketType = ZMQ.PUSH;
	private Monitor monitor;

	public SenderConfig() {
		this(new StandardPulseIdProvider(), new StandardTimeProvider(), new MatlabByteConverter());
	}

	public SenderConfig(PulseIdProvider pulseIdProvider, TimeProvider globalTimeProvider, ByteConverter byteConverter) {
		this(pulseIdProvider, globalTimeProvider, byteConverter, Compression.none);
	}

	public SenderConfig(PulseIdProvider pulseIdProvider, TimeProvider globalTimeProvider, ByteConverter byteConverter,
			Compression dataHeaderCompression) {
		this(pulseIdProvider, globalTimeProvider, byteConverter, dataHeaderCompression, new ByteBufferAllocator(),
				new ByteBufferAllocator());
	}

	public SenderConfig(PulseIdProvider pulseIdProvider, TimeProvider globalTimeProvider, ByteConverter byteConverter,
			Compression dataHeaderCompression, IntFunction<ByteBuffer> valueAllocator,
			IntFunction<ByteBuffer> compressedValueAllocator) {
		this.pulseIdProvider = pulseIdProvider;
		this.globalTimeProvider = globalTimeProvider;
		this.byteConverter = byteConverter;
		this.dataHeaderCompression = dataHeaderCompression;
		this.valueAllocator = valueAllocator;
		this.compressedValueAllocator = compressedValueAllocator;

		this.setObjectMapper(new ObjectMapper());
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
}
