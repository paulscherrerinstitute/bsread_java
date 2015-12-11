package ch.psi.bsread;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import ch.psi.bsread.allocator.ByteBufferAllocator;
import ch.psi.bsread.command.Command;
import ch.psi.bsread.compression.Compression;
import ch.psi.bsread.converter.ByteConverter;
import ch.psi.bsread.converter.MatlabByteConverter;
import ch.psi.bsread.helper.ByteBufferHelper;
import ch.psi.bsread.impl.StandardPulseIdProvider;
import ch.psi.bsread.impl.StandardTimeProvider;
import ch.psi.bsread.message.DataHeader;
import ch.psi.bsread.message.MainHeader;
import ch.psi.bsread.message.Timestamp;
import ch.psi.bsread.message.Type;

public class Sender {
	private static final Logger LOGGER = LoggerFactory.getLogger(Sender.class.getName());

	public static final String DEFAULT_SENDING_ADDRESS = "tcp://*:9999";
	public static final int HIGH_WATER_MARK = 100;

	private Context context;
	private Socket socket;

	private ObjectMapper mapper = new ObjectMapper();

	private MainHeader mainHeader = new MainHeader();
	private byte[] dataHeaderBytes;
	private String dataHeaderMD5 = "";
	private final Compression dataHeaderCompression;
	private final IntFunction<ByteBuffer> valueAllocator;
	private final IntFunction<ByteBuffer> compressedValueAllocator;

	private final PulseIdProvider pulseIdProvider;
	private final TimeProvider globalTimeProvider;
	private final ByteConverter byteConverter;

	private List<DataChannel<?>> channels = new ArrayList<>();

	public Sender() {
		this(new StandardPulseIdProvider(), new StandardTimeProvider(), new MatlabByteConverter());
	}

	public Sender(PulseIdProvider pulseIdProvider, TimeProvider globalTimeProvider, ByteConverter byteConverter) {
		this(pulseIdProvider, globalTimeProvider, byteConverter, Compression.none);
	}

	public Sender(PulseIdProvider pulseIdProvider, TimeProvider globalTimeProvider, ByteConverter byteConverter,
			Compression dataHeaderCompression) {
		this(pulseIdProvider, globalTimeProvider, byteConverter, dataHeaderCompression, new ByteBufferAllocator(),
				new ByteBufferAllocator());
	}

	public Sender(PulseIdProvider pulseIdProvider, TimeProvider globalTimeProvider, ByteConverter byteConverter,
			Compression dataHeaderCompression, IntFunction<ByteBuffer> valueAllocator,
			IntFunction<ByteBuffer> compressedValueAllocator) {
		this.pulseIdProvider = pulseIdProvider;
		this.globalTimeProvider = globalTimeProvider;
		this.byteConverter = byteConverter;
		this.dataHeaderCompression = dataHeaderCompression;
		this.valueAllocator = valueAllocator;
		this.compressedValueAllocator = compressedValueAllocator;
	}

	public void bind() {
		bind(DEFAULT_SENDING_ADDRESS);
	}

	public void bind(String address) {
		this.bind(address, HIGH_WATER_MARK);
	}

	public void bind(String address, int highWaterMark) {
		this.context = ZMQ.context(1);
		this.socket = this.context.socket(ZMQ.PUSH);
		this.socket.setSndHWM(highWaterMark);
		this.socket.bind(address);
	}

	public void close() {
		socket.close();
		context.close();
	}

	public void send() {
		long pulseId = pulseIdProvider.getNextPulseId();
		boolean isSendNeeded = false;
		DataChannel<?> channel;
		ByteOrder byteOrder;
		// check if it is realy necessary to send something (e.g. if there is
		// only only a 10Hz it should send only every 10th call)
		for (int i = 0; i < channels.size() && !isSendNeeded; ++i) {
			isSendNeeded = isSendNeeded(pulseId, channels.get(i));
		}

		if (isSendNeeded) {
			mainHeader.setPulseId(pulseId);
			mainHeader.setGlobalTimestamp(this.globalTimeProvider.getTime(pulseId));
			mainHeader.setHash(dataHeaderMD5);
			mainHeader.setDataHeaderCompression(dataHeaderCompression);

			try {
				// Send header
				socket.send(mapper.writeValueAsBytes(mainHeader), ZMQ.NOBLOCK | ZMQ.SNDMORE);

				// Send data header
				socket.send(dataHeaderBytes, ZMQ.NOBLOCK | ZMQ.SNDMORE);

				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug(
							"Send message for pulse '{}' and channels '{}'.",
							mainHeader.getPulseId(),
							channels.stream().map(dataChannel -> dataChannel.getConfig().getName())
									.collect(Collectors.joining(", ")));
				}

				// Send data
				int lastSendMore;
				for (int i = 0; i < channels.size(); ++i) {
					channel = channels.get(i);
					byteOrder = channel.getConfig().getByteOrder();
					lastSendMore = ((i + 1) < channels.size() ? ZMQ.NOBLOCK | ZMQ.SNDMORE : ZMQ.NOBLOCK);
					isSendNeeded = isSendNeeded(pulseId, channel);

					if (isSendNeeded) {
						final Object value = channel.getValue(pulseId);

						// TODO: conversion could be done in parallel as a
						// pre-step (Important: change allocators to non-reusing
						// types)
						ByteBuffer valueBuffer =
								this.byteConverter.getBytes(value, channel.getConfig().getType(), byteOrder, valueAllocator);
						valueBuffer =
								channel
										.getConfig()
										.getCompression()
										.getCompressor()
										.compressData(valueBuffer, valueBuffer.position(), valueBuffer.remaining(), 0,
												compressedValueAllocator, channel.getConfig().getType().getBytes());
						socket.sendByteBuffer(valueBuffer, ZMQ.NOBLOCK | ZMQ.SNDMORE);

						Timestamp timestamp = channel.getTime(pulseId);
						// c-implementation uses a unsigned long (Json::UInt64,
						// uint64_t) for time -> decided to ignore this here
						ByteBuffer timeBuffer =
								this.byteConverter.getBytes(timestamp.getAsLongArray(), Type.Int64, byteOrder, valueAllocator);
						socket.sendByteBuffer(timeBuffer, lastSendMore);
					}
					else {
						// Send placeholder
						socket.send((byte[]) null, ZMQ.NOBLOCK | ZMQ.SNDMORE);
						socket.send((byte[]) null, lastSendMore);
					}
				}
			} catch (JsonProcessingException e) {
				throw new IllegalStateException("Unable to serialize message", e);
			}
		}
	}

	private boolean isSendNeeded(long pulseId, DataChannel<?> channel) {
		// Check if this channel sends data for given pulseId
		return ((pulseId - channel.getConfig().getOffset()) % channel.getConfig().getModulo()) == 0;
	}

	/**
	 * (Re)Generate the data header based on the configured data channels
	 */
	private void generateDataHeader() {
		DataHeader dataHeader = new DataHeader();

		for (DataChannel<?> channel : channels) {
			dataHeader.getChannels().add(channel.getConfig());
		}

		try {
			dataHeaderBytes = mapper.writeValueAsBytes(dataHeader);
			if (!Compression.none.equals(dataHeaderCompression)) {
				ByteBuffer tmpBuf =
						dataHeaderCompression.getCompressor().compressDataHeader(ByteBuffer.wrap(dataHeaderBytes),
								compressedValueAllocator);
				dataHeaderBytes = ByteBufferHelper.copyToByteArray(tmpBuf);
			}
			// decided to compute hash from the bytes that are send to Receivers
			// (allows to check consistency without uncompressing the bytes at
			// receivers side)
			dataHeaderMD5 = Utils.computeMD5(dataHeaderBytes);
		} catch (JsonProcessingException e) {
			throw new RuntimeException("Unable to generate data header", e);
		}
	}

	public void sendCommand(Command command) {
		try {
			socket.send(mapper.writeValueAsBytes(command), ZMQ.NOBLOCK);
		} catch (JsonProcessingException e) {
			String message = "Could not send command.";
			LOGGER.error(message, e);
			throw new RuntimeException(message, e);
		}
	}

	public void addSource(DataChannel<?> channel) {
		channels.add(channel);
		generateDataHeader();
	}

	public void removeSource(DataChannel<?> channel) {
		channels.remove(channel);
		generateDataHeader();
	}

	/**
	 * Returns the currently configured data channels as an unmodifiable list
	 * 
	 * @return Unmodifiable list of data channels
	 */
	public List<DataChannel<?>> getChannels() {
		return Collections.unmodifiableList(channels);
	}
}
