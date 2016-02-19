package ch.psi.bsread.impl;

import java.nio.ByteBuffer;
import java.util.function.IntFunction;

import ch.psi.bsread.converter.ValueConverter;
import ch.psi.bsread.copy.common.allocator.ByteBufferAllocator;
import ch.psi.bsread.copy.common.helper.ByteBufferHelper;
import ch.psi.bsread.message.ChannelConfig;
import ch.psi.bsread.message.MainHeader;
import ch.psi.bsread.message.Timestamp;

public class DirectByteBufferValueConverter implements ValueConverter {
	private IntFunction<ByteBuffer> allocator;
	private int directThreshold;

	public DirectByteBufferValueConverter() {
		this(Integer.MAX_VALUE);
	}

	public DirectByteBufferValueConverter(int directThreshold) {
		this.directThreshold = directThreshold;
		this.allocator = new ByteBufferAllocator(directThreshold);
	}

	@SuppressWarnings("unchecked")
	@Override
	public ByteBuffer getValue(ByteBuffer receivedValueBytes, ChannelConfig config, MainHeader mainHeader,
	         Timestamp iocTimestamp) {
		receivedValueBytes = config.getCompression().getCompressor().decompressData(receivedValueBytes, receivedValueBytes.position(), allocator, config.getType().getBytes());
		receivedValueBytes.order(config.getByteOrder());

		if (receivedValueBytes.remaining() <= directThreshold) {
			return receivedValueBytes;
		} else {
			return ByteBufferHelper.asDirect(receivedValueBytes);
		}
	}
}
