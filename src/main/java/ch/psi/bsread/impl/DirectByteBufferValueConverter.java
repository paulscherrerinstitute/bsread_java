package ch.psi.bsread.impl;

import java.nio.ByteBuffer;
import java.util.function.IntFunction;

import ch.psi.bsread.common.allocator.ByteBufferAllocator;
import ch.psi.bsread.common.helper.ByteBufferHelper;
import ch.psi.bsread.converter.ValueConverter;
import ch.psi.bsread.message.ChannelConfig;
import ch.psi.bsread.message.DataHeader;
import ch.psi.bsread.message.MainHeader;
import ch.psi.bsread.message.Timestamp;

public class DirectByteBufferValueConverter implements ValueConverter {
	private IntFunction<ByteBuffer> allocator;
	private long directThreshold;

	public DirectByteBufferValueConverter() {
		this.directThreshold = ByteBufferAllocator.DIRECT_ALLOCATION_THRESHOLD;
		this.allocator = ByteBufferAllocator.DEFAULT_ALLOCATOR;
	}

	public DirectByteBufferValueConverter(long directThreshold) {
		this.directThreshold = directThreshold;
		this.allocator = new ByteBufferAllocator(directThreshold);
	}

	@SuppressWarnings("unchecked")
	@Override
	public ByteBuffer getValue(MainHeader mainHeader, DataHeader dataHeader, ChannelConfig channelConfig, ByteBuffer receivedValueBytes,
			Timestamp iocTimestamp) {
		receivedValueBytes = channelConfig.getCompression().getCompressor().decompressData(receivedValueBytes, receivedValueBytes.position(), allocator, channelConfig.getType().getBytes());
		receivedValueBytes.order(channelConfig.getByteOrder());

		if (receivedValueBytes.remaining() <= directThreshold) {
			return receivedValueBytes;
		} else {
			return ByteBufferHelper.asDirect(receivedValueBytes);
		}
	}
}
