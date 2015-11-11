package ch.psi.bsread.converter;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.IntFunction;

import ch.psi.bsread.message.ChannelConfig;

public interface ByteConverter extends ValueConverter {

	/**
	 * Converts a value into its byte representation.
	 * 
	 * @param value
	 *            The value
	 * @param config
	 *            The ChannelConfig
	 * @param allocator
	 *            The ByteBuffer allocator function (e.g. allocating a
	 *            DirectByteBuffer or reuses ByteBuffers)
	 * @return The byte representation
	 */
	public ByteBuffer getBytes(Object value, ChannelConfig config, IntFunction<ByteBuffer> allocator);
}
