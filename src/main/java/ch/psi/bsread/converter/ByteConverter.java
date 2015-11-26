package ch.psi.bsread.converter;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.IntFunction;

import ch.psi.bsread.message.Type;

public interface ByteConverter extends ValueConverter {

	/**
	 * Converts a value into its byte representation.
	 * 
	 * @param value
	 *            The value
	 * @param type
	 *            The type of the value (needed for unsigned types since they
	 *            are not part of JAVA)
	 * @param byteOrder
	 *            The byte order
	 * @param valueAllocator
	 *            The ByteBuffer allocator function (e.g. allocating a
	 *            DirectByteBuffer or reuses ByteBuffers) for the value bytes
	 * @return The byte representation
	 */
	public ByteBuffer getBytes(Object value, Type type, ByteOrder byteOrder, IntFunction<ByteBuffer> valueAllocator);
}
