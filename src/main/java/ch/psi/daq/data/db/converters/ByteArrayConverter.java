package ch.psi.daq.data.db.converters;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Converts bytes into a specific value type (double, integer, long etc.).
 */
public interface ByteArrayConverter<T, U, V> extends ByteConverter<T, U, V> {

	/**
	 * Reads value(s) from a byte representation and stores them in the provided
	 * array.
	 * 
	 * @param bytes
	 *            The bytes
	 * @param endianness
	 *            The endianness
	 * @param arrayVal
	 *            The array to save the values
	 * @return int The number of elements written into arrayVal
	 */
	public int convert(ByteBuffer bytes, ByteOrder endianness, T arrayVal);

	/**
	 * Calculates the number of elements in the byte representation.
	 * 
	 * @param bytes
	 *            The byte representation.
	 * @param endianness
	 *            The endianness
	 * @return int The number of elements
	 * @throws UnsupportedOperationException
	 *             If this method is not supported
	 */
	public int getArraySize(ByteBuffer bytes, ByteOrder endianness) throws UnsupportedOperationException;
}
