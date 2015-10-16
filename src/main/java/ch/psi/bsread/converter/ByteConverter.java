package ch.psi.bsread.converter;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public interface ByteConverter extends ValueConverter {

	/**
	 * Converts a value into its byte representation.
	 * 
	 * @param type
	 *            The type of the value (needed for unsigned types since they
	 *            are not part of JAVA)
	 * @param value
	 *            The value
	 * @param byteOrder
	 *            The byte order
	 * @return The byte representation
	 */
	public ByteBuffer getBytes(String type, Object value, ByteOrder byteOrder);
	
	/**
	 * Converts a value into its byte representation.
	 * 
	 * @param value
	 *            The value
	 * @param byteOrder
	 *            The byte order
	 * @return The byte representation
	 */
	public ByteBuffer getBytes(Object value, ByteOrder byteOrder);
}
