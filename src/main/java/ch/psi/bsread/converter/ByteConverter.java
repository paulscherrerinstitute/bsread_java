package ch.psi.bsread.converter;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public interface ByteConverter {

	/**
	 * Converts a byte representation of a value into the actual value.
	 * 
	 * @param byteValue
	 *            The byte representation of a value
	 * @param type
	 *            The type of the value
	 * @param shape
	 *            The shape of the value
	 * @return The converted value
	 */
	public <T> T getValue(ByteBuffer byteValue, String type, int[] shape);

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
	public <T> ByteBuffer getBytes(String type, T value, ByteOrder byteOrder);
	
	/**
	 * Converts a value into its byte representation.
	 * 
	 * @param value
	 *            The value
	 * @param byteOrder
	 *            The byte order
	 * @return The byte representation
	 */
	public <T> ByteBuffer getBytes(T value, ByteOrder byteOrder);
}
