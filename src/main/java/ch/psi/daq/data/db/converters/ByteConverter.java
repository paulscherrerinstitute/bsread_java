package ch.psi.daq.data.db.converters;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Converts bytes into a specific value type (double, integer, long etc.).
 */
public interface ByteConverter<T, U, V> extends ByteValueSpliterer<V>, StreamProvider<V>{
	public static final int DYNAMIC_NUMBER_OF_BYTES = Integer.MAX_VALUE;

	/**
	 * Returns the number of bytes used to represent one element. This method
	 * can return a value smaller than one to indicate that there is no fixed
	 * number of bytes to represent one value.
	 * 
	 * @return int The number of bytes
	 */
	public int getBytes();

	/**
	 * Provides the JAVA class this converter generates.
	 * 
	 * @return Class<T> The JAVA class
	 */
	public Class<T> getValueClass();

	/**
	 * Provides the JAVA class this converter uses to generate signed values.
	 * 
	 * @return Class<T> The JAVA class
	 */
	public Class<U> getSignedValueClass();

	/**
	 * Reads value(s) from a byte representation.
	 * 
	 * @param bytes
	 *            The bytes of the value
	 * @param endianness
	 *            The endianness
	 * @return The converted value
	 */
	public T convert(ByteBuffer bytes, ByteOrder endianness);

	/**
	 * Converts the value into a signed JAVA representation. For signed values,
	 * this method returns the same value as the method convert().<br>
	 * Java does not know unsigned values. The trick is to represent unsigned
	 * values as signed values where the negative range is used for the unsigned
	 * range that cannot be represented in java (this has also the advantage
	 * that exactly the same amount of bits are needed). When unsigned values
	 * should be displayed correctly in JAVA (and not it's negative
	 * counterpart), the values need to be converted into its next bigger
	 * representation (ushort to int, uint to long, ulong to BigInteger).
	 * 
	 * @param bytes
	 *            The bytes of the value
	 * @param endianness
	 *            The endianness
	 * @return The unsigned value
	 */
	public U convertAsSigned(ByteBuffer bytes, ByteOrder endianness);

	/**
	 * Converts a value(s) into a byte representation.
	 *
	 * @param value
	 *            The value(s)
	 * @param endianness
	 *            The endianness
	 * @return The byte representation
	 */
	public ByteBuffer convertObject(Object value, ByteOrder endianness);

	/**
	 * Converts a value(s) into a byte representation.
	 *
	 * @param value
	 *            The value(s)
	 * @param endianness
	 *            The endianness
	 * @return The byte representation
	 */
	public ByteBuffer convert(T value, ByteOrder endianness);

	/**
	 * Converts a value(s) into a byte representation.
	 *
	 * @param value
	 *            The value(s)
	 * @param endianness
	 *            The endianness
	 * @return The byte representation
	 */
	public ByteBuffer convertAsSigned(U value, ByteOrder endianness);
}
