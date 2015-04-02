package ch.psi.daq.data.db.converters;

import java.nio.ByteBuffer;

import ch.psi.daq.data.stream.converter.ByteValueConverter;

public interface ByteValueSpliterer<V> {

	/**
	 * Returns a converter that converts ByteBuffer's content into values.
	 * 
	 * @return ByteValueConverter The ByteValueConverter
	 */
	public ByteValueConverter<V> getByteValueConverter();
	
	/**
	 * Gets the start index for the provided ByteBuffer.
	 * 
	 * @param bytes
	 *            The ByteBuffer
	 * @return int The start Index
	 */
	public int getStartIndex(ByteBuffer bytes);

	/**
	 * Gets the end index for the provided ByteBuffer.
	 * 
	 * @param bytes
	 *            The ByteBuffer
	 * @return int The end Index
	 */
	public int getEndIndex(ByteBuffer bytes);

	/**
	 * Splits the range into two (possibly) equal ranges
	 * 
	 * @param bytes
	 *            The ByteBuffer
	 * @param startIndex
	 *            The start index
	 * @param endIndex
	 *            The end index
	 * @return int The index that splits the range (endIndex-startIndex) in
	 *         (possibly) two equal sized parts
	 */
	public int splitIndex(ByteBuffer bytes, int startIndex, int endIndex);

	/**
	 * Calculates the next index
	 * 
	 * @param bytes
	 *            The ByteBuffer
	 * @param index
	 *            The current index
	 * @return int The next index
	 */
	public int nextIndex(ByteBuffer bytes, int index);

	/**
	 * Estimates the remaining number of elements between start and end index.
	 * 
	 * @param bytes
	 *            The ByteBuffer
	 * @param startIndex
	 *            The start index
	 * @param endIndex
	 *            The end index
	 * @return int The remaining number of elements
	 */
	public int estimateSize(ByteBuffer bytes, int startIndex, int endIndex);
}
