package ch.psi.daq.data.db.converters;

import java.nio.ByteBuffer;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public interface StreamProvider<V> {

	/**
	 * Generates an IntStream from the provided ByteBuffer. This stream has the
	 * advantage that no boxing/unboxing is necessary.
	 * 
	 * @param valueBytes
	 *            The ByteBuffer containing the values
	 * @param endianness
	 *            The ByteOrder
	 * @param parallel
	 *            Defines if the Stream should be parallel
	 * @return IntStream The IntStream
	 * @throws UnsupportedOperationException
	 *             In case IntStream is not supported
	 */
	public IntStream getIntStream(ByteBuffer valueBytes, boolean parallel) throws UnsupportedOperationException;

	/**
	 * Generates an LongStream from the provided ByteBuffer. This stream has the
	 * advantage that no boxing/unboxing is necessary.
	 * 
	 * @param valueBytes
	 *            The ByteBuffer containing the values
	 * @param parallel
	 *            Defines if the Stream should be parallel
	 * @return LongStream The Stream
	 * @throws UnsupportedOperationException
	 *             In case LongStream is not supported
	 */
	public LongStream getLongStream(ByteBuffer valueBytes, boolean parallel) throws UnsupportedOperationException;

	/**
	 * Generates an DoubleStream from the provided ByteBuffer. This stream has
	 * the advantage that no boxing/unboxing is necessary.
	 * 
	 * @param valueBytes
	 *            The ByteBuffer containing the values
	 * @param parallel
	 *            Defines if the Stream should be parallel
	 * @return DoubleStream The Stream
	 * @throws UnsupportedOperationException
	 *             In case DoubleStream is not supported
	 */
	public DoubleStream getDoubleStream(ByteBuffer bytes, boolean parallel) throws UnsupportedOperationException;

	/**
	 * Generates an Stream from the provided ByteBuffer. This stream operates on
	 * wrapper types.
	 * 
	 * @param valueBytes
	 *            The ByteBuffer containing the values
	 * @param parallel
	 *            Defines if the Stream should be parallel
	 * @return Stream The Stream
	 */
	public Stream<V> getStream(ByteBuffer bytes, boolean parallel);
}
