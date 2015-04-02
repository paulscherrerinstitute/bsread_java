package ch.psi.daq.data.db.converters;

import java.nio.ByteBuffer;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import ch.psi.daq.data.stream.DoubleByteBufferSpliterator;
import ch.psi.daq.data.stream.GenericByteBufferSpliterator;
import ch.psi.daq.data.stream.IntByteBufferSpliterator;
import ch.psi.daq.data.stream.LongByteBufferSpliterator;

public abstract class AbstractByteConverter<T, U, V> implements ByteConverter<T, U, V> {

	@Override
	public IntStream getIntStream(ByteBuffer valueBytes, boolean parallel) throws UnsupportedOperationException {
		return StreamSupport.intStream(new IntByteBufferSpliterator<V>(valueBytes, this), parallel);
	}

	@Override
	public LongStream getLongStream(ByteBuffer valueBytes, boolean parallel) throws UnsupportedOperationException {
		return StreamSupport.longStream(new LongByteBufferSpliterator<V>(valueBytes, this), parallel);
	}

	@Override
	public DoubleStream getDoubleStream(ByteBuffer valueBytes, boolean parallel) throws UnsupportedOperationException {
		return StreamSupport.doubleStream(new DoubleByteBufferSpliterator<V>(valueBytes, this), parallel);
	}

	@Override
	public Stream<V> getStream(ByteBuffer valueBytes, boolean parallel) {
		return StreamSupport.stream(new GenericByteBufferSpliterator<V>(valueBytes, this), parallel);
	}
	
	public int getStartIndex(ByteBuffer bytes) {
		return bytes.position();
	}

	public int getEndIndex(ByteBuffer bytes) {
		return bytes.limit();
	}

	public int splitIndex(ByteBuffer bytes, int startIndex, int endIndex) {
		return startIndex + (((endIndex - startIndex) / (2 * this.getBytes())) * this.getBytes());
	}

	public int nextIndex(ByteBuffer bytes, int index) {
		return index + this.getBytes();
	}

	public int estimateSize(ByteBuffer bytes, int startIndex, int endIndex) {
		return (endIndex - startIndex) / this.getBytes();
	}
}
