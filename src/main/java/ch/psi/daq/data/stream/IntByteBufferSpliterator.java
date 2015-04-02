package ch.psi.daq.data.stream;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Spliterator;
import java.util.function.IntConsumer;

import ch.psi.daq.data.db.converters.ByteValueSpliterer;
import ch.psi.daq.data.stream.converter.ByteValueConverter;

/**
 * A Spliterator.OfInt designed for use by sources that traverse and split
 * elements maintained in an unmodifiable {@code ByteBuffer}.
 */
public class IntByteBufferSpliterator<T> implements Spliterator.OfInt {
	private final ByteBuffer buffer;
	private int startIndex; // current index, modified on advance/split
	private final int endIndex; // one past last index
	private final ByteValueSpliterer<T> spliterer;
	private final int characteristics;

	/**
	 * Creates a spliterator covering all of the given buffer.
	 * 
	 * @param buffer
	 *            the ByteBuffer, assumed to be unmodified during use
	 * @param spliterer
	 *            The ByteValueSpliterer
	 */
	public IntByteBufferSpliterator(ByteBuffer buffer, ByteValueSpliterer<T> spliterer) {
		this(buffer, spliterer.getStartIndex(buffer), spliterer.getEndIndex(buffer), spliterer, Spliterator.SIZED | Spliterator.SUBSIZED | Spliterator.IMMUTABLE | Spliterator.ORDERED
				| Spliterator.NONNULL);
	}

	/**
	 * Creates a spliterator covering the given buffer and range
	 * 
	 * @param buffer
	 *            the ByteBuffer, assumed to be unmodified during use
	 * @param origin
	 *            the least index (inclusive) to cover
	 * @param fence
	 *            one past the greatest index to cover
	 * @param spliterer
	 *            The ByteValueSpliterer
	 * @param characteristics
	 *            The characteristics
	 */
	protected IntByteBufferSpliterator(ByteBuffer buffer, int origin, int fence, ByteValueSpliterer<T> spliterer, int additionalCharacteristics) {
		this.buffer = buffer;
		this.startIndex = origin;
		this.endIndex = fence;
		this.spliterer = spliterer;
		this.characteristics = additionalCharacteristics;
	}

	@Override
	public OfInt trySplit() {
		int lo = startIndex;
		int mid = this.spliterer.splitIndex(buffer, startIndex, endIndex);

		return (lo >= mid)
				? null
				: new IntByteBufferSpliterator<T>(this.buffer, lo, startIndex = mid, this.spliterer, characteristics);
	}

	@Override
	public void forEachRemaining(IntConsumer action) {
		ByteBuffer buf;
		int i, hi; // hoist accesses and checks from loop
		if (action == null) {
			throw new NullPointerException();
		}

		if ((buf = buffer).limit() >= (hi = endIndex) &&
				(i = startIndex) >= 0 && i < (startIndex = hi)) {
			ByteValueSpliterer<T> spliterer = this.spliterer;
			ByteValueConverter<T> conv = spliterer.getByteValueConverter();

			do {
				action.accept(conv.getAsInt(buf, i));
			} while ((i = spliterer.nextIndex(buffer, i)) < hi);
		}
	}

	@Override
	public boolean tryAdvance(IntConsumer action) {
		if (action == null) {
			throw new NullPointerException();
		}
		if (startIndex >= 0 && startIndex < endIndex) {
			ByteValueSpliterer<T> spliterer = this.spliterer;
			action.accept(spliterer.getByteValueConverter().getAsInt(buffer, startIndex));
			startIndex = spliterer.nextIndex(buffer, startIndex);
			return true;
		}
		return false;
	}

	@Override
	public long estimateSize() {
		return (long) this.spliterer.estimateSize(buffer, startIndex, endIndex);
	}

	@Override
	public int characteristics() {
		return characteristics;
	}

	@Override
	public Comparator<? super Integer> getComparator() {
		if (hasCharacteristics(Spliterator.SORTED))
			return null;
		throw new IllegalStateException();
	}
}