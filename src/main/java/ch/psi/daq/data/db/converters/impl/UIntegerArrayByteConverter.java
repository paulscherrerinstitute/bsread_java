package ch.psi.daq.data.db.converters.impl;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.function.IntConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.IntStream;

import ch.psi.daq.data.db.converters.AbstractByteArrayConverter;
import ch.psi.daq.data.stream.converter.ByteValueConverter;
import ch.psi.daq.data.stream.converter.UIntegerByteValueConverter;

public class UIntegerArrayByteConverter extends AbstractByteArrayConverter<long[], int[], Long> {
	private static final Logger LOGGER = Logger.getLogger(UIntegerArrayByteConverter.class.getName());
	private static final IntegerArrayByteConverter SIGNED_ARRAY_BYTE_CONVERTER = new IntegerArrayByteConverter();
	private static final UIntegerByteValueConverter BYTE_BUFFER_CONVERTER = new UIntegerByteValueConverter();

	@Override
	public long[] convert(ByteBuffer bytes, ByteOrder endianness) {
		long[] values = new long[bytes.remaining() / SIGNED_ARRAY_BYTE_CONVERTER.getBytes()];
		this.convert(bytes, endianness, values);
		return values;
	}

	@Override
	public int convert(ByteBuffer bytes, ByteOrder endianness, long[] values) {
		ByteBuffer buffer = bytes.order(endianness);
		int startPos = bytes.position();
		int nBytes = SIGNED_ARRAY_BYTE_CONVERTER.getBytes();
		int length = Math.min(values.length, buffer.remaining() / nBytes);

		IntStream.range(0, length).parallel().forEach(new IntConsumer() {

			@Override
			public void accept(int i) {
				values[i] = UIntegerByteValueConverter.convertVal(buffer.getInt(startPos + i * nBytes));
			}
		});

		return length;
	}

	@Override
	public ByteBuffer convertObject(Object value, ByteOrder endianness) {
		if (value instanceof int[]) {
			return this.convertAsSigned((int[]) value, endianness);
		}
		if (value instanceof long[]) {
			return this.convert((long[]) value, endianness);
		}
		else {
			String message = String.format("Could not cast '%s' into '%s' or '%s'", value.getClass().getName(), int[].class.getName(), long[].class.getName());
			LOGGER.log(Level.SEVERE, message);
			throw new RuntimeException(message);
		}
	}

	@Override
	public ByteBuffer convert(long[] value, ByteOrder endianness) {
		ByteBuffer ret = ByteBuffer.allocate(value.length * SIGNED_ARRAY_BYTE_CONVERTER.getBytes()).order(endianness);
		IntBuffer buffer = ret.asIntBuffer();

		IntStream.range(0, value.length).parallel().forEach(new IntConsumer() {

			@Override
			public void accept(int i) {
				buffer.put(i, UIntegerByteValueConverter.convertVal(value[i]));
			}
		});

		return ret;
	}

	@Override
	public int getBytes() {
		return IntegerArrayByteConverter.getNBytes();
	}

	@Override
	public Class<long[]> getValueClass() {
		return long[].class;
	}

	@Override
	public Class<int[]> getSignedValueClass() {
		return int[].class;
	}

	@Override
	public int[] convertAsSigned(ByteBuffer bytes, ByteOrder endianness) {
		return SIGNED_ARRAY_BYTE_CONVERTER.convert(bytes, endianness);
	}

	@Override
	public ByteBuffer convertAsSigned(int[] value, ByteOrder endianness) {
		return SIGNED_ARRAY_BYTE_CONVERTER.convert(value, endianness);
	}

	@Override
	public int getArraySize(ByteBuffer bytes, ByteOrder endianness) throws UnsupportedOperationException {
		return bytes.remaining() / getBytes();
	}

	@Override
	public ByteValueConverter<Long> getByteValueConverter() {
		return BYTE_BUFFER_CONVERTER;
	}
}
