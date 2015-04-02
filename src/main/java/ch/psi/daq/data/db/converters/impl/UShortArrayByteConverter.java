package ch.psi.daq.data.db.converters.impl;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ShortBuffer;
import java.util.function.IntConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.IntStream;

import ch.psi.daq.data.db.converters.AbstractByteArrayConverter;
import ch.psi.daq.data.stream.converter.ByteValueConverter;
import ch.psi.daq.data.stream.converter.UShortByteValueConverter;

public class UShortArrayByteConverter extends AbstractByteArrayConverter<int[], short[], Integer> {
	private static final Logger LOGGER = Logger.getLogger(UShortArrayByteConverter.class.getName());
	private static final ShortArrayByteConverter SIGNED_ARRAY_BYTE_CONVERTER = new ShortArrayByteConverter();
	private static final UShortByteValueConverter BYTE_BUFFER_CONVERTER = new UShortByteValueConverter();

	@Override
	public int[] convert(ByteBuffer bytes, ByteOrder endianness) {
		int[] values = new int[bytes.remaining() / SIGNED_ARRAY_BYTE_CONVERTER.getBytes()];
		this.convert(bytes, endianness, values);
		return values;
	}

	@Override
	public int convert(ByteBuffer bytes, ByteOrder endianness, int[] values) {
		ByteBuffer buffer = bytes.order(endianness);
		int startPos = bytes.position();
		int nBytes = SIGNED_ARRAY_BYTE_CONVERTER.getBytes();
		int length = Math.min(values.length, buffer.remaining() / nBytes);

		IntStream.range(0, length).parallel().forEach(new IntConsumer() {

			@Override
			public void accept(int i) {
				values[i] = UShortByteValueConverter.convertVal(buffer.getShort(startPos + i * nBytes));
			}
		});

		return length;
	}

	@Override
	public ByteBuffer convertObject(Object value, ByteOrder endianness) {
		if (value instanceof short[]) {
			return this.convertAsSigned((short[]) value, endianness);
		}
		if (value instanceof int[]) {
			return this.convert((int[]) value, endianness);
		}
		else {
			String message = String.format("Could not cast '%s' into '%s' or '%s'", value.getClass().getName(), short[].class.getName(), int[].class.getName());
			LOGGER.log(Level.SEVERE, message);
			throw new RuntimeException(message);
		}
	}

	@Override
	public ByteBuffer convert(int[] value, ByteOrder endianness) {
		ByteBuffer ret = ByteBuffer.allocate(value.length * SIGNED_ARRAY_BYTE_CONVERTER.getBytes()).order(endianness);
		ShortBuffer buffer = ret.asShortBuffer();

		IntStream.range(0, value.length).parallel().forEach(new IntConsumer() {

			@Override
			public void accept(int i) {
				buffer.put(i, UShortByteValueConverter.convertVal(value[i]));
			}
		});

		return ret;
	}

	@Override
	public int getBytes() {
		return ShortArrayByteConverter.getNBytes();
	}

	@Override
	public Class<int[]> getValueClass() {
		return int[].class;
	}

	@Override
	public Class<short[]> getSignedValueClass() {
		return short[].class;
	}

	@Override
	public short[] convertAsSigned(ByteBuffer bytes, ByteOrder endianness) {
		return SIGNED_ARRAY_BYTE_CONVERTER.convert(bytes, endianness);
	}

	@Override
	public ByteBuffer convertAsSigned(short[] value, ByteOrder endianness) {
		return SIGNED_ARRAY_BYTE_CONVERTER.convert(value, endianness);
	}

	@Override
	public int getArraySize(ByteBuffer bytes, ByteOrder endianness) throws UnsupportedOperationException {
		return bytes.remaining() / getBytes();
	}

	@Override
	public ByteValueConverter<Integer> getByteValueConverter() {
		return BYTE_BUFFER_CONVERTER;
	}
}
