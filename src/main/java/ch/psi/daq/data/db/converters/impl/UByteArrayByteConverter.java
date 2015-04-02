package ch.psi.daq.data.db.converters.impl;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.IntConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.IntStream;

import ch.psi.daq.data.db.converters.AbstractByteArrayConverter;
import ch.psi.daq.data.stream.converter.ByteValueConverter;
import ch.psi.daq.data.stream.converter.UByteByteValueConverter;

public class UByteArrayByteConverter extends AbstractByteArrayConverter<short[], byte[], Short> {
	private static final Logger LOGGER = Logger.getLogger(UByteArrayByteConverter.class.getName());
	private static final ByteArrayByteConverter SIGNED_ARRAY_BYTE_CONVERTER = new ByteArrayByteConverter();
	private static final UByteByteValueConverter BYTE_BUFFER_CONVERTER = new UByteByteValueConverter();

	@Override
	public short[] convert(ByteBuffer bytes, ByteOrder endianness) {
		short[] values = new short[bytes.remaining() / SIGNED_ARRAY_BYTE_CONVERTER.getBytes()];
		this.convert(bytes, endianness, values);
		return values;
	}

	@Override
	public int convert(ByteBuffer bytes, ByteOrder endianness, short[] values) {
		ByteBuffer buffer = bytes.order(endianness);
		int startPos = bytes.position();
		int nBytes = SIGNED_ARRAY_BYTE_CONVERTER.getBytes();
		int length = Math.min(values.length, buffer.remaining() / nBytes);

		IntStream.range(0, length).parallel().forEach(new IntConsumer() {

			@Override
			public void accept(int i) {
				values[i] = UByteByteValueConverter.convertVal(buffer.get(startPos + i * nBytes));
			}
		});

		return length;
	}

	@Override
	public ByteBuffer convertObject(Object value, ByteOrder endianness) {
		if (value instanceof byte[]) {
			return this.convertAsSigned((byte[]) value, endianness);
		}
		if (value instanceof short[]) {
			return this.convert((short[]) value, endianness);
		}
		else {
			String message = String.format("Could not cast '%s' into '%s' or '%s'", value.getClass().getName(), byte[].class.getName(), short[].class.getName());
			LOGGER.log(Level.SEVERE, message);
			throw new RuntimeException(message);
		}
	}

	@Override
	public ByteBuffer convert(short[] value, ByteOrder endianness) {
		ByteBuffer ret = ByteBuffer.allocate(value.length * SIGNED_ARRAY_BYTE_CONVERTER.getBytes()).order(endianness);

		IntStream.range(0, value.length).parallel().forEach(new IntConsumer() {

			@Override
			public void accept(int i) {
				ret.put(i, UByteByteValueConverter.convertVal(value[i]));
			}
		});

		// no flip necessary since we use absolut put()
		return ret;
	}

	@Override
	public int getBytes() {
		return ByteArrayByteConverter.getNBytes();
	}

	@Override
	public Class<short[]> getValueClass() {
		return short[].class;
	}

	@Override
	public Class<byte[]> getSignedValueClass() {
		return byte[].class;
	}

	@Override
	public byte[] convertAsSigned(ByteBuffer bytes, ByteOrder endianness) {
		return SIGNED_ARRAY_BYTE_CONVERTER.convert(bytes, endianness);
	}

	@Override
	public ByteBuffer convertAsSigned(byte[] value, ByteOrder endianness) {
		return SIGNED_ARRAY_BYTE_CONVERTER.convert(value, endianness);
	}

	@Override
	public int getArraySize(ByteBuffer bytes, ByteOrder endianness) throws UnsupportedOperationException {
		return bytes.remaining() / getBytes();
	}

	@Override
	public ByteValueConverter<Short> getByteValueConverter() {
		return BYTE_BUFFER_CONVERTER;
	}
}
