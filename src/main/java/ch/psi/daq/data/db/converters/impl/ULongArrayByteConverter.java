package ch.psi.daq.data.db.converters.impl;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.util.function.IntConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.IntStream;

import ch.psi.daq.data.db.converters.AbstractByteArrayConverter;
import ch.psi.daq.data.stream.converter.ByteValueConverter;
import ch.psi.daq.data.stream.converter.ULongByteValueConverter;

public class ULongArrayByteConverter extends AbstractByteArrayConverter<BigInteger[], long[], BigInteger> {
	private static final Logger LOGGER = Logger.getLogger(ULongArrayByteConverter.class.getName());
	private static final LongArrayByteConverter SIGNED_ARRAY_BYTE_CONVERTER = new LongArrayByteConverter();
	private static final ULongByteValueConverter BYTE_BUFFER_CONVERTER = new ULongByteValueConverter();

	@Override
	public BigInteger[] convert(ByteBuffer bytes, ByteOrder endianness) {
		BigInteger[] values = new BigInteger[bytes.remaining() / SIGNED_ARRAY_BYTE_CONVERTER.getBytes()];
		this.convert(bytes, endianness, values);
		return values;
	}

	@Override
	public int convert(ByteBuffer bytes, ByteOrder endianness, BigInteger[] values) {
		ByteBuffer buffer = bytes.order(endianness);
		int startPos = bytes.position();
		int nBytes = SIGNED_ARRAY_BYTE_CONVERTER.getBytes();
		int length = Math.min(values.length, buffer.remaining() / nBytes);

		IntStream.range(0, length).parallel().forEach(new IntConsumer() {

			@Override
			public void accept(int i) {
				values[i] = ULongByteValueConverter.convertVal(buffer.getLong(startPos + i * nBytes));
			}
		});

		return length;
	}

	@Override
	public ByteBuffer convertObject(Object value, ByteOrder endianness) {
		if (value instanceof long[]) {
			return this.convertAsSigned((long[]) value, endianness);
		}
		if (value instanceof BigInteger[]) {
			return this.convert((BigInteger[]) value, endianness);
		}
		else {
			String message = String.format("Could not cast '%s' into '%s' or '%s'", value.getClass().getName(), long[].class.getName(), BigInteger[].class.getName());
			LOGGER.log(Level.SEVERE, message);
			throw new RuntimeException(message);
		}
	}

	@Override
	public ByteBuffer convert(BigInteger[] value, ByteOrder endianness) {
		ByteBuffer ret = ByteBuffer.allocate(value.length * SIGNED_ARRAY_BYTE_CONVERTER.getBytes()).order(endianness);
		LongBuffer buffer = ret.asLongBuffer();

		IntStream.range(0, value.length).parallel().forEach(new IntConsumer() {

			@Override
			public void accept(int i) {
				buffer.put(i, ULongByteValueConverter.convertVal(value[i]));
			}
		});

		return ret;
	}

	@Override
	public int getBytes() {
		return LongArrayByteConverter.getNBytes();
	}

	@Override
	public Class<BigInteger[]> getValueClass() {
		return BigInteger[].class;
	}

	@Override
	public Class<long[]> getSignedValueClass() {
		return long[].class;
	}

	@Override
	public long[] convertAsSigned(ByteBuffer bytes, ByteOrder endianness) {
		return SIGNED_ARRAY_BYTE_CONVERTER.convert(bytes, endianness);
	}

	@Override
	public ByteBuffer convertAsSigned(long[] value, ByteOrder endianness) {
		return SIGNED_ARRAY_BYTE_CONVERTER.convert(value, endianness);
	}

	@Override
	public int getArraySize(ByteBuffer bytes, ByteOrder endianness) throws UnsupportedOperationException {
		return bytes.remaining() / getBytes();
	}
	
	@Override
	public ByteValueConverter<BigInteger> getByteValueConverter() {
		return BYTE_BUFFER_CONVERTER;
	}
}
