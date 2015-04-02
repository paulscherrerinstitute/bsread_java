package ch.psi.daq.data.db.converters.impl;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.logging.Level;
import java.util.logging.Logger;

import ch.psi.daq.data.db.converters.AbstractByteArrayConverter;
import ch.psi.daq.data.stream.converter.ByteValueConverter;
import ch.psi.daq.data.stream.converter.LongByteValueConverter;

public class LongArrayByteConverter extends AbstractByteArrayConverter<long[], long[], Long> {
	private static final Logger LOGGER = Logger.getLogger(LongArrayByteConverter.class.getName());
	private static final LongByteValueConverter BYTE_BUFFER_CONVERTER = new LongByteValueConverter();

	@Override
	public long[] convert(ByteBuffer bytes, ByteOrder endianness) {
		long[] values = new long[bytes.remaining() / getNBytes()];
		this.convert(bytes, endianness, values);
		return values;
	}

	@Override
	public int convert(ByteBuffer bytes, ByteOrder endianness, long[] values) {
		int length = Math.min(values.length, bytes.remaining() / getNBytes());
		bytes.order(endianness).asLongBuffer().get(values);

		return length;
	}

	@Override
	public ByteBuffer convertObject(Object value, ByteOrder endianness) {
		if (value instanceof long[]) {
			return this.convert((long[]) value, endianness);
		} else {
			String message = String.format("Could not cast '%s' into '%s'", value.getClass().getName(), long[].class.getName());
			LOGGER.log(Level.SEVERE, message);
			throw new RuntimeException(message);
		}
	}

	@Override
	public ByteBuffer convert(long[] value, ByteOrder endianness) {
		return convertBytes(value, endianness);
	}

	public static ByteBuffer convertBytes(long[] value, ByteOrder endianness) {
		ByteBuffer ret = ByteBuffer.allocate(value.length * getNBytes()).order(endianness);
		ret.asLongBuffer().put(value);
		return ret;
	}

	@Override
	public int getBytes() {
		return getNBytes();
	}

	protected static int getNBytes() {
		return Long.BYTES;
	}
	
	@Override
	public Class<long[]> getValueClass() {
		return long[].class;
	}
	
	@Override
	public Class<long[]> getSignedValueClass() {
		return this.getValueClass();
	}

	@Override
	public long[] convertAsSigned(ByteBuffer bytes, ByteOrder endianness) {
		return this.convert(bytes, endianness);
	}
	
	@Override
	public ByteBuffer convertAsSigned(long[] value, ByteOrder endianness) {
		return convertBytes(value, endianness);
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
