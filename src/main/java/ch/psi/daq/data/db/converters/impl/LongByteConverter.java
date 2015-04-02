package ch.psi.daq.data.db.converters.impl;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.logging.Level;
import java.util.logging.Logger;

import ch.psi.daq.data.db.converters.AbstractByteConverter;
import ch.psi.daq.data.stream.converter.ByteValueConverter;
import ch.psi.daq.data.stream.converter.LongByteValueConverter;

public class LongByteConverter extends AbstractByteConverter<Long, Long, Long> {
	private static final Logger LOGGER = Logger.getLogger(LongByteConverter.class.getName());
	private static final LongByteValueConverter BYTE_BUFFER_CONVERTER = new LongByteValueConverter();

	@Override
	public Long convert(ByteBuffer bytes, ByteOrder endianness) {
		return convertBytes(bytes, endianness);
	}

	public static long convertBytes(ByteBuffer bytes, ByteOrder endianness) {
		return bytes.order(endianness).asLongBuffer().get();
	}

	@Override
	public ByteBuffer convertObject(Object value, ByteOrder endianness) {
		if (value instanceof Long) {
			return this.convert((Long) value, endianness);
		} else {
			String message = String.format("Could not cast '%s' into '%s'", value.getClass().getName(), Long.class.getName());
			LOGGER.log(Level.SEVERE, message);
			throw new RuntimeException(message);
		}
	}

	@Override
	public ByteBuffer convert(Long value, ByteOrder endianness) {
		return this.convert(value.longValue(), endianness);
	}

	public ByteBuffer convert(long value, ByteOrder endianness) {
		return convertBytes(value, endianness);
	}

	public static ByteBuffer convertBytes(long value, ByteOrder endianness) {
		ByteBuffer ret = ByteBuffer.allocate(getNBytes()).order(endianness);
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
	public Class<Long> getValueClass() {
		return Long.class;
	}
	
	@Override
	public Class<Long> getSignedValueClass() {
		return this.getValueClass();
	}

	@Override
	public Long convertAsSigned(ByteBuffer bytes, ByteOrder endianness) {
		return convertBytes(bytes, endianness);
	}

	public long convertAsSignedPrim(ByteBuffer bytes, ByteOrder endianness) {
		return convertBytes(bytes, endianness);
	}

	@Override
	public ByteBuffer convertAsSigned(Long value, ByteOrder endianness) {
		return this.convertAsSigned(value.longValue(), endianness);
	}

	public ByteBuffer convertAsSigned(long value, ByteOrder endianness) {
		return convertBytes(value, endianness);
	}
	
	@Override
	public ByteValueConverter<Long> getByteValueConverter() {
		return BYTE_BUFFER_CONVERTER;
	}
}
