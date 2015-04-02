package ch.psi.daq.data.db.converters.impl;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.logging.Level;
import java.util.logging.Logger;

import ch.psi.daq.data.db.converters.AbstractByteConverter;
import ch.psi.daq.data.stream.converter.ByteValueConverter;
import ch.psi.daq.data.stream.converter.UIntegerByteValueConverter;

public class UIntegerByteConverter extends AbstractByteConverter<Long, Integer, Long> {
	private static final Logger LOGGER = Logger.getLogger(UIntegerByteConverter.class.getName());
	private static final UIntegerByteValueConverter BYTE_BUFFER_CONVERTER = new UIntegerByteValueConverter();

	@Override
	public Long convert(ByteBuffer bytes, ByteOrder endianness) {
		return convertBytes(bytes, endianness);
	}

	public static long convertBytes(ByteBuffer bytes, ByteOrder endianness) {
		return UIntegerByteValueConverter.convertVal(IntegerByteConverter.convertBytes(bytes, endianness));
	}

	@Override
	public ByteBuffer convertObject(Object value, ByteOrder endianness) {
		if (value instanceof Integer) {
			return this.convertAsSigned((Integer) value, endianness);
		}
		if (value instanceof Long) {
			return this.convert((Long) value, endianness);
		}
		else {
			String message = String.format("Could not cast '%s' into '%s' or '%s'", value.getClass().getName(), Integer.class.getName(), Long.class.getName());
			LOGGER.log(Level.SEVERE, message);
			throw new RuntimeException(message);
		}
	}

	@Override
	public ByteBuffer convert(Long value, ByteOrder endianness) {
		return this.convert(value.longValue(), endianness);
	}

	public ByteBuffer convert(long value, ByteOrder endianness) {
		return convertBytes(UIntegerByteValueConverter.convertVal(value), endianness);
	}

	public static ByteBuffer convertBytes(int value, ByteOrder endianness) {
		return IntegerByteConverter.convertBytes(value, endianness);
	}

	@Override
	public int getBytes() {
		return IntegerByteConverter.getNBytes();
	}

	@Override
	public Class<Long> getValueClass() {
		return Long.class;
	}

	@Override
	public Class<Integer> getSignedValueClass() {
		return Integer.class;
	}

	@Override
	public Integer convertAsSigned(ByteBuffer bytes, ByteOrder endianness) {
		return IntegerByteConverter.convertBytes(bytes, endianness);
	}

	public int convertAsSignedPrim(ByteBuffer bytes, ByteOrder endianness) {
		return IntegerByteConverter.convertBytes(bytes, endianness);
	}

	@Override
	public ByteBuffer convertAsSigned(Integer value, ByteOrder endianness) {
		return this.convertAsSigned(value.intValue(), endianness);
	}

	public ByteBuffer convertAsSigned(int value, ByteOrder endianness) {
		return convertBytes(value, endianness);
	}

	@Override
	public ByteValueConverter<Long> getByteValueConverter() {
		return BYTE_BUFFER_CONVERTER;
	}
}
