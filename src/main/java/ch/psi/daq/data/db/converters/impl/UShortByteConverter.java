package ch.psi.daq.data.db.converters.impl;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.logging.Level;
import java.util.logging.Logger;

import ch.psi.daq.data.db.converters.AbstractByteConverter;
import ch.psi.daq.data.stream.converter.ByteValueConverter;
import ch.psi.daq.data.stream.converter.UShortByteValueConverter;

public class UShortByteConverter extends AbstractByteConverter<Integer, Short, Integer> {
	private static final Logger LOGGER = Logger.getLogger(UShortByteConverter.class.getName());
	private static final UShortByteValueConverter BYTE_BUFFER_CONVERTER = new UShortByteValueConverter();

	@Override
	public Integer convert(ByteBuffer bytes, ByteOrder endianness) {
		return convertBytes(bytes, endianness);
	}

	public static int convertBytes(ByteBuffer bytes, ByteOrder endianness) {
		return UShortByteValueConverter.convertVal(ShortByteConverter.convertBytes(bytes, endianness));
	}

	@Override
	public ByteBuffer convertObject(Object value, ByteOrder endianness) {
		if (value instanceof Short) {
			return this.convert((Short) value, endianness);
		}
		if (value instanceof Integer) {
			return this.convert((Integer) value, endianness);
		}
		else {
			String message = String.format("Could not cast '%s' into '%s' or '%s'", value.getClass().getName(), Short.class.getName(), Integer.class.getName());
			LOGGER.log(Level.SEVERE, message);
			throw new RuntimeException(message);
		}
	}

	@Override
	public ByteBuffer convert(Integer value, ByteOrder endianness) {
		return this.convert(value.intValue(), endianness);
	}

	public ByteBuffer convert(int value, ByteOrder endianness) {
		return convertBytes(UShortByteValueConverter.convertVal(value), endianness);
	}

	public static ByteBuffer convertBytes(short value, ByteOrder endianness) {
		return ShortByteConverter.convertBytes(value, endianness);
	}

	@Override
	public int getBytes() {
		return ShortByteConverter.getNBytes();
	}

	@Override
	public Class<Integer> getValueClass() {
		return Integer.class;
	}

	@Override
	public Class<Short> getSignedValueClass() {
		return Short.class;
	}

	@Override
	public Short convertAsSigned(ByteBuffer bytes, ByteOrder endianness) {
		return ShortByteConverter.convertBytes(bytes, endianness);
	}

	public short convertAsSignedPrim(ByteBuffer bytes, ByteOrder endianness) {
		return ShortByteConverter.convertBytes(bytes, endianness);
	}

	@Override
	public ByteBuffer convertAsSigned(Short value, ByteOrder endianness) {
		return this.convertAsSigned(value.shortValue(), endianness);
	}

	public ByteBuffer convertAsSigned(short value, ByteOrder endianness) {
		return convertBytes(value, endianness);
	}

	@Override
	public ByteValueConverter<Integer> getByteValueConverter() {
		return BYTE_BUFFER_CONVERTER;
	}
}
