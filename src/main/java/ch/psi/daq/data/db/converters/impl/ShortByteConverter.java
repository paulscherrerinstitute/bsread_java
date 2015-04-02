package ch.psi.daq.data.db.converters.impl;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.logging.Level;
import java.util.logging.Logger;

import ch.psi.daq.data.db.converters.AbstractByteConverter;
import ch.psi.daq.data.stream.converter.ByteValueConverter;
import ch.psi.daq.data.stream.converter.ShortByteValueConverter;

public class ShortByteConverter extends AbstractByteConverter<Short, Short, Short> {
	private static final Logger LOGGER = Logger.getLogger(ShortByteConverter.class.getName());
	private static final ShortByteValueConverter BYTE_BUFFER_CONVERTER = new ShortByteValueConverter();

	@Override
	public Short convert(ByteBuffer bytes, ByteOrder endianness) {
		return convertBytes(bytes, endianness);
	}

	public static short convertBytes(ByteBuffer bytes, ByteOrder endianness) {
		return bytes.order(endianness).asShortBuffer().get();
	}

	@Override
	public ByteBuffer convertObject(Object value, ByteOrder endianness) {
		if (value instanceof Short) {
			return this.convert((Short) value, endianness);
		} else {
			String message = String.format("Could not cast '%s' into '%s'", value.getClass().getName(), Short.class.getName());
			LOGGER.log(Level.SEVERE, message);
			throw new RuntimeException(message);
		}
	}

	@Override
	public ByteBuffer convert(Short value, ByteOrder endianness) {
		return this.convert(value.shortValue(), endianness);
	}

	public ByteBuffer convert(short value, ByteOrder endianness) {
		return convertBytes(value, endianness);
	}

	public static ByteBuffer convertBytes(short value, ByteOrder endianness) {
		ByteBuffer ret = ByteBuffer.allocate(getNBytes()).order(endianness);
		ret.asShortBuffer().put(value);
		return ret;
	}

	@Override
	public int getBytes() {
		return getNBytes();
	}

	protected static int getNBytes() {
		return Short.BYTES;
	}
	
	@Override
	public Class<Short> getValueClass() {
		return Short.class;
	}
	
	@Override
	public Class<Short> getSignedValueClass() {
		return this.getValueClass();
	}

	@Override
	public Short convertAsSigned(ByteBuffer bytes, ByteOrder endianness) {
		return convertBytes(bytes, endianness);
	}

	public short convertAsSignedPrim(ByteBuffer bytes, ByteOrder endianness) {
		return convertBytes(bytes, endianness);
	}
	
	@Override
	public ByteBuffer convertAsSigned(Short value, ByteOrder endianness) {
		return this.convertAsSigned(value.shortValue(), endianness);
	}

	public ByteBuffer convertAsSigned(short value, ByteOrder endianness) {
		return convertBytes(value, endianness);
	}
	
	@Override
	public ByteValueConverter<Short> getByteValueConverter() {
		return BYTE_BUFFER_CONVERTER;
	}
}
