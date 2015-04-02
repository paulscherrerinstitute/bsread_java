package ch.psi.daq.data.db.converters.impl;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.logging.Level;
import java.util.logging.Logger;

import ch.psi.daq.data.db.converters.AbstractByteConverter;
import ch.psi.daq.data.stream.converter.ByteValueConverter;
import ch.psi.daq.data.stream.converter.UByteByteValueConverter;

public class UByteByteConverter extends AbstractByteConverter<Short, Byte, Short> {
	private static final Logger LOGGER = Logger.getLogger(UByteByteConverter.class.getName());
	private static final UByteByteValueConverter BYTE_BUFFER_CONVERTER = new UByteByteValueConverter();

	@Override
	public Short convert(ByteBuffer bytes, ByteOrder endianness) {
		return convertBytes(bytes, endianness);
	}

	public static short convertBytes(ByteBuffer bytes, ByteOrder endianness) {
		return UByteByteValueConverter.convertVal(ByteByteConverter.convertBytes(bytes, endianness));
	}

	@Override
	public ByteBuffer convertObject(Object value, ByteOrder endianness) {
		if (value instanceof Byte) {
			return this.convertAsSigned((Byte) value, endianness);
		}
		if (value instanceof Short) {
			return this.convert((Short) value, endianness);
		}
		else {
			String message = String.format("Could not cast '%s' into '%s' or '%s'", value.getClass().getName(), Byte.class.getName(), Short.class.getName());
			LOGGER.log(Level.SEVERE, message);
			throw new RuntimeException(message);
		}
	}

	@Override
	public ByteBuffer convert(Short value, ByteOrder endianness) {
		return this.convert(value.shortValue(), endianness);
	}

	public ByteBuffer convert(short value, ByteOrder endianness) {
		return convertBytes(UByteByteValueConverter.convertVal(value), endianness);
	}

	public static ByteBuffer convertBytes(byte value, ByteOrder endianness) {
		return ByteByteConverter.convertBytes(value, endianness);
	}

	@Override
	public int getBytes() {
		return ByteByteConverter.getNBytes();
	}

	@Override
	public Class<Short> getValueClass() {
		return Short.class;
	}

	@Override
	public Class<Byte> getSignedValueClass() {
		return Byte.class;
	}

	@Override
	public Byte convertAsSigned(ByteBuffer bytes, ByteOrder endianness) {
		return ByteByteConverter.convertBytes(bytes, endianness);
	}

	public byte convertAsSignedPrim(ByteBuffer bytes, ByteOrder endianness) {
		return ByteByteConverter.convertBytes(bytes, endianness);
	}

	@Override
	public ByteBuffer convertAsSigned(Byte value, ByteOrder endianness) {
		return this.convertAsSigned(value.byteValue(), endianness);
	}

	public ByteBuffer convertAsSigned(byte value, ByteOrder endianness) {
		return convertBytes(value, endianness);
	}

	@Override
	public ByteValueConverter<Short> getByteValueConverter() {
		return BYTE_BUFFER_CONVERTER;
	}
}
