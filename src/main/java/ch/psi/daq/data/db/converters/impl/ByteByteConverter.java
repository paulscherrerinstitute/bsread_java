package ch.psi.daq.data.db.converters.impl;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.logging.Level;
import java.util.logging.Logger;

import ch.psi.daq.data.db.converters.AbstractByteConverter;
import ch.psi.daq.data.stream.converter.ByteByteValueConverter;
import ch.psi.daq.data.stream.converter.ByteValueConverter;

public class ByteByteConverter extends AbstractByteConverter<Byte, Byte, Byte> {
	private static final Logger LOGGER = Logger.getLogger(ByteByteConverter.class.getName());
	private static final ByteByteValueConverter BYTE_BUFFER_CONVERTER = new ByteByteValueConverter();

	@Override
	public Byte convert(ByteBuffer bytes, ByteOrder endianness) {
		return convertBytes(bytes, endianness);
	}

	public static byte convertBytes(ByteBuffer bytes, ByteOrder endianness) {
		return bytes.order(endianness).get(bytes.position());
	}

	@Override
	public ByteBuffer convertObject(Object value, ByteOrder endianness) {
		if (value instanceof Byte) {
			return this.convert((Byte) value, endianness);
		} else {
			String message = String.format("Could not cast '%s' into '%s'", value.getClass().getName(), Byte.class.getName());
			LOGGER.log(Level.SEVERE, message);
			throw new RuntimeException(message);
		}
	}

	@Override
	public ByteBuffer convert(Byte value, ByteOrder endianness) {
		return this.convert(value.byteValue(), endianness);
	}

	public ByteBuffer convert(byte value, ByteOrder endianness) {
		return convertBytes(value, endianness);
	}

	public static ByteBuffer convertBytes(byte value, ByteOrder endianness) {
		ByteBuffer ret = ByteBuffer.allocate(getNBytes()).order(endianness);
		ret.put(value);
		// need to flip here since put was directly on ret
		ret.flip();
		return ret;
	}

	@Override
	public int getBytes() {
		return getNBytes();
	}

	protected static int getNBytes() {
		return Byte.BYTES;
	}
	
	@Override
	public Class<Byte> getValueClass() {
		return Byte.class;
	}
	
	@Override
	public Class<Byte> getSignedValueClass() {
		return this.getValueClass();
	}

	@Override
	public Byte convertAsSigned(ByteBuffer valueBytes, ByteOrder endianness) {
		return convertBytes(valueBytes, endianness);
	}

	public byte convertAsSignedPrim(ByteBuffer valueBytes, ByteOrder endianness) {
		return convertBytes(valueBytes, endianness);
	}

	@Override
	public ByteBuffer convertAsSigned(Byte value, ByteOrder endianness) {
		return this.convertAsSigned(value.byteValue(), endianness);
	}

	public ByteBuffer convertAsSigned(byte value, ByteOrder endianness) {
		return convertBytes(value, endianness);
	}
	
	@Override
	public ByteValueConverter<Byte> getByteValueConverter() {
		return BYTE_BUFFER_CONVERTER;
	}
}
