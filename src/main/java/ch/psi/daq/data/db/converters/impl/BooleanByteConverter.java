package ch.psi.daq.data.db.converters.impl;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.logging.Level;
import java.util.logging.Logger;

import ch.psi.daq.data.db.converters.AbstractByteConverter;
import ch.psi.daq.data.stream.converter.BooleanByteValueConverter;
import ch.psi.daq.data.stream.converter.ByteValueConverter;

public class BooleanByteConverter extends AbstractByteConverter<Boolean, Boolean, Boolean> {
	private static final Logger LOGGER = Logger.getLogger(BooleanByteConverter.class.getName());
	private static final BooleanByteValueConverter BYTE_BUFFER_CONVERTER = new BooleanByteValueConverter();

	@Override
	public Boolean convert(ByteBuffer bytes, ByteOrder endianness) {
		return convertBytes(bytes, endianness);
	}

	public static boolean convertBytes(ByteBuffer bytes, ByteOrder endianness) {
		byte val = bytes.order(endianness).get(bytes.position());
		return BooleanByteValueConverter.convertVal(val);
	}

	@Override
	public ByteBuffer convertObject(Object value, ByteOrder endianness) {
		if (value instanceof Boolean) {
			return this.convert((Boolean) value, endianness);
		} else {
			String message = String.format("Could not cast '%s' into '%s'", value.getClass().getName(), Boolean.class.getName());
			LOGGER.log(Level.SEVERE, message);
			throw new RuntimeException(message);
		}
	}

	@Override
	public ByteBuffer convert(Boolean value, ByteOrder endianness) {
		return this.convert(value.booleanValue(), endianness);
	}

	public ByteBuffer convert(boolean value, ByteOrder endianness) {
		return convertBytes(value, endianness);
	}

	public static ByteBuffer convertBytes(boolean value, ByteOrder endianness) {
		byte bVal = BooleanByteValueConverter.convertVal(value);

		ByteBuffer ret = ByteBuffer.allocate(getNBytes()).order(endianness);
		ret.put(bVal);
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
	public Class<Boolean> getValueClass() {
		return Boolean.class;
	}

	@Override
	public Class<Boolean> getSignedValueClass() {
		return this.getValueClass();
	}

	@Override
	public Boolean convertAsSigned(ByteBuffer valueBytes, ByteOrder endianness) {
		return convertBytes(valueBytes, endianness);
	}

	public boolean convertAsSignedPrim(ByteBuffer valueBytes, ByteOrder endianness) {
		return convertBytes(valueBytes, endianness);
	}

	@Override
	public ByteBuffer convertAsSigned(Boolean value, ByteOrder endianness) {
		return this.convertAsSigned(value.booleanValue(), endianness);
	}

	public ByteBuffer convertAsSigned(boolean value, ByteOrder endianness) {
		return convertBytes(value, endianness);
	}

	@Override
	public ByteValueConverter<Boolean> getByteValueConverter() {
		return BYTE_BUFFER_CONVERTER;
	}
}
