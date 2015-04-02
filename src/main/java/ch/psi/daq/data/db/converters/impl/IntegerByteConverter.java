package ch.psi.daq.data.db.converters.impl;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.logging.Level;
import java.util.logging.Logger;

import ch.psi.daq.data.db.converters.AbstractByteConverter;
import ch.psi.daq.data.stream.converter.ByteValueConverter;
import ch.psi.daq.data.stream.converter.IntegerByteValueConverter;

public class IntegerByteConverter extends AbstractByteConverter<Integer, Integer, Integer> {
	private static final Logger LOGGER = Logger.getLogger(IntegerByteConverter.class.getName());
	private static final IntegerByteValueConverter BYTE_BUFFER_CONVERTER = new IntegerByteValueConverter();

	@Override
	public Integer convert(ByteBuffer bytes, ByteOrder endianness) {
		return convertBytes(bytes, endianness);
	}

	public static int convertBytes(ByteBuffer bytes, ByteOrder endianness) {
		return bytes.order(endianness).asIntBuffer().get();
	}

	@Override
	public ByteBuffer convertObject(Object value, ByteOrder endianness) {
		if (value instanceof Integer) {
			return this.convert((Integer) value, endianness);
		} else {
			String message = String.format("Could not cast '%s' into '%s'", value.getClass().getName(), Integer.class.getName());
			LOGGER.log(Level.SEVERE, message);
			throw new RuntimeException(message);
		}
	}

	@Override
	public ByteBuffer convert(Integer value, ByteOrder endianness) {
		return this.convert(value.intValue(), endianness);
	}

	public ByteBuffer convert(int value, ByteOrder endianness) {
		return convertBytes(value, endianness);
	}

	public static ByteBuffer convertBytes(int value, ByteOrder endianness) {
		ByteBuffer ret = ByteBuffer.allocate(getNBytes()).order(endianness);
		ret.asIntBuffer().put(value);
		return ret;
	}

	@Override
	public int getBytes() {
		return getNBytes();
	}

	protected static int getNBytes() {
		return Integer.BYTES;
	}
	
	@Override
	public Class<Integer> getValueClass() {
		return Integer.class;
	}
	
	@Override
	public Class<Integer> getSignedValueClass() {
		return this.getValueClass();
	}

	@Override
	public Integer convertAsSigned(ByteBuffer valueBytes, ByteOrder endianness) {
		return convertBytes(valueBytes, endianness);
	}

	public int convertAsSignedPrim(ByteBuffer valueBytes, ByteOrder endianness) {
		return convertBytes(valueBytes, endianness);
	}
	
	@Override
	public ByteBuffer convertAsSigned(Integer value, ByteOrder endianness) {
		return this.convertAsSigned(value.intValue(), endianness);
	}

	public ByteBuffer convertAsSigned(int value, ByteOrder endianness) {
		return convertBytes(value, endianness);
	}
	
	@Override
	public ByteValueConverter<Integer> getByteValueConverter() {
		return BYTE_BUFFER_CONVERTER;
	}
}
