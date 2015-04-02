package ch.psi.daq.data.db.converters.impl;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.logging.Level;
import java.util.logging.Logger;

import ch.psi.daq.data.db.converters.AbstractByteConverter;
import ch.psi.daq.data.stream.converter.ByteValueConverter;
import ch.psi.daq.data.stream.converter.FloatByteValueConverter;

public class FloatByteConverter extends AbstractByteConverter<Float, Float, Float> {
	private static final Logger LOGGER = Logger.getLogger(FloatByteConverter.class.getName());
	private static final FloatByteValueConverter BYTE_BUFFER_CONVERTER = new FloatByteValueConverter();

	@Override
	public Float convert(ByteBuffer bytes, ByteOrder endianness) {
		return convertBytes(bytes, endianness);
	}

	public static float convertBytes(ByteBuffer bytes, ByteOrder endianness) {
		return bytes.order(endianness).asFloatBuffer().get();
	}

	@Override
	public ByteBuffer convertObject(Object value, ByteOrder endianness) {
		if (value instanceof Float) {
			return this.convert((Float) value, endianness);
		} else {
			String message = String.format("Could not cast '%s' into '%s'", value.getClass().getName(), Float.class.getName());
			LOGGER.log(Level.SEVERE, message);
			throw new RuntimeException(message);
		}
	}

	@Override
	public ByteBuffer convert(Float value, ByteOrder endianness) {
		return this.convert(value.floatValue(), endianness);
	}

	public ByteBuffer convert(float value, ByteOrder endianness) {
		return convertBytes(value, endianness);
	}

	public static ByteBuffer convertBytes(float value, ByteOrder endianness) {
		ByteBuffer ret = ByteBuffer.allocate(getNBytes()).order(endianness);
		ret.asFloatBuffer().put(value);
		return ret;
	}

	@Override
	public int getBytes() {
		return getNBytes();
	}

	protected static int getNBytes() {
		return Float.BYTES;
	}

	@Override
	public Class<Float> getValueClass() {
		return Float.class;
	}

	@Override
	public Class<Float> getSignedValueClass() {
		return this.getValueClass();
	}

	@Override
	public Float convertAsSigned(ByteBuffer valueBytes, ByteOrder endianness) {
		return convert(valueBytes, endianness);
	}

	public float convertAsSignedPrim(ByteBuffer valueBytes, ByteOrder endianness) {
		return convert(valueBytes, endianness);
	}

	@Override
	public ByteBuffer convertAsSigned(Float value, ByteOrder endianness) {
		return this.convertAsSigned(value.floatValue(), endianness);
	}

	public ByteBuffer convertAsSigned(float value, ByteOrder endianness) {
		return convertBytes(value, endianness);
	}

	@Override
	public ByteValueConverter<Float> getByteValueConverter() {
		return BYTE_BUFFER_CONVERTER;
	}
}
