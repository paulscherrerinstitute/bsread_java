package ch.psi.daq.data.db.converters.impl;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.logging.Level;
import java.util.logging.Logger;

import ch.psi.daq.data.db.converters.AbstractByteArrayConverter;
import ch.psi.daq.data.stream.converter.ByteValueConverter;
import ch.psi.daq.data.stream.converter.FloatByteValueConverter;

public class FloatArrayByteConverter extends AbstractByteArrayConverter<float[], float[], Float> {
	private static final Logger LOGGER = Logger.getLogger(FloatArrayByteConverter.class.getName());
	private static final FloatByteValueConverter BYTE_BUFFER_CONVERTER = new FloatByteValueConverter();

	@Override
	public float[] convert(ByteBuffer bytes, ByteOrder endianness) {
		float[] values = new float[bytes.remaining() / getNBytes()];
		this.convert(bytes, endianness, values);
		return values;
	}

	@Override
	public int convert(ByteBuffer bytes, ByteOrder endianness, float[] values) {
		int length = Math.min(values.length, bytes.remaining() / getNBytes());
		bytes.order(endianness).asFloatBuffer().get(values);

		return length;
	}

	@Override
	public ByteBuffer convertObject(Object value, ByteOrder endianness) {
		if (value instanceof float[]) {
			return this.convert((float[]) value, endianness);
		} else {
			String message = String.format("Could not cast '%s' into '%s'", value.getClass().getName(), float[].class.getName());
			LOGGER.log(Level.SEVERE, message);
			throw new RuntimeException(message);
		}
	}

	@Override
	public ByteBuffer convert(float[] value, ByteOrder endianness) {
		return convertBytes(value, endianness);
	}

	public static ByteBuffer convertBytes(float[] value, ByteOrder endianness) {
		ByteBuffer ret = ByteBuffer.allocate(value.length * getNBytes()).order(endianness);
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
	public Class<float[]> getValueClass() {
		return float[].class;
	}
	
	@Override
	public Class<float[]> getSignedValueClass() {
		return this.getValueClass();
	}

	@Override
	public float[] convertAsSigned(ByteBuffer bytes, ByteOrder endianness) {
		return convert(bytes, endianness);
	}
	
	@Override
	public ByteBuffer convertAsSigned(float[] value, ByteOrder endianness) {
		return convertBytes(value, endianness);
	}

	@Override
	public int getArraySize(ByteBuffer bytes, ByteOrder endianness) throws UnsupportedOperationException {
		return bytes.remaining() / getBytes();
	}
	
	@Override
	public ByteValueConverter<Float> getByteValueConverter() {
		return BYTE_BUFFER_CONVERTER;
	}
}
