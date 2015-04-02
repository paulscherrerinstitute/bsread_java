package ch.psi.daq.data.db.converters.impl;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.logging.Level;
import java.util.logging.Logger;

import ch.psi.daq.data.db.converters.AbstractByteArrayConverter;
import ch.psi.daq.data.stream.converter.ByteValueConverter;
import ch.psi.daq.data.stream.converter.IntegerByteValueConverter;

public class IntegerArrayByteConverter extends AbstractByteArrayConverter<int[], int[], Integer> {
	private static final Logger LOGGER = Logger.getLogger(IntegerArrayByteConverter.class.getName());
	private static final IntegerByteValueConverter BYTE_BUFFER_CONVERTER = new IntegerByteValueConverter();

	@Override
	public int[] convert(ByteBuffer bytes, ByteOrder endianness) {
		int[] values = new int[bytes.remaining() / getNBytes()];
		this.convert(bytes, endianness, values);
		return values;
	}

	@Override
	public int convert(ByteBuffer bytes, ByteOrder endianness, int[] values) {
		int length = Math.min(values.length, bytes.remaining() / getNBytes());
		bytes.order(endianness).asIntBuffer().get(values);

		return length;
	}

	@Override
	public ByteBuffer convertObject(Object value, ByteOrder endianness) {
		if (value instanceof int[]) {
			return this.convert((int[]) value, endianness);
		} else {
			String message = String.format("Could not cast '%s' into '%s'", value.getClass().getName(), int[].class.getName());
			LOGGER.log(Level.SEVERE, message);
			throw new RuntimeException(message);
		}
	}

	@Override
	public ByteBuffer convert(int[] value, ByteOrder endianness) {
		return convertBytes(value, endianness);
	}

	public static ByteBuffer convertBytes(int[] value, ByteOrder endianness) {
		ByteBuffer ret = ByteBuffer.allocate(value.length * getNBytes()).order(endianness);
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
	public Class<int[]> getValueClass() {
		return int[].class;
	}
	
	@Override
	public Class<int[]> getSignedValueClass() {
		return this.getValueClass();
	}

	@Override
	public int[] convertAsSigned(ByteBuffer bytes, ByteOrder endianness) {
		return this.convert(bytes, endianness);
	}
	
	@Override
	public ByteBuffer convertAsSigned(int[] value, ByteOrder endianness) {
		return convertBytes(value, endianness);
	}

	@Override
	public int getArraySize(ByteBuffer bytes, ByteOrder endianness) throws UnsupportedOperationException {
		return bytes.remaining() / getBytes();
	}
	
	@Override
	public ByteValueConverter<Integer> getByteValueConverter() {
		return BYTE_BUFFER_CONVERTER;
	}
}
