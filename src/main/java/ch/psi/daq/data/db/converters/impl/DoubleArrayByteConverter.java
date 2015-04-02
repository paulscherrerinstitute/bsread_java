package ch.psi.daq.data.db.converters.impl;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.logging.Level;
import java.util.logging.Logger;

import ch.psi.daq.data.db.converters.AbstractByteArrayConverter;
import ch.psi.daq.data.stream.converter.ByteValueConverter;
import ch.psi.daq.data.stream.converter.DoubleByteValueConverter;

public class DoubleArrayByteConverter extends AbstractByteArrayConverter<double[], double[], Double> {
	private static final Logger LOGGER = Logger.getLogger(DoubleArrayByteConverter.class.getName());
	private static final DoubleByteValueConverter BYTE_BUFFER_CONVERTER = new DoubleByteValueConverter();

	@Override
	public double[] convert(ByteBuffer bytes, ByteOrder endianness) {
		double[] values = new double[bytes.remaining() / getNBytes()];
		this.convert(bytes, endianness, values);
		return values;
	}

	@Override
	public int convert(ByteBuffer bytes, ByteOrder endianness, double[] values) {
		int length = Math.min(values.length, bytes.remaining() / getNBytes());
		bytes.order(endianness).asDoubleBuffer().get(values);
		
		return length;
	}

	@Override
	public ByteBuffer convertObject(Object value, ByteOrder endianness) {
		if (value instanceof double[]) {
			return this.convert((double[]) value, endianness);
		} else {
			String message = String.format("Could not cast '%s' into '%s'", value.getClass().getName(), double[].class.getName());
			LOGGER.log(Level.SEVERE, message);
			throw new RuntimeException(message);
		}
	}

	@Override
	public ByteBuffer convert(double[] value, ByteOrder endianness) {
		return convertBytes(value, endianness);
	}

	public static ByteBuffer convertBytes(double[] value, ByteOrder endianness) {
		ByteBuffer ret = ByteBuffer.allocate(value.length * getNBytes()).order(endianness);
		ret.asDoubleBuffer().put(value);
		return ret;
	}

	@Override
	public int getBytes() {
		return getNBytes();
	}

	protected static int getNBytes() {
		return Double.BYTES;
	}
	
	@Override
	public Class<double[]> getValueClass() {
		return double[].class;
	}
	
	@Override
	public Class<double[]> getSignedValueClass() {
		return this.getValueClass();
	}

	@Override
	public double[] convertAsSigned(ByteBuffer bytes, ByteOrder endianness) {
		return this.convert(bytes, endianness);
	}
	
	@Override
	public ByteBuffer convertAsSigned(double[] value, ByteOrder endianness) {
		return convertBytes(value, endianness);
	}

	@Override
	public int getArraySize(ByteBuffer bytes, ByteOrder endianness) throws UnsupportedOperationException {
		return bytes.remaining() / getBytes();
	}
	
	@Override
	public ByteValueConverter<Double> getByteValueConverter() {
		return BYTE_BUFFER_CONVERTER;
	}
}
