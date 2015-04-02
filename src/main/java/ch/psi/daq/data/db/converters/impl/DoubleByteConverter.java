package ch.psi.daq.data.db.converters.impl;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.logging.Level;
import java.util.logging.Logger;

import ch.psi.daq.data.db.converters.AbstractByteConverter;
import ch.psi.daq.data.stream.converter.ByteValueConverter;
import ch.psi.daq.data.stream.converter.DoubleByteValueConverter;

public class DoubleByteConverter extends AbstractByteConverter<Double, Double, Double> {
	private static final Logger LOGGER = Logger.getLogger(DoubleByteConverter.class.getName());
	private static final DoubleByteValueConverter BYTE_BUFFER_CONVERTER = new DoubleByteValueConverter();

	@Override
	public Double convert(ByteBuffer bytes, ByteOrder endianness) {
		return convertBytes(bytes, endianness);
	}

	public static double convertBytes(ByteBuffer bytes, ByteOrder endianness) {
		return bytes.order(endianness).asDoubleBuffer().get();
	}

	@Override
	public ByteBuffer convertObject(Object value, ByteOrder endianness) {
		if (value instanceof Double) {
			return this.convert((Double) value, endianness);
		} else {
			String message = String.format("Could not cast '%s' into '%s'", value.getClass().getName(), Double.class.getName());
			LOGGER.log(Level.SEVERE, message);
			throw new RuntimeException(message);
		}
	}

	@Override
	public ByteBuffer convert(Double value, ByteOrder endianness) {
		return this.convert(value.doubleValue(), endianness);
	}

	public ByteBuffer convert(double value, ByteOrder endianness) {
		return convertBytes(value, endianness);
	}

	public static ByteBuffer convertBytes(double value, ByteOrder endianness) {
		ByteBuffer ret = ByteBuffer.allocate(getNBytes()).order(endianness);
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
	public Class<Double> getValueClass() {
		return Double.class;
	}
	
	@Override
	public Class<Double> getSignedValueClass() {
		return this.getValueClass();
	}

	@Override
	public Double convertAsSigned(ByteBuffer valueBytes, ByteOrder endianness) {
		return convertBytes(valueBytes, endianness);
	}
	
	public double convertAsSignedPrim(ByteBuffer valueBytes, ByteOrder endianness) {
		return convertBytes(valueBytes, endianness);
	}
	
	@Override
	public ByteBuffer convertAsSigned(Double value, ByteOrder endianness) {
		return this.convertAsSigned(value.doubleValue(), endianness);
	}

	public ByteBuffer convertAsSigned(double value, ByteOrder endianness) {
		return convertBytes(value, endianness);
	}
	
	@Override
	public ByteValueConverter<Double> getByteValueConverter() {
		return BYTE_BUFFER_CONVERTER;
	}
}
