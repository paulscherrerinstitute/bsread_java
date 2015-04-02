package ch.psi.daq.data.db.converters.impl;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.logging.Level;
import java.util.logging.Logger;

import ch.psi.daq.data.db.converters.AbstractByteConverter;
import ch.psi.daq.data.stream.converter.ByteValueConverter;
import ch.psi.daq.data.stream.converter.ULongByteValueConverter;

public class ULongByteConverter extends AbstractByteConverter<BigInteger, Long, BigInteger> {
	private static final Logger LOGGER = Logger.getLogger(ULongByteConverter.class.getName());
	private static final ULongByteValueConverter BYTE_BUFFER_CONVERTER = new ULongByteValueConverter();

	@Override
	public BigInteger convert(ByteBuffer bytes, ByteOrder endianness) {
		return convertBytes(bytes, endianness);
	}

	public static BigInteger convertBytes(ByteBuffer bytes, ByteOrder endianness) {
		return ULongByteValueConverter.convertVal(LongByteConverter.convertBytes(bytes, endianness));
	}

	@Override
	public ByteBuffer convertObject(Object value, ByteOrder endianness) {
		if (value instanceof Long) {
			return this.convertAsSigned((Long) value, endianness);
		}
		if (value instanceof BigInteger) {
			return this.convert((BigInteger) value, endianness);
		}
		else {
			String message = String.format("Could not cast '%s' into '%s' or '%s'", value.getClass().getName(), Long.class.getName(), BigInteger.class.getName());
			LOGGER.log(Level.SEVERE, message);
			throw new RuntimeException(message);
		}
	}

	@Override
	public ByteBuffer convert(BigInteger value, ByteOrder endianness) {
		return this.convert(ULongByteValueConverter.convertVal(value), endianness);
	}

	public ByteBuffer convert(long value, ByteOrder endianness) {
		return convertBytes(value, endianness);
	}

	public static ByteBuffer convertBytes(long value, ByteOrder endianness) {
		return LongByteConverter.convertBytes(value, endianness);
	}

	@Override
	public int getBytes() {
		return LongByteConverter.getNBytes();
	}
	
	@Override
	public Class<BigInteger> getValueClass() {
		return BigInteger.class;
	}
	
	@Override
	public Class<Long> getSignedValueClass() {
		return Long.class;
	}

	@Override
	public Long convertAsSigned(ByteBuffer bytes, ByteOrder endianness) {
		return LongByteConverter.convertBytes(bytes, endianness);
	}

	public long convertAsSignedPrim(ByteBuffer bytes, ByteOrder endianness) {
		return LongByteConverter.convertBytes(bytes, endianness);
	}

	@Override
	public ByteBuffer convertAsSigned(Long value, ByteOrder endianness) {
		return this.convertAsSigned(value.longValue(), endianness);
	}

	public ByteBuffer convertAsSigned(long value, ByteOrder endianness) {
		return convertBytes(value, endianness);
	}
	
	@Override
	public ByteValueConverter<BigInteger> getByteValueConverter() {
		return BYTE_BUFFER_CONVERTER;
	}
}
