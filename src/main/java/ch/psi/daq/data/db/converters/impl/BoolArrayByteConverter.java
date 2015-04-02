package ch.psi.daq.data.db.converters.impl;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.logging.Level;
import java.util.logging.Logger;

import ch.psi.daq.data.db.converters.AbstractByteArrayConverter;
import ch.psi.daq.data.db.converters.ByteConverter;
import ch.psi.daq.data.stream.converter.ByteByteValueConverter;
import ch.psi.daq.data.stream.converter.ByteValueConverter;

public class BoolArrayByteConverter extends AbstractByteArrayConverter<boolean[], boolean[], Byte> {
	private static final Logger LOGGER = Logger.getLogger(BoolArrayByteConverter.class.getName());
	private static final ByteByteValueConverter BYTE_BUFFER_CONVERTER = new ByteByteValueConverter();

	@Override
	public boolean[] convert(ByteBuffer bytes, ByteOrder endianness) {
		boolean[] values = new boolean[this.getArraySize(bytes, endianness)];

		this.convert(bytes, endianness, values);

		return values;
	}

	@Override
	public int convert(ByteBuffer bytes, ByteOrder endianness, boolean[] values) {
		ByteBuffer buf = bytes.order(endianness);

		int i = 0;
		final int startBitPos = 7;
		int bitPos = startBitPos;
		int pos = bytes.position() + Integer.BYTES;
		byte byteVal = 0;

		for (; i < values.length && pos < bytes.limit(); ++i) {
			if (bitPos == startBitPos) {
				byteVal = buf.get(pos);
			}

			values[i] = (byteVal & (1 << bitPos)) != 0;

			bitPos--;
			if (bitPos < 0) {
				pos += Byte.BYTES;
				bitPos = startBitPos;
			}
		}

		return i;
	}

	@Override
	public ByteBuffer convertObject(Object value, ByteOrder endianness) {
		if (value instanceof boolean[]) {
			return this.convert((boolean[]) value, endianness);
		} else {
			String message = String.format("Could not cast '%s' into '%s'", value.getClass().getName(), boolean[].class.getName());
			LOGGER.log(Level.SEVERE, message);
			throw new RuntimeException(message);
		}
	}

	@Override
	public ByteBuffer convert(boolean[] value, ByteOrder endianness) {
		return convertBytes(value, endianness);
	}

	public static ByteBuffer convertBytes(boolean[] value, ByteOrder endianness) {
		int nrBytes = (int) Math.ceil((double) value.length / Byte.SIZE);

		ByteBuffer ret = ByteBuffer.allocate(Integer.BYTES + nrBytes).order(endianness);
		ret.putInt(0, value.length);

		final int startBitPos = 7;
		int bitPos = startBitPos;
		int pos = Integer.BYTES;
		byte byteVal = 0;

		for (int i = 0; i < value.length; ++i) {
			if (value[i]) {
				byteVal |= (1 << bitPos);
			}

			bitPos--;
			if (bitPos < 0) {
				ret.put(pos, byteVal);

				pos += Byte.BYTES;
				byteVal = 0;
				bitPos = startBitPos;
			}
		}
		if (bitPos != startBitPos) {
			ret.put(pos, byteVal);
		}

		return ret;
	}

	@Override
	public int getBytes() {
		// TODO: Should we save one boolean per byte (not very mermory efficient
		// but e.g. Protocol Buffers does it like this)?
		return ByteConverter.DYNAMIC_NUMBER_OF_BYTES;
	}

	@Override
	public Class<boolean[]> getValueClass() {
		return boolean[].class;
	}

	@Override
	public Class<boolean[]> getSignedValueClass() {
		return this.getValueClass();
	}

	@Override
	public boolean[] convertAsSigned(ByteBuffer bytes, ByteOrder endianness) {
		return this.convert(bytes, endianness);
	}

	@Override
	public ByteBuffer convertAsSigned(boolean[] value, ByteOrder endianness) {
		return convertBytes(value, endianness);
	}

	@Override
	public int getArraySize(ByteBuffer bytes, ByteOrder endianness) throws UnsupportedOperationException {
		ByteBuffer buf = bytes.order(endianness);
		return buf.getInt(bytes.position());
	}

	@Override
	public ByteValueConverter<Byte> getByteValueConverter() {
		return BYTE_BUFFER_CONVERTER;
	}
	
	public int getStartIndex(ByteBuffer bytes) {
		// add due to size int at beginning
		return bytes.position() + Integer.BYTES;
	}
}
