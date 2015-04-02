package ch.psi.daq.data.db.converters.impl;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.logging.Level;
import java.util.logging.Logger;

import ch.psi.daq.data.db.converters.AbstractByteArrayConverter;
import ch.psi.daq.data.stream.converter.ByteByteValueConverter;
import ch.psi.daq.data.stream.converter.ByteValueConverter;

public class ByteArrayByteConverter extends AbstractByteArrayConverter<byte[], byte[], Byte> {
	private static final Logger LOGGER = Logger.getLogger(ByteArrayByteConverter.class.getName());
	private static final ByteByteValueConverter BYTE_BUFFER_CONVERTER = new ByteByteValueConverter();

	@Override
	public byte[] convert(ByteBuffer bytes, ByteOrder endianness) {
		byte[] values = new byte[bytes.remaining() / getNBytes()];
		this.convert(bytes, endianness, values);
		return values;
	}

	@Override
	public int convert(ByteBuffer bytes, ByteOrder endianness, byte[] values) {
		int length = Math.min(values.length, bytes.remaining() / getNBytes());
		bytes.order(endianness).asReadOnlyBuffer().get(values);

		return length;
	}

	@Override
	public ByteBuffer convertObject(Object value, ByteOrder endianness) {
		if (value instanceof byte[]) {
			return this.convert((byte[]) value, endianness);
		} else {
			String message = String.format("Could not cast '%s' into '%s'", value.getClass().getName(), byte[].class.getName());
			LOGGER.log(Level.SEVERE, message);
			throw new RuntimeException(message);
		}
	}

	@Override
	public ByteBuffer convert(byte[] bytes, ByteOrder endianness) {
		return convertBytes(bytes, endianness);
	}

	public static ByteBuffer convertBytes(byte[] bytes, ByteOrder endianness) {
		ByteBuffer ret = ByteBuffer.allocate(bytes.length / getNBytes());
		ret.put(bytes);
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
	public Class<byte[]> getValueClass() {
		return byte[].class;
	}

	@Override
	public Class<byte[]> getSignedValueClass() {
		return this.getValueClass();
	}

	@Override
	public byte[] convertAsSigned(ByteBuffer bytes, ByteOrder endianness) {
		return this.convert(bytes, endianness);
	}

	@Override
	public ByteBuffer convertAsSigned(byte[] value, ByteOrder endianness) {
		return convertBytes(value, endianness);
	}

	@Override
	public int getArraySize(ByteBuffer bytes, ByteOrder endianness) throws UnsupportedOperationException {
		return bytes.remaining() / getBytes();
	}

	@Override
	public ByteValueConverter<Byte> getByteValueConverter() {
		return BYTE_BUFFER_CONVERTER;
	}
}
