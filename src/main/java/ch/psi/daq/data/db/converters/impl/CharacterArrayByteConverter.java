package ch.psi.daq.data.db.converters.impl;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.logging.Level;
import java.util.logging.Logger;

import ch.psi.daq.data.db.converters.AbstractByteArrayConverter;
import ch.psi.daq.data.stream.converter.ByteValueConverter;
import ch.psi.daq.data.stream.converter.CharacterByteValueConverter;

// See for charset stuff: http://examples.javacodegeeks.com/core-java/nio/charbuffer/convert-between-character-set-encodings-with-charbuffer/
public class CharacterArrayByteConverter extends AbstractByteArrayConverter<char[], char[], Character> {
	private static final Logger LOGGER = Logger.getLogger(CharacterArrayByteConverter.class.getName());
	private static final CharacterByteValueConverter BYTE_BUFFER_CONVERTER = new CharacterByteValueConverter();

	@Override
	public char[] convert(ByteBuffer bytes, ByteOrder endianness) {
		char[] values = new char[bytes.remaining() / getNBytes()];
		this.convert(bytes, endianness, values);
		return values;
	}

	@Override
	public int convert(ByteBuffer bytes, ByteOrder endianness, char[] values) {
		int length = Math.min(values.length, bytes.remaining() / getNBytes());
		bytes.order(endianness).asCharBuffer().get(values);

		return length;
	}

	@Override
	public ByteBuffer convertObject(Object value, ByteOrder endianness) {
		if (value instanceof char[]) {
			return this.convert((char[]) value, endianness);
		} else {
			String message = String.format("Could not cast '%s' into '%s'", value.getClass().getName(), char[].class.getName());
			LOGGER.log(Level.SEVERE, message);
			throw new RuntimeException(message);
		}
	}

	@Override
	public ByteBuffer convert(char[] value, ByteOrder endianness) {
		return convertBytes(value, endianness);
	}

	public static ByteBuffer convertBytes(char[] value, ByteOrder endianness) {
		ByteBuffer ret = ByteBuffer.allocate(value.length * getNBytes()).order(endianness);
		ret.asCharBuffer().put(value);
		return ret;
	}

	@Override
	public int getBytes() {
		return getNBytes();
	}

	protected static int getNBytes() {
		return Character.BYTES;
	}

	@Override
	public Class<char[]> getValueClass() {
		return char[].class;
	}

	@Override
	public Class<char[]> getSignedValueClass() {
		return this.getValueClass();
	}

	@Override
	public char[] convertAsSigned(ByteBuffer bytes, ByteOrder endianness) {
		return this.convert(bytes, endianness);
	}

	@Override
	public ByteBuffer convertAsSigned(char[] value, ByteOrder endianness) {
		return convertBytes(value, endianness);
	}

	@Override
	public int getArraySize(ByteBuffer bytes, ByteOrder endianness) throws UnsupportedOperationException {
		return bytes.remaining() / getBytes();
	}

	@Override
	public ByteValueConverter<Character> getByteValueConverter() {
		return BYTE_BUFFER_CONVERTER;
	}
}
