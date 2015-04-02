package ch.psi.daq.data.db.converters.impl;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.logging.Level;
import java.util.logging.Logger;

import ch.psi.daq.data.db.converters.AbstractByteConverter;
import ch.psi.daq.data.stream.converter.ByteValueConverter;
import ch.psi.daq.data.stream.converter.CharacterByteValueConverter;

public class CharacterByteConverter extends AbstractByteConverter<Character, Character, Character> {
	private static final Logger LOGGER = Logger.getLogger(CharacterByteConverter.class.getName());
	private static final CharacterByteValueConverter BYTE_BUFFER_CONVERTER = new CharacterByteValueConverter();

	@Override
	public Character convert(ByteBuffer bytes, ByteOrder endianness) {
		return convertBytes(bytes, endianness);
	}

	public static char convertBytes(ByteBuffer bytes, ByteOrder endianness) {
		return bytes.order(endianness).asCharBuffer().get();
	}

	@Override
	public ByteBuffer convertObject(Object value, ByteOrder endianness) {
		if (value instanceof Character) {
			return this.convert((Character) value, endianness);
		} else {
			String message = String.format("Could not cast '%s' into '%s'", value.getClass().getName(), Character.class.getName());
			LOGGER.log(Level.SEVERE, message);
			throw new RuntimeException(message);
		}
	}

	@Override
	public ByteBuffer convert(Character value, ByteOrder endianness) {
		return this.convert(value.charValue(), endianness);
	}

	public ByteBuffer convert(char value, ByteOrder endianness) {
		return convertBytes(value, endianness);
	}

	public static ByteBuffer convertBytes(char value, ByteOrder endianness) {
		ByteBuffer ret = ByteBuffer.allocate(getNBytes()).order(endianness);
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
	public Class<Character> getValueClass() {
		return Character.class;
	}
	
	@Override
	public Class<Character> getSignedValueClass() {
		return this.getValueClass();
	}

	@Override
	public Character convertAsSigned(ByteBuffer valueBytes, ByteOrder endianness) {
		return convertBytes(valueBytes, endianness);
	}
	
	public char convertAsSignedPrim(ByteBuffer valueBytes, ByteOrder endianness) {
		return convertBytes(valueBytes, endianness);
	}
	
	@Override
	public ByteBuffer convertAsSigned(Character value, ByteOrder endianness) {
		return this.convertAsSigned(value.charValue(), endianness);
	}

	public ByteBuffer convertAsSigned(char value, ByteOrder endianness) {
		return convertBytes(value, endianness);
	}
	
	@Override
	public ByteValueConverter<Character> getByteValueConverter() {
		return BYTE_BUFFER_CONVERTER;
	}
}
