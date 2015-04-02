package ch.psi.daq.data.db.converters.impl;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import ch.psi.daq.data.db.converters.AbstractByteConverter;
import ch.psi.daq.data.stream.converter.ByteValueConverter;
import ch.psi.daq.data.stream.converter.StringByteValueConverter;

public class StringByteConverter extends AbstractByteConverter<String, String, String> {
	private static final Logger LOGGER = Logger.getLogger(StringByteConverter.class.getName());

	private final Charset charset;
	private final StringByteValueConverter byteBufferConverter;

	public StringByteConverter() {
		this(StandardCharsets.UTF_8);
	}

	public StringByteConverter(Charset charset) {
		this.charset = charset;
		this.byteBufferConverter = new StringByteValueConverter(charset);
	}

	@Override
	public String convert(ByteBuffer bytes, ByteOrder endianness) {
		return this.convert(bytes, endianness, this.charset);
	}

	public String convert(ByteBuffer bytes, ByteOrder endianness, Charset charset) {
		return this.byteBufferConverter.getAsNative(bytes.order(endianness), bytes.position());
	}

	@Override
	public ByteBuffer convertObject(Object value, ByteOrder endianness) {
		if (value instanceof String) {
			return this.convert((String) value, endianness);
		} else {
			String message = String.format("Could not cast '%s' into '%s'", value.getClass().getName(), String.class.getName());
			LOGGER.log(Level.SEVERE, message);
			throw new RuntimeException(message);
		}
	}

	@Override
	public ByteBuffer convert(String value, ByteOrder endianness) {
		return convertBytes(value, endianness, this.charset);
	}

	public static ByteBuffer convertBytes(String value, ByteOrder endianness, Charset charset) {
		// return charset.encode(CharBuffer.wrap(value));

		// The Charset should take care of the ByteOrder if necessary (e.g.
		// UTF-16BE, UTF-16LE)
		return ByteBuffer.wrap(value.getBytes(charset));
	}

	@Override
	public int getBytes() {
		return this.byteBufferConverter.getBytes();
	}

	@Override
	public Class<String> getValueClass() {
		return String.class;
	}

	@Override
	public Class<String> getSignedValueClass() {
		return this.getValueClass();
	}

	@Override
	public String convertAsSigned(ByteBuffer bytes, ByteOrder endianness) {
		return this.convert(bytes, endianness);
	}

	@Override
	public ByteBuffer convertAsSigned(String value, ByteOrder endianness) {
		return this.convert(value, endianness);
	}

	@Override
	public ByteValueConverter<String> getByteValueConverter() {
		return this.byteBufferConverter;
	}

	@Override
	public IntStream getIntStream(ByteBuffer valueBytes, boolean parallel) throws UnsupportedOperationException {
		String message = String.format("'%s' does not support 'IntStream'", this.getClass().getName());
		LOGGER.log(Level.SEVERE, message);
		throw new UnsupportedOperationException(message);
	}

	@Override
	public LongStream getLongStream(ByteBuffer valueBytes, boolean parallel) throws UnsupportedOperationException {
		String message = String.format("'%s' does not support 'IntStream'", this.getClass().getName());
		LOGGER.log(Level.SEVERE, message);
		throw new UnsupportedOperationException(message);
	}

	@Override
	public DoubleStream getDoubleStream(ByteBuffer valueBytes, boolean parallel) throws UnsupportedOperationException {
		String message = String.format("'%s' does not support 'IntStream'", this.getClass().getName());
		LOGGER.log(Level.SEVERE, message);
		throw new UnsupportedOperationException(message);
	}
	
	public int getStartIndex(ByteBuffer bytes) {
		return bytes.position();
	}

	public int getEndIndex(ByteBuffer bytes) {
		return bytes.limit();
	}

	public int splitIndex(ByteBuffer bytes, int startIndex, int endIndex) {
		return startIndex;
	}

	public int nextIndex(ByteBuffer bytes, int index) {
		return bytes.limit();
	}

	public int estimateSize(ByteBuffer bytes, int startIndex, int endIndex) {
		return startIndex < endIndex ? 1 : 0;
	}
}
