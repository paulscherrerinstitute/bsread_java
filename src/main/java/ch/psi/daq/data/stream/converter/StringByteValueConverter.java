package ch.psi.daq.data.stream.converter;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.Logger;

import ch.psi.daq.data.db.converters.ByteConverter;

public class StringByteValueConverter implements ByteValueConverter<String> {
	private static final Logger LOGGER = Logger.getLogger(StringByteValueConverter.class.getName());

	private Charset charset;

	public StringByteValueConverter() {
		this(StandardCharsets.UTF_8);
	}

	public StringByteValueConverter(Charset charset) {
		this.charset = charset;
	}

	@Override
	public int getBytes() {
		// TODO: Does it depend on the Charset?
		return ByteConverter.DYNAMIC_NUMBER_OF_BYTES;
	}

	public String getAsNative(ByteBuffer buf, int index) {
		return charset.decode(buf.duplicate()).toString();

		// The Charset should take care of the ByteOrder if necessary (e.g.
		// UTF-16BE, UTF-16LE)
		// return new String(bytes.array(), this.charset);
	}

	@Override
	public boolean getAsBoolean(ByteBuffer buf, int index) {
		String message = "Unsupported conversion from String to byte";
		LOGGER.log(Level.SEVERE, message);
		throw new UnsupportedOperationException(message);
	}

	@Override
	public byte getAsByte(ByteBuffer buf, int index) {
		String message = "Unsupported conversion from String to byte";
		LOGGER.log(Level.SEVERE, message);
		throw new UnsupportedOperationException(message);
	}

	@Override
	public short getAsShort(ByteBuffer buf, int index) {
		String message = "Unsupported conversion from String to short";
		LOGGER.log(Level.SEVERE, message);
		throw new UnsupportedOperationException(message);
	}

	@Override
	public char getAsChar(ByteBuffer buf, int index) {
		String message = "Unsupported conversion from String to char";
		LOGGER.log(Level.SEVERE, message);
		throw new UnsupportedOperationException(message);
	}

	@Override
	public int getAsInt(ByteBuffer buf, int index) {
		String message = "Unsupported conversion from String to int";
		LOGGER.log(Level.SEVERE, message);
		throw new UnsupportedOperationException(message);
	}

	@Override
	public long getAsLong(ByteBuffer buf, int index) {
		String message = "Unsupported conversion from String to long";
		LOGGER.log(Level.SEVERE, message);
		throw new UnsupportedOperationException(message);
	}

	@Override
	public float getAsFloat(ByteBuffer buf, int index) {
		String message = "Unsupported conversion from String to float";
		LOGGER.log(Level.SEVERE, message);
		throw new UnsupportedOperationException(message);
	}

	@Override
	public double getAsDouble(ByteBuffer buf, int index) {
		String message = "Unsupported conversion from String to double";
		LOGGER.log(Level.SEVERE, message);
		throw new UnsupportedOperationException(message);
	}

	@Override
	public BigInteger getAsBigInteger(ByteBuffer buf, int index) {
		String message = "Unsupported conversion from String to BigInteger";
		LOGGER.log(Level.SEVERE, message);
		throw new UnsupportedOperationException(message);
	}

	@Override
	public String getAsString(ByteBuffer buf, int index) {
		return this.getAsNative(buf, index);
	}

	@Override
	public Number getAsNumber(ByteBuffer buf, int index) {
		String message = "Unsupported conversion from String to Number";
		LOGGER.log(Level.SEVERE, message);
		throw new UnsupportedOperationException(message);
	}

	@Override
	public Object getAsObject(ByteBuffer buf, int index) {
		return this.getAsNative(buf, index);
	}
	
	@Override
	public String get(ByteBuffer buf, int index) {
		return this.getAsNative(buf, index);
	}
}
