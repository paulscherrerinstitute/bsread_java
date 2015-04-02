package ch.psi.daq.data.stream.converter;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;

public class UIntegerByteValueConverter implements ByteValueConverter<Long> {
	private static final Logger LOGGER = Logger.getLogger(UIntegerByteValueConverter.class.getName());

	public static long convertVal(int value) {
		return value & 0xffffffffL;
	}

	public static int convertVal(long value) {
		return (int) value;
	}
	
	@Override
	public int getBytes() {
		return Integer.BYTES;
	}

	public long getAsNative(ByteBuffer buf, int index) {
		return convertVal(buf.getInt(index));
	}

	@Override
	public boolean getAsBoolean(ByteBuffer buf, int index) {
		LOGGER.log(Level.FINE, () -> "It is not recommended to convert unsigned int to boolean");
		return this.getAsNative(buf, index) != 0;
	}
	
	@Override
	public byte getAsByte(ByteBuffer buf, int index) {
		LOGGER.log(Level.FINE, () -> "It is not recommended to convert unsigned int to byte");
		return (byte) this.getAsNative(buf, index);
	}

	@Override
	public short getAsShort(ByteBuffer buf, int index) {
		LOGGER.log(Level.FINE, () -> "It is not recommended to convert unsigned int to short");
		return (short) this.getAsNative(buf, index);
	}
	
	@Override
	public char getAsChar(ByteBuffer buf, int index) {
		LOGGER.log(Level.FINE, () -> "It is not recommended to convert unsigned int to char");
		return (char) this.getAsNative(buf, index);
	}

	@Override
	public int getAsInt(ByteBuffer buf, int index) {
		LOGGER.log(Level.FINE, () -> "It is not recommended to convert unsigned int to int");
		return (int) this.getAsNative(buf, index);
	}

	@Override
	public long getAsLong(ByteBuffer buf, int index) {
		return this.getAsNative(buf, index);
	}

	@Override
	public float getAsFloat(ByteBuffer buf, int index) {
		LOGGER.log(Level.FINE, () -> "It is not recommended to convert unsigned int to float");
		return this.getAsNative(buf, index);
	}

	@Override
	public double getAsDouble(ByteBuffer buf, int index) {
		return this.getAsNative(buf, index);
	}

	@Override
	public BigInteger getAsBigInteger(ByteBuffer buf, int index) {
		return BigInteger.valueOf(this.getAsNative(buf, index));
	}

	@Override
	public Number getAsNumber(ByteBuffer buf, int index) {
		return this.getAsNative(buf, index);
	}
	
	@Override
	public String getAsString(ByteBuffer buf, int index) {
		return this.getAsNumber(buf, index).toString();
	}

	@Override
	public Object getAsObject(ByteBuffer buf, int index) {
		return this.getAsNative(buf, index);
	}
	
	@Override
	public Long get(ByteBuffer buf, int index) {
		return this.getAsNative(buf, index);
	}
}
