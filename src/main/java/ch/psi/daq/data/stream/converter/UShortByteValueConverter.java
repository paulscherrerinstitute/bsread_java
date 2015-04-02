package ch.psi.daq.data.stream.converter;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;

public class UShortByteValueConverter implements ByteValueConverter<Integer> {
	private static final Logger LOGGER = Logger.getLogger(UShortByteValueConverter.class.getName());

	
	public static int convertVal(short value) {
		return value & 0xffff;
	}

	public static short convertVal(int value) {
		return (short) value;
	}
	
	@Override
	public int getBytes() {
		return Short.BYTES;
	}

	public int getAsNative(ByteBuffer buf, int index) {
		return convertVal(buf.getShort(index));
	}
	
	@Override
	public boolean getAsBoolean(ByteBuffer buf, int index) {
		LOGGER.log(Level.FINE, () -> "It is not recommended to convert unsigned short to boolean");
		return this.getAsNative(buf, index) != 0;
	}

	@Override
	public byte getAsByte(ByteBuffer buf, int index) {
		LOGGER.log(Level.FINE, () -> "It is not recommended to convert unsigned short to byte");
		return (byte) this.getAsNative(buf, index);
	}

	@Override
	public short getAsShort(ByteBuffer buf, int index) {
		LOGGER.log(Level.FINE, () -> "It is not recommended to convert unsigned short to short");
		return (short) this.getAsNative(buf, index);
	}
	
	@Override
	public char getAsChar(ByteBuffer buf, int index) {
		return (char) this.getAsNative(buf, index);
	}

	@Override
	public int getAsInt(ByteBuffer buf, int index) {
		return this.getAsNative(buf, index);
	}

	@Override
	public long getAsLong(ByteBuffer buf, int index) {
		return this.getAsNative(buf, index);
	}

	@Override
	public float getAsFloat(ByteBuffer buf, int index) {
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
	public Integer get(ByteBuffer buf, int index) {
		return this.getAsNative(buf, index);
	}
}
