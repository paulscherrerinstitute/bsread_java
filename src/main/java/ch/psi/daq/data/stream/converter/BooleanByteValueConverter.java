package ch.psi.daq.data.stream.converter;

import java.math.BigInteger;
import java.nio.ByteBuffer;

public class BooleanByteValueConverter implements ByteValueConverter<Boolean> {
	private static final int BOOLEAN_POSITION = 0;

	public static boolean convertVal(byte val) {
		return (val & (1 << BOOLEAN_POSITION)) != 0;
	}

	public static byte convertVal(boolean val) {
		byte bVal = 0;
		if (val) {
			bVal |= (1 << BOOLEAN_POSITION);
		}
		return bVal;
	}

	@Override
	public int getBytes() {
		return Byte.BYTES;
	}

	public byte getAsNative(ByteBuffer buf, int index) {
		return buf.get(index);
	}

	// NOTE: This only works properly for boolean arrays which use one byte per
	// boolean value (one byte could represent 8 booleans).
	@Override
	public boolean getAsBoolean(ByteBuffer buf, int index) {
		return this.getAsNative(buf, index) != 0;
	}

	@Override
	public byte getAsByte(ByteBuffer buf, int index) {
		return this.getAsNative(buf, index);
	}

	@Override
	public short getAsShort(ByteBuffer buf, int index) {
		return this.getAsNative(buf, index);
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
	public Boolean get(ByteBuffer buf, int index) {
		return this.getAsBoolean(buf, index);
	}
}
