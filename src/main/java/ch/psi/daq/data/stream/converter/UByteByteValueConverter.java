package ch.psi.daq.data.stream.converter;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;

public class UByteByteValueConverter implements ByteValueConverter<Short> {
	private static final Logger LOGGER = Logger.getLogger(UByteByteValueConverter.class.getName());
	
	public static short convertVal(byte value) {
		return (short) (value & 0xff);
	}
	
	public static byte convertVal(short value) {
		return (byte) value;
	}

	@Override
	public int getBytes() {
		return Byte.BYTES;
	}
	
	public short getAsNative(ByteBuffer buf, int index) {
		return convertVal(buf.get(index));
	}
	
	@Override
	public boolean getAsBoolean(ByteBuffer buf, int index) {
		LOGGER.log(Level.FINE, () -> "It is not recommended to convert unsigned byte to boolean");
		return this.getAsNative(buf, index) != 0;
	}

	@Override
	public byte getAsByte(ByteBuffer buf, int index) {
		LOGGER.log(Level.FINE, () -> "It is not recommended to convert unsigned byte to byte");
		return (byte) this.getAsNative(buf, index);
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
	public Short get(ByteBuffer buf, int index) {
		return this.getAsNative(buf, index);
	}
}
