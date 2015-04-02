package ch.psi.daq.data.stream.converter;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ULongByteValueConverter implements ByteValueConverter<BigInteger> {
	private static final Logger LOGGER = Logger.getLogger(ULongByteValueConverter.class.getName());

	public static BigInteger convertVal(long value) {
		BigInteger bigInt = BigInteger.valueOf(value & 0x7fffffffffffffffL);
		if (value < 0) {
			bigInt = bigInt.setBit(Long.SIZE - 1);
		}
		return bigInt;
	}

	public static long convertVal(BigInteger value) {
		return value.longValue();
	}
	
	@Override
	public int getBytes() {
		return Long.BYTES;
	}

	public BigInteger getAsNative(ByteBuffer buf, int index) {
		return convertVal(buf.getLong(index));
	}
	
	@Override
	public boolean getAsBoolean(ByteBuffer buf, int index) {
		LOGGER.log(Level.FINE, () -> "It is not recommended to convert unsigned long to boolean");
		return this.getAsNative(buf, index).longValue() != 0;
	}

	@Override
	public byte getAsByte(ByteBuffer buf, int index) {
		LOGGER.log(Level.FINE, () -> "It is not recommended to convert unsinged long to byte");
		return this.getAsNative(buf, index).byteValue();
	}

	@Override
	public short getAsShort(ByteBuffer buf, int index) {
		LOGGER.log(Level.FINE, () -> "It is not recommended to convert unsinged long to short");
		return this.getAsNative(buf, index).shortValue();
	}
	
	@Override
	public char getAsChar(ByteBuffer buf, int index) {
		LOGGER.log(Level.FINE, () -> "It is not recommended to convert unsigned long to char");
		return (char) this.getAsNative(buf, index).shortValue();
	}

	@Override
	public int getAsInt(ByteBuffer buf, int index) {
		LOGGER.log(Level.FINE, () -> "It is not recommended to convert unsinged long to int");
		return (int) this.getAsNative(buf, index).intValue();
	}

	@Override
	public long getAsLong(ByteBuffer buf, int index) {
		LOGGER.log(Level.FINE, () -> "It is not recommended to convert unsinged long to long");
		return this.getAsNative(buf, index).longValue();
	}

	@Override
	public float getAsFloat(ByteBuffer buf, int index) {
		LOGGER.log(Level.FINE, () -> "It is not recommended to convert unsinged long to float");
		return this.getAsNative(buf, index).floatValue();
	}

	@Override
	public double getAsDouble(ByteBuffer buf, int index) {
		return this.getAsNative(buf, index).doubleValue();
	}

	@Override
	public BigInteger getAsBigInteger(ByteBuffer buf, int index) {
		return this.getAsNative(buf, index);
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
	public BigInteger get(ByteBuffer buf, int index) {
		return this.getAsNative(buf, index);
	}
}
