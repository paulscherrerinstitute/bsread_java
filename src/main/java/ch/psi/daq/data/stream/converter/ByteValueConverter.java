package ch.psi.daq.data.stream.converter;

import java.math.BigInteger;
import java.nio.ByteBuffer;

/**
 * This interface provides the possibility to convert byte representations into
 * primitive types (no Wrapper meaning no boxing/unboxing overhead) and thus to
 * create primitive JAVA 8 streams without an additional conversion step into
 * the actual array type (or into a Collection containing their wrappers).
 */
public interface ByteValueConverter<T> {
	
	/**
	 * The number of bytes used to represent a value.
	 *
	 * @return int The number of bytes
	 */
	public int getBytes();
	
	public boolean getAsBoolean(ByteBuffer buf, int index);

	public byte getAsByte(ByteBuffer buf, int index);

	public short getAsShort(ByteBuffer buf, int index);

	public char getAsChar(ByteBuffer buf, int index);

	public int getAsInt(ByteBuffer buf, int index);

	public long getAsLong(ByteBuffer buf, int index);

	public float getAsFloat(ByteBuffer buf, int index);

	public double getAsDouble(ByteBuffer buf, int index);

	public BigInteger getAsBigInteger(ByteBuffer buf, int index);

	public Number getAsNumber(ByteBuffer buf, int index);

	public String getAsString(ByteBuffer buf, int index);

	public Object getAsObject(ByteBuffer buf, int index);
	
	public T get(ByteBuffer buf, int index);
}
