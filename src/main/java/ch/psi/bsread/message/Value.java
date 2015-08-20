package ch.psi.bsread.message;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class Value implements Serializable {
	private static final long serialVersionUID = -3889961098156334653L;
	private static final byte DIRECT_POSITION = 0;
	private static final byte ORDER_POSITION = 1;

	private transient ByteBuffer value;
	private Timestamp timestamp;

	public Value() {
	}

	public Value(ByteBuffer value, Timestamp timestamp) {
		this.value = value;
		this.timestamp = timestamp;
	}

	public void setTimestamp(Timestamp timestamp) {
		this.timestamp = timestamp;
	}

	public Timestamp getTimestamp() {
		return timestamp;
	}

	public void setValue(ByteBuffer value) {
		this.value = value;
	}

	public ByteBuffer getValue() {
		return value;
	}

	/*
	 * This method is called through reflection when it is serialized. See
	 * {@link ObjectOutputStream#writeSerialData} and the private constructor
	 * {@link ObjectStreamClass#ObjectStreamClass(Class cl)}.
	 */
	private void writeObject(ObjectOutputStream oos) throws IOException {
		// default serialization (all fields except ByteBuffer)
		oos.defaultWriteObject();

		byte descriptor = 0;
		if (value.isDirect()) {
			descriptor = Value.setPosition(descriptor, DIRECT_POSITION);
		}
		if (ByteOrder.LITTLE_ENDIAN.equals(value.order())) {
			descriptor = Value.setPosition(descriptor, ORDER_POSITION);
		}

		oos.writeByte(descriptor);
		oos.writeInt(value.remaining());

		// TODO: should we use compression for value-bytes
		if (value.hasArray()) {
			oos.write(value.array(), value.position(), value.remaining());
		} else {
			// Is there a way to overcome the allocation if this byte[], e.g.
			// using ThreadLocal<byte[]>?
			byte[] bytes = new byte[value.remaining()];
			// bulk methods are way faster than reading/writing single bytes
			value.duplicate().get(bytes);
			oos.write(bytes);
		}
	}

	/*
	 * This method is called through reflection when it is deserialized. See
	 * {@link ObjectInputStream#readSerialData} and the private constructor
	 * {@link ObjectStreamClass#ObjectStreamClass(Class cl)}.
	 */
	private void readObject(ObjectInputStream ois)
			throws ClassNotFoundException, IOException {
		// default deserialization (all fields except ByteBuffer)
		ois.defaultReadObject();

		byte descriptor = ois.readByte();
		int size = ois.readInt();

		boolean isDirect = Value.isPositionSet(descriptor, DIRECT_POSITION);
		ByteOrder byteOrder =
				Value.isPositionSet(descriptor, ORDER_POSITION) ? ByteOrder.LITTLE_ENDIAN
						: ByteOrder.BIG_ENDIAN;

		if (isDirect) {
			value = ByteBuffer.allocateDirect(size);
		} else {
			value = ByteBuffer.allocate(size);
		}
		value.order(byteOrder);

		// TODO: should we use compression for value-bytes
		if (value.hasArray()) {
			ois.read(value.array());
		} else {
			// Is there a way to overcome the allocation if this byte[], e.g.
			// using ThreadLocal<byte[]>?
			byte[] valBytes = new byte[size];
			// bulk methods are way faster than reading/writing single bytes
			ois.read(valBytes);
			value.put(valBytes);
			// make ready for read
			value.flip();
		}
	}

	/**
	 * Sets the bit of a specific position.
	 * 
	 * @param descriptor
	 *            The initial descriptor
	 * @param position
	 *            The position to set
	 * @return int The modified descriptor
	 */
	public static byte setPosition(byte descriptor, byte position) {
		return descriptor |= (1 << position);
	}

	/**
	 * Determines if the bit of a specific position is set.
	 * 
	 * @param descriptor
	 *            The descriptor
	 * @param position
	 *            The position
	 * @return boolean <tt>true</tt> if the bit is set, <tt>false</tt> otherwise
	 */
	public static boolean isPositionSet(byte descriptor, byte position) {
		return (descriptor & (1 << position)) != 0;
	}
}
