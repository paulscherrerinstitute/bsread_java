package ch.psi.bsread.message;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.Supplier;

import ch.psi.bsread.common.allocator.ThreadLocalByteArrayAllocator;

public class ValueImpl<V> implements Value<V> {
	private static final long serialVersionUID = -3889961098156334653L;
	public static final long DEFAULT_TIMEOUT_IN_MILLIS = 30000;
	private static final ThreadLocalByteArrayAllocator TMP_SERIALIZATION_ALLOCATOR = new ThreadLocalByteArrayAllocator();

	private static final byte IS_JAVA_VALUE_POSITION = 0;
	private static final byte DIRECT_POSITION = 1;
	private static final byte ORDER_POSITION = 2;

	private transient Supplier<V> value;
	private Timestamp timestamp;

	public ValueImpl() {
	}

	public ValueImpl(Supplier<V> value, Timestamp timestamp) {
		this.value = value;
		this.timestamp = timestamp;
	}

	public ValueImpl(V value, Timestamp timestamp) {
		this.setValue(value);
		this.timestamp = timestamp;
	}

	public void setTimestamp(Timestamp timestamp) {
		this.timestamp = timestamp;
	}

	public Timestamp getTimestamp() {
		return timestamp;
	}

	public void setValue(Supplier<V> value) {
		this.value = value;
	}

	public void setValue(V value) {
		this.value = ()->value;
	}

	public V getValue() {
		return value.get();
	}

	public <W> W getValue(Class<W> clazz) {
		Object value = getValue();
		if (clazz.isAssignableFrom(value.getClass())) {
			return clazz.cast(value);
		} else {
			throw new ClassCastException("Cast from '" + value.getClass().getName() + "' to '" + clazz.getClass().getName() + "' not possible.");
		}
	}

	public <W> W getValueOrDefault(Class<W> clazz, W defaultValue) {
		Object val = getValue();
		if (clazz.isAssignableFrom(val.getClass())) {
			return clazz.cast(val);
		} else {
			return defaultValue;
		}
	}

	/*
	 * This method is called through reflection when it is serialized. See
	 * {@link ObjectOutputStream#writeSerialData} and the private constructor
	 * {@link ObjectStreamClass#ObjectStreamClass(Class cl)}.
	 */
	private void writeObject(ObjectOutputStream oos) throws IOException {
		// default serialization (all fields except ByteBuffer)
		oos.defaultWriteObject();

		Object val = getValue();
		byte descriptor = 0;
		if (val instanceof ByteBuffer) {
			ByteBuffer byteBuffer = (ByteBuffer) val;

			if (byteBuffer.isDirect()) {
				descriptor = ValueImpl.setPosition(descriptor, DIRECT_POSITION);
			}
			if (ByteOrder.LITTLE_ENDIAN.equals(byteBuffer.order())) {
				descriptor = ValueImpl.setPosition(descriptor, ORDER_POSITION);
			}

			oos.writeByte(descriptor);
			oos.writeInt(byteBuffer.remaining());

			if (byteBuffer.hasArray()) {
				oos.write(byteBuffer.array(), byteBuffer.position(), byteBuffer.remaining());
			} else {
				byte[] bytes = TMP_SERIALIZATION_ALLOCATOR.apply(byteBuffer.remaining());
				// bulk methods are way faster than reading/writing single bytes
				byteBuffer.duplicate().get(bytes, 0, byteBuffer.remaining());
				oos.write(bytes, 0, byteBuffer.remaining());
			}
		} else {
			descriptor = ValueImpl.setPosition(descriptor, IS_JAVA_VALUE_POSITION);
			oos.writeByte(descriptor);
			oos.writeObject(val);
		}
	}

	/*
	 * This method is called through reflection when it is deserialized. See
	 * {@link ObjectInputStream#readSerialData} and the private constructor
	 * {@link ObjectStreamClass#ObjectStreamClass(Class cl)}.
	 */
	@SuppressWarnings("unchecked")
	private void readObject(ObjectInputStream ois)
			throws ClassNotFoundException, IOException {
		// default deserialization (all fields except ByteBuffer)
		ois.defaultReadObject();

		byte descriptor = ois.readByte();
		if (!ValueImpl.isPositionSet(descriptor, IS_JAVA_VALUE_POSITION)) {
			int size = ois.readInt();
			ByteBuffer byteBuffer;

			boolean isDirect = ValueImpl.isPositionSet(descriptor, DIRECT_POSITION);
			ByteOrder byteOrder =
					ValueImpl.isPositionSet(descriptor, ORDER_POSITION) ? ByteOrder.LITTLE_ENDIAN
							: ByteOrder.BIG_ENDIAN;

			if (isDirect) {
				byteBuffer = ByteBuffer.allocateDirect(size);
			} else {
				byteBuffer = ByteBuffer.allocate(size);
			}
			byteBuffer.order(byteOrder);

			if (byteBuffer.hasArray()) {
				ois.read(byteBuffer.array());
			} else {
				byte[] valBytes = TMP_SERIALIZATION_ALLOCATOR.apply(size);
				// bulk methods are way faster than reading/writing single bytes
				ois.read(valBytes, 0, size);
				byteBuffer.put(valBytes, 0, size);
				// make ready for read
				byteBuffer.flip();
			}

			this.setValue((V) byteBuffer);
		} else {
			this.setValue((V) ois.readObject());
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
