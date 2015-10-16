package ch.psi.bsread.message;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Value<V> implements Serializable {
	private static final long serialVersionUID = -3889961098156334653L;
	private static final Logger LOGGER = LoggerFactory.getLogger(Value.class);

	private static final byte IS_JAVA_VALUE_POSITION = 0;
	private static final byte DIRECT_POSITION = 1;
	private static final byte ORDER_POSITION = 2;

	private transient CompletableFuture<V> futureValue;
	private Timestamp timestamp;

	public Value() {
	}

	public Value(CompletableFuture<V> futureValue, Timestamp timestamp) {
		this.futureValue = futureValue;
		this.timestamp = timestamp;
	}

	public Value(V value, Timestamp timestamp) {
		futureValue = CompletableFuture.completedFuture(value);
		this.timestamp = timestamp;
	}

	public void setTimestamp(Timestamp timestamp) {
		this.timestamp = timestamp;
	}

	public Timestamp getTimestamp() {
		return timestamp;
	}

	public void setFutureValue(CompletableFuture<V> futureValue) {
		this.futureValue = futureValue;
	}

	public void setValue(V value) {
		futureValue = CompletableFuture.completedFuture(value);
	}

	public V getValue() {
		try {
			return futureValue.get(30, TimeUnit.SECONDS);
		} catch (Exception e) {
			// log since exceptions can get lost (e.g.in JAVA Streams)
			LOGGER.error("Could not load value from future.", e);
			throw new RuntimeException(e);
		}
	}

	public <W> W getValue(Class<W> clazz) {
		Object val = getValue();
		if (clazz.isAssignableFrom(val.getClass())) {
			return clazz.cast(val);
		} else {
			LOGGER.warn("Cast from '{}' to '{}' not possible. Check your code!", val.getClass(), clazz,
					new RuntimeException() /* show stacktrace */);
			return null;
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
				descriptor = Value.setPosition(descriptor, DIRECT_POSITION);
			}
			if (ByteOrder.LITTLE_ENDIAN.equals(byteBuffer.order())) {
				descriptor = Value.setPosition(descriptor, ORDER_POSITION);
			}

			oos.writeByte(descriptor);
			oos.writeInt(byteBuffer.remaining());

			if (byteBuffer.hasArray()) {
				oos.write(byteBuffer.array(), byteBuffer.position(), byteBuffer.remaining());
			} else {
				// Is there a way to overcome the allocation if this byte[],
				// e.g.
				// using ThreadLocal<byte[]>?
				byte[] bytes = new byte[byteBuffer.remaining()];
				// bulk methods are way faster than reading/writing single bytes
				byteBuffer.duplicate().get(bytes);
				oos.write(bytes);
			}
		} else {
			descriptor = Value.setPosition(descriptor, IS_JAVA_VALUE_POSITION);
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
		if (!Value.isPositionSet(descriptor, IS_JAVA_VALUE_POSITION)) {
			int size = ois.readInt();
			ByteBuffer byteBuffer;

			boolean isDirect = Value.isPositionSet(descriptor, DIRECT_POSITION);
			ByteOrder byteOrder =
					Value.isPositionSet(descriptor, ORDER_POSITION) ? ByteOrder.LITTLE_ENDIAN
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
				// Is there a way to overcome the allocation if this byte[],
				// e.g. using ThreadLocal<byte[]>?
				byte[] valBytes = new byte[size];
				// bulk methods are way faster than reading/writing single bytes
				ois.read(valBytes);
				byteBuffer.put(valBytes);
				// make ready for read
				byteBuffer.flip();
			}

			futureValue = CompletableFuture.completedFuture((V) byteBuffer);
		} else {
			futureValue = CompletableFuture.completedFuture((V) ois.readObject());
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
