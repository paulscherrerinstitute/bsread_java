package ch.psi.bsread.converter;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import ch.psi.bsread.message.ChannelConfig;

/**
 * This is a convenience class for not being dependent to byte_converters
 * package that should be usually used to serialize/de-serialize values to bytes
 * However we copied the basic functionality here to be able to remove the
 * dependency to be able to run this code in Matlab (Matlab 2015a runs on Java
 * 1.7) as well.
 * 
 * Once the Matlab release supports Java 1.8 this class might be remove in favor
 * of the byte_converters package!
 */
public class MatlabByteConverter extends AbstractByteConverter {

	@SuppressWarnings("unchecked")
	@Override
	public <V> V getValue(ByteBuffer byteValue, ChannelConfig config) {
		type = type.toLowerCase();
		final boolean array = isArray(shape);

		switch (type) {
		case "int8":
			if (array) {
				if (byteValue.hasArray()) {
					//TODO: Clone array?
					return (V) byteValue.array();
				} else {
					byte[] values = new byte[byteValue.remaining() / Byte.BYTES];
					byteValue.duplicate().get(values);
					return (V) values;
				}
			}
			else {
				return (V) ((Byte) byteValue.duplicate().get());
			}
		case "int16":
			if (array) {
				short[] values = new short[byteValue.remaining() / Short.BYTES];
				byteValue.asShortBuffer().get(values);
				return (V) values;
			}
			else {
				return (V) ((Short) byteValue.asShortBuffer().get());
			}
		case "int32":
			if (array) {
				int[] values = new int[byteValue.remaining() / Integer.BYTES];
				byteValue.asIntBuffer().get(values);
				return (V) values;
			}
			else {
				return (V) ((Integer) byteValue.asIntBuffer().get());
			}
		case "int64":
			if (array) {
				long[] values = new long[byteValue.remaining() / Long.BYTES];
				byteValue.asLongBuffer().get(values);
				return (V) values;
			}
			else {
				return (V) ((Long) byteValue.asLongBuffer().get());
			}
		case "float32":
			if (array) {
				float[] values = new float[byteValue.remaining() / Float.BYTES];
				byteValue.asFloatBuffer().get(values);
				return (V) values;
			}
			else {
				return (V) ((Float) byteValue.asFloatBuffer().get());
			}
		case "float64":
			if (array) {
				double[] values = new double[byteValue.remaining() / Double.BYTES];
				byteValue.asDoubleBuffer().get(values);
				return (V) values;
			}
			else {
				return (V) ((Double) byteValue.asDoubleBuffer().get());
			}
		case "string":
			return (V) new String(byteValue.array());

		default:
			throw new RuntimeException("Type " + type + " not supported");
		}

	}

	@Override
	public ByteBuffer getBytes(Object value, ChannelConfig config) {
		ByteBuffer buffer;

		if (value instanceof byte[]) {
			// TODO: Clone value?
			buffer = ByteBuffer.wrap((byte[]) value).order(byteOrder);
		}
		else if (value instanceof Byte) {
			buffer = ByteBuffer.allocate(Byte.BYTES).order(byteOrder);
			buffer.duplicate().put((Byte) value);
		}
		else if (value instanceof short[]) {
			buffer = ByteBuffer.allocate(((short[]) value).length * Short.BYTES).order(byteOrder);
			buffer.asShortBuffer().put((short[]) value);
		}
		else if (value instanceof Short) {
			buffer = ByteBuffer.allocate(Short.BYTES).order(byteOrder);
			buffer.asShortBuffer().put((Short) value);
		}
		else if (value instanceof int[]) {
			buffer = ByteBuffer.allocate(((int[]) value).length * Integer.BYTES).order(byteOrder);
			buffer.asIntBuffer().put((int[]) value);
		}
		else if (value instanceof Integer) {
			buffer = ByteBuffer.allocate(Integer.BYTES).order(byteOrder);
			buffer.asIntBuffer().put((Integer) value);
		}
		else if (value instanceof long[]) {
			buffer = ByteBuffer.allocate(((long[]) value).length * Long.BYTES).order(byteOrder);
			buffer.asLongBuffer().put((long[]) value);
		}
		else if (value instanceof Long) {
			buffer = ByteBuffer.allocate(Long.BYTES).order(byteOrder);
			buffer.asLongBuffer().put((Long) value);
		}
		else if (value instanceof float[]) {
			buffer = ByteBuffer.allocate(((float[]) value).length * Float.BYTES).order(byteOrder);
			buffer.asFloatBuffer().put((float[]) value);
		}
		else if (value instanceof Float) {
			buffer = ByteBuffer.allocate(Float.BYTES).order(byteOrder);
			buffer.asFloatBuffer().put((Float) value);
		}
		else if (value instanceof double[]) {
			buffer = ByteBuffer.allocate(((double[]) value).length * Double.BYTES).order(byteOrder);
			buffer.asDoubleBuffer().put((double[]) value);
		}
		else if (value instanceof Double) {
			buffer = ByteBuffer.allocate(Double.BYTES).order(byteOrder);
			buffer.asDoubleBuffer().put((Double) value);
		}
		else if (value instanceof String) {
			return ByteBuffer.wrap(((String) value).getBytes());
		}
		else {
			throw new RuntimeException("Type " + value.getClass().getName() + " not supported");
		}

		return buffer;
	}
}
