package ch.psi.bsread;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;


/**
 * This is a convenience class for not being dependent to byte_converters package
 * that should be usually used to serialize/de-serialize values to bytes
 * However we copied the basic functionality here to be able to remove the dependency
 * to be able to run this code in Matlab (Matlab 2015a runs on Java 1.7) as well.
 * 
 * Once the Matlab release supports Java 1.8 this class might be remove in favor of
 * the byte_converters package!
 */
public class Converter {
	
	/**
	 * Converts a given byte buffer into a specific value. 
	 * 
	 * @param byteValue
	 * @param type
	 * @param shape
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <T> T getValue(ByteBuffer byteValue, String type, int[] shape) {
		type = type.toLowerCase();
		final boolean array = isArray(shape);
		
		switch (type) {
		case "double":
			if(array){
				double[] values = new double[byteValue.remaining() / Double.BYTES];
				byteValue.asDoubleBuffer().get(values);
				return (T) values;
			}
			else{
				return (T)((Double) byteValue.asDoubleBuffer().get());
			}
		case "float":
			if(array){
				float[] values = new float[byteValue.remaining() / Float.BYTES];
				byteValue.asFloatBuffer().get(values);
				return (T) values;
			}
			else {
				return (T)((Float)byteValue.asFloatBuffer().get());
			}
		case "integer":
			if(array){
				int[] values = new int[byteValue.remaining() / Integer.BYTES];
				 byteValue.asIntBuffer().get(values);
				return (T) values;
			}
			else{
				return (T)((Integer)byteValue.asIntBuffer().get());
			}
		case "long":
			if(array){
				long[] values = new long[byteValue.remaining() / Long.BYTES];
				byteValue.asLongBuffer().get(values);
				return (T) values;
			}
			else{
				return (T)((Long)byteValue.asLongBuffer().get());
			}
		case "short":
			if(array){
				short[] values = new short[byteValue.remaining() / Short.BYTES];
				byteValue.asShortBuffer().get(values);
				return (T) values;
			}
			else{
				return (T)((Short)byteValue.asShortBuffer().get());
			}
		case "string":
			return (T) new String(byteValue.array());

		default:
			throw new RuntimeException("Type "+type+" not supported");
		}
	
	}
	
	/**
	 * Convert a given value to bytes
	 * @param value
	 * @param byteOrder
	 * @return
	 */
	public static <T> ByteBuffer getBytes(T value, ByteOrder byteOrder ){ 
		ByteBuffer buffer;
		
		if(value instanceof double[]){
			buffer = ByteBuffer.allocate(((double[])value).length * Double.BYTES).order(byteOrder);
			buffer.asDoubleBuffer().put((double[]) value);
		}
		else if(value instanceof Double){
			buffer = ByteBuffer.allocate(Double.BYTES).order(byteOrder);
			buffer.asDoubleBuffer().put((Double) value);
		}
		else if(value instanceof float[]){
			buffer = ByteBuffer.allocate(((float[])value).length * Float.BYTES).order(byteOrder);
			buffer.asFloatBuffer().put((float[]) value);
		}
		else if(value instanceof Float){
			buffer = ByteBuffer.allocate(Float.BYTES).order(byteOrder);
			buffer.asFloatBuffer().put((Float) value);
		}
		else if(value instanceof int[]){
			buffer = ByteBuffer.allocate(((int[])value).length * Integer.BYTES).order(byteOrder);
			buffer.asIntBuffer().put((int[]) value);
		}
		else if(value instanceof Integer){
			buffer = ByteBuffer.allocate(Integer.BYTES).order(byteOrder);
			buffer.asIntBuffer().put((Integer) value);
		}
		else if(value instanceof long[]){
			buffer = ByteBuffer.allocate(((long[])value).length * Long.BYTES).order(byteOrder);
			buffer.asLongBuffer().put((long[]) value);
		}
		else if(value instanceof Long){
			buffer = ByteBuffer.allocate(Long.BYTES).order(byteOrder);
			buffer.asLongBuffer().put((Long) value);
		}
		else if(value instanceof short[]){
			buffer = ByteBuffer.allocate(((short[])value).length * Short.BYTES).order(byteOrder);
			buffer.asShortBuffer().put((short[]) value);
		}
		else if(value instanceof Short){
			buffer = ByteBuffer.allocate(Short.BYTES).order(byteOrder);
			buffer.asShortBuffer().put((Short) value);
		}
		else if(value instanceof String){
			return ByteBuffer.wrap(((String) value).getBytes());
		}
		else{
			throw new RuntimeException("Type "+value.getClass().getName()+" not supported");
		}
		
		return buffer;
	}
	
	private static boolean isArray(int[] shape) {
		if (shape != null) {
			return (shape.length > 1) ? true : shape[0] > 1;
		} else {
			return false;
		}
	}
}
