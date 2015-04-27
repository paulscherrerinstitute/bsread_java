package ch.psi.bsread.basic;

import java.nio.ByteBuffer;

public class Converter {
	
	/**
	 * Converts a given byte buffer into a specific value
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
				return (T) byteValue.asDoubleBuffer().array();
			}
			else{
				return (T)((Double) byteValue.asDoubleBuffer().get());
			}
		case "float":
			if(array){
				return (T) byteValue.asFloatBuffer().array();
			}
			else {
				return (T)((Float)byteValue.asFloatBuffer().get());
			}
		case "integer":
			if(array){
				return (T) byteValue.asIntBuffer().array();
			}
			else{
				return (T)((Integer)byteValue.asIntBuffer().get());
			}
		case "long":
			if(array){
				return (T) byteValue.asLongBuffer().array();
			}
			else{
				return (T)((Long)byteValue.asLongBuffer().get());
			}
		case "short":
			if(array){
				return (T) byteValue.asShortBuffer().array();
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
	
	private static boolean isArray(int[] shape) {
		if (shape != null) {
			return (shape.length > 1) ? true : shape[0] > 1;
		} else {
			return false;
		}
	}
}
