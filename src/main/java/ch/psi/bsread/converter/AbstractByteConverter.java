package ch.psi.bsread.converter;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractByteConverter implements ByteConverter {

	@Override
	public ByteBuffer getBytes(Object value, ByteOrder byteOrder) {
		return this.getBytes(getTypeName(value.getClass()), value, byteOrder);
	}

	/*
	 * The rest of this code is copied from
	 * ch.psi.data.converters.ConverterProvider and ch.psi.data.DataConverter
	 * for not being dependent to byte_converters package that should be usually
	 * used to serialize/de-serialize values to bytes However we copied the
	 * basic functionality here to be able to remove the dependency to be able
	 * to run this code in Matlab (Matlab 2015a runs on Java 1.7) as well.
	 */
	private static final Map<Class<?>, Class<?>> PRIMITIVE_WRAPPER_MAP = new HashMap<Class<?>, Class<?>>();
	private static final Map<Class<?>, Class<?>> WRAPPER_PRIMITIVE_MAP = new HashMap<Class<?>, Class<?>>();
	static {
		PRIMITIVE_WRAPPER_MAP.put(Boolean.TYPE, Boolean.class);
		PRIMITIVE_WRAPPER_MAP.put(Byte.TYPE, Byte.class);
		PRIMITIVE_WRAPPER_MAP.put(Character.TYPE, Character.class);
		PRIMITIVE_WRAPPER_MAP.put(Short.TYPE, Short.class);
		PRIMITIVE_WRAPPER_MAP.put(Integer.TYPE, Integer.class);
		PRIMITIVE_WRAPPER_MAP.put(Long.TYPE, Long.class);
		PRIMITIVE_WRAPPER_MAP.put(Double.TYPE, Double.class);
		PRIMITIVE_WRAPPER_MAP.put(Float.TYPE, Float.class);
		PRIMITIVE_WRAPPER_MAP.put(Void.TYPE, Void.TYPE);

		for (final Map.Entry<Class<?>, Class<?>> entry : PRIMITIVE_WRAPPER_MAP.entrySet()) {
			if (!entry.getKey().equals(entry.getValue())) {
				WRAPPER_PRIMITIVE_MAP.put(entry.getValue(), entry.getKey());
			}
		}
	}
	private static final String INT_STRING = "int";
	private static final String INTEGER_STRING = "integer";
	private static final String CHAR_STRING = "char";
	private static final String CHARACTER_STRING = "character";

	public static String getTypeName(Class<?> valueClazz) {
		if (valueClazz.isArray()) {
			valueClazz = valueClazz.getComponentType();
		}

		Class<?> clazz = AbstractByteConverter.wrapperToPrimitive(valueClazz);
		// e.g. String.class has no primitive
		if (clazz != null) {
			valueClazz = clazz;
		}

		return AbstractByteConverter.getCorrectType(valueClazz.getSimpleName().toLowerCase());
	}

	public static final String getCorrectType(String type) {
		if (INT_STRING.equals(type)) {
			type = INTEGER_STRING;
		} else if (CHAR_STRING.equals(type)) {
			type = CHARACTER_STRING;
		}
		return type;
	}

	public static Class<?> wrapperToPrimitive(final Class<?> cls) {
		return WRAPPER_PRIMITIVE_MAP.get(cls);
	}

	/**
	 * Determines if a shape is an array.
	 * 
	 * @param shape
	 *            The shape
	 * @return boolean <tt>true</tt> if it is an array, <tt>false</tt> otherwise
	 */
	public static boolean isArray(int[] shape) {
		if (shape != null) {
			return (shape.length > 1) ? true : shape[0] > 1;
		} else {
			return false;
		}
	}

	/**
	 * Determines the length of the corresponding array.
	 * 
	 * @param shape
	 *            The shape
	 * @return int The length
	 */
	public static int getArrayLength(int[] shape) {
		int length = 1;
		for (int i : shape) {
			length *= i;
		}
		return length;
	}
}
