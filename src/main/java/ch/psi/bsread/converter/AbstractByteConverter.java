package ch.psi.bsread.converter;

public abstract class AbstractByteConverter implements ByteConverter {

	/*
	 * The rest of this code is copied from
	 * ch.psi.data.converters.ConverterProvider and ch.psi.data.DataConverter
	 * for not being dependent to byte_converters package that should be usually
	 * used to serialize/de-serialize values to bytes However we copied the
	 * basic functionality here to be able to remove the dependency to be able
	 * to run this code in Matlab (Matlab 2015a runs on Java 1.7) as well.
	 */

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
