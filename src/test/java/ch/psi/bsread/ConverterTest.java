package ch.psi.bsread;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.junit.Test;

import ch.psi.bsread.converter.ByteConverter;
import ch.psi.bsread.converter.MatlabByteConverter;

public class ConverterTest {

	@Test
	public void test() {
		ByteConverter byteConverter = new MatlabByteConverter();
		ByteBuffer buffer = byteConverter.getBytes("double", 1.0, ByteOrder.BIG_ENDIAN);
		assertEquals(Double.valueOf(1.0), byteConverter.getValue(buffer, "float64", new int[] { 1 }), 0.000000000000001);
		buffer = byteConverter.getBytes("float64", new double[] { 1.0, 2.0 }, ByteOrder.BIG_ENDIAN);
		assertArrayEquals(new double[] { 1.0, 2.0 }, byteConverter.getValue(buffer, "float64", new int[] { 2 }), 0.000000000000001);
		buffer = byteConverter.getBytes("string", "This is a test", ByteOrder.BIG_ENDIAN);
		assertEquals("This is a test", byteConverter.getValue(buffer, "string", new int[] { 1 }));
	}

}
