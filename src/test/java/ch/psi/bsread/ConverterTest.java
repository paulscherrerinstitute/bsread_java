package ch.psi.bsread;

import java.nio.ByteOrder;

import org.junit.Test;

public class ConverterTest {

	@Test
	public void test() {
		Converter.getBytes(1.0, ByteOrder.BIG_ENDIAN);
		Converter.getBytes(new double[]{1.0}, ByteOrder.BIG_ENDIAN);
		Converter.getBytes("This is a test", ByteOrder.BIG_ENDIAN);
	}

}
