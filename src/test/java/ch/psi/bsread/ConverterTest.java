package ch.psi.bsread;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;

import org.junit.Test;

import ch.psi.bsread.common.allocator.ByteBufferAllocator;
import ch.psi.bsread.converter.ByteConverter;
import ch.psi.bsread.converter.MatlabByteConverter;
import ch.psi.bsread.message.ChannelConfig;
import ch.psi.bsread.message.Type;

public class ConverterTest {

	@Test
	public void test() {
		ByteBufferAllocator allocator = ByteBufferAllocator.DEFAULT_ALLOCATOR;
		ByteConverter byteConverter = new MatlabByteConverter();
		ChannelConfig config = new ChannelConfig("Bla", Type.Float64, new int[] { 1 }, 1, 0, ChannelConfig.ENCODING_BIG_ENDIAN);

		ByteBuffer buffer = byteConverter.getBytes(1.0, config.getType(), config.getByteOrder(), allocator);
		assertEquals(Double.valueOf(1.0), byteConverter.getValue(buffer, config, null, null), 0.000000000000001);

		config.setShape(new int[] { 2 });
		buffer = byteConverter.getBytes(new double[] { 1.0, 2.0 }, config.getType(), config.getByteOrder(), allocator);
		assertArrayEquals(new double[] { 1.0, 2.0 }, byteConverter.getValue(buffer, config, null, null), 0.000000000000001);

		config.setType(Type.String);
		config.setShape(new int[] { 1 });
		buffer = byteConverter.getBytes("This is a test", config.getType(), config.getByteOrder(), allocator);
		assertEquals("This is a test", byteConverter.getValue(buffer, config, null, null));
	}

}
