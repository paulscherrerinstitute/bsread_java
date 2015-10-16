package ch.psi.bsread.message;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.junit.Test;

public class ValueTest {

	@Test
	public void testSerialization() throws Exception {
		ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.BIG_ENDIAN);
		int intVal = Integer.MAX_VALUE / 3;
		buf.asIntBuffer().put(intVal);
		Value<ByteBuffer> val = new Value<>(buf, new Timestamp(10, 2));

		Value<ByteBuffer> copy = SerializationHelper.copy(val);
		assertEquals(val.getTimestamp().getEpoch(), copy.getTimestamp().getEpoch());
		assertEquals(val.getTimestamp().getNs(), copy.getTimestamp().getNs());
		assertEquals(val.getValue().isDirect(), copy.getValue().isDirect());
		assertEquals(val.getValue().order(), copy.getValue().order());
		assertEquals(val.getValue().position(), copy.getValue().position());
		assertEquals(val.getValue().limit(), copy.getValue().limit());
		assertEquals(intVal, copy.getValue().asIntBuffer().get());

		buf = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
		intVal = Integer.MAX_VALUE / 5;
		buf.asIntBuffer().put(intVal);
		val = new Value<>(buf, new Timestamp(Long.MAX_VALUE / 2, 459));

		copy = SerializationHelper.copy(val);
		assertEquals(val.getTimestamp().getEpoch(), copy.getTimestamp().getEpoch());
		assertEquals(val.getTimestamp().getNs(), copy.getTimestamp().getNs());
		assertEquals(val.getValue().isDirect(), copy.getValue().isDirect());
		assertEquals(val.getValue().order(), copy.getValue().order());
		assertEquals(val.getValue().position(), copy.getValue().position());
		assertEquals(val.getValue().limit(), copy.getValue().limit());
		assertEquals(intVal, copy.getValue().asIntBuffer().get());

		buf = ByteBuffer.allocateDirect(Integer.BYTES).order(ByteOrder.BIG_ENDIAN);
		intVal = Integer.MAX_VALUE / 9;
		buf.asIntBuffer().put(intVal);
		val = new Value<>(buf, new Timestamp(Long.MAX_VALUE / 5, 459));

		copy = SerializationHelper.copy(val);
		assertEquals(val.getTimestamp().getEpoch(), copy.getTimestamp().getEpoch());
		assertEquals(val.getTimestamp().getNs(), copy.getTimestamp().getNs());
		assertEquals(val.getValue().isDirect(), copy.getValue().isDirect());
		assertEquals(val.getValue().order(), copy.getValue().order());
		assertEquals(val.getValue().position(), copy.getValue().position());
		assertEquals(val.getValue().limit(), copy.getValue().limit());
		assertEquals(intVal, copy.getValue().asIntBuffer().get());

		buf = ByteBuffer.allocateDirect(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
		intVal = Integer.MAX_VALUE / 2;
		buf.asIntBuffer().put(intVal);
		val = new Value<>(buf, new Timestamp(Long.MAX_VALUE / 3, 459));

		copy = SerializationHelper.copy(val);
		assertEquals(val.getTimestamp().getEpoch(), copy.getTimestamp().getEpoch());
		assertEquals(val.getTimestamp().getNs(), copy.getTimestamp().getNs());
		assertEquals(val.getValue().isDirect(), copy.getValue().isDirect());
		assertEquals(val.getValue().order(), copy.getValue().order());
		assertEquals(val.getValue().position(), copy.getValue().position());
		assertEquals(val.getValue().limit(), copy.getValue().limit());
		assertEquals(intVal, copy.getValue().asIntBuffer().get());
	}

}
