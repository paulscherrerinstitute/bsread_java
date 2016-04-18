package ch.psi.bsread.common.helper;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ByteBufferHelperTest {

   @Before
   public void setUp() throws Exception {}

   @After
   public void tearDown() {}

   @Test
   public void testByteEncoding_Byte() throws Exception {
      byte identifier = 0;

      assertFalse(ByteBufferHelper.isPositionSet(identifier, 0));
      identifier = ByteBufferHelper.setPosition(identifier, 0);
      assertTrue(ByteBufferHelper.isPositionSet(identifier, 0));

      identifier = 0;
      assertFalse(ByteBufferHelper.isPositionSet(identifier, 1));
      identifier = ByteBufferHelper.setPosition(identifier, 1);
      assertTrue(ByteBufferHelper.isPositionSet(identifier, 1));

      identifier = 0;
      identifier = ByteBufferHelper.setPosition(identifier, 0);
      identifier = ByteBufferHelper.setPosition(identifier, 1);
      assertTrue(ByteBufferHelper.isPositionSet(identifier, 0));
      assertTrue(ByteBufferHelper.isPositionSet(identifier, 1));
      assertFalse(ByteBufferHelper.isPositionSet(identifier, 3));

      assertFalse(ByteBufferHelper.isPositionSet(identifier, 8));

      assertFalse(ByteBufferHelper.isPositionSet(identifier, 7));
      identifier = ByteBufferHelper.setPosition(identifier, 7);
      assertTrue(ByteBufferHelper.isPositionSet(identifier, 7));

      assertFalse(ByteBufferHelper.isPositionSet(identifier, 8));
      try {
         identifier = ByteBufferHelper.setPosition(identifier, 8);
         assertTrue(false);
      } catch (Exception e) {
         // good
      }
      assertFalse(ByteBufferHelper.isPositionSet(identifier, 8));

      assertFalse(ByteBufferHelper.isPositionSet(identifier, 63));
      try {
         identifier = ByteBufferHelper.setPosition(identifier, 63);
         assertTrue(false);
      } catch (Exception e) {
         // good
      }
      assertFalse(ByteBufferHelper.isPositionSet(identifier, 63));
   }
   
   @Test
   public void testByteEncoding_Short() throws Exception {
      short identifier = 0;

      assertFalse(ByteBufferHelper.isPositionSet(identifier, 0));
      identifier = ByteBufferHelper.setPosition(identifier, 0);
      assertTrue(ByteBufferHelper.isPositionSet(identifier, 0));

      identifier = 0;
      assertFalse(ByteBufferHelper.isPositionSet(identifier, 1));
      identifier = ByteBufferHelper.setPosition(identifier, 1);
      assertTrue(ByteBufferHelper.isPositionSet(identifier, 1));

      identifier = 0;
      identifier = ByteBufferHelper.setPosition(identifier, 0);
      identifier = ByteBufferHelper.setPosition(identifier, 1);
      assertTrue(ByteBufferHelper.isPositionSet(identifier, 0));
      assertTrue(ByteBufferHelper.isPositionSet(identifier, 1));
      assertFalse(ByteBufferHelper.isPositionSet(identifier, 3));

      assertFalse(ByteBufferHelper.isPositionSet(identifier, 16));

      assertFalse(ByteBufferHelper.isPositionSet(identifier, 15));
      identifier = ByteBufferHelper.setPosition(identifier, 15);
      assertTrue(ByteBufferHelper.isPositionSet(identifier, 15));

      assertFalse(ByteBufferHelper.isPositionSet(identifier, 16));
      try {
         identifier = ByteBufferHelper.setPosition(identifier, 16);
         assertTrue(false);
      } catch (Exception e) {
         // good
      }
      assertFalse(ByteBufferHelper.isPositionSet(identifier, 16));

      assertFalse(ByteBufferHelper.isPositionSet(identifier, 63));
      try {
         identifier = ByteBufferHelper.setPosition(identifier, 63);
         assertTrue(false);
      } catch (Exception e) {
         // good
      }
      assertFalse(ByteBufferHelper.isPositionSet(identifier, 63));
   }
   
   @Test
   public void testByteEncoding_Int() throws Exception {
      int identifier = 0;

      assertFalse(ByteBufferHelper.isPositionSet(identifier, 0));
      identifier = ByteBufferHelper.setPosition(identifier, 0);
      assertTrue(ByteBufferHelper.isPositionSet(identifier, 0));

      identifier = 0;
      assertFalse(ByteBufferHelper.isPositionSet(identifier, 1));
      identifier = ByteBufferHelper.setPosition(identifier, 1);
      assertTrue(ByteBufferHelper.isPositionSet(identifier, 1));

      identifier = 0;
      identifier = ByteBufferHelper.setPosition(identifier, 0);
      identifier = ByteBufferHelper.setPosition(identifier, 1);
      assertTrue(ByteBufferHelper.isPositionSet(identifier, 0));
      assertTrue(ByteBufferHelper.isPositionSet(identifier, 1));
      assertFalse(ByteBufferHelper.isPositionSet(identifier, 3));

      assertFalse(ByteBufferHelper.isPositionSet(identifier, 32));

      assertFalse(ByteBufferHelper.isPositionSet(identifier, 31));
      identifier = ByteBufferHelper.setPosition(identifier, 31);
      assertTrue(ByteBufferHelper.isPositionSet(identifier, 31));

      assertFalse(ByteBufferHelper.isPositionSet(identifier, 32));
      try {
         identifier = ByteBufferHelper.setPosition(identifier, 32);
         assertTrue(false);
      } catch (Exception e) {
         // good
      }
      assertFalse(ByteBufferHelper.isPositionSet(identifier, 32));

      assertFalse(ByteBufferHelper.isPositionSet(identifier, 63));
      try {
         identifier = ByteBufferHelper.setPosition(identifier, 63);
         assertTrue(false);
      } catch (Exception e) {
         // good
      }
      assertFalse(ByteBufferHelper.isPositionSet(identifier, 63));
   }
   
   @Test
   public void testByteEncoding_Long() throws Exception {
      long identifier = 0;

      assertFalse(ByteBufferHelper.isPositionSet(identifier, 0));
      identifier = ByteBufferHelper.setPosition(identifier, 0);
      assertTrue(ByteBufferHelper.isPositionSet(identifier, 0));

      identifier = 0;
      assertFalse(ByteBufferHelper.isPositionSet(identifier, 1));
      identifier = ByteBufferHelper.setPosition(identifier, 1);
      assertTrue(ByteBufferHelper.isPositionSet(identifier, 1));

      identifier = 0;
      identifier = ByteBufferHelper.setPosition(identifier, 0);
      identifier = ByteBufferHelper.setPosition(identifier, 1);
      assertTrue(ByteBufferHelper.isPositionSet(identifier, 0));
      assertTrue(ByteBufferHelper.isPositionSet(identifier, 1));
      assertFalse(ByteBufferHelper.isPositionSet(identifier, 3));

      assertFalse(ByteBufferHelper.isPositionSet(identifier, 64));

      assertFalse(ByteBufferHelper.isPositionSet(identifier, 63));
      identifier = ByteBufferHelper.setPosition(identifier, 63);
      assertTrue(ByteBufferHelper.isPositionSet(identifier, 63));

      assertFalse(ByteBufferHelper.isPositionSet(identifier, 64));
      try {
         identifier = ByteBufferHelper.setPosition(identifier, 64);
         assertTrue(false);
      } catch (Exception e) {
         // good
      }
      assertFalse(ByteBufferHelper.isPositionSet(identifier, 64));
   }

   @Test
   public void testByteBufferSerialization() throws Exception {
      ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.BIG_ENDIAN);
      int intVal = Integer.MAX_VALUE / 3;
      buf.asIntBuffer().put(intVal);

      ByteBuffer copy = copy(buf);
      assertEquals(buf.isDirect(), copy.isDirect());
      assertEquals(buf.order(), copy.order());
      assertEquals(buf.position(), copy.position());
      assertEquals(buf.limit(), copy.limit());
      assertEquals(intVal, copy.asIntBuffer().get());

      buf = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
      intVal = Integer.MAX_VALUE / 5;
      buf.asIntBuffer().put(intVal);

      copy = copy(buf);
      assertEquals(buf.isDirect(), copy.isDirect());
      assertEquals(buf.order(), copy.order());
      assertEquals(buf.position(), copy.position());
      assertEquals(buf.limit(), copy.limit());
      assertEquals(intVal, copy.asIntBuffer().get());

      buf = ByteBuffer.allocateDirect(Integer.BYTES).order(ByteOrder.BIG_ENDIAN);
      intVal = Integer.MAX_VALUE / 9;
      buf.asIntBuffer().put(intVal);

      copy = copy(buf);
      assertEquals(buf.isDirect(), copy.isDirect());
      assertEquals(buf.order(), copy.order());
      assertEquals(buf.position(), copy.position());
      assertEquals(buf.limit(), copy.limit());
      assertEquals(intVal, copy.asIntBuffer().get());

      buf = ByteBuffer.allocateDirect(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
      intVal = Integer.MAX_VALUE / 2;
      buf.asIntBuffer().put(intVal);

      copy = copy(buf);
      assertEquals(buf.isDirect(), copy.isDirect());
      assertEquals(buf.order(), copy.order());
      assertEquals(buf.position(), copy.position());
      assertEquals(buf.limit(), copy.limit());
      assertEquals(intVal, copy.asIntBuffer().get());

      buf = ByteBuffer.allocateDirect(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
      intVal = Integer.MAX_VALUE / 2;
      buf.asIntBuffer().put(intVal);

      copy = copy(buf);
      assertEquals(true, copy.isDirect());
      assertEquals(buf.order(), copy.order());
      assertEquals(buf.position(), copy.position());
      assertEquals(buf.limit(), copy.limit());
      assertEquals(intVal, copy.asIntBuffer().get());

      int size = 8 * 2048 / Integer.BYTES + 1024;
      int[] intVals = new int[size];
      int[] intValsCopy = new int[size];
      for (int i = 0; i < size; ++i) {
         intVals[i] = i;
      }
      buf = ByteBuffer.allocateDirect(size * Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
      buf.asIntBuffer().put(intVals);
      copy = copy(buf);
      assertEquals(buf.isDirect(), copy.isDirect());
      assertEquals(buf.order(), copy.order());
      assertEquals(buf.position(), copy.position());
      assertEquals(buf.limit(), copy.limit());
      copy.asIntBuffer().get(intValsCopy);
      assertArrayEquals(intVals, intValsCopy);
   }

   @Test
   public void testAsDirect() throws Exception {
      ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES);
      int intVal = Integer.MAX_VALUE / 3;
      buf.asIntBuffer().put(intVal);

      ByteBuffer direct = ByteBufferHelper.asDirect(buf);
      assertTrue(direct.isDirect());
      assertEquals(0, direct.position());
      assertEquals(4, direct.remaining());
      assertEquals(4, direct.limit());
      assertEquals(4, direct.capacity());
      assertEquals(buf.asIntBuffer().get(), direct.asIntBuffer().get());

      buf = ByteBuffer.allocate(3 * Integer.BYTES);
      buf.position(Integer.BYTES);
      buf.asIntBuffer().put(intVal);
      buf.limit(2 * Integer.BYTES);
      direct = ByteBufferHelper.asDirect(buf);
      assertTrue(direct.isDirect());
      assertEquals(0, direct.position());
      assertEquals(4, direct.remaining());
      assertEquals(4, direct.limit());
      assertEquals(4, direct.capacity());
      assertEquals(buf.asIntBuffer().get(), direct.asIntBuffer().get());


      buf = ByteBuffer.allocateDirect(Integer.BYTES);
      buf.asIntBuffer().put(intVal);
      direct = ByteBufferHelper.asDirect(buf);
      assertSame(buf, direct);
   }

   @Test
   public void testCopyToByteArray() throws Exception {
      ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES);
      int intVal = Integer.MAX_VALUE / 3;
      buf.asIntBuffer().put(intVal);

      byte[] copy = ByteBufferHelper.copyToByteArray(buf);
      assertNotSame(buf.array(), copy);
      assertArrayEquals(buf.array(), copy);

      buf = ByteBuffer.allocate(3 * Integer.BYTES);
      buf.position(Integer.BYTES);
      buf.asIntBuffer().put(intVal);
      buf.limit(2 * Integer.BYTES);

      copy = ByteBufferHelper.copyToByteArray(buf);
      assertEquals(Integer.BYTES, copy.length);
      assertEquals(intVal, ByteBuffer.wrap(copy).asIntBuffer().get());

      buf = ByteBuffer.allocateDirect(Integer.BYTES);
      buf.asIntBuffer().put(intVal);
      copy = ByteBufferHelper.copyToByteArray(buf);
      assertEquals(intVal, ByteBuffer.wrap(copy).asIntBuffer().get());

      buf = ByteBuffer.allocateDirect(3 * Integer.BYTES);
      buf.position(Integer.BYTES);
      buf.asIntBuffer().put(intVal);
      buf.limit(2 * Integer.BYTES);

      copy = ByteBufferHelper.copyToByteArray(buf);
      assertEquals(Integer.BYTES, copy.length);
      assertEquals(intVal, ByteBuffer.wrap(copy).asIntBuffer().get());
   }

   @Test
   public void testExtractByteArray() throws Exception {
      ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES);
      int intVal = Integer.MAX_VALUE / 3;
      buf.asIntBuffer().put(intVal);

      byte[] copy = ByteBufferHelper.extractByteArray(buf);
      assertSame(buf.array(), copy);

      buf = ByteBuffer.allocate(3 * Integer.BYTES);
      buf.position(Integer.BYTES);
      buf.asIntBuffer().put(intVal);
      buf.limit(2 * Integer.BYTES);

      copy = ByteBufferHelper.extractByteArray(buf);
      assertNotSame(buf.array(), copy);
      assertEquals(Integer.BYTES, copy.length);
      assertEquals(intVal, ByteBuffer.wrap(copy).asIntBuffer().get());

      buf = ByteBuffer.allocateDirect(Integer.BYTES);
      buf.asIntBuffer().put(intVal);
      copy = ByteBufferHelper.extractByteArray(buf);
      assertEquals(intVal, ByteBuffer.wrap(copy).asIntBuffer().get());

      buf = ByteBuffer.allocateDirect(3 * Integer.BYTES);
      buf.position(Integer.BYTES);
      buf.asIntBuffer().put(intVal);
      buf.limit(2 * Integer.BYTES);

      copy = ByteBufferHelper.extractByteArray(buf);
      assertEquals(Integer.BYTES, copy.length);
      assertEquals(intVal, ByteBuffer.wrap(copy).asIntBuffer().get());
   }

   ByteBuffer copy(ByteBuffer buffer) throws IOException {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      ObjectOutputStream out = new ObjectOutputStream(bos);
      ByteBufferHelper.write(buffer, out);
      out.flush();

      // De-serialization of object
      ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
      ObjectInputStream in = new ObjectInputStream(bis);

      return ByteBufferHelper.read(in);
   }
}
