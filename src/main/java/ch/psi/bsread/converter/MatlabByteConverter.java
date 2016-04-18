package ch.psi.bsread.converter;

import java.lang.reflect.Array;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.ShortBuffer;
import java.nio.charset.StandardCharsets;
import java.util.function.IntFunction;
import java.util.stream.IntStream;

import ch.psi.bsread.common.allocator.ThreadLocalByteBufferAllocator;
import ch.psi.bsread.common.helper.ByteBufferHelper;
import ch.psi.bsread.message.ChannelConfig;
import ch.psi.bsread.message.MainHeader;
import ch.psi.bsread.message.Timestamp;
import ch.psi.bsread.message.Type;

/**
 * This is a convenience class for not being dependent to byte_converters package that should be
 * usually used to serialize/de-serialize values to bytes However we copied the basic functionality
 * here to be able to remove the dependency to be able to run this code in Matlab (Matlab 2015a runs
 * on Java 1.7) as well.
 * 
 * Once the Matlab release supports Java 1.8 this class might be remove in favor of the
 * byte_converters package!
 */
public class MatlabByteConverter extends AbstractByteConverter {
   private static final ThreadLocalByteBufferAllocator TMP_DECOMPRESS_BYTEBUFFER_ALLOCATOR =
         new ThreadLocalByteBufferAllocator();
   // must be the same as in BooleanByteValueConverter.BOOLEAN_POSITION
   private static final int BOOLEAN_POSITION = 0;


   @SuppressWarnings("unchecked")
   @Override
   public <V> V getValue(ByteBuffer receivedValueBytes, ChannelConfig config, MainHeader mainHeader,
         Timestamp iocTimestamp) {

      // IMPORTANT: receivedValueBytes can become a shared ByteBuffer!!!
      receivedValueBytes =
            config.getCompression()
                  .getCompressor()
                  .decompressData(receivedValueBytes, receivedValueBytes.position(),
                        TMP_DECOMPRESS_BYTEBUFFER_ALLOCATOR, config.getType().getBytes());
      receivedValueBytes.order(config.getByteOrder());

      final boolean array = isArray(config.getShape());
      switch (config.getType()) {
         case Bool:
            if (array) {
               final ByteBuffer finalReceivedValueBytes = receivedValueBytes;
               boolean[] values = new boolean[finalReceivedValueBytes.remaining()];
               int startPos = finalReceivedValueBytes.position();

               IntStream stream = IntStream.range(0, values.length);
               stream.forEach(i -> values[i] = (finalReceivedValueBytes.get(startPos + i) & (1 << BOOLEAN_POSITION)) != 0);
               
               return (V) values;
            }
            else {
               Boolean value = (receivedValueBytes.get(receivedValueBytes.position()) & (1 << BOOLEAN_POSITION)) != 0;
               return (V) value;
            }
         case Int8:
            if (array) {        
               return (V) ByteBufferHelper.copyToByteArray(receivedValueBytes);
            }
            else {
               return (V) ((Byte) receivedValueBytes.get(receivedValueBytes.position()));
            }
         case Int16:
            if (array) {
               short[] values = new short[receivedValueBytes.remaining() / Short.BYTES];
               receivedValueBytes.asShortBuffer().get(values);
               return (V) values;
            }
            else {
               return (V) ((Short) receivedValueBytes.getShort(receivedValueBytes.position()));
            }
         case Int32:
            if (array) {
               int[] values = new int[receivedValueBytes.remaining() / Integer.BYTES];
               receivedValueBytes.asIntBuffer().get(values);
               return (V) values;
            }
            else {
               return (V) ((Integer) receivedValueBytes.getInt(receivedValueBytes.position()));
            }
         case Int64:
            if (array) {
               long[] values = new long[receivedValueBytes.remaining() / Long.BYTES];
               receivedValueBytes.asLongBuffer().get(values);
               return (V) values;
            }
            else {
               return (V) ((Long) receivedValueBytes.getLong(receivedValueBytes.position()));
            }
         case UInt8:
            if (array) {
               final ByteBuffer finalReceivedValueBytes = receivedValueBytes;
               short[] values = new short[finalReceivedValueBytes.remaining()];
               int startPos = finalReceivedValueBytes.position();

               IntStream stream = IntStream.range(0, values.length);
               stream.forEach(i -> values[i] = (short) (finalReceivedValueBytes.get(startPos + i) & 0xff));

               return (V) values;
            }
            else {
               Short value = (short) (receivedValueBytes.get(receivedValueBytes.position()) & 0xff);
               return (V) value;
            }
         case UInt16:
            if (array) {           
               final ShortBuffer finalReceivedValueBytes = receivedValueBytes.asShortBuffer();
               int[] values = new int[finalReceivedValueBytes.remaining()];

               IntStream stream = IntStream.range(0, values.length);
               stream.forEach(i -> values[i] = finalReceivedValueBytes.get(i) & 0xffff);

               return (V) values;
            }
            else {
               return (V) ((Integer) (receivedValueBytes.getShort(receivedValueBytes.position()) & 0xffff));
            }
         case UInt32:
            if (array) {
               final IntBuffer finalReceivedValueBytes = receivedValueBytes.asIntBuffer();
               long[] values = new long[finalReceivedValueBytes.remaining()];

               IntStream stream = IntStream.range(0, values.length);
               stream.forEach(i -> values[i] = finalReceivedValueBytes.get(i) & 0xffffffffL);

               return (V) values;
            }
            else {
               return (V) ((Long) (receivedValueBytes.getInt() & 0xffffffffL));
            }
         case UInt64:
            if (array) {
               final LongBuffer finalReceivedValueBytes = receivedValueBytes.asLongBuffer();
               BigInteger[] values = new BigInteger[finalReceivedValueBytes.remaining()];

               IntStream stream = IntStream.range(0, values.length);
               stream.forEach(i -> {
                  long val = finalReceivedValueBytes.get(i);
                  BigInteger bigInt = BigInteger.valueOf(val & 0x7fffffffffffffffL);
                  if (val < 0) {
                     bigInt = bigInt.setBit(Long.SIZE - 1);
                  }
                  values[i] = bigInt;
               });

               return (V) values;
            }
            else {
               long val = receivedValueBytes.getLong(receivedValueBytes.position());
               BigInteger bigInt = BigInteger.valueOf(val & 0x7fffffffffffffffL);
               if (val < 0) {
                  bigInt = bigInt.setBit(Long.SIZE - 1);
               }
               return (V) bigInt;
            }
         case Float32:
            if (array) {
               float[] values = new float[receivedValueBytes.remaining() / Float.BYTES];
               receivedValueBytes.asFloatBuffer().get(values);
               return (V) values;
            }
            else {
               return (V) ((Float) receivedValueBytes.getFloat(receivedValueBytes.position()));
            }
         case Float64:
            if (array) {
               double[] values = new double[receivedValueBytes.remaining() / Double.BYTES];
               receivedValueBytes.asDoubleBuffer().get(values);
               return (V) values;
            }
            else {
               return (V) ((Double) receivedValueBytes.getDouble(receivedValueBytes.position()));
            }
         case String:
            return (V) StandardCharsets.UTF_8.decode(receivedValueBytes.duplicate()).toString();
         default:
            throw new RuntimeException("Type " + config.getType() + " not supported");
      }

   }

   @Override
   public ByteBuffer getBytes(Object value, Type type, ByteOrder byteOrder, IntFunction<ByteBuffer> allocator) {
      final boolean array = value.getClass().isArray();
      ByteBuffer buffer;

      switch (type) {
         case Bool:
            if (array) {
               int length = Array.getLength(value);
               buffer = allocator.apply(((boolean[]) value).length * Byte.BYTES).order(byteOrder);
               ByteBuffer valBuffer = buffer.duplicate();

               IntStream stream = IntStream.range(0, length);
               stream.forEach(i -> {
                  boolean val = ((boolean[]) value)[i];
                  byte bVal = 0;
                  if (val) {
                     bVal |= (1 << BOOLEAN_POSITION);
                  }
                  valBuffer.put(i, bVal);
               });  
            }
            else {
               buffer = allocator.apply(Byte.BYTES).order(byteOrder);
               boolean val = (Boolean) value;
               byte bVal = 0;
               if (val) {
                  bVal |= (1 << BOOLEAN_POSITION);
               }
               buffer.duplicate().put(bVal);
            }
            break;
         case Int8:
            if (array) {               
               buffer = ByteBuffer.wrap((byte[]) value).order(byteOrder);
            }
            else {
               buffer = allocator.apply(Byte.BYTES).order(byteOrder);
               buffer.duplicate().put((Byte) value);
            }
            break;
         case Int16:
            if (array) {
               buffer = allocator.apply(((short[]) value).length * Short.BYTES).order(byteOrder);
               buffer.asShortBuffer().put((short[]) value);
            }
            else {
               buffer = allocator.apply(Short.BYTES).order(byteOrder);
               buffer.asShortBuffer().put((Short) value);
            }
            break;
         case Int32:
            if (array) {
               buffer = allocator.apply(((int[]) value).length * Integer.BYTES).order(byteOrder);
               buffer.asIntBuffer().put((int[]) value);
            }
            else {
               buffer = allocator.apply(Integer.BYTES).order(byteOrder);
               buffer.asIntBuffer().put((Integer) value);
            }
            break;
         case Int64:
            if (array) {
               buffer = allocator.apply(((long[]) value).length * Long.BYTES).order(byteOrder);
               buffer.asLongBuffer().put((long[]) value);
            }
            else {
               buffer = allocator.apply(Long.BYTES).order(byteOrder);
               buffer.asLongBuffer().put((Long) value);
            }
            break;
         case UInt8:
            if (array) {
               int length = Array.getLength(value);
               buffer = allocator.apply(length * Byte.BYTES).order(byteOrder);
               ByteBuffer valBuffer = buffer.duplicate();

               IntStream stream = IntStream.range(0, length);
               stream.forEach(i -> valBuffer.put(i, (byte) ((short[]) value)[i]));
            }
            else {
               buffer = allocator.apply(Byte.BYTES).order(byteOrder);
               ByteBuffer valBuffer = buffer.duplicate();

               valBuffer.put(0, ((Short) value).byteValue());
            }
            break;
         case UInt16:
            if (array) {
               int length = Array.getLength(value);
               buffer = allocator.apply(length * Short.BYTES).order(byteOrder);
               ShortBuffer valBuffer = buffer.asShortBuffer();

               IntStream stream = IntStream.range(0, length);
               stream.forEach(i -> valBuffer.put(i, (short) ((int[]) value)[i]));
            }
            else {
               buffer = allocator.apply(Short.BYTES).order(byteOrder);
               ShortBuffer valBuffer = buffer.asShortBuffer();

               valBuffer.put(0, ((Integer) value).shortValue());
            }
            break;
         case UInt32:
            if (array) {
               int length = Array.getLength(value);
               buffer = allocator.apply(length * Integer.BYTES).order(byteOrder);
               IntBuffer valBuffer = buffer.asIntBuffer();

               IntStream stream = IntStream.range(0, length);
               stream.forEach(i -> valBuffer.put(i, (int) ((long[]) value)[i]));
            }
            else {
               buffer = allocator.apply(Integer.BYTES).order(byteOrder);
               IntBuffer valBuffer = buffer.asIntBuffer();

               valBuffer.put(0, ((Long) value).intValue());
            }
            break;
         case UInt64:
            if (array) {
               int length = Array.getLength(value);
               buffer = allocator.apply(length * Long.BYTES).order(byteOrder);
               LongBuffer valBuffer = buffer.asLongBuffer();

               IntStream stream = IntStream.range(0, length);
               stream.forEach(i -> valBuffer.put(i, (((BigInteger[]) value)[i]).longValue()));
            }
            else {
               buffer = allocator.apply(Long.BYTES).order(byteOrder);
               LongBuffer valBuffer = buffer.asLongBuffer();

               valBuffer.put(0, ((BigInteger) value).longValue());
            }
            break;
         case Float32:
            if (array) {
               buffer = allocator.apply(((float[]) value).length * Float.BYTES).order(byteOrder);
               buffer.asFloatBuffer().put((float[]) value);
            }
            else {
               buffer = allocator.apply(Float.BYTES).order(byteOrder);
               buffer.asFloatBuffer().put((Float) value);
            }
            break;
         case Float64:
            if (array) {
               buffer = allocator.apply(((double[]) value).length * Double.BYTES).order(byteOrder);
               buffer.asDoubleBuffer().put((double[]) value);
            }
            else {
               buffer = allocator.apply(Double.BYTES).order(byteOrder);
               buffer.asDoubleBuffer().put((Double) value);
            }
            break;
         case String:           
            buffer = ByteBuffer.wrap(((String) value).getBytes(StandardCharsets.UTF_8)).order(byteOrder);
            break;
         default:
            throw new RuntimeException("Type " + type + " not supported");
      }
      
      return buffer;
   }
}
