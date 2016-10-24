package ch.psi.bsread.message;

import java.io.Serializable;
import java.nio.ByteOrder;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import ch.psi.bsread.compression.Compression;

@JsonInclude(Include.NON_DEFAULT)
public class ChannelConfig implements Serializable {
   private static final long serialVersionUID = 1L;
   // use a static variable due to Include.NON_DEFAULT
   private static final int[] DEFAULT_SHAPE = {1};
   public static final String ENCODING_BIG_ENDIAN = "big";
   public static final String ENCODING_LITTLE_ENDIAN = "little";
   public static final String DEFAULT_ENCODING = ENCODING_LITTLE_ENDIAN;

   private String name;
   private Type type = Type.Float64;
   private int[] shape = DEFAULT_SHAPE;
   private int modulo = 1;
   private int offset = 0;
   private String encoding = DEFAULT_ENCODING;
   private Compression compression = Compression.none;

   public ChannelConfig() {}

   public ChannelConfig(String name, Type type) {
      this.name = name;
      this.type = type;
   }

   public ChannelConfig(String name, Type type, int modulo, int offset) {
      this(name, type);

      this.modulo = modulo;
      this.offset = offset;
   }

   public ChannelConfig(String name, Type type, int[] shape, int modulo, int offset) {
      this(name, type, modulo, offset);

      this.shape = shape;

      // if (type.getBytes() == ValueConverter.DYNAMIC_NUMBER_OF_BYTES ||
      // Compressor.DEFAULT_COMPRESS_THRESHOLD < type.getBytes() *
      // AbstractByteConverter.getArrayLength(shape)) {
      // compression = Compression.DEFAULT;
      // }
   }

   public ChannelConfig(String name, Type type, int[] shape, int modulo, int offset, String encoding) {
      this(name, type, shape, modulo, offset);

      this.encoding = encoding;
   }

   public ChannelConfig(String name, Type type, int[] shape, int modulo, int offset, String encoding,
         Compression compression) {
      this(name, type, shape, modulo, offset, encoding);

      this.compression = compression;
   }

   public ChannelConfig(ChannelConfig config) {
      this(config.name, config.type, config.shape, config.modulo, config.offset, config.encoding, config.compression);
   }

   public void copy(ChannelConfig other) {
      this.name = other.name;
      this.type = other.type;
      this.shape = other.shape;
      this.modulo = other.modulo;
      this.offset = other.offset;
      this.encoding = other.encoding;
      this.compression = other.compression;
   }

   public String getName() {
      return this.name;
   }

   public void setName(String name) {
      this.name = name;
   }

   public Type getType() {
      return this.type;
   }

   public void setType(Type type) {
      this.type = type;
   }

   public int[] getShape() {
      return this.shape;
   }

   public void setShape(int[] shape) {
      this.shape = shape;
   }

   public int getModulo() {
      return this.modulo;
   }

   public void setModulo(int modulo) {
      this.modulo = modulo;
   }

   public int getOffset() {
      return this.offset;
   }

   public void setOffset(int offset) {
      this.offset = offset;
   }

   public String getEncoding() {
      return encoding;
   }

   public void setEncoding(String encoding) {
      this.encoding = encoding;
   }

   public Compression getCompression() {
      return compression;
   }

   public void setCompression(Compression compression) {
      this.compression = compression;
   }

   /**
    * Get the byte order based on the specified endianess
    * 
    * @return ByteOrder of data
    */
   @JsonIgnore
   public ByteOrder getByteOrder() {
      return ChannelConfig.getByteOrder(this.encoding);
   }

   public static ByteOrder getByteOrder(String byteOrder) {
      if (byteOrder != null && byteOrder.contains(ENCODING_BIG_ENDIAN)) {
         return ByteOrder.BIG_ENDIAN;
      } else {
         return ByteOrder.LITTLE_ENDIAN;
      }
   }

   public static String getEncoding(ByteOrder byteOrder) {
      if (byteOrder != null && byteOrder.equals(ByteOrder.BIG_ENDIAN)) {
         return ENCODING_BIG_ENDIAN;
      } else {
         return ENCODING_LITTLE_ENDIAN;
      }
   }

   @JsonIgnore
   public void setByteOrder(ByteOrder byteOrder) {
      if (byteOrder != null && byteOrder.equals(ByteOrder.BIG_ENDIAN)) {
         encoding = ChannelConfig.ENCODING_BIG_ENDIAN;
      }
      else {
         encoding = ChannelConfig.ENCODING_LITTLE_ENDIAN;
      }
   }
}
