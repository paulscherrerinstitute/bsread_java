package ch.psi.bsread.message;

import java.io.Serializable;
import java.nio.ByteOrder;
import java.util.Arrays;

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
   private static final Type DEFAULT_TYPE = Type.Float64;

   private String name;
   private int modulo = 1;
   private int offset = 0;
   private Type type = DEFAULT_TYPE;
   private int[] shape = DEFAULT_SHAPE;
   private String encoding = DEFAULT_ENCODING;
   private Compression compression = Compression.none;

   public ChannelConfig() {}

   public ChannelConfig(String name, Type type) {
      this.name = name;
      setType(type);
   }

   public ChannelConfig(String name, Type type, int modulo, int offset) {
      this(name, type);

      this.modulo = modulo;
      this.offset = offset;
   }

   public ChannelConfig(String name, Type type, int[] shape, int modulo, int offset) {
      this(name, type, modulo, offset);

      setShape(shape);

      // if (type.getBytes() == ValueConverter.DYNAMIC_NUMBER_OF_BYTES ||
      // Compressor.DEFAULT_COMPRESS_THRESHOLD < type.getBytes() *
      // AbstractByteConverter.getArrayLength(shape)) {
      // compression = Compression.DEFAULT;
      // }
   }

   public ChannelConfig(String name, Type type, int[] shape, int modulo, int offset, String encoding) {
      this(name, type, shape, modulo, offset);

      setEncoding(encoding);
   }

   public ChannelConfig(String name, Type type, int[] shape, int modulo, int offset, String encoding,
         Compression compression) {
      this(name, type, shape, modulo, offset, encoding);

      setCompression(compression);
   }

   public ChannelConfig(ChannelConfig config) {
      this(config.name, config.type, config.shape, config.modulo, config.offset, config.encoding, config.compression);
   }

   public void copy(ChannelConfig other) {
      this.name = other.name;      
      this.modulo = other.modulo;
      this.offset = other.offset;
      setType(other.type);
      setShape(other.shape);
      setEncoding (other.encoding);
      setCompression(other.compression);
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
      this.type =  (type == null) ? DEFAULT_TYPE : type;
   }

   public int[] getShape() {
      return this.shape;
   }

   public void setShape(int[] shape) {
      this.shape = (shape == null) ? DEFAULT_SHAPE : shape;
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
      this.encoding = (encoding == null) ? DEFAULT_ENCODING : encoding;
   }

   public Compression getCompression() {
      return compression;
   }

   public void setCompression(Compression compression) {
      this.compression = (compression == null) ? Compression.none : compression;
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

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((compression == null) ? 0 : compression.hashCode());
      result = prime * result + ((encoding == null) ? 0 : encoding.hashCode());
      result = prime * result + modulo;
      result = prime * result + ((name == null) ? 0 : name.hashCode());
      result = prime * result + offset;
      result = prime * result + Arrays.hashCode(shape);
      result = prime * result + ((type == null) ? 0 : type.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      ChannelConfig other = (ChannelConfig) obj;
      if (compression != other.compression)
         return false;
      if (encoding == null) {
         if (other.encoding != null)
            return false;
      } else if (!encoding.equals(other.encoding))
         return false;
      if (modulo != other.modulo)
         return false;
      if (name == null) {
         if (other.name != null)
            return false;
      } else if (!name.equals(other.name))
         return false;
      if (offset != other.offset)
         return false;
      if (!Arrays.equals(shape, other.shape))
         return false;
      if (type != other.type)
         return false;
      return true;
   }
}
