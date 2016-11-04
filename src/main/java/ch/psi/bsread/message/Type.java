package ch.psi.bsread.message;

import java.io.Serializable;
import java.util.Arrays;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonValue;

import ch.psi.bsread.converter.ValueConverter;

public enum Type implements Serializable {
   Bool("bool", Byte.BYTES),
   Int8("int8", Byte.BYTES),
   UInt8("uint8", Byte.BYTES),
   Int16("int16", Short.BYTES),
   UInt16("uint16", Short.BYTES),
   Int32("int32", Integer.BYTES),
   UInt32("uint32", Integer.BYTES),
   Int64("int64", Long.BYTES),
   UInt64("uint64", Long.BYTES),
   Float32("float32", Float.BYTES),
   Float64("float64", Double.BYTES),
   String("string", ValueConverter.DYNAMIC_NUMBER_OF_BYTES);

   private final String key;
   private final int nBytes;

   Type(String key, int nBytes) {
      this.key = key;
      this.nBytes = nBytes;
   }

   @JsonValue
   public String getKey() {
      return key;
   }

   /**
    * Returns the number of bytes used to represent one element. This method can return
    * DYNAMIC_NUMBER_OF_BYTES to indicate that there is no fixed number of bytes to represent one
    * value.
    * 
    * @return int The number of bytes
    */
   @JsonIgnore
   public int getBytes() {
      return nBytes;
   }

   @JsonCreator
   public static Type newInstance(String key) {
      for (Type type : Type.values()) {
         if (key.equalsIgnoreCase(type.key)) {
            return type;
         }
      }
      throw new IllegalArgumentException("Type '" + key + "' does not exist. Possible values are '"
            + Arrays.toString(Type.values()) + "'");
   }
}
