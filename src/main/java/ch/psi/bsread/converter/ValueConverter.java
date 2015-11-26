package ch.psi.bsread.converter;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.psi.bsread.message.ChannelConfig;
import ch.psi.bsread.message.MainHeader;
import ch.psi.bsread.message.Timestamp;

public interface ValueConverter {
   public static final Logger LOGGER = LoggerFactory.getLogger(ValueConverter.class);
   // This value MUST correspond to ch.psi.data.converters.ByteConverter.DYNAMIC_NUMBER_OF_BYTES
   public static final int DYNAMIC_NUMBER_OF_BYTES = Integer.MAX_VALUE;

   /**
    * Converts a byte representation of a value into the actual value.
    * 
    * @param <V> The JAVA type
    * @param receivedValueBytes The byte representation of a value (might be compressed)
    * @param channelConfig The ChannelConfig
    * @param mainHeader The MainHeader
    * @param iocTimestamp The ioc Timestamp
    * @return The converted value
    */
   public <V> V getValue(ByteBuffer receivedValueBytes, ChannelConfig channelConfig, MainHeader mainHeader,
         Timestamp iocTimestamp);

   /**
    * Converts a byte representation of a value into the actual value.
    *
    * @param <V> The JAVA type
    * @param byteValue The byte representation of a value
    * @param channelConfig The ChannelConfig
    * @param mainHeader The MainHeader
    * @param iocTimestamp The ioc Timestamp
    * @param clazz The clazz to cast the object into.
    * @return The converted/casted value
    */
   default public <V> V getValue(ByteBuffer byteValue, ChannelConfig channelConfig, MainHeader mainHeader,
         Timestamp iocTimestamp, Class<V> clazz) {
      Object value = getValue(byteValue, channelConfig, mainHeader, iocTimestamp);
      if (clazz.isAssignableFrom(value.getClass())) {
         return clazz.cast(value);
      } else {
         throw new ClassCastException("Cast from '" + value.getClass().getName() + "' to '"
               + clazz.getClass().getName() + "' not possible.");
      }
   }

   /**
    * Converts a byte representation of a value into the actual value.
    * 
    * @param <V> The JAVA type
    * @param byteValue The byte representation of a value
    * @param channelConfig The ChannelConfig
    * @param mainHeader The MainHeader
    * @param iocTimestamp The ioc Timestamp
    * @param clazz The clazz to cast the object into.
    * @param defaultValue The default value to return if the cast is not possible
    * @return The converted/casted value
    */
   default public <V> V getValueOrDefault(ByteBuffer byteValue, ChannelConfig channelConfig, MainHeader mainHeader,
         Timestamp iocTimestamp, Class<V> clazz, V defaultValue) {
      Object value = getValue(byteValue, channelConfig, mainHeader, iocTimestamp);
      if (clazz.isAssignableFrom(value.getClass())) {
         return clazz.cast(value);
      } else {
         return defaultValue;
      }
   }
}
