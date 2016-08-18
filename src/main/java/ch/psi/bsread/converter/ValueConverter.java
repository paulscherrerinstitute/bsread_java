package ch.psi.bsread.converter;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.psi.bsread.message.ChannelConfig;
import ch.psi.bsread.message.DataHeader;
import ch.psi.bsread.message.MainHeader;
import ch.psi.bsread.message.Timestamp;
import ch.psi.bsread.message.Value;
import ch.psi.bsread.message.ValueImpl;

public interface ValueConverter {
	public static final Logger LOGGER = LoggerFactory.getLogger(ValueConverter.class);
	// This value MUST correspond to
	// ch.psi.data.converters.ByteConverter.DYNAMIC_NUMBER_OF_BYTES
	public static final int DYNAMIC_NUMBER_OF_BYTES = Integer.MAX_VALUE;

	/**
	 * Converts a byte representation of a value into the actual value.
	 * 
	 * @param <V>
	 *            The JAVA type
	 * @param mainHeader
	 *            The MainHeader
	 * @param dataHeader
	 *            The DataHeader
	 * @param channelConfig
	 *            The ChannelConfig
	 * @param byteValue
	 *            The byte representation of a value (might be compressed)
	 * @param iocTimestamp
	 *            The ioc Timestamp
	 * @return The converted value
	 */
	default public <V> Value<V> getMessageValue(MainHeader mainHeader, DataHeader dataHeader, ChannelConfig channelConfig, ByteBuffer byteValue,
			Timestamp iocTimestamp) {
		return new ValueImpl<>(getValue(mainHeader, dataHeader, channelConfig, byteValue, iocTimestamp), iocTimestamp);
	}

	/**
	 * Converts a byte representation of a value into the actual value.
	 * 
	 * @param <V>
	 *            The JAVA type
	 * @param mainHeader
	 *            The MainHeader
	 * @param dataHeader
	 *            The DataHeader
	 * @param channelConfig
	 *            The ChannelConfig
	 * @param byteValue
	 *            The byte representation of a value (might be compressed)
	 * @param iocTimestamp
	 *            The ioc Timestamp
	 * @return The converted value
	 */
	public <V> V getValue(MainHeader mainHeader, DataHeader dataHeader, ChannelConfig channelConfig, ByteBuffer byteValue,
			Timestamp iocTimestamp);

	/**
	 * Converts a byte representation of a value into the actual value.
	 *
	 * @param <V>
	 *            The JAVA type
	 * @param mainHeader
	 *            The MainHeader
	 * @param dataHeader
	 *            The DataHeader
	 * @param channelConfig
	 *            The ChannelConfig
	 * @param byteValue
	 *            The byte representation of a value (might be compressed)
	 * @param iocTimestamp
	 *            The ioc Timestamp
	 * @param clazz
	 *            The clazz to cast the object into.
	 * @return The converted/casted value
	 */
	default public <V> V getValue(MainHeader mainHeader, DataHeader dataHeader, ChannelConfig channelConfig, ByteBuffer byteValue,
			Timestamp iocTimestamp, Class<V> clazz) {
		Object value = getValue(mainHeader, dataHeader, channelConfig, byteValue, iocTimestamp);
		if (clazz.isAssignableFrom(value.getClass())) {
			return clazz.cast(value);
		} else {
			throw new ClassCastException("Cast from '" + value.getClass().getName() +
					"' to '"
					+ clazz.getClass().getName() + "' not possible.");
		}
	}

	/**
	 * Converts a byte representation of a value into the actual value.
	 *
	 * @param <V>
	 *            The JAVA type
	 * @param mainHeader
	 *            The MainHeader
	 * @param dataHeader
	 *            The DataHeader
	 * @param channelConfig
	 *            The ChannelConfig
	 * @param byteValue
	 *            The byte representation of a value (might be compressed)
	 * @param iocTimestamp
	 *            The ioc Timestamp
	 * @param clazz
	 *            The clazz to cast the object into.
	 * @param defaultValue
	 *            The default value to return if the cast is not possible
	 * @return The converted/casted value
	 */
	default public <V> V getValueOrDefault(MainHeader mainHeader, DataHeader dataHeader, ChannelConfig channelConfig, ByteBuffer byteValue,
			Timestamp iocTimestamp, Class<V> clazz, V defaultValue) {
		Object value = getValue(mainHeader, dataHeader, channelConfig, byteValue, iocTimestamp);
		if (clazz.isAssignableFrom(value.getClass())) {
			return clazz.cast(value);
		} else {
			return defaultValue;
		}
	}
}
