package ch.psi.bsread.converter;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.psi.bsread.message.ChannelConfig;

public interface ValueConverter {
	public static final Logger LOGGER = LoggerFactory.getLogger(ValueConverter.class);

	/**
	 * Converts a byte representation of a value into the actual value.
	 * 
	 * @param <V>
	 *            The JAVA type
	 * @param byteValue
	 *            The byte representation of a value
	 * @param config
	 *            The ChannelConfig
	 * @return The converted value
	 */
	public <V> V getValue(ByteBuffer byteValue, ChannelConfig config);

	/**
	 * Converts a byte representation of a value into the actual value.
	 *
	 * @param <V>
	 *            The JAVA type
	 * @param byteValue
	 *            The byte representation of a value
	 * @param config
	 *            The ChannelConfig
	 * @param clazz
	 *            The clazz to cast the object into.
	 * @return The converted/casted value
	 */
	default public <V> V getValue(ByteBuffer byteValue, ChannelConfig config, Class<V> clazz) {
		Object value = getValue(byteValue, config);
		if (clazz.isAssignableFrom(value.getClass())) {
			return clazz.cast(value);
		} else {
			throw new ClassCastException("Cast from '" + value.getClass().getName() + "' to '" + clazz.getClass().getName() + "' not possible.");
		}
	}

	/**
	 * Converts a byte representation of a value into the actual value.
	 * 
	 * @param <V>
	 *            The JAVA type
	 * @param byteValue
	 *            The byte representation of a value
	 * @param config
	 *            The ChannelConfig
	 * @param clazz
	 *            The clazz to cast the object into.
	 * @param defaultValue
	 *            The default value to return if the cast is not possible
	 * @return The converted/casted value
	 */
	default public <V> V getValueOrDefault(ByteBuffer byteValue, ChannelConfig config, Class<V> clazz, V defaultValue) {
		Object value = getValue(byteValue, config);
		if (clazz.isAssignableFrom(value.getClass())) {
			return clazz.cast(value);
		} else {
			return defaultValue;
		}
	}
}
