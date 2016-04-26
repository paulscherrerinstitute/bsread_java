package ch.psi.bsread.message;

import java.io.Serializable;
import java.util.function.Supplier;

public interface Value<V> extends Serializable {
	public static final long DEFAULT_TIMEOUT_IN_MILLIS = 30000;

	public void setTimestamp(Timestamp timestamp);

	public Timestamp getTimestamp();

	public void setValue(Supplier<V> value);

	public void setValue(V value);

	public V getValue();

	default <W> W getValue(Class<W> clazz) {
		Object value = getValue();
		if (clazz.isAssignableFrom(value.getClass())) {
			return clazz.cast(value);
		} else {
			throw new ClassCastException("Cast from '" + value.getClass().getName() + "' to '" + clazz.getClass().getName() + "' not possible.");
		}
	}

	default <W> W getValueOrDefault(Class<W> clazz, W defaultValue) {
		Object val = getValue();
		if (clazz.isAssignableFrom(val.getClass())) {
			return clazz.cast(val);
		} else {
			return defaultValue;
		}
	}
}
