package ch.psi.bsread.common.concurrent.singleton;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * Lazy initialization of a Singleton when the blueprint is known at creation
 * time.
 */
public final class Deferred<T> implements Supplier<T> {
	private volatile Supplier<T> supplier = null;
	private T object = null;

	public Deferred(Supplier<T> supplier) {
		this.supplier = Objects.requireNonNull(supplier);
	}

	public Deferred(T object) {
		this.object = object;
	}

	@Override
	public T get() {
		if (supplier != null) {
			synchronized (this) {
				if (supplier != null) {
					object = supplier.get();
					supplier = null;
				}
			}
		}
		return object;
	}
}