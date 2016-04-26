package ch.psi.bsread.common.concurrent.singleton;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FutureSupplier<T> implements Supplier<T> {
	private static final Logger LOGGER = LoggerFactory.getLogger(FutureSupplier.class);
	
	private Future<T> future;
	private long timeout;
	
	public FutureSupplier(Future<T> future, long timeout, TimeUnit timeUnit){
		this.future = future;
		this.timeout = timeUnit.toNanos(timeout);
	}

	@Override
	public T get() {
		try {
			return future.get(timeout, TimeUnit.NANOSECONDS);
		} catch (Exception e) {
			// log since exceptions can get lost (e.g.in JAVA Streams)
			LOGGER.error("Could not load value from future.", e);
			throw new RuntimeException(e);
		}
	}

}
