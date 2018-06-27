package ch.psi.bsread.common.concurrent.executor;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public abstract class AbstractMonitoringExecutorService implements ExecutorService {
	private final ThreadPoolExecutor target;

	public AbstractMonitoringExecutorService(ThreadPoolExecutor target) {
		this.target = target;
	}
	
	public ThreadPoolExecutor getTarget(){
		return target;
	}

	@Override
	public void execute(Runnable command) {
		target.execute(wrap(command));
	}

	@Override
	public void shutdown() {
		target.shutdown();
	}

	@Override
	public List<Runnable> shutdownNow() {
		return target.shutdownNow();
	}

	@Override
	public boolean isShutdown() {
		return target.isShutdown();
	}

	@Override
	public boolean isTerminated() {
		return target.isTerminated();
	}

	@Override
	public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
		return target.awaitTermination(timeout, unit);
	}

	@Override
	public <T> Future<T> submit(Callable<T> task) {
		return target.submit(wrap(task));
	}

	@Override
	public <T> Future<T> submit(Runnable task, T result) {
		return target.submit(wrap(task), result);
	}

	@Override
	public Future<?> submit(Runnable task) {
		return target.submit(wrap(task));
	}

	@Override
	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
		return target.invokeAll(tasks.stream().map(this::wrap).collect(Collectors.toList()));
	}

	@Override
	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
			throws InterruptedException {
		return target.invokeAll(tasks.stream().map(this::wrap).collect(Collectors.toList()), timeout, unit);
	}

	@Override
	public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
		return target.invokeAny(tasks.stream().map(this::wrap).collect(Collectors.toList()));
	}

	@Override
	public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
			throws InterruptedException, ExecutionException, TimeoutException {
		return target.invokeAny(tasks.stream().map(this::wrap).collect(Collectors.toList()), timeout, unit);
	}

	protected abstract <T> Callable<T> wrap(final Callable<T> task);

	protected abstract Runnable wrap(final Runnable run);

	protected Exception clientTrace() {
		return new Exception("Client stack trace");
	}
}
