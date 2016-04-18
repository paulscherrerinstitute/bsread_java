package ch.psi.bsread.common.concurrent.executor;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MonitoringExecutorService implements ExecutorService {
	private static Logger LOGGER = LoggerFactory.getLogger(MonitoringExecutorService.class);

	private final ExecutorService target;
	private final BlockingQueue<Runnable> workQueue;
	private final int logMessageAtQueueSize;

	public MonitoringExecutorService(ExecutorService target, BlockingQueue<Runnable> workQueue, int logMessageAtQueueSize) {
		this.target = target;
		this.workQueue = workQueue;
		this.logMessageAtQueueSize = logMessageAtQueueSize;
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

	private <T> Callable<T> wrap(final Callable<T> task) {
		final Exception clientStack = clientTrace();
		final String clientThreadName = Thread.currentThread().getName();
		final long startTime = System.nanoTime();

		int submitSize = workQueue.size();
		if (submitSize > logMessageAtQueueSize) {
			LOGGER.info("Submit task '{}' at queue size '{}'.", task, submitSize);
		}
		return () -> {
			if (submitSize > logMessageAtQueueSize) {
				LOGGER.info("Task '{}' spent {}ns in the queue having size of '{}' and submit size of '{}'.", task,
						(System.nanoTime() - startTime), workQueue.size(), submitSize);
			}

			try {
				return task.call();
			} catch (Exception e) {
				LOGGER.error("Exception '{}' in task submitted from thread '{}' here:", e, clientThreadName, clientStack);
				throw e;
			}
		};
	}

	private Runnable wrap(final Runnable run) {
		final Exception clientStack = clientTrace();
		final String clientThreadName = Thread.currentThread().getName();
		final long startTime = System.nanoTime();

		int submitSize = workQueue.size();
		if (submitSize > logMessageAtQueueSize) {
			LOGGER.info("Submit runnable '{}' at queue size '{}'.", run, submitSize);
		}
		return () -> {
			if (submitSize > logMessageAtQueueSize) {
				LOGGER.info("Runnable '{}' spent {}ns in the queue having size of '{}' and submit size of '{}'.", run,
						(System.nanoTime() - startTime), workQueue.size(), submitSize);
			}

			try {
				run.run();
			} catch (Exception e) {
				LOGGER.error("Exception '{}' in task submitted from thread '{}' here:", e, clientThreadName, clientStack);
				throw e;
			}
		};
	}

	private Exception clientTrace() {
		return new Exception("Client stack trace");
	}
}
