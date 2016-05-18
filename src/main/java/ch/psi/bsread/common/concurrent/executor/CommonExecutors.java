package ch.psi.bsread.common.concurrent.executor;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;

public class CommonExecutors {
	public static final boolean DEFAULT_IS_MONITORING = false;
	private static final RejectedExecutionHandler DEFAULT_HANDLER = new AbortPolicy();

	public static ExecutorService newFixedThreadPool(int nThreads, int queueSize, String poolName,
			boolean monitoring) {
		final ThreadFactory threadFactory =
				new BasicThreadFactory.Builder().namingPattern(poolName + "-%d").build();

		BlockingQueue<Runnable> workQueue;
		if (queueSize > 0) {
			workQueue = new LinkedBlockingQueue<>(queueSize);
		} else {
			workQueue = new LinkedBlockingQueue<>();
		}

		RejectedExecutionHandler rejectedExecutionHandler = DEFAULT_HANDLER;

		// always monitor rejections
		rejectedExecutionHandler = new MonitoringRejectedExecutionHandler(rejectedExecutionHandler, poolName);

		ThreadPoolExecutor executor =
				new ThreadPoolExecutor(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS, workQueue, threadFactory,
						rejectedExecutionHandler);

		if (monitoring) {
			return new MonitoringExecutorService(executor, 0);
		} else {
			return executor;
		}
	}

	public static ScheduledExecutorService newScheduledThreadPool(int nThreads, String poolName, boolean monitoring) {
		final ThreadFactory threadFactory =
				new BasicThreadFactory.Builder().namingPattern(poolName + "-%d").build();

		RejectedExecutionHandler rejectedExecutionHandler = DEFAULT_HANDLER;

		// always monitor rejections
		rejectedExecutionHandler = new MonitoringRejectedExecutionHandler(rejectedExecutionHandler, poolName);

		ScheduledThreadPoolExecutor executor =
				new ScheduledThreadPoolExecutor(nThreads, threadFactory, rejectedExecutionHandler);

		if (monitoring) {
			return new MonitoringScheduledExecutorService(executor, 0);
		} else {
			return executor;
		}
	}

	/**
	 * A handler for rejected tasks that throws a
	 * {@code RejectedExecutionException}.
	 */
	public static class AbortPolicy implements RejectedExecutionHandler {
		/**
		 * Creates an {@code AbortPolicy}.
		 */
		public AbortPolicy() {
		}

		/**
		 * Always throws RejectedExecutionException.
		 *
		 * @param r
		 *            the runnable task requested to be executed
		 * @param e
		 *            the executor attempting to execute this task
		 * @throws RejectedExecutionException
		 *             always
		 */
		public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
			throw new RejectedExecutionException("Task " + r.toString() +
					" rejected from " +
					e.toString());
		}
	}
}
