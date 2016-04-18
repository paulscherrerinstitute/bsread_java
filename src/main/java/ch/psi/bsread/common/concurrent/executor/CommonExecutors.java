package ch.psi.bsread.common.concurrent.executor;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;

public class CommonExecutors {
	public static final boolean DEFAULT_IS_MONITORING = true;
	private static final RejectedExecutionHandler DEFAULT_HANDLER = new AbortPolicy();

	public static ExecutorService newFixedThreadPool(int nThreads, int queueSize, String threadsNamePrefix,
			boolean monitoring) {
		final ThreadFactory threadFactory =
				new BasicThreadFactory.Builder().namingPattern(threadsNamePrefix + "-%d").build();

		BlockingQueue<Runnable> workQueue;
		if (queueSize > 0) {
			workQueue = new LinkedBlockingQueue<>(queueSize);
		} else {
			workQueue = new LinkedBlockingQueue<>();
		}

		RejectedExecutionHandler rejectedExecutionHandler = DEFAULT_HANDLER;

		if (monitoring) {
			rejectedExecutionHandler = new MonitoringRejectedExecutionHandler(rejectedExecutionHandler, workQueue);
		}

		ExecutorService executor =
				new ThreadPoolExecutor(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS, workQueue, threadFactory,
						rejectedExecutionHandler);

		if (monitoring) {
			executor = new MonitoringExecutorService(executor, workQueue, 0);
		}

		return executor;
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
