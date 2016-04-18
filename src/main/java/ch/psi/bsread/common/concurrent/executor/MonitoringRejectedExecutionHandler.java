package ch.psi.bsread.common.concurrent.executor;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MonitoringRejectedExecutionHandler implements RejectedExecutionHandler {
	private static Logger LOGGER = LoggerFactory.getLogger(MonitoringRejectedExecutionHandler.class);

	private final RejectedExecutionHandler target;
	private final BlockingQueue<Runnable> workQueue;

	public MonitoringRejectedExecutionHandler(RejectedExecutionHandler target, BlockingQueue<Runnable> workQueue) {
		this.target = target;
		this.workQueue = workQueue;
	}

	@Override
	public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
		LOGGER.warn("Reject Runnable '{}' at queue size of '{}'.", r, workQueue.size());
		target.rejectedExecution(r, executor);
	}
}
