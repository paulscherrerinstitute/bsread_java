package ch.psi.bsread.common.concurrent.executor;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MonitoringRejectedExecutionHandler implements RejectedExecutionHandler {
	private static Logger LOGGER = LoggerFactory.getLogger(MonitoringRejectedExecutionHandler.class);

	private final RejectedExecutionHandler target;
	private final String poolName;
	private final BlockingQueue<Runnable> workQueue;

	public MonitoringRejectedExecutionHandler(RejectedExecutionHandler target, String poolName, BlockingQueue<Runnable> workQueue) {
		this.target = target;
		this.poolName = poolName;
		this.workQueue = workQueue;
	}

	@Override
	public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
		LOGGER.warn("'{}' at queue size of '{}' rejects '{}' .", poolName, workQueue.size(), r);
		target.rejectedExecution(r, executor);
	}
}
