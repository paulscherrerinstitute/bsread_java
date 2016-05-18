package ch.psi.bsread.common.concurrent.executor;

import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MonitoringRejectedExecutionHandler implements RejectedExecutionHandler {
	private static Logger LOGGER = LoggerFactory.getLogger(MonitoringRejectedExecutionHandler.class);

	private final RejectedExecutionHandler target;
	private final String poolName;

	public MonitoringRejectedExecutionHandler(RejectedExecutionHandler target, String poolName) {
		this.target = target;
		this.poolName = poolName;
	}

	@Override
	public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
		LOGGER.warn("'{}' at queue size of '{}' rejects '{}' .", poolName, executor.getQueue().size(), r);
		target.rejectedExecution(r, executor);
	}
}
