package ch.psi.bsread;

import ch.psi.bsread.message.ChannelConfig;
import ch.psi.bsread.message.Timestamp;

public abstract class DataChannel<T> implements TimeProvider {

	private final ChannelConfig config;

	public DataChannel(ChannelConfig config) {
		this.config = config;
	}

	public ChannelConfig getConfig() {
		return config;
	}

	public abstract T getValue(long pulseId);

	public Timestamp getTime(long pulseId) {
		return Timestamp.ofMillis(System.currentTimeMillis());
	}

}
