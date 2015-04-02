package ch.psi.bsread.message;

public class GlobalTimestamp {
	
	private long epoch;
	private long ns;

	public GlobalTimestamp() {
	}

	public GlobalTimestamp(long epoch, long ns) {
		this.epoch = epoch;
		this.ns = ns;
	}

	public long getEpoch() {
		return epoch;
	}

	public void setEpoch(long epoch) {
		this.epoch = epoch;
	}

	public long getNs() {
		return ns;
	}

	public void setNs(long ns) {
		this.ns = ns;
	}
}
