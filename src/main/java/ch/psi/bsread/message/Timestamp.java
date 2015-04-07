package ch.psi.bsread.message;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class Timestamp {
	
	private long epoch;
	private long ns;

	public Timestamp() {
	}

	public Timestamp(long epoch, long ns) {
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
	
	@JsonIgnore
	public long[] getAsLongArray(){
		return new long[] {epoch, ns};
	}
}
