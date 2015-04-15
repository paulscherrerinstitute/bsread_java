package ch.psi.bsread.message;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class Timestamp implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	private long epoch;
	private long ns;

	public Timestamp() {
	}

	public Timestamp(long epoch, long ns) {
		this.epoch = epoch;
		this.ns = ns;
	}
	
	public Timestamp(long[] values){
		this.epoch = values[0];
		this.ns = values[1];
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
