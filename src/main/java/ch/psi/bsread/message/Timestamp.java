package ch.psi.bsread.message;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class Timestamp implements Serializable {
	private static final long serialVersionUID = 2481654141864121974L;

	// the milliseconds (like UNIX or JAVA)
	private long epoch;
	// the ns offset (to millisecond)
	private long ns;

	public Timestamp() {
	}

	public Timestamp(long epoch, long ns) {
		this.epoch = epoch;
		this.ns = ns;
	}

	public Timestamp(long[] values) {
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
	public long[] getAsLongArray() {
		return new long[] { epoch, ns };
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (epoch ^ (epoch >>> 32));
		result = prime * result + (int) (ns ^ (ns >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Timestamp other = (Timestamp) obj;
		if (epoch != other.epoch)
			return false;
		if (ns != other.ns)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return epoch + " " + ns;
	}
}
