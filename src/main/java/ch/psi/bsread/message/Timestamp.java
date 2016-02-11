package ch.psi.bsread.message;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Timestamp implements Serializable {
	private static final long serialVersionUID = 2481654141864121974L;

	// the milliseconds (like UNIX or JAVA)
	private long ms;
	// the ns offset (to millisecond)
	private long nsOffset;

	public Timestamp() {
	}

	public Timestamp(long ms, long nsOffset) {
		this.ms = ms;
		this.nsOffset = nsOffset;
	}

	public Timestamp(long[] values) {
		this.ms = values[0];
		this.nsOffset = values[1];
	}

	public long getMs() {
		return ms;
	}

	public void setMs(long ms) {
		this.ms = ms;
	}

	@JsonProperty("ns_offset")
	public long getNsOffset() {
		return nsOffset;
	}

	@JsonProperty("ns_offset")
	public void setNsOffset(long ns) {
		this.nsOffset = ns;
	}

	@JsonIgnore
	public long[] getAsLongArray() {
		return new long[] { ms, nsOffset };
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (ms ^ (ms >>> 32));
		result = prime * result + (int) (nsOffset ^ (nsOffset >>> 32));
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
		if (ms != other.ms)
			return false;
		if (nsOffset != other.nsOffset)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return ms + " " + nsOffset;
	}
}
