package ch.psi.bsread.message;

import com.fasterxml.jackson.annotation.JsonProperty;

public class MainHeader {

	public static final String DEFAULT_HTYPE = "bsr_m-1.0";
	
	private String htype = DEFAULT_HTYPE;
	private long pulseId;
	private GlobalTimestamp globalTimestamp;
	private String hash;

	public MainHeader() {
	}

	public MainHeader(String htype, long pulseId, GlobalTimestamp globalTimestamp, String hash) {
		this.htype = htype;
		this.pulseId = pulseId;
		this.globalTimestamp = globalTimestamp;
		this.hash = hash;
	}

	public String getHtype() {
		return htype;
	}

	public void setHtype(String htype) {
		this.htype = htype;
	}

	@JsonProperty("pulse_id")
	public long getPulseId() {
		return pulseId;
	}

	@JsonProperty("pulse_id")
	public void setPulseId(long pulseId) {
		this.pulseId = pulseId;
	}

	@JsonProperty("global_timestamp")
	public GlobalTimestamp getGlobalTimestamp() {
		return this.globalTimestamp;
	}

	@JsonProperty("global_timestamp")
	public void setGlobalTimestamp(GlobalTimestamp globalTimestamp) {
		this.globalTimestamp = globalTimestamp;
	}

	public String getHash() {
		return hash;
	}

	public void setHash(String hash) {
		this.hash = hash;
	}
	
}
