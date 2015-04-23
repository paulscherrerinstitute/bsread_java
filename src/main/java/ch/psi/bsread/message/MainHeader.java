package ch.psi.bsread.message;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

@JsonInclude(Include.NON_DEFAULT)
public class MainHeader implements Serializable {

	private static final long serialVersionUID = 1L;
	
	public static final String HTYPE_VALUE_NO_VERSION = "bsr_m";
	public static final String DEFAULT_HTYPE = HTYPE_VALUE_NO_VERSION + "-1.0";

	private String htype = DEFAULT_HTYPE;
	private long pulseId;
	private Timestamp globalTimestamp;
	private String hash;

	public MainHeader() {
	}

	public MainHeader(String htype, long pulseId, Timestamp globalTimestamp, String hash) {
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
	public Timestamp getGlobalTimestamp() {
		return this.globalTimestamp;
	}

	@JsonProperty("global_timestamp")
	public void setGlobalTimestamp(Timestamp globalTimestamp) {
		this.globalTimestamp = globalTimestamp;
	}

	public String getHash() {
		return hash;
	}

	public void setHash(String hash) {
		this.hash = hash;
	}

}
