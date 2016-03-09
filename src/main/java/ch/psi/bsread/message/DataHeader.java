package ch.psi.bsread.message;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

@JsonInclude(Include.NON_DEFAULT)
public class DataHeader implements Serializable {

	private static final long serialVersionUID = -6607503120756755170L;

	public static final String DEFAULT_HTYPE = "bsr_d-1.1";

	@JsonInclude
	private String htype = DEFAULT_HTYPE;
	private List<ChannelConfig> channels = new ArrayList<>();

	public DataHeader() {
	}

	public DataHeader(String htype) {
		this.htype = htype;
	}

	public String getHtype() {
		return htype;
	}

	public void setHtype(String htype) {
		this.htype = htype;
	}

	public List<ChannelConfig> getChannels() {
		return channels;
	}

	public void setChannels(List<ChannelConfig> channels) {
		this.channels = channels;
	}

	public void addChannel(ChannelConfig channel) {
		this.channels.add(channel);
	}
}
