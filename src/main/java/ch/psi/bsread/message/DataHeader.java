package ch.psi.bsread.message;

import java.io.Serializable;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

@JsonInclude(Include.NON_DEFAULT)
public class DataHeader implements Serializable {

	private static final long serialVersionUID = -6607503120756755170L;

	public static final String DEFAULT_HTYPE = "bsr_d-1.1";

	@JsonInclude
	private String htype = DEFAULT_HTYPE;
	private Map<String, ChannelConfig> channelsMapping = new LinkedHashMap<>();

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

	public Collection<ChannelConfig> getChannels() {
		return channelsMapping.values();
	}

	public void setChannels(Collection<ChannelConfig> channels) {
		this.channelsMapping.clear();
		for (ChannelConfig channelConfig : channels) {
			this.channelsMapping.put(channelConfig.getName(), channelConfig);
		}
	}

	public void addChannel(ChannelConfig channel) {
		this.channelsMapping.put(channel.getName(), channel);
	}

	@JsonIgnore
	public Map<String, ChannelConfig> getChannelsMapping() {
		return channelsMapping;
	}
}
