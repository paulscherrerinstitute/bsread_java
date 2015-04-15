package ch.psi.bsread.message;

import java.io.Serializable;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class DataHeader implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	public static final String ENCODING_BIG_ENDIAN = "big";
	public static final String ENCODING_LITTLE_ENDIAN = "little";
	public static final String DEFAULT_ENCODING = ENCODING_LITTLE_ENDIAN;
	public static final String DEFAULT_HTYPE = "bsr_d-1.0";
	
	
	private String htype = DEFAULT_HTYPE;
	private String encoding = DEFAULT_ENCODING;
	private List<ChannelConfig> channels = new ArrayList<>();

	public DataHeader() {
	}

	public DataHeader(String htype, String encoding) {
		this.htype = htype;
		this.encoding = encoding;
	}

	public String getHtype() {
		return htype;
	}

	public void setHtype(String htype) {
		this.htype = htype;
	}
	
	public String getEncoding() {
		return encoding;
	}

	public void setEncoding(String encoding) {
		this.encoding = encoding;
	}

	public List<ChannelConfig> getChannels() {
		return channels;
	}

	public void setChannels(List<ChannelConfig> channels) {
		this.channels = channels;
	}
	
	/**
	 * Get the byte order based on the specified endianess
	 * @return	ByteOrder of data
	 */
	@JsonIgnore
	public ByteOrder getByteOrder() {
		if (this.encoding != null && this.encoding.contains(ENCODING_BIG_ENDIAN)) {
			return ByteOrder.BIG_ENDIAN;
		} else {
			return ByteOrder.LITTLE_ENDIAN;
		}
	}
	
	@JsonIgnore
	public void setByteOrder(ByteOrder byteOrder){
		if (byteOrder.equals(ByteOrder.BIG_ENDIAN)) {
			encoding = DataHeader.ENCODING_BIG_ENDIAN;
		}
		else{
			encoding = DataHeader.ENCODING_LITTLE_ENDIAN;
		}
	}
}
