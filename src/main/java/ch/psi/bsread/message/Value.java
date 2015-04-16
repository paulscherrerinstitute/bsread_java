package ch.psi.bsread.message;

import java.io.Serializable;

public class Value implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	private byte[] value;
	private Timestamp timestamp;

	public Value(){
	}
	
	public Value(String channelName, byte[] value, Timestamp timestamp){
		this.value = value;
		this.timestamp = timestamp;
	}

	public void setTimestamp(Timestamp timestamp) {
		this.timestamp = timestamp;
	}

	public Timestamp getTimestamp() {
		return timestamp;
	}

	public void setValue(byte[] value) {
		this.value = value;
	}

	public byte[] getValue() {
		return value;
	}
}
