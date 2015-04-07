package ch.psi.bsread.message;

import java.nio.ByteBuffer;

public class Value {
	
	private ByteBuffer value;
	private Timestamp timestamp;

	public Value(){
	}

	public Value(ByteBuffer value, Timestamp timestamp){
		this.value = value;
		this.timestamp = timestamp;
	}

	public void setTimestamp(Timestamp timestamp) {
		this.timestamp = timestamp;
	}

	public Timestamp getTimestamp() {
		return timestamp;
	}

	public void setValue(ByteBuffer value) {
		this.value = value;
	}

	public ByteBuffer getValue() {
		return value;
	}
}
