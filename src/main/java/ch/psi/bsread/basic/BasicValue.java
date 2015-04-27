package ch.psi.bsread.basic;

import java.io.Serializable;

import ch.psi.bsread.message.Timestamp;

public class  BasicValue<T> implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	private T value;
	private Timestamp timestamp;

	public BasicValue(T value, Timestamp timestamp){
		this.value = value;
		this.timestamp = timestamp;
	}

	public void setTimestamp(Timestamp timestamp) {
		this.timestamp = timestamp;
	}

	public Timestamp getTimestamp() {
		return timestamp;
	}

	public void setValue(T value) {
		this.value = value;
	}

	public T getValue() {
		return value;
	}
}
