package ch.psi.bsread.message;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

@JsonInclude(Include.NON_DEFAULT)
public class ChannelConfig implements Serializable {
	private static final long serialVersionUID = 1L;
	// use a static variable due to Include.NON_DEFAULT
	private static final int[] DEFAULT_SHAPE = {1};
	
	private String name;
	private Type type = Type.Double;
	private int[] shape = DEFAULT_SHAPE;
	private double frequency = 100;
	private int offset = 0;

	public ChannelConfig() {
	}
	
	public ChannelConfig(String name, Type type) {
		this.name = name;
		this.type = type;
	}

	public ChannelConfig(String name, Type type, double frequency, int offset) {
		this.name = name;
		this.type = type;
		this.frequency = frequency;
		this.offset = offset;
	}
	
	public ChannelConfig(String name, Type type, int[] shape, double frequency, int offset) {
		this.name = name;
		this.type = type;
		this.shape = shape;
		this.frequency = frequency;
		this.offset = offset;
	}

	public String getName() {
		return this.name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Type getType() {
		return this.type;
	}

	public void setType(Type type) {
		this.type = type;
	}

	public int[] getShape() {
		return this.shape;
	}

	public void setShape(int[] shape) {
		this.shape = shape;
	}

	public double getFrequency() {
		return this.frequency;
	}

	public void setFrequency(double frequency) {
		this.frequency = frequency;
	}

	public int getOffset() {
		return this.offset;
	}

	public void setOffset(int offset) {
		this.offset = offset;
	}
}
