package ch.psi.bsread.message;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

@JsonInclude(Include.NON_DEFAULT)
public class ChannelConfig {

	public  enum Type {Double, String, Long, ULong, Short, UShort};
	
	private String name;
	private String type = Type.Double.name().toLowerCase();
	private int[] shape = {1};
	private double frequency = 100;
	private int offset = 0;

	public ChannelConfig() {
	}

	public ChannelConfig(String name, String type, int[] shape, double frequency, int offset) {
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

	public String getType() {
		return this.type;
	}

	public void setType(String type) {
		this.type = type.toLowerCase();
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
