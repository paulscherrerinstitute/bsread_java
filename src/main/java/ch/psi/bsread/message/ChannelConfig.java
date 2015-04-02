package ch.psi.bsread.message;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

@JsonInclude(Include.NON_DEFAULT)
public class ChannelConfig {

	public static final String DEFAULT_TYPE = "double";
	public static final int[] DEFAULT_SHAPE = {1};
	public static final double DEFAULT_FREQUENCY = 100;
	public static final int DEFAULT_OFFSET = 0;
	
	
	private String name;
	private String type = DEFAULT_TYPE;
	private int[] shape = DEFAULT_SHAPE;
	private double frequency = DEFAULT_FREQUENCY;
	private int offset = DEFAULT_OFFSET;

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
