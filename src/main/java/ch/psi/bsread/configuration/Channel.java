package ch.psi.bsread.configuration;

import java.io.Serializable;

public class Channel implements Serializable {
	private static final long serialVersionUID = 286422739407172968L;
	
	private String name;
	private double frequency = 100;
	private int offset = 0;

	public Channel() {
	}

	public Channel(String name) {
		this.name = name;
	}

	public Channel(String name, double frequency) {
		this.name = name;
		this.frequency = frequency;
	}
	
	public Channel(String name, double frequency, int offset) {
		this.name = name;
		this.frequency = frequency;
		this.offset = offset;
	}

	public int getOffset() {
		return offset;
	}

	public void setOffset(int offset) {
		this.offset = offset;
	}

	public double getFrequency() {
		return frequency;
	}

	public void setFrequency(double frequency) {
		this.frequency = frequency;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

}
