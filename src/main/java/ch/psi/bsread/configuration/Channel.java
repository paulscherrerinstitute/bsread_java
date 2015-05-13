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

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		long temp;
		temp = Double.doubleToLongBits(frequency);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + offset;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Channel other = (Channel) obj;
		if (Double.doubleToLongBits(frequency) != Double.doubleToLongBits(other.frequency))
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (offset != other.offset)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return name + " " + frequency + " " + offset;
	}
}
