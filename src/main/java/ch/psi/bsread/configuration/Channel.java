package ch.psi.bsread.configuration;

import java.io.Serializable;

public class Channel implements Serializable {
	private static final long serialVersionUID = 286422739407172968L;

	private String name;
	private int modulo = 1;
	private int offset = 0;

	public Channel() {
	}

	public Channel(String name) {
		this.name = name;
	}

	public Channel(String name, int modulo) {
		this.name = name;
		this.modulo = modulo;
	}

	public Channel(String name, int modulo, int offset) {
		this.name = name;
		this.modulo = modulo;
		this.offset = offset;
	}

	public int getOffset() {
		return offset;
	}

	public void setOffset(int offset) {
		this.offset = offset;
	}

	public int getModulo() {
		return modulo;
	}

	public void setModulo(int modulo) {
		this.modulo = modulo;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	// Overwrites for hashCode and equals is need as we don't wan't to have duplicate entries
	// in e.g. hashmaps for objects with the same values.
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + modulo;
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
		if (modulo != other.modulo)
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
		return name + " modulo: " + modulo + " offset: " + offset;
	}
}
