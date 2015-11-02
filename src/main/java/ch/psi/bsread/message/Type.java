package ch.psi.bsread.message;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum Type implements Serializable {
	Bool("bool"),
	Int8("int8"),
	UInt8("uint8"),
	Int16("int16"),
	UInt16("uint16"),
	Int32("int32"),
	UInt32("uint32"),
	Int64("int64"),
	UInt64("uint64"),
	Float32("float32"),
	Float64("float64"),
	String("string");

	private final String key;

	Type(String key) {
		this.key = key;
	}

	@JsonValue
	public String getKey() {
		return key;
	}

	@JsonCreator
	public static Type newInstance(String key) {
		for (Type type : Type.values()) {
			if (key.equalsIgnoreCase(type.key)) {
				return type;
			}
		}
		throw new NullPointerException("Type does not exist");
	}
}