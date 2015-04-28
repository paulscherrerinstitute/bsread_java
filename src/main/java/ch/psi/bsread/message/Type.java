package ch.psi.bsread.message;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum Type implements Serializable {
	Boolean("Boolean"),
	Byte("Byte"),
	UByte("UByte"),
	Short("Short"),
	UShort("UShort"),
    Integer("Integer"),
    UInteger("UInteger"),
    Long("Long"),
    ULong("ULong"),
    Float("Float"),
    Double("Double"), 
    String("String");
    
    private String key;

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