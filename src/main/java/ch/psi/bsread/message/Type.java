package ch.psi.bsread.message;

import java.io.Serializable;

import ch.psi.data.converters.ConverterProvider;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum Type implements Serializable {
	Boolean(ConverterProvider.TYPE_BOOLEAN),
	Byte(ConverterProvider.TYPE_BYTE),
	UByte(ConverterProvider.TYPE_UBYTE),
	Short(ConverterProvider.TYPE_SHORT),
	UShort(ConverterProvider.TYPE_USHORT),
    Integer(ConverterProvider.TYPE_INTEGER),
    UInteger(ConverterProvider.TYPE_UINTEGER),
    Long(ConverterProvider.TYPE_LONG),
    ULong(ConverterProvider.TYPE_ULONG),
    Float(ConverterProvider.TYPE_FLOAT),
    Double(ConverterProvider.TYPE_DOUBLE), 
    String(ConverterProvider.TYPE_STRING);
    
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