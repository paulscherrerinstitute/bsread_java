package ch.psi.bsread.message;

import ch.psi.daq.data.db.converters.ByteConverter;
import ch.psi.daq.data.db.converters.impl.DoubleByteConverter;


import com.fasterxml.jackson.annotation.JsonIgnore;
//import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum Type {
    Double("double", new DoubleByteConverter()), 
    String("string", null),
    Integer("integer", null),
    Long("long", null),
    ULong("ulong",  null),
    Short("short", null),
    UShort("ushort", null);
    
    private String key;
    private ByteConverter<?, ?, ?> converter;

    Type(String key, ByteConverter<?, ?, ?> converter) {
        this.key = key;
        this.converter = converter;
    }

    @JsonValue
    public String getKey() {
        return key;
    }
    
    @JsonIgnore
    public ByteConverter<?, ?, ?> getConverter(){
    	return converter;
    }
}