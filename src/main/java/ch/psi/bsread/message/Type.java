package ch.psi.bsread.message;

import ch.psi.daq.data.db.converters.ByteConverter;
import ch.psi.daq.data.db.converters.impl.DoubleByteConverter;
import ch.psi.daq.data.db.converters.impl.IntegerByteConverter;
import ch.psi.daq.data.db.converters.impl.LongByteConverter;
import ch.psi.daq.data.db.converters.impl.ShortByteConverter;
import ch.psi.daq.data.db.converters.impl.StringByteConverter;
import ch.psi.daq.data.db.converters.impl.ULongByteConverter;
import ch.psi.daq.data.db.converters.impl.UShortByteConverter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonValue;

public enum Type {
    Double("double", new DoubleByteConverter()), 
    String("string", new StringByteConverter()),
    Integer("integer", new IntegerByteConverter()),
    Long("long", new LongByteConverter()),
    ULong("ulong",  new ULongByteConverter()),
    Short("short", new ShortByteConverter()),	
    UShort("ushort", new UShortByteConverter());
    
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