package ch.psi.bsread;

import ch.psi.bsread.message.ChannelConfig;

public abstract class DataChannel<T> {
	
	private final ChannelConfig config;
	
	public DataChannel(ChannelConfig config){
		this.config = config;
	}
	
	public ChannelConfig getConfig(){
		return config;
	}
	
	public abstract T getValue(long pulseId);
	
}
