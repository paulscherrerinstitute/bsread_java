package ch.psi.bsread;

import ch.psi.bsread.message.ChannelConfig;

public class DataChannel<T> {
	
	public ChannelConfig getConfig(){
		return new ChannelConfig();
	}

	
	public T getValue(long pulseId){
		return null;
	}
	
	public long getTimestamp(){
		return 0;
	}
}
