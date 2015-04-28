package ch.psi.bsread;

import ch.psi.bsread.message.Timestamp;

public class StandardTimeProvider implements TimeProvider{
	
	@Override
	public Timestamp getTime(long pulseId){
		return new Timestamp(System.currentTimeMillis(), 0);
	}

}
