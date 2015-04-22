package ch.psi.bsread;

public class StandardPulseIdProvider implements PulseIdProvider{

	private long pulseId = -1; // We start at one as the first pulseId should be 0
	
	@Override
	public long getNextPulseId() {
		pulseId++; // Increment PulseId by one
		return pulseId;
	}

}
