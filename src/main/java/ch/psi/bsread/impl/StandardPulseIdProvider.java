package ch.psi.bsread.impl;

import ch.psi.bsread.PulseIdProvider;

public class StandardPulseIdProvider implements PulseIdProvider {

	// We start at one as the first pulseId should be 0
	private long pulseId = -1;

	@Override
	public long getNextPulseId() {
		pulseId++; // Increment PulseId by one
		return pulseId;
	}
}
