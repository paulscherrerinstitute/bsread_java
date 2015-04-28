package ch.psi.bsread;

import ch.psi.bsread.message.Timestamp;

public interface TimeProvider {
	public Timestamp getTime(long pulseId);
}
