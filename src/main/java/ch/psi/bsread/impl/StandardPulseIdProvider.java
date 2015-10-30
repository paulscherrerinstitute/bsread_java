package ch.psi.bsread.impl;

import ch.psi.bsread.PulseIdProvider;

public class StandardPulseIdProvider implements PulseIdProvider {

   // We start at one as the first pulseId should be 0
   private long pulseId = 0;

   public StandardPulseIdProvider() {}

   public StandardPulseIdProvider(long startPulseId) {
      this.pulseId = startPulseId;
   }

   @Override
   public long getNextPulseId() {
      return pulseId++;
   }
}
