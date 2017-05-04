package ch.psi.bsread.impl;

import ch.psi.bsread.PulseIdProvider;

public class StandardPulseIdProvider implements PulseIdProvider {

   private long pulseId;
   private final long inc;

   public StandardPulseIdProvider() {
      this(0);
   }

   public StandardPulseIdProvider(long startPulseId) {
      this(startPulseId, 1);
   }

   public StandardPulseIdProvider(long startPulseId, long inc) {
      this.pulseId = startPulseId - 1;
      this.inc = inc;
   }

   @Override
   public long getNextPulseId() {
      return (pulseId += inc);
   }
}
