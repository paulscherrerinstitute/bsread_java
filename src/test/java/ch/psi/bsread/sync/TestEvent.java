package ch.psi.bsread.sync;

public class TestEvent {
   private String channel;
   private long pulseId;
   private long sec;
   private long ns;

   public TestEvent(String channel, long pulseId, long sec, long ns) {
      this.channel = channel;
      this.pulseId = pulseId;
      this.sec = ns;
   }

   public String getChannel() {
      return channel;
   }

   public long getPulseId() {
      return pulseId;
   }

   public long getGlobalSec() {
      return sec;
   }

   public long getGlobalNs() {
      return ns;
   }

   @Override
   public String toString() {
      return "" + pulseId;
   }
}
