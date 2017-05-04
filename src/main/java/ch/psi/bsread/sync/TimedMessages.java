package ch.psi.bsread.sync;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TimedMessages<Msg> {
   private long submitTime;
   private ConcurrentMap<String, Msg> messagesMap;

   public TimedMessages(long submitTime, int nrOfChannels) {
      this.submitTime = submitTime;
      messagesMap = new ConcurrentHashMap<>(nrOfChannels, 0.75f, 4);
   }

   public long getSubmitTime() {
      return submitTime;
   }

   public Map<String, Msg> getMessagesMap() {
      return messagesMap;
   }

   public int availableChannels() {
      return messagesMap.size();
   }
}
