package ch.psi.bsread.sync;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.psi.bsread.message.Timestamp;

public class AssembledMessage {
   private static final Logger LOGGER = LoggerFactory.getLogger(AssembledMessage.class.getName());

   private long pulseId;
   private Timestamp globalTimestamp;
   // SortedMap to ensure ordering does not change
   // and thus, the DataHeader's hash value will not change either.
   private SortedMap<String, TestEvent> values;

   public AssembledMessage(Map<String, TestEvent> messages) {
      this.values = new TreeMap<>();

      for (Map.Entry<String, TestEvent> entry : messages.entrySet()) {
         final TestEvent event = entry.getValue();

         if (globalTimestamp == null) {
            globalTimestamp = new Timestamp(event.getGlobalSec(), event.getGlobalNs());
            pulseId = event.getPulseId();
         } else {
            if (pulseId != event.getPulseId()) {
               LOGGER.warn("PulseIds do not match ('{}' vs. '{}').", pulseId, event.getPulseId());
            }
            if (globalTimestamp.getSec() != event.getGlobalSec()
                  && globalTimestamp.getNs() != event.getGlobalNs()) {
               LOGGER.warn("Global timestamps do not match ('{}.{}' vs. '{}.{}').", 
                     globalTimestamp.getSec(),
                     globalTimestamp.getNs(),
                     event.getGlobalSec(), 
                     event.getGlobalNs());
            }
         }

         values.put(event.getChannel(), event);
      }
   }

   public long getPulseId() {
      return pulseId;
   }

   public SortedMap<String, TestEvent> getValues() {
      return values;
   }

   public Timestamp getGlobalTimestamp() {
      return globalTimestamp;
   }
}
