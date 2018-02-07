package ch.psi.bsread.message.commands;

import java.nio.charset.StandardCharsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import ch.psi.bsread.ConfigIReceiver;
import ch.psi.bsread.command.Command;
import ch.psi.bsread.message.Message;

public class StopCommand implements Command {
   private static final long serialVersionUID = 552749029819075031L;
   private static final Logger LOGGER = LoggerFactory.getLogger(StopCommand.class);

   public static final String HTYPE_VALUE_NO_VERSION = "bsr_stop";
   // update AbstractCommand when version increases to support old and new
   // Command
   public static final String DEFAULT_HTYPE = HTYPE_VALUE_NO_VERSION + "-1.0";
   public static final long SENT_MESSAGES_UNKNOWN = -1;

   @JsonInclude
   private String htype = DEFAULT_HTYPE;

   @JsonInclude
   private long sentMessages = SENT_MESSAGES_UNKNOWN;

   public StopCommand() {}

   public StopCommand(final long sentMessages) {
      this.sentMessages = sentMessages;
   }

   public String getHtype() {
      return htype;
   }

   public void setHtype(String htype) {
      this.htype = htype;
   }

   public long getSentMessages() {
      return sentMessages;
   }

   public void setSentMessages(long sentMessages) {
      this.sentMessages = sentMessages;
   }

   @Override
   public <V> Message<V> process(ConfigIReceiver<V> receiver) {
      if (receiver.getReceiverConfig().isKeepListeningOnStop()) {
         receiver.drain();
         return null;
      } else {
         LOGGER.info("Stop '{}' due to stop command.", receiver.getReceiverConfig().getAddress());
         receiver.close();
         return null;
      }
   }

   public static byte[] getAsBytes(final ObjectMapper objectMapper, final long sentMessages)
         throws JsonProcessingException {
      final String stopCommandStr = objectMapper.writeValueAsString(new StopCommand(sentMessages));
      return stopCommandStr.getBytes(StandardCharsets.UTF_8);
   }
}
