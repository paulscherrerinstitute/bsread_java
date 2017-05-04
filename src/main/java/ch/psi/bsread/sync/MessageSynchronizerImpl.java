package ch.psi.bsread.sync;

import java.util.Collection;
import java.util.function.Function;
import java.util.function.ToLongFunction;

/**
 * MessageBuffer based on a max. allowed size. Accordingly, the time limit messages are kept is
 * given by the frequency and the buffer size (assuming there are constantly messages arriving)
 */
public class MessageSynchronizerImpl<Msg> extends MessageSynchronizerLockFree<Msg> {

   public MessageSynchronizerImpl(
         int maxNumberOfMessagesToKeep,
         boolean sendIncompleteMessages,
         boolean sendFirstComplete,
         Collection<? extends SyncChannel> channels,
         Function<Msg, String> channelNameProvider,
         ToLongFunction<Msg> pulseIdProvider) {
      this(maxNumberOfMessagesToKeep,
            Long.MAX_VALUE,
            sendIncompleteMessages,
            sendFirstComplete,
            channels,
            channelNameProvider,
            pulseIdProvider);
   }

   public MessageSynchronizerImpl(
         long messageSendTimeoutMillis,
         boolean sendIncompleteMessages,
         boolean sendFirstComplete,
         Collection<? extends SyncChannel> channels,
         Function<Msg, String> channelNameProvider,
         ToLongFunction<Msg> pulseIdProvider) {
      this(Integer.MAX_VALUE,
            messageSendTimeoutMillis,
            sendIncompleteMessages,
            sendFirstComplete,
            channels,
            channelNameProvider,
            pulseIdProvider);
   }

   public MessageSynchronizerImpl(
         int maxNumberOfMessagesToKeep,
         long messageSendTimeoutMillis,
         boolean sendIncompleteMessages,
         boolean sendFirstComplete,
         Collection<? extends SyncChannel> channels,
         Function<Msg, String> channelNameProvider,
         ToLongFunction<Msg> pulseIdProvider) {
      super(maxNumberOfMessagesToKeep,
            messageSendTimeoutMillis,
            sendIncompleteMessages,
            sendFirstComplete,
            channels,
            channelNameProvider,
            pulseIdProvider);
   }
}
