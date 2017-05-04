package ch.psi.bsread.sync;

import java.util.Collection;
import java.util.function.Function;
import java.util.function.ToLongFunction;

public class MessageSynchronizerTestLocking extends MessageSynchronizerImplTest {

   @Override
   protected AbstractMessageSynchronizer<TestEvent> createMessageSynchronizer(
         long messageSendTimeoutMillis,
         boolean sendIncompleteMessages,
         boolean sendFirstComplete,
         Collection<? extends SyncChannel> channels,
         Function<TestEvent, String> channelNameProvider,
         ToLongFunction<TestEvent> pulseIdProvider) {
      return new MessageSynchronizerLocking<>(
            messageSendTimeoutMillis,
            sendIncompleteMessages,
            sendFirstComplete,
            channels,
            channelNameProvider,
            pulseIdProvider);
   }

   @Override
   protected AbstractMessageSynchronizer<TestEvent> createMessageSynchronizer(
         int maxNumberOfMessagesToKeep,
         boolean sendIncompleteMessages,
         boolean sendFirstComplete,
         Collection<? extends SyncChannel> channels,
         Function<TestEvent, String> channelNameProvider,
         ToLongFunction<TestEvent> pulseIdProvider) {
      return new MessageSynchronizerLocking<>(
            maxNumberOfMessagesToKeep,
            sendIncompleteMessages,
            sendFirstComplete,
            channels,
            channelNameProvider,
            pulseIdProvider);
   }

//   @Test
//   public void testSpecific() throws Exception {
//      testMessageSynchronizer_LoadTestTime_3_100Hz_Forget();
//   }
}
