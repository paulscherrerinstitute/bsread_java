package ch.psi.bsread.sync;

import java.util.Collection;
import java.util.function.Function;
import java.util.function.ToLongFunction;

public class MessageSynchronizerBestEffortTestLockFree extends MessageSynchronizerBestEffortTest {

   @Override
   protected AbstractMessageSynchronizer<TestEvent> createMessageSynchronizer(
         long messageSendTimeoutMillis,
         boolean sendIncompleteMessages,
         boolean sendFirstComplete,
         Collection<? extends SyncChannel> channels,
         Function<TestEvent, String> channelNameProvider,
         ToLongFunction<TestEvent> pulseIdProvider) {
      return new MessageSynchronizerBestEffortLockFree<>(
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
      return new MessageSynchronizerBestEffortLockFree<>(
            maxNumberOfMessagesToKeep,
            sendIncompleteMessages,
            sendFirstComplete,
            channels,
            channelNameProvider,
            pulseIdProvider);
   }

   // @Test
   // public void testSpecific() throws Exception {
   // testMessageSynchronizer_LoadTestTime_50_100Hz_ForgetChannel();
   // }
}
