package ch.psi.bsread.sync;

import java.io.Closeable;
import java.util.Collection;
import java.util.Map;

public interface MessageSynchronizer<Msg> extends Closeable {

   void addMessage(Msg msg);
   
   void onFirstMessage(Runnable callback);

   Map<String, Msg> nextMessage();
   
   Collection<SyncChannel> getChannels();
}
