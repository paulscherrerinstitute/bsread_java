package ch.psi.bsread.sync;

import java.io.Closeable;
import java.util.Collection;
import java.util.Map;

public interface MessageSynchronizer<Msg> extends Closeable {

   void addMessage(Msg msg);

   Map<String, Msg> nextMessage();
   
   Collection<String> getChannels();
}
