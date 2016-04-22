package ch.psi.bsread;

import java.util.Collection;
import java.util.Map;
import java.util.function.Consumer;

import org.zeromq.ZMQ.Socket;

import ch.psi.bsread.message.DataHeader;
import ch.psi.bsread.message.MainHeader;
import ch.psi.bsread.message.Message;
import ch.psi.bsread.message.Value;

public interface ConfigIReceiver<V> extends IReceiver<V> {

   /**
    * Drains the socket.
    * 
    * @return int number of discarded multi-part messages
    */
   int drain();

   /**
    * Provides access to the socket.
    * 
    * @return Socket The Socket
    */
   Socket getSocket();

   /**
    * Provides the configuration of the Receiver.
    * 
    * @return ReceiverConfig The config.
    */
   ReceiverConfig<V> getReceiverConfig();

   /**
    * Returns an object describing the current state.
    * 
    * @return ReceiverState The ReceiverState
    */
   ReceiverState getReceiverState();
}
