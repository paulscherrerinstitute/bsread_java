package ch.psi.bsread;

import org.zeromq.ZMQ.Socket;

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
