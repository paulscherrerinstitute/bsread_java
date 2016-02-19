package ch.psi.bsread.monitors;

import java.util.UUID;

import org.zeromq.ZMQ.Context;

import ch.psi.bsread.SenderConfig;

import zmq.SocketBase;

// see https://github.com/zeromq/jeromq/blob/master/src/test/java/zmq/TestMonitor.java or
// https://github.com/zeromq/jeromq/issues/61
public interface Monitor {

   /**
    * Starts monitoring
    * 
    * @param context The Context
    * @param socket The SocketBase
    * @param senderConfig The configuration of the sender
    */
   default void start(Context context, SocketBase socket, SenderConfig senderConfig) {
      this.start(context, socket, senderConfig, UUID.randomUUID().toString());
   }

   /**
    * Starts monitoring
    * 
    * @param context The Context
    * @param socket The SocketBase
    * @param senderConfig The configuration of the sender
    * @param monitorItentifier An identifier of the monitor
    */
   void start(Context context, SocketBase socket, SenderConfig senderConfig, String monitorItentifier);

   /**
    * Stops monitoring
    */
   void stop();
}
