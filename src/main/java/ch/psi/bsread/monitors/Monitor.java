package ch.psi.bsread.monitors;

import java.util.UUID;

import org.zeromq.ZMQ.Context;

import zmq.SocketBase;

// see https://github.com/zeromq/jeromq/blob/master/src/test/java/zmq/TestMonitor.java or
// https://github.com/zeromq/jeromq/issues/61
public interface Monitor {

   /**
    * Starts monitoring
    * 
    * @param context The Context
    * @param socket The SocketBase
    */
   default void start(Context context, SocketBase socket) {
      this.start(context, socket, UUID.randomUUID().toString());
   }

   /**
    * Starts monitoring
    * 
    * @param context The Context
    * @param socket The SocketBase
    * @param monitorItentifier An identifier of the monitor
    */
   void start(Context context, SocketBase socket, String monitorItentifier);

   /**
    * Stops monitoring
    */
   void stop();
}
