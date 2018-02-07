package ch.psi.bsread.monitors;

import ch.psi.bsread.message.commands.StopCommand;

// see https://github.com/zeromq/jeromq/blob/master/src/test/java/zmq/TestMonitor.java or
// https://github.com/zeromq/jeromq/issues/61
public interface Monitor {

   /**
    * Starts monitoring
    * 
    * @param monitorConfig Configuration info for the Monitor
    */
   void start(final MonitorConfig monitorConfig);

   /**
    * Stops monitoring
    */
   void stop();

   /**
    * Stops monitoring
    * 
    * @param sentMessages The number of sent messages (use {@link StopCommand#SENT_MESSAGES_UNKNOWN}
    *        if unknown)
    */
   void stop(long sentMessages);
}
