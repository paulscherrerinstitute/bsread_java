package ch.psi.bsread.monitors;

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
}
