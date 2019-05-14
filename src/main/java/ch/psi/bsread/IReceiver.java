package ch.psi.bsread;

import java.io.Closeable;
import java.util.Collection;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.IntConsumer;

import ch.psi.bsread.message.DataHeader;
import ch.psi.bsread.message.MainHeader;
import ch.psi.bsread.message.Message;
import ch.psi.bsread.message.Value;

public interface IReceiver<V> extends Closeable {

   /**
    * Stop receiving and close resources.
    */
   void close();

   /**
    * Establishes the connection.
    */
   void connect();

   /**
    * Receive the next message (blocks for the next). Users must not call Thread.interrupt() to stop
    * (see: https://github.com/zeromq/jeromq/issues/116) but use close() and check for null
    * as termination condition.
    * 
    * @return Message The next message or null for termination.
    * @throws RuntimeException Might throw a RuntimeException
    */
   Message<V> receive() throws RuntimeException;

   /**
    * Provides access to the ValueHandlers
    * 
    * @return Collection The handlers
    */
   Collection<Consumer<Map<String, Value<V>>>> getValueHandlers();

   /**
    * Provides access to the MainHeaderHandlers
    * 
    * @return Collection The handlers
    */
   Collection<Consumer<MainHeader>> getMainHeaderHandlers();

   /**
    * Provides access to the DataHeaderHandlers
    * 
    * @return Collection The handlers
    */
   Collection<Consumer<DataHeader>> getDataHeaderHandlers();

   /**
    * Provides access to the connection count handlers
    * 
    * @return Collection The handlers
    */
   Collection<IntConsumer> getConnectionCountHandlers();

   /**
    * Provides access to the connection idle handlers
    * 
    * @return Collection The handlers
    */
   Collection<Consumer<Boolean>> getConnectionIdleHandlers();
   
   /**
    * Provides access to the connection inactive handlers
    * 
    * @return Collection The handlers
    */
   Collection<Consumer<Boolean>> getConnectionInactiveHandlers();
}
