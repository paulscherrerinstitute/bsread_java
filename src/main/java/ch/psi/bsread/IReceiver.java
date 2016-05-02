package ch.psi.bsread;

import java.io.Closeable;
import java.util.Collection;
import java.util.Map;
import java.util.function.Consumer;

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
    * Receive the next message (blocks for the next).
    * 
    * @return Message The next message
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
}
