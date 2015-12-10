package ch.psi.bsread.command;

import java.io.Serializable;

import ch.psi.bsread.IReceiver;
import ch.psi.bsread.message.Message;

public interface Command extends Serializable{

   /**
    * Processes information retrieved from a sender.
    * 
    * @param receiver Object to retrieve the information from
    * @return Message The extracted message or <tt>null</tt> if the command does not extract 
    */
   public <V> Message<V> process(IReceiver<V> receiver);
}
