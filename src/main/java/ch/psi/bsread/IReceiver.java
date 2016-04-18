package ch.psi.bsread;

import java.util.Collection;
import java.util.Map;
import java.util.function.Consumer;

import org.zeromq.ZMQ.Socket;

import ch.psi.bsread.message.DataHeader;
import ch.psi.bsread.message.MainHeader;
import ch.psi.bsread.message.Value;

public interface IReceiver<V> {

	/**
	 * Stop receiving and close resources.
	 */
	void close();

	/**
	 * Establishes the connection.
	 */
	void connect();

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
