package ch.psi.bsread;

import java.util.function.Consumer;

import org.zeromq.ZMQ.Socket;

import ch.psi.bsread.message.DataHeader;
import ch.psi.bsread.message.MainHeader;
import ch.psi.bsread.message.Message;

public interface MessageExtractor<V> extends Consumer<DataHeader> {

	/**
	 * Extracts the value bytes of a channel.
	 * 
	 * @param socket
	 *            The Socket to extract the Message
	 * @param mainHeader
	 *            The MainHeader
	 * @return Message The extracted Message
	 */
	Message<V> extractMessage(Socket socket, MainHeader mainHeader);

	/**
	 * Sets the ReceiverConfig.
	 * 
	 * @param config The ReceiverConfig
	 */
	public void setReceiverConfig(ReceiverConfig<V> config);
}
