package ch.psi.bsread;

import java.util.Set;
import java.util.function.Consumer;

import org.zeromq.ZMQ.Socket;

import ch.psi.bsread.message.DataHeader;
import ch.psi.bsread.message.MainHeader;
import ch.psi.bsread.message.Message;

public interface MessageExtractor<V> extends Consumer<DataHeader> {

	/**
	 * Extracts the value bytes of a channel.
	 * 
	 * @param receiver
     *            The Receiver 
	 * @param socket
	 *            The Socket to extract the Message
	 * @param mainHeader
	 *            The MainHeader
	 * @param requestedChannels
	 *            The requested channels of the stream (based on the filter)
	 * @return Message The extracted Message
	 */
	Message<V> extractMessage(ConfigIReceiver<V> receiver, Socket socket, MainHeader mainHeader, Set<String> requestedChannels);
}
