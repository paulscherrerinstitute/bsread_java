package ch.psi.bsread;

import java.util.function.Consumer;

import org.zeromq.ZMQ.Socket;

import ch.psi.bsread.message.DataHeader;
import ch.psi.bsread.message.MainHeader;
import ch.psi.bsread.message.Message;

public interface MessageExtractor extends Consumer<DataHeader> {

	/**
	 * Extracts the value bytes of a channel.
	 * 
	 * @param socket
	 *            The Socket to extract the Message
	 * @param mainHeader
	 *            The MainHeader
	 * @return Message The extracted Message
	 */
	Message extractMessage(Socket socket, MainHeader mainHeader);
}
