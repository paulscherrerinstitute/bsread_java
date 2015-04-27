package ch.psi.bsread.basic;

import java.nio.ByteBuffer;

import ch.psi.bsread.Receiver;
import ch.psi.bsread.message.ChannelConfig;
import ch.psi.bsread.message.DataHeader;
import ch.psi.bsread.message.Message;
import ch.psi.bsread.message.Value;

/**
 * A simplified receiver delivering values as real values and not byte blobs.
 */
public class BasicReceiver {

	private Receiver receiver = new Receiver();
	
	public void connect() {
		receiver.connect();
	}

	public void connect(String address) {
		receiver.connect(address);
	}

	public void close() {
		receiver.close();
	}
	
	
	public BasicMessage receive() throws IllegalStateException {
		final Message message = receiver.receive();
		final DataHeader dataHeader = message.getDataHeader();
		
		final BasicMessage nMessage = new BasicMessage();
		nMessage.setDataHeader(dataHeader);
		nMessage.setMainHeader(message.getMainHeader());
		
		
		
		for(ChannelConfig channelConfig: dataHeader.getChannels()){
			final String channel = channelConfig.getName();
			final Value value = message.getValues().get(channel);

			// Convert byte blob into type
			nMessage.getValues().put(channel, new BasicValue<>(Converter.getValue(ByteBuffer.wrap(value.getValue()).order(dataHeader.getByteOrder()), channelConfig.getType().name().toLowerCase(), channelConfig.getShape()), value.getTimestamp()));
		}
		
		return nMessage;
	}
}
