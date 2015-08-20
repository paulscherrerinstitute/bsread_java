package ch.psi.bsread.impl;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQException;
import org.zeromq.ZMQ.Socket;

import zmq.Msg;

import ch.psi.bsread.MessageExtractor;
import ch.psi.bsread.message.ChannelConfig;
import ch.psi.bsread.message.DataHeader;
import ch.psi.bsread.message.MainHeader;
import ch.psi.bsread.message.Message;
import ch.psi.bsread.message.Timestamp;
import ch.psi.bsread.message.Value;

/**
 * A MessageExtractor that allows to use DirectBuffers to store data blobs that
 * are bigger than a predefined threshold. This helps to overcome
 * OutOfMemoryError when Messages are buffered since the JAVA heap space will
 * not be the limiting factor.
 */
public abstract class AbstractMessageExtractor implements MessageExtractor {
	private static final Logger LOGGER = LoggerFactory.getLogger(AbstractMessageExtractor.class.getName());

	private int directThreshold = Integer.MAX_VALUE;
	private DataHeader dataHeader;

	public AbstractMessageExtractor() {
	}

	/**
	 * Constructor
	 * 
	 * @param directThreshold
	 *            The number of byte threshold defining when direct ByteBuffers
	 *            should be used
	 */
	public AbstractMessageExtractor(int directThreshold) {
		this.directThreshold = directThreshold;
	}

	protected Value getValue(ChannelConfig channelConfig) {
		return new Value(null, new Timestamp());
	}

	@Override
	public Message extractMessage(Socket socket, MainHeader mainHeader) {
		Message message = new Message();
		message.setMainHeader(mainHeader);
		message.setDataHeader(dataHeader);
		Map<String, Value> values = message.getValues();
		List<ChannelConfig> channelConfigs = dataHeader.getChannels();

		int i = 0;
		for (; i < channelConfigs.size() && socket.hasReceiveMore(); ++i) {
			final ChannelConfig currentConfig = channelConfigs.get(i);
			final ByteOrder byteOrder = currentConfig.getByteOrder();

			// # read data blob #
			// ##################
			if (!socket.hasReceiveMore()) {
				final String errorMessage = String.format("There is no data for channel '%s'.", currentConfig.getName());
				LOGGER.error(errorMessage);
				throw new RuntimeException(errorMessage);
			}

			final Msg valueMsg = receiveMsg(socket);
			ByteBuffer valueBytes = valueMsg.buf().order(byteOrder);

			// # read timestamp blob #
			// #######################
			if (!socket.hasReceiveMore()) {
				final String errorMessage =
						String.format("There is no timestamp for channel '%s'.", currentConfig.getName());
				LOGGER.error(errorMessage);
				throw new RuntimeException(errorMessage);
			}
			final Msg timeMsg = receiveMsg(socket);
			ByteBuffer timestampBytes = timeMsg.buf().order(byteOrder);

			// Create value object
			if (valueBytes != null && valueBytes.remaining() > 0) {
				final Value value = getValue(currentConfig);
				values.put(currentConfig.getName(), value);

				if (valueMsg.size() > directThreshold) {
					toDirect(value, valueBytes, currentConfig);
				} else {
					value.setValue(valueBytes);
				}

				// c-implementation uses a unsigned long (Json::UInt64,
				// uint64_t) for time -> decided to ignore this here
				final Timestamp timestamp = value.getTimestamp();
				timestamp.setEpoch(timestampBytes.getLong(0));
				timestamp.setNs(timestampBytes.getLong(Long.BYTES));
			}
		}

		// Sanity check of value list
		if (i != channelConfigs.size()) {
			LOGGER.warn("Number of received values does not match number of channels.");
		}

		return message;
	}

	@Override
	public void accept(DataHeader dataHeader) {
		this.dataHeader = dataHeader;
	}

	protected void toDirect(Value value, ByteBuffer valueBytes, ChannelConfig config) {
		if (valueBytes.isDirect()) {
			value.setValue(valueBytes);
		}

		ByteBuffer direct = value.getValue();
		if (direct == null || !direct.isDirect()) {
			direct = ByteBuffer.allocateDirect(valueBytes.remaining());
		}
		direct.position(0);
		direct.limit(direct.capacity());
		if (direct.remaining() < valueBytes.remaining()) {
			// might happen for string or (when enabled) compressed channels
			LOGGER.info("Realocate direct ByteBuffer for '{}' of type '{}'.", config.getName(), config
					.getType().getKey());
			direct = ByteBuffer.allocateDirect(valueBytes.remaining());
		}

		direct.order(valueBytes.order());
		direct.put(valueBytes.duplicate());
		direct.flip();

		value.setValue(direct);
	}

	protected Msg receiveMsg(Socket socket) {
		Msg msg = socket.base().recv(0);

		if (msg == null) {
			mayRaise(socket);
		}

		return msg;
	}

	private void mayRaise(Socket socket) {
		int errno = socket.base().errno();
		if (errno != 0 && errno != zmq.ZError.EAGAIN) {
			throw new ZMQException(errno);
		}
	}
}
