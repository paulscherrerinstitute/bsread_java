package ch.psi.bsread.impl;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQException;

import zmq.Msg;

import ch.psi.bsread.MessageExtractor;
import ch.psi.bsread.ReceiverConfig;
import ch.psi.bsread.converter.ValueConverter;
import ch.psi.bsread.message.ChannelConfig;
import ch.psi.bsread.message.DataHeader;
import ch.psi.bsread.message.MainHeader;
import ch.psi.bsread.message.Message;
import ch.psi.bsread.message.Timestamp;
import ch.psi.bsread.message.Value;
import ch.psi.bsread.message.ValueImpl;

/**
 * A MessageExtractor that allows to use DirectBuffers to store data blobs that
 * are bigger than a predefined threshold. This helps to overcome
 * OutOfMemoryError when Messages are buffered since the JAVA heap space will
 * not be the limiting factor.
 */
public abstract class AbstractMessageExtractor<V> implements MessageExtractor<V> {
	private static final Logger LOGGER = LoggerFactory.getLogger(AbstractMessageExtractor.class.getName());

	private DataHeader dataHeader;
	private ValueConverter valueConverter;
	private ReceiverConfig<V> receiverConfig;

	public AbstractMessageExtractor(ValueConverter valueConverter) {
		this.valueConverter = valueConverter;
	}

	protected Value<V> getValue(ChannelConfig channelConfig) {
		return new ValueImpl<V>((V) null, new Timestamp());
	}

	@Override
	public Message<V> extractMessage(Socket socket, MainHeader mainHeader) {
		Message<V> message = new Message<V>();
		message.setMainHeader(mainHeader);
		message.setDataHeader(dataHeader);
		Map<String, Value<V>> values = message.getValues();

		int i = 0;
		final Iterator<ChannelConfig> configIter = dataHeader.getChannels().iterator();
		while (configIter.hasNext() && socket.hasReceiveMore()) {
			final ChannelConfig currentConfig = configIter.next();
			final ByteOrder byteOrder = currentConfig.getByteOrder();

			// # read data blob #
			// ##################
			if (!socket.hasReceiveMore()) {
				final String errorMessage = String.format("There is no data for channel '%s'.", currentConfig.getName());
				LOGGER.error(errorMessage);
				throw new RuntimeException(errorMessage);
			}

			final Msg valueMsg = receiveMsg(socket);
			ByteBuffer receivedValueBytes = valueMsg.buf().order(byteOrder);

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
			if (receivedValueBytes != null && receivedValueBytes.remaining() > 0) {
				final Value<V> value = getValue(currentConfig);
				values.put(currentConfig.getName(), value);

				// c-implementation uses a unsigned long (Json::UInt64,
				// uint64_t) for time -> decided to ignore this here
				final Timestamp iocTimestamp = value.getTimestamp();
				iocTimestamp.setSec(timestampBytes.getLong(timestampBytes.position()));
				iocTimestamp.setNs(timestampBytes.getLong(timestampBytes.position() + Long.BYTES));

				// offload value conversion work from receiver thread
				CompletableFuture<V> futureValue =
						CompletableFuture.supplyAsync(
								() -> valueConverter.getValue(receivedValueBytes, currentConfig, mainHeader, iocTimestamp),
								receiverConfig.getValueConversionService());
				value.setFutureValue(futureValue);
			}
			
			++i;
		}

		// // ensure async conversion is completed
		// for (Entry<String, Value<V>> entry : values.entrySet()) {
		// entry.getValue().getValue();
		// }

		// Sanity check of value list
		if (i != dataHeader.getChannels().size()) {
			LOGGER.warn("Number of received values does not match number of channels.");
		}

		return message;
	}

	@Override
	public void accept(DataHeader dataHeader) {
		this.dataHeader = dataHeader;
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

	@Override
	public void setReceiverConfig(ReceiverConfig<V> receiverConfig) {
		this.receiverConfig = receiverConfig;
	}
}
