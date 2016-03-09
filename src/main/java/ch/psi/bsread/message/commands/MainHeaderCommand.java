package ch.psi.bsread.message.commands;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ.Socket;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

import ch.psi.bsread.IReceiver;
import ch.psi.bsread.ReceiverConfig;
import ch.psi.bsread.ReceiverState;
import ch.psi.bsread.command.Command;
import ch.psi.bsread.compression.Compression;
import ch.psi.bsread.copy.common.helper.ByteBufferHelper;
import ch.psi.bsread.message.DataHeader;
import ch.psi.bsread.message.MainHeader;
import ch.psi.bsread.message.Message;
import ch.psi.bsread.message.Value;

public class MainHeaderCommand extends MainHeader implements Command {
	private static final Logger LOGGER = LoggerFactory.getLogger(MainHeaderCommand.class);
	private static final long serialVersionUID = -2505074745338960088L;

	public MainHeaderCommand() {
	}

	@Override
	public <V> Message<V> process(IReceiver<V> receiver) {
		ReceiverConfig<V> receiverConfig = receiver.getReceiverConfig();
		ReceiverState receiverState = receiver.getReceiverState();
		Socket socket = receiver.getSocket();
		DataHeader dataHeader;

		try {
			if (!getHtype().startsWith(MainHeaderCommand.HTYPE_VALUE_NO_VERSION)) {
				String message =
						String.format("Expect 'bsr_d-[version]' for 'htype' but was '%s'. Skip messge", getHtype());
				LOGGER.error(message);
				receiver.drain();
				throw new RuntimeException(message);
			}

			if (receiverConfig.isParallelProcessing()) {
				receiver.getMainHeaderHandlers().parallelStream().forEach(handler -> handler.accept(this));
			} else {
				receiver.getMainHeaderHandlers().forEach(handler -> handler.accept(this));
			}

			// Receive data header
			if (socket.hasReceiveMore()) {
				if (getHash().equals(receiverState.getDataHeaderHash())) {
					dataHeader = receiverState.getDataHeader();
					// The data header did not change so no interpretation of
					// the header ...
					socket.base().recv(0);
				}
				else {
					byte[] dataHeaderBytes = socket.recv();
					Compression compression = getDataHeaderCompression();
					if (compression != null) {
						ByteBuffer tmpBuf = compression.getCompressor().decompressDataHeader(ByteBuffer.wrap(dataHeaderBytes), receiverState.getDataHeaderAllocator());
						dataHeaderBytes = ByteBufferHelper.copyToByteArray(tmpBuf);
					}

					try {
						dataHeader = receiverConfig.getObjectMapper().readValue(dataHeaderBytes, DataHeader.class);
						receiverState.setDataHeader(dataHeader);
						receiverState.setDataHeaderHash(getHash());

						if (receiverConfig.isParallelProcessing()) {
							receiver.getDataHeaderHandlers().parallelStream().forEach(handler -> handler.accept(dataHeader));
						} else {
							receiver.getDataHeaderHandlers().forEach(handler -> handler.accept(dataHeader));
						}
					} catch (JsonParseException | JsonMappingException e) {
						LOGGER.error("Could not parse DataHeader.", e);
						String dataHeaderJson = new String(dataHeaderBytes, StandardCharsets.UTF_8);
						LOGGER.info("DataHeader was '{}'", dataHeaderJson);
						throw new RuntimeException("Could not parse DataHeader.", e);
					}
				}
			}
			else {
				String message = "There is no data header. Skip complete message.";
				LOGGER.error(message);
				receiver.drain();
				throw new RuntimeException(message);
			}

			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Receive message for pulse '{}' and channels '{}'.", getPulseId(),
						dataHeader.getChannels().stream().map(channel -> channel.getName()).collect(Collectors.joining(", ")));
			}
			// Receiver data
			Message<V> message = receiverConfig.getMessageExtractor().extractMessage(socket, this);
			Map<String, Value<V>> values = message.getValues();

			if (socket.hasReceiveMore()) {
				// Some sender implementations add an empty additional message
				// at the end
				// If there is more than 1 trailing message something is wrong!
				int messagesDrained = receiver.drain();
				if (messagesDrained > 1) {
					throw new RuntimeException("There were more than 1 trailing submessages to the message than expected");
				}
			}
			// notify hooks with complete values
			if (!values.isEmpty()) {
				if (receiverConfig.isParallelProcessing()) {
					receiver.getValueHandlers().parallelStream().forEach(handler -> handler.accept(values));
				}
				else {
					receiver.getValueHandlers().forEach(handler -> handler.accept(values));
				}
			}

			return message;

		} catch (IOException e) {
			throw new RuntimeException("Unable to deserialize message", e);
		}
	}
}
