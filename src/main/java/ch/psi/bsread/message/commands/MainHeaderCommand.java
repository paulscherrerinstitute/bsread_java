package ch.psi.bsread.message.commands;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ.Socket;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

import ch.psi.bsread.ConfigIReceiver;
import ch.psi.bsread.ReceiverConfig;
import ch.psi.bsread.ReceiverState;
import ch.psi.bsread.command.Command;
import ch.psi.bsread.common.helper.ByteBufferHelper;
import ch.psi.bsread.compression.Compression;
import ch.psi.bsread.configuration.Channel;
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
	public <V> Message<V> process(ConfigIReceiver<V> receiver) {
		ReceiverConfig<V> receiverConfig = receiver.getReceiverConfig();

		Set<String> requestedChannels = null;
		Collection<Channel> channelFilters = receiver.getReceiverConfig().getRequestedChannels();
		if (channelFilters != null && !channelFilters.isEmpty()) {
			requestedChannels = new HashSet<>();
			for (Channel channelFilter : channelFilters) {
				if (isRequestedChannel(this.getPulseId(), channelFilter)) {
					requestedChannels.add(channelFilter.getName());
				}
			}
		}

		// check if there is something to process
		if (requestedChannels != null && requestedChannels.isEmpty()) {
			// stop here if channels are not requested
			// this also means, that clients might not get updated on DataHeader
			// changes (immediately). However, they stated that they are not
			// interested in the current pulse-id (and if the filter is applied
			// earlier in the chain (e.g. server) they would not be informed
			// either.
			receiver.drain();
			return null;
		}

		ReceiverState receiverState = receiver.getReceiverState();
		Socket socket = receiver.getSocket();
		DataHeader dataHeader;
		boolean dataHeaderChanged = false;

		LOGGER.debug("Receive pulse-id '{}' from '{}'.", getPulseId(), receiverConfig.getAddress());

		try {
			if (!getHtype().startsWith(MainHeaderCommand.HTYPE_VALUE_NO_VERSION)) {
				String message =
						String.format("Expect 'bsr_d-[version]' for 'htype' but was '%s'. Skip messge for '%s'", getHtype(), receiverConfig.getAddress());
				LOGGER.error(message);
				receiver.drain();
				throw new RuntimeException(message);
			}

			if (receiverConfig.isParallelHandlerProcessing()) {
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
					dataHeaderChanged = true;
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

						if (receiverConfig.isParallelHandlerProcessing()) {
							receiver.getDataHeaderHandlers().parallelStream().forEach(handler -> handler.accept(dataHeader));
						} else {
							receiver.getDataHeaderHandlers().forEach(handler -> handler.accept(dataHeader));
						}
					} catch (JsonParseException | JsonMappingException e) {
						String message = String.format("Could not parse DataHeader of '%s'.", receiverConfig.getAddress());
						LOGGER.error(message, e);
						String dataHeaderJson = new String(dataHeaderBytes, StandardCharsets.UTF_8);
						LOGGER.info("DataHeader was '{}'", dataHeaderJson);
						throw new RuntimeException(message, e);
					}
				}
			}
			else {
				String message = String.format("There is no data header for '%s'. Skip complete message.", receiverConfig.getAddress());
				LOGGER.error(message);
				receiver.drain();
				throw new RuntimeException(message);
			}

			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Receive message from '{}' with pulse '{}' and channels '{}'.", receiverConfig.getAddress(), getPulseId(),
						dataHeader.getChannels().stream().map(channel -> channel.getName()).collect(Collectors.joining(", ")));
			}
			// Receiver data
			Message<V> message = receiverConfig.getMessageExtractor().extractMessage(socket, this, requestedChannels);
			message.setDataHeaderChanged(dataHeaderChanged);
			Map<String, Value<V>> values = message.getValues();

			if (socket.hasReceiveMore()) {
				// Some sender implementations add an empty additional message
				// at the end
				// If there is more than 1 trailing message something is wrong!
				int messagesDrained = receiver.drain();
				if (messagesDrained > 1) {
					LOGGER.warn("'{}' had more than 1 trailing submessages to the message than expected", receiverConfig.getAddress());
					// throw new RuntimeException(message2);
				}
			}
			// notify hooks with complete values
			if (!values.isEmpty()) {
				if (receiverConfig.isParallelHandlerProcessing()) {
					receiver.getValueHandlers().parallelStream().forEach(handler -> handler.accept(values));
				} else {
					receiver.getValueHandlers().forEach(handler -> handler.accept(values));
				}
			}

			return message;

		} catch (IOException e) {
			String message2 = String.format("Unable to deserialize message for '%s'", receiverConfig.getAddress());
			throw new RuntimeException(message2, e);
		}
	}

	private boolean isRequestedChannel(long pulseId, Channel channel) {
		// Check if this channel sends data for given pulseId
		return ((pulseId - channel.getOffset()) % channel.getModulo()) == 0;
	}
}
