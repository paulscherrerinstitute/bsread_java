package ch.psi.bsread.message;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ.Socket;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import ch.psi.bsread.IReceiver;
import ch.psi.bsread.ReceiverConfig;
import ch.psi.bsread.ReceiverState;
import ch.psi.bsread.command.AbstractCommand;
import ch.psi.bsread.compression.Compression;
import ch.psi.bsread.helper.ByteBufferHelper;

@JsonInclude(Include.NON_DEFAULT)
public class MainHeader extends AbstractCommand {
	private static final Logger LOGGER = LoggerFactory.getLogger(MainHeader.class);
	private static final long serialVersionUID = -2505074745338960088L;

	public static final String HTYPE_VALUE_NO_VERSION = "bsr_m";
	// update AbstractCommand when version increases to support old and new
	// Command
	public static final String DEFAULT_HTYPE = HTYPE_VALUE_NO_VERSION + "-1.0";

	private String htype = DEFAULT_HTYPE;
	private long pulseId;
	private Timestamp globalTimestamp;
	private String hash;
	private Compression dataHeaderCompression = null;

	public MainHeader() {
	}

	public MainHeader(String htype, long pulseId, Timestamp globalTimestamp, String hash) {
		this.htype = htype;
		this.pulseId = pulseId;
		this.globalTimestamp = globalTimestamp;
		this.hash = hash;
	}

	public MainHeader(String htype, long pulseId, Timestamp globalTimestamp, String hash, Compression dataHeaderCompression) {
		this.htype = htype;
		this.pulseId = pulseId;
		this.globalTimestamp = globalTimestamp;
		this.hash = hash;
		this.dataHeaderCompression = dataHeaderCompression;
	}

	public String getHtype() {
		return htype;
	}

	public void setHtype(String htype) {
		this.htype = htype;
	}

	@JsonProperty("pulse_id")
	public long getPulseId() {
		return pulseId;
	}

	@JsonProperty("pulse_id")
	public void setPulseId(long pulseId) {
		this.pulseId = pulseId;
	}

	@JsonProperty("global_timestamp")
	public Timestamp getGlobalTimestamp() {
		return this.globalTimestamp;
	}

	@JsonProperty("global_timestamp")
	public void setGlobalTimestamp(Timestamp globalTimestamp) {
		this.globalTimestamp = globalTimestamp;
	}

	@JsonProperty("dh_compression")
	public Compression getDataHeaderCompression() {
		return dataHeaderCompression;
	}

	@JsonProperty("dh_compression")
	public void setDataHeaderCompression(Compression dataHeaderCompression) {
		this.dataHeaderCompression = dataHeaderCompression;
	}

	public String getHash() {
		return hash;
	}

	public void setHash(String hash) {
		this.hash = hash;
	}

	@Override
	public <V> Message<V> process(IReceiver<V> receiver) {
		ReceiverConfig<V> receiverConfig = receiver.getReceiverConfig();
		ReceiverState receiverState = receiver.getReceiverState();
		Socket socket = receiver.getSocket();
		DataHeader dataHeader;

		try {
			if (!getHtype().startsWith(MainHeader.HTYPE_VALUE_NO_VERSION)) {
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
					socket.recv();
				}
				else {
					byte[] dataHeaderBytes = socket.recv();
					Compression compression = getDataHeaderCompression();
					if (compression != null) {
						ByteBuffer tmpBuf = compression.getCompressor().decompressDataHeader(ByteBuffer.wrap(dataHeaderBytes), receiverState.getDataHeaderAllocator());
						dataHeaderBytes = ByteBufferHelper.copyToByteArray(tmpBuf);
					}
					dataHeader = receiverConfig.getObjectMapper().readValue(dataHeaderBytes, DataHeader.class);
					receiverState.setDataHeader(dataHeader);
					receiverState.setDataHeaderHash(getHash());

					if (receiverConfig.isParallelProcessing()) {
						receiver.getDataHeaderHandlers().parallelStream().forEach(handler -> handler.accept(dataHeader));
					} else {
						receiver.getDataHeaderHandlers().forEach(handler -> handler.accept(dataHeader));
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
