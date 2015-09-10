package ch.psi.bsread;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import com.fasterxml.jackson.databind.ObjectMapper;

import ch.psi.bsread.impl.StandardMessageExtractor;
import ch.psi.bsread.message.DataHeader;
import ch.psi.bsread.message.MainHeader;
import ch.psi.bsread.message.Message;
import ch.psi.bsread.message.Value;

public class Receiver {
	private static final Logger LOGGER = LoggerFactory.getLogger(Receiver.class.getName());
	public static final int HIGH_WATER_MARK = 100;
	private static final int MAX_ALIGNMENT_RETRY = 10;

	private Context context;
	private Socket socket;

	private ObjectMapper mapper = new ObjectMapper();

	private List<Consumer<MainHeader>> mainHeaderHandlers = new ArrayList<>();
	private List<Consumer<DataHeader>> dataHeaderHandlers = new ArrayList<>();
	private List<Consumer<Map<String, Value>>> valueHandlers = new ArrayList<>();
	private boolean parallelProcessing = false;
	private MessageExtractor messageExtractor;

	private String dataHeaderHash = "";
	private DataHeader dataHeader = null;

	public Receiver() {
		this(false, new StandardMessageExtractor());
	}

	public Receiver(boolean parallelProcessing, MessageExtractor messageExtractor) {
		this.parallelProcessing = parallelProcessing;
		this.messageExtractor = messageExtractor;

		this.dataHeaderHandlers.add(this.messageExtractor);
	}

	public void connect() {
		connect("tcp://localhost:9999");
	}

	public void connect(String address) {
		this.connect(address, HIGH_WATER_MARK);
	}

	public void connect(String address, int highWaterMark) {
		this.context = ZMQ.context(1);
		this.socket = this.context.socket(ZMQ.PULL);
		this.socket.setRcvHWM(highWaterMark);
		this.socket.connect(address);
	}

	public void close() {
		socket.close();
		context.close();
		socket = null;
		context = null;
	}

	public Message receive() throws RuntimeException {
		// Receive main header
		MainHeader mainHeader = null;
		int nrOfAlignmentTrys = 0;

		while (mainHeader == null && nrOfAlignmentTrys < MAX_ALIGNMENT_RETRY) {
			/*
			 * It can happen that bytes received do not represent the start of a
			 * new multipart message but the start of a submessage (e.g. after
			 * connection or when messages get lost). Therefore, make sure
			 * receiver is aligned with the start of the multipart message
			 * (i.e., it is possible that we loose the first message)
			 */
			try {
				// test if mainHaderBytes can be interpreted as MainHeader
				mainHeader = mapper.readValue(socket.recv(), MainHeader.class);
			} catch (IOException e) {
				++nrOfAlignmentTrys;
				LOGGER.info("Received bytes were not aligned with multipart message.");
				// drain the socket
				drain();
			}
		}

		return receive(mainHeader);
	}

	private Message receive(MainHeader mainHeader) throws RuntimeException {
		try {
			if (!mainHeader.getHtype().startsWith(MainHeader.HTYPE_VALUE_NO_VERSION)) {
				String message =
						String.format("Expect 'bsr_d-[version]' for 'htype' but was '%s'. Skip messge", mainHeader.getHtype());
				LOGGER.error(message);
				drain();
				throw new RuntimeException(message);
			}

			if (this.parallelProcessing) {
				mainHeaderHandlers.parallelStream().forEach(handler -> handler.accept(mainHeader));
			} else {
				mainHeaderHandlers.forEach(handler -> handler.accept(mainHeader));
			}

			// Receive data header
			if (socket.hasReceiveMore()) {
				if (mainHeader.getHash().equals(dataHeaderHash)) {
					// The data header did not change so no interpretation of
					// the header ...
					socket.recv();
				}
				else {
					dataHeaderHash = mainHeader.getHash();
					dataHeader = mapper.readValue(socket.recv(), DataHeader.class);
					if (this.parallelProcessing) {
						dataHeaderHandlers.parallelStream().forEach(handler -> handler.accept(dataHeader));
					} else {
						dataHeaderHandlers.forEach(handler -> handler.accept(dataHeader));
					}
				}
			}
			else {
				String message = "There is no data header. Skip complete message.";
				LOGGER.error(message);
				drain();
				throw new RuntimeException(message);
			}

			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Receive message for pulse '{}' and channels '{}'.", mainHeader.getPulseId(),
						dataHeader.getChannels().stream().map(channel -> channel.getName()).collect(Collectors.joining(", ")));
			}
			// Receiver data
			Message message = messageExtractor.extractMessage(socket, mainHeader);
			Map<String, Value> values = message.getValues();

			if (this.socket.hasReceiveMore()) {
				// Some sender implementations add an empty additional message
				// at the end
				// If there is more than 1 trailing message something is wrong!
				int messagesDrained = this.drain();
				if (messagesDrained > 1) {
					throw new RuntimeException("There were more than 1 trailing submessages to the message than expected");
				}
			}
			// notify hooks with complete values
			if (!values.isEmpty()) {
				if (this.parallelProcessing) {
					valueHandlers.parallelStream().forEach(handler -> handler.accept(values));
				}
				else {
					valueHandlers.forEach(handler -> handler.accept(values));
				}
			}

			return message;

		} catch (IOException e) {
			throw new RuntimeException("Unable to deserialize message", e);
		}
	}

	public int drain() {
		int count = 0;
		while (socket.hasReceiveMore()) {
			// is there a way to avoid copying data to user space here?
			socket.recv();
			count++;
		}
		return count;
	}

	public void addValueHandler(Consumer<Map<String, Value>> handler) {
		valueHandlers.add(handler);
	}

	public void removeValueHandler(Consumer<Map<String, Value>> handler) {
		valueHandlers.remove(handler);
	}

	public void addMainHeaderHandler(Consumer<MainHeader> handler) {
		mainHeaderHandlers.add(handler);
	}

	public void removeMainHeaderHandler(Consumer<MainHeader> handler) {
		mainHeaderHandlers.remove(handler);
	}

	public void addDataHeaderHandler(Consumer<DataHeader> handler) {
		dataHeaderHandlers.add(handler);
	}

	public void removeDataHeaderHandler(Consumer<DataHeader> handler) {
		dataHeaderHandlers.remove(handler);
	}
}
