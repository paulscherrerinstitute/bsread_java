package ch.psi.bsread;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import com.fasterxml.jackson.databind.ObjectMapper;

import ch.psi.bsread.command.Command;
import ch.psi.bsread.message.DataHeader;
import ch.psi.bsread.message.MainHeader;
import ch.psi.bsread.message.Message;
import ch.psi.bsread.message.Value;

public class Receiver<V> implements IReceiver<V> {
	private static final Logger LOGGER = LoggerFactory.getLogger(Receiver.class);
	public static final String DEFAULT_RECEIVING_ADDRESS = "tcp://localhost:9999";

	private AtomicBoolean isConnected = new AtomicBoolean();
	private Context context;
	private Socket socket;

	private List<Consumer<MainHeader>> mainHeaderHandlers = new ArrayList<>();
	private List<Consumer<DataHeader>> dataHeaderHandlers = new ArrayList<>();
	private List<Consumer<Map<String, Value<V>>>> valueHandlers = new ArrayList<>();

	private ReceiverConfig<V> receiverConfig;
	private ReceiverState receiverState = new ReceiverState();

	public Receiver() {
		this(new ReceiverConfig<V>());
	}

	public Receiver(ReceiverConfig<V> receiverConfig) {
		this.receiverConfig = receiverConfig;

		this.dataHeaderHandlers.add(this.receiverConfig.getMessageExtractor());
	}

	public void connect() {
		connect(DEFAULT_RECEIVING_ADDRESS);
	}

	@Override
	public void connect(String address) {
		if (isConnected.compareAndSet(false, true)) {
			this.context = ZMQ.context(1);
			this.socket = this.context.socket(ZMQ.PULL);
			this.socket.setRcvHWM(receiverConfig.getHighWaterMark());
			this.socket.connect(address);
		}
	}

	@Override
	public void close() {
		if (isConnected.compareAndSet(true, false)) {
			socket.close();
			socket = null;
			context.close();
			context = null;
		}
	}

	public Message<V> receive() throws RuntimeException {
		Message<V> message = null;
		Command command = null;
		int nrOfAlignmentTrys = 0;
		ObjectMapper objectMapper = receiverConfig.getObjectMapper();

		while (message == null && isConnected.get()) {
			/*
			 * It can happen that bytes received do not represent the start of a
			 * new multipart message but the start of a submessage (e.g. after
			 * connection or when messages get lost). Therefore, make sure
			 * receiver is aligned with the start of the multipart message
			 * (i.e., it is possible that we loose the first message)
			 */
			try {
				// test if mainHaderBytes can be interpreted as Command
				command = objectMapper.readValue(socket.recv(), Command.class);
				message = command.process(this);
			} catch (IOException e) {
				++nrOfAlignmentTrys;
				LOGGER.info("Received bytes were not aligned with multipart message.", e);
				// drain the socket
				drain();

				if (nrOfAlignmentTrys > receiverConfig.getAlignmentRetries()) {
					throw new RuntimeException("Could not extract Command within max alignment retry.");
				}
			}
		}

		return message;
	}

	@Override
	public int drain() {
		int count = 0;
		while (socket.hasReceiveMore()) {
			// is there a way to avoid copying data to user space here?
			socket.recv();
			count++;
		}
		return count;
	}

	@Override
	public Socket getSocket() {
		return socket;
	}

	@Override
	public ReceiverConfig<V> getReceiverConfig() {
		return receiverConfig;
	}

	@Override
	public ReceiverState getReceiverState() {
		return receiverState;
	}

	@Override
	public Collection<Consumer<Map<String, Value<V>>>> getValueHandlers() {
		return valueHandlers;
	}

	public void addValueHandler(Consumer<Map<String, Value<V>>> handler) {
		valueHandlers.add(handler);
	}

	public void removeValueHandler(Consumer<Map<String, Value<V>>> handler) {
		valueHandlers.remove(handler);
	}

	@Override
	public Collection<Consumer<MainHeader>> getMainHeaderHandlers() {
		return mainHeaderHandlers;
	}

	public void addMainHeaderHandler(Consumer<MainHeader> handler) {
		mainHeaderHandlers.add(handler);
	}

	public void removeMainHeaderHandler(Consumer<MainHeader> handler) {
		mainHeaderHandlers.remove(handler);
	}

	@Override
	public Collection<Consumer<DataHeader>> getDataHeaderHandlers() {
		return dataHeaderHandlers;
	}

	public void addDataHeaderHandler(Consumer<DataHeader> handler) {
		dataHeaderHandlers.add(handler);
	}

	public void removeDataHeaderHandler(Consumer<DataHeader> handler) {
		dataHeaderHandlers.remove(handler);
	}
}
