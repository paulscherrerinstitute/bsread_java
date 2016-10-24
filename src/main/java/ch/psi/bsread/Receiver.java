package ch.psi.bsread;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
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
import org.zeromq.ZMQException;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import ch.psi.bsread.command.Command;
import ch.psi.bsread.message.DataHeader;
import ch.psi.bsread.message.MainHeader;
import ch.psi.bsread.message.Message;
import ch.psi.bsread.message.Value;

public class Receiver<V> implements ConfigIReceiver<V> {
	private static final Logger LOGGER = LoggerFactory.getLogger(Receiver.class);

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
		if (isConnected.compareAndSet(false, true)) {
			this.context = ZMQ.context(1);
			this.socket = this.context.socket(receiverConfig.getSocketType());
			this.socket.setRcvHWM(receiverConfig.getHighWaterMark());
			if (receiverConfig.getReceiveTimeout() != null) {
				this.socket.setReceiveTimeOut(receiverConfig.getReceiveTimeout());
			}
			if (receiverConfig.getMsgAllocator() != null) {
				this.socket.base().setSocketOpt(zmq.ZMQ.ZMQ_MSG_ALLOCATOR, receiverConfig.getMsgAllocator());
			}
			this.socket.connect(receiverConfig.getAddress());

			if (ZMQ.SUB == receiverConfig.getSocketType()) {
				this.socket.subscribe("".getBytes());
			}
		}
	}

	@Override
	public void close() {
		if (isConnected.compareAndSet(true, false)) {
			LOGGER.info("Receiver '{}' stopping...", this.receiverConfig.getAddress());
			socket.close();
			socket = null;
			context.close();
			context = null;
			LOGGER.info("Receiver '{}' stopped.", this.receiverConfig.getAddress());
		}
	}

	public Message<V> receive() throws RuntimeException {
		Message<V> message = null;
		Command command = null;
		int nrOfAlignmentTrys = 0;
		ObjectMapper objectMapper = receiverConfig.getObjectMapper();
		byte[] mainHeaderBytes;

		while (message == null && isConnected.get()) {
			mainHeaderBytes = null;
			/*
			 * It can happen that bytes received do not represent the start of a
			 * new multipart message but the start of a submessage (e.g. after
			 * connection or when messages get lost). Therefore, make sure
			 * receiver is aligned with the start of the multipart message
			 * (i.e., it is possible that we loose the first message)
			 */
			try {
				mainHeaderBytes = socket.recv();

				if (mainHeaderBytes != null) {
					// test if mainHaderBytes can be interpreted as Command
					command = objectMapper.readValue(mainHeaderBytes, Command.class);
					message = command.process(this);
				}
				else {
					if (receiverConfig.getReceiveTimeout() != null) {
						switch (receiverConfig.getReceiveTimeoutBehavior()) {
						case RECONNECT:
							LOGGER.info("Reconnect '{}' due to timeout.", receiverConfig.getAddress());
							this.close();
							this.connect();
							continue;
						case RETURN:
						default:
							LOGGER.warn("Return null for '{}' due to timeout.", receiverConfig.getAddress());
							return null;
						}
					} else {
						LOGGER.warn("Unknown receive state for '{}'.", receiverConfig.getAddress());
						message = null;
					}
				}
			} catch (JsonParseException | JsonMappingException e) {
				++nrOfAlignmentTrys;

				LOGGER.info("Could not parse MainHeader.", e);
				if (mainHeaderBytes != null) {
					String mainHeaderJson = new String(mainHeaderBytes, StandardCharsets.UTF_8);
					LOGGER.info("MainHeader was '{}'", mainHeaderJson);
				}
				// drain the socket
				drain();

				if (nrOfAlignmentTrys > receiverConfig.getAlignmentRetries()) {
					throw new RuntimeException("Could not extract Command within max retries.");
				}
			} catch (IOException e) {
				++nrOfAlignmentTrys;
				LOGGER.info("Received bytes were not aligned with multipart message.", e);
				// drain the socket
				drain();

				if (nrOfAlignmentTrys > receiverConfig.getAlignmentRetries()) {
					throw new RuntimeException("Could not extract Command within max retries.");
				}
			} catch (ZMQException e) {
				if (e.getErrorCode() == ZMQ.Error.ETERM.getCode()) {
					LOGGER.debug("ZMQ stream stopped/closed due to '{}'. This is considered as a valid state to stop sending.",
							e.getMessage());
				} else {
					throw e;
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
			socket.base().recv(0);
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
