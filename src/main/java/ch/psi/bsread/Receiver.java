package ch.psi.bsread;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

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

	private AtomicBoolean isRunning = new AtomicBoolean();
	private AtomicBoolean isCleaned = new AtomicBoolean();
	private Socket socket;

	private List<Consumer<MainHeader>> mainHeaderHandlers = new ArrayList<>();
	private List<Consumer<DataHeader>> dataHeaderHandlers = new ArrayList<>();
	private List<Consumer<Map<String, Value<V>>>> valueHandlers = new ArrayList<>();

	private ReceiverConfig<V> receiverConfig;
	private ReceiverState receiverState = new ReceiverState();

	private long idleConnectionDuration = 0;
	private CompletableFuture<Void> mainLoopExitSync;
	// helps to speedup close in case main receiving thread is not blocked in
	// receiving
	private volatile Thread receivingThread;

	public Receiver() {
		this(new ReceiverConfig<V>());
	}

	public Receiver(ReceiverConfig<V> receiverConfig) {
		this.receiverConfig = receiverConfig;

		this.dataHeaderHandlers.add(this.receiverConfig.getMessageExtractor());
	}

	public void connect() {
		if (isRunning.compareAndSet(false, true)) {
			this.receivingThread = null;
			this.isCleaned.set(false);
			this.mainLoopExitSync = new CompletableFuture<>();
			
			this.socket = this.receiverConfig.getContext().socket(receiverConfig.getSocketType());
			this.socket.setRcvHWM(receiverConfig.getHighWaterMark());
			this.socket.setReceiveTimeOut((int) receiverConfig.getReceiveTimeout());
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
		if (isRunning.compareAndSet(true, false)) {
			LOGGER.info("Receiver '{}' stopping...", this.receiverConfig.getAddress());

			if (Thread.currentThread().equals(receivingThread)) {
				// is receiving thread -> do cleanup
				cleanup();
			} else {
				// is not receiving thread - wait until receiving thread exited and did cleanup. 
				try {
					this.mainLoopExitSync.get((long) Math.max(0, 1.5 * this.receiverConfig.getReceiveTimeout()), TimeUnit.MILLISECONDS);
				} catch (Exception e) {
					LOGGER.warn("Could not stop '{}' within timelimits (it might not be receiving messages).",
							this.receiverConfig.getAddress());

					// let this thread do the cleanup
					cleanup();
				}
			}
		}
	}

	protected void cleanup() {
		if (isCleaned.compareAndSet(false, true)) {
			// make sure isRunning is set to false
			isRunning.set(false);
			if (socket != null) {
				socket.close();
				socket = null;
			}
			receivingThread = null;
			mainLoopExitSync.complete(null);
			LOGGER.info("Receiver '{}' stopped.", this.receiverConfig.getAddress());
		}
	}

	public Message<V> receive() throws RuntimeException {
		receivingThread = Thread.currentThread();
		Message<V> message = null;
		Command command = null;
		final ObjectMapper objectMapper = receiverConfig.getObjectMapper();
		byte[] mainHeaderBytes;
		idleConnectionDuration = 0;

		try {
			while (message == null && isRunning.get()) {
				mainHeaderBytes = null;
				/*
				 * It can happen that bytes received do not represent the start
				 * of a new multipart message but the start of a submessage
				 * (e.g. after connection or when messages get lost). Therefore,
				 * make sure receiver is aligned with the start of the multipart
				 * message (i.e., it is possible that we loose the first
				 * message)
				 */
				try {
					mainHeaderBytes = socket.recv();

					if (mainHeaderBytes != null) {
						// test if mainHaderBytes can be interpreted as Command
						command = objectMapper.readValue(mainHeaderBytes, Command.class);
						message = command.process(this);
					}
					else {
						idleConnectionDuration += receiverConfig.getReceiveTimeout();
						if (idleConnectionDuration > receiverConfig.getIdleConnectionTimeout()) {
							switch (receiverConfig.getIdleConnectionTimeoutBehavior()) {
							case RECONNECT:
								LOGGER.info("Reconnect '{}' due to timeout.", receiverConfig.getAddress());
								message = null;
								this.cleanup();
								this.connect();
								break;
							case STOP:
								LOGGER.warn("Stop running and return null for '{}' due to idle connection.", receiverConfig.getAddress());
								isRunning.set(false);
								break;
							case KEEP_RUNNING:
							default:
								LOGGER.info("Idle connection timeout for '{}'. Keep running.", receiverConfig.getAddress());
								message = null;
								break;
							}
						} else {
							message = null;
						}
					}
				} catch (JsonParseException | JsonMappingException e) {
					LOGGER.info("Could not parse MainHeader of '{}'.", receiverConfig.getAddress(), e);
					// drain the socket
					drain();
				} catch (IOException e) {
					LOGGER.info("Received bytes of '{}' were not aligned with multipart message.", receiverConfig.getAddress(),
							e);
					// drain the socket
					drain();
				} catch (ZMQException e) {
					LOGGER.info(
							"ZMQ stream of '{}' stopped/closed due to '{}'. This is considered as a valid state to stop sending.",
							receiverConfig.getAddress(), e.getMessage());
					isRunning.set(false);
				}
			}
		} catch (Exception e) {
			LOGGER.error(
					"ZMQ stream of '{}' stopped unexpectedly.", receiverConfig.getAddress(), e);
			isRunning.set(false);
			throw e;
		} finally {
			if (!isRunning.get()) {
				message = null;
				cleanup();
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
