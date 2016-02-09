package ch.psi.bsread.monitors;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import zmq.SocketBase;
import zmq.ZError;
import zmq.ZMQ;
import zmq.ZMQ.Event;

// builds on https://github.com/zeromq/jeromq/blob/master/src/test/java/zmq/TestMonitor.java
public class ConnectionMonitor implements Monitor {
	private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionMonitor.class);
	private ExecutorService executor = Executors.newSingleThreadExecutor();
	private AtomicInteger connectionCounter = new AtomicInteger();
	private List<IntConsumer> handlers = new ArrayList<>();

	public ConnectionMonitor() {
	}

	@Override
	public void start(Context context, SocketBase socket, String monitorItentifier) {
		executor.execute(() -> {
			String address = "inproc://" + monitorItentifier;
			Socket monitorSock = null;
			try {
				socket.monitor(address, ZMQ.ZMQ_EVENT_ACCEPTED | ZMQ.ZMQ_EVENT_DISCONNECTED);
				monitorSock = context.socket(ZMQ.ZMQ_PAIR);
				monitorSock.connect(address);

				Event event;
				// TestMonitor uses (event == null && s.errno() == ZError.ETERM)
				// -> ?
				while ((event = Event.read(monitorSock.base())) != null && socket.errno() != ZError.ETERM
						&& !Thread.currentThread().isInterrupted()) {

					switch (event.event) {
					case zmq.ZMQ.ZMQ_EVENT_ACCEPTED:
						updateHandlers(connectionCounter.incrementAndGet());
						break;
					case zmq.ZMQ.ZMQ_EVENT_DISCONNECTED:
						updateHandlers(connectionCounter.decrementAndGet());
						break;
					default:
						LOGGER.info("Unexpected event '{}' received for identifier '{} monitoring '{}'", event.event,
								monitorItentifier, event.addr);
					}
				}
			} catch (Throwable e) {
				LOGGER.warn("Monitoring zmq connections failed for identifier '{}'.", monitorItentifier, e);
			} finally {
				// Clear interrupted state as this might cause problems with the
				// rest of the remaining code - i.e. closing of the zmq socket
				// Thread.interrupted();

				if (monitorSock != null) {
					monitorSock.close();
				}

				connectionCounter.set(0);
				updateHandlers(connectionCounter.get());
				// clear references to ensure they can be gc
				handlers.clear();

				executor.shutdown();
			}
		});
	}

	@Override
	public void stop() {
		executor.shutdown();
	}

	public int getConnectionCount() {
		return connectionCounter.get();
	}

	public void addHandler(IntConsumer handler) {
		handlers.add(handler);
	}

	public void removeHandler(IntConsumer handler) {
		handlers.remove(handler);
	}

	private void updateHandlers(int currentCounter) {
		handlers.forEach(handler -> handler.accept(currentCounter));
	}
}
