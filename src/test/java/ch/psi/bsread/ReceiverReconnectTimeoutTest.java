package ch.psi.bsread;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.nio.ByteBuffer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import ch.psi.bsread.ReceiverConfig.ReceiveTimeoutBehavior;
import ch.psi.bsread.converter.MatlabByteConverter;
import ch.psi.bsread.impl.StandardPulseIdProvider;
import ch.psi.bsread.message.ChannelConfig;
import ch.psi.bsread.message.Message;
import ch.psi.bsread.message.Timestamp;
import ch.psi.bsread.message.Type;

public class ReceiverReconnectTimeoutTest {

	@Test
	public void testReceiveTimeoutDefaultSettings() {
		// default is block until message available
		ReceiverConfig<ByteBuffer> receiverConfig = new ReceiverConfig<>();
		assertNull(receiverConfig.getReceiveTimeout());
		assertEquals(ReceiverConfig.ReceiveTimeoutBehavior.RECONNECT, receiverConfig.getReceiveTimeoutBehavior());
	}

	@Test
	public void testSenderReceiverTimeout_Reconnect() {
		Sender sender = new Sender(
				new SenderConfig(
						SenderConfig.DEFAULT_SENDING_ADDRESS,
						new StandardPulseIdProvider(),
						new TimeProvider() {

							@Override
							public Timestamp getTime(long pulseId) {
								return new Timestamp(pulseId, 0L);
							}
						},
						new MatlabByteConverter())
				);

		// Register data sources ...
		sender.addSource(new DataChannel<Double>(new ChannelConfig("ABC", Type.Float64, 1, 0)) {
			@Override
			public Double getValue(long pulseId) {
				return (double) pulseId;
			}

			@Override
			public Timestamp getTime(long pulseId) {
				return new Timestamp(pulseId, 0L);
			}
		});
		sender.bind();

		ScheduledExecutorService scheduledExecutor = Executors.newScheduledThreadPool(1);

		long receiveTimeout = TimeUnit.MILLISECONDS.toMillis(500);
		ReceiverConfig<ByteBuffer> receiverConfig = new ReceiverConfig<>();
		receiverConfig.setReceiveTimeout((int) receiveTimeout);
		receiverConfig.setReceiveTimeoutBehavior(ReceiveTimeoutBehavior.RECONNECT);
		Receiver<ByteBuffer> receiver = new Receiver<ByteBuffer>(receiverConfig);
		receiver.connect();

		// Send/Receive data
		Message<ByteBuffer> message = null;

		sender.send();
		message = receiver.receive();
		assertNotNull(message);

		sender.send();
		message = receiver.receive();
		assertNotNull(message);

		scheduledExecutor.schedule(() -> sender.send(), (long) (1.5 * receiveTimeout), TimeUnit.MILLISECONDS);
		// should reconnect and wait for new messages
		message = receiver.receive();
		assertNotNull(message);

		sender.send();
		message = receiver.receive();
		assertNotNull(message);

		sender.send();
		message = receiver.receive();
		assertNotNull(message);

		scheduledExecutor.schedule(() -> sender.send(), (long) (2.0 * receiveTimeout), TimeUnit.MILLISECONDS);
		// should reconnect and wait for new messages
		message = receiver.receive();
		assertNotNull(message);

		sender.send();
		message = receiver.receive();
		assertNotNull(message);

		sender.send();
		message = receiver.receive();
		assertNotNull(message);

		scheduledExecutor.schedule(() -> sender.send(), (long) (3.0 * receiveTimeout), TimeUnit.MILLISECONDS);
		// should reconnect and wait for new messages
		message = receiver.receive();
		assertNotNull(message);

		sender.send();
		message = receiver.receive();
		assertNotNull(message);

		sender.send();
		message = receiver.receive();
		assertNotNull(message);

		scheduledExecutor.shutdown();
		receiver.close();
		sender.close();
	}

	@Test
	public void testSenderReceiverTimeout_Return() {
		Sender sender = new Sender(
				new SenderConfig(
						SenderConfig.DEFAULT_SENDING_ADDRESS,
						new StandardPulseIdProvider(),
						new TimeProvider() {

							@Override
							public Timestamp getTime(long pulseId) {
								return new Timestamp(pulseId, 0L);
							}
						},
						new MatlabByteConverter())
				);

		// Register data sources ...
		sender.addSource(new DataChannel<Double>(new ChannelConfig("ABC", Type.Float64, 1, 0)) {
			@Override
			public Double getValue(long pulseId) {
				return (double) pulseId;
			}

			@Override
			public Timestamp getTime(long pulseId) {
				return new Timestamp(pulseId, 0L);
			}
		});
		sender.bind();

		long receiveTimeout = TimeUnit.MILLISECONDS.toMillis(500);
		ReceiverConfig<ByteBuffer> receiverConfig = new ReceiverConfig<>();
		receiverConfig.setReceiveTimeout((int) receiveTimeout);
		receiverConfig.setReceiveTimeoutBehavior(ReceiveTimeoutBehavior.RETURN);
		Receiver<ByteBuffer> receiver = new Receiver<ByteBuffer>(receiverConfig);
		receiver.connect();

		// Send/Receive data
		Message<ByteBuffer> message = null;

		sender.send();
		message = receiver.receive();
		assertNotNull(message);

		sender.send();
		message = receiver.receive();
		assertNotNull(message);

		// should timeout and return null
		message = receiver.receive();
		assertNull(message);

		receiver.close();
		sender.close();
	}
}