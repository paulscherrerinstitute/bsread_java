package ch.psi.bsread.commands;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;
import org.zeromq.ZMQ;

import ch.psi.bsread.DataChannel;
import ch.psi.bsread.Receiver;
import ch.psi.bsread.ReceiverConfig;
import ch.psi.bsread.Sender;
import ch.psi.bsread.SenderConfig;
import ch.psi.bsread.TimeProvider;
import ch.psi.bsread.basic.BasicReceiver;
import ch.psi.bsread.converter.MatlabByteConverter;
import ch.psi.bsread.impl.StandardMessageExtractor;
import ch.psi.bsread.impl.StandardPulseIdProvider;
import ch.psi.bsread.message.ChannelConfig;
import ch.psi.bsread.message.DataHeader;
import ch.psi.bsread.message.MainHeader;
import ch.psi.bsread.message.Message;
import ch.psi.bsread.message.Timestamp;
import ch.psi.bsread.message.Type;
import ch.psi.bsread.message.Value;
import ch.psi.bsread.message.commands.StopCommand;
import ch.psi.bsread.monitors.ConnectionCounterMonitor;

public class StopCommandTest {
	private MainHeader hookMainHeader;
	private boolean hookMainHeaderCalled;
	private DataHeader hookDataHeader;
	private boolean hookDataHeaderCalled;
	private Map<String, Value<ByteBuffer>> hookValues;
	private boolean hookValuesCalled;
	private Map<String, ChannelConfig> channelConfigs = new HashMap<>();

	@Test
	public void testStop_01() throws Exception {
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

		Receiver<ByteBuffer> receiver = new Receiver<ByteBuffer>(new ReceiverConfig<ByteBuffer>(ReceiverConfig.DEFAULT_RECEIVING_ADDRESS, false, true, new StandardMessageExtractor<ByteBuffer>()));
		// Optional - register callbacks
		receiver.addMainHeaderHandler(header -> setMainHeader(header));
		receiver.addDataHeaderHandler(header -> setDataHeader(header));
		receiver.addValueHandler(values -> setValues(values));
		receiver.connect();

		CountDownLatch latch = new CountDownLatch(1);
		ExecutorService executor = Executors.newSingleThreadExecutor();
		executor.execute(() -> {
			try {
				sender.send();
				TimeUnit.MILLISECONDS.sleep(100);
				sender.send();
				TimeUnit.MILLISECONDS.sleep(100);
				sender.sendCommand(new StopCommand());
				TimeUnit.MILLISECONDS.sleep(100);
				sender.send();

			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				latch.countDown();
			}
		});

		hookMainHeaderCalled = false;
		hookDataHeaderCalled = false;
		hookValuesCalled = false;
		Message<ByteBuffer> message = receiver.receive();
		assertTrue("Main header hook should always be called.", hookMainHeaderCalled);
		assertEquals("Data header hook should only be called the first time.", true, hookDataHeaderCalled);
		assertTrue("Value hook should always be called.", hookValuesCalled);
		// should be the same instance
		assertSame(hookMainHeader, message.getMainHeader());
		assertSame(hookDataHeader, message.getDataHeader());
		assertSame(hookValues, message.getValues());
		assertEquals(0, hookMainHeader.getPulseId());

		hookMainHeaderCalled = false;
		hookDataHeaderCalled = false;
		hookValuesCalled = false;
		message = receiver.receive();
		assertTrue("Main header hook should always be called.", hookMainHeaderCalled);
		assertEquals("Data header hook should only be called the first time.", false, hookDataHeaderCalled);
		assertTrue("Value hook should always be called.", hookValuesCalled);
		// should be the same instance
		assertSame(hookMainHeader, message.getMainHeader());
		assertSame(hookDataHeader, message.getDataHeader());
		assertSame(hookValues, message.getValues());
		assertEquals(1, hookMainHeader.getPulseId());

		hookMainHeaderCalled = false;
		hookDataHeaderCalled = false;
		hookValuesCalled = false;
		// stops on stop
		message = receiver.receive();
		assertNull(message);

		latch.await();

		receiver.close();
		sender.close();
		executor.shutdown();
	}

	@Test
	public void testStop_02() throws Exception {
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

		Receiver<ByteBuffer> receiver = new Receiver<ByteBuffer>(new ReceiverConfig<ByteBuffer>(ReceiverConfig.DEFAULT_RECEIVING_ADDRESS, true, true, new StandardMessageExtractor<ByteBuffer>()));
		// Optional - register callbacks
		receiver.addMainHeaderHandler(header -> setMainHeader(header));
		receiver.addDataHeaderHandler(header -> setDataHeader(header));
		receiver.addValueHandler(values -> setValues(values));
		receiver.connect();

		CountDownLatch latch = new CountDownLatch(1);
		ExecutorService executor = Executors.newSingleThreadExecutor();
		executor.execute(() -> {
			try {
				sender.send();
				TimeUnit.MILLISECONDS.sleep(100);
				sender.send();
				TimeUnit.MILLISECONDS.sleep(100);
				sender.sendCommand(new StopCommand());
				TimeUnit.MILLISECONDS.sleep(100);
				sender.send();

			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				latch.countDown();
			}
		});

		hookMainHeaderCalled = false;
		hookDataHeaderCalled = false;
		hookValuesCalled = false;
		Message<ByteBuffer> message = receiver.receive();
		assertTrue("Main header hook should always be called.", hookMainHeaderCalled);
		assertEquals("Data header hook should only be called the first time.", true, hookDataHeaderCalled);
		assertTrue("Value hook should always be called.", hookValuesCalled);
		// should be the same instance
		assertSame(hookMainHeader, message.getMainHeader());
		assertSame(hookDataHeader, message.getDataHeader());
		assertSame(hookValues, message.getValues());
		assertEquals(0, hookMainHeader.getPulseId());

		hookMainHeaderCalled = false;
		hookDataHeaderCalled = false;
		hookValuesCalled = false;
		message = receiver.receive();
		assertTrue("Main header hook should always be called.", hookMainHeaderCalled);
		assertEquals("Data header hook should only be called the first time.", false, hookDataHeaderCalled);
		assertTrue("Value hook should always be called.", hookValuesCalled);
		// should be the same instance
		assertSame(hookMainHeader, message.getMainHeader());
		assertSame(hookDataHeader, message.getDataHeader());
		assertSame(hookValues, message.getValues());
		assertEquals(1, hookMainHeader.getPulseId());

		hookMainHeaderCalled = false;
		hookDataHeaderCalled = false;
		hookValuesCalled = false;
		message = receiver.receive();
		// does not stop on stop but keeps listening
		assertNotNull(message);
		assertTrue("Main header hook should always be called.", hookMainHeaderCalled);
		assertEquals("Data header hook should only be called the first time.", false, hookDataHeaderCalled);
		assertTrue("Value hook should always be called.", hookValuesCalled);
		// should be the same instance
		assertSame(hookMainHeader, message.getMainHeader());
		assertSame(hookDataHeader, message.getDataHeader());
		assertSame(hookValues, message.getValues());
		assertEquals(2, hookMainHeader.getPulseId());

		latch.await();

		receiver.close();
		sender.close();
		executor.shutdown();
	}

	@Test
	public void testStop_03_PUSH_PULL() throws Exception {
		SenderConfig senderConfig = new SenderConfig(
				SenderConfig.DEFAULT_SENDING_ADDRESS,
				new StandardPulseIdProvider(),
				new TimeProvider() {

					@Override
					public Timestamp getTime(long pulseId) {
						return new Timestamp(pulseId, 0L);
					}
				},
				new MatlabByteConverter());
		senderConfig.setSocketType(ZMQ.PUSH);
		senderConfig.setMonitor(new ConnectionCounterMonitor());
		Sender sender = new Sender(senderConfig);

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

		ReceiverConfig<Object> config1 = new ReceiverConfig<Object>(ReceiverConfig.DEFAULT_RECEIVING_ADDRESS, false, true, new StandardMessageExtractor<Object>(new MatlabByteConverter()));
		config1.setSocketType(ZMQ.PULL);
		Receiver<Object> receiver1 = new BasicReceiver(config1);
		AtomicLong mainHeaderCounter1 = new AtomicLong();
		AtomicLong dataHeaderCounter1 = new AtomicLong();
		AtomicLong valCounter1 = new AtomicLong();
		receiver1.addMainHeaderHandler(header -> mainHeaderCounter1.incrementAndGet());
		receiver1.addDataHeaderHandler(header -> dataHeaderCounter1.incrementAndGet());
		receiver1.addValueHandler(values -> valCounter1.incrementAndGet());
		receiver1.connect();

		ReceiverConfig<Object> config2 = new ReceiverConfig<Object>(ReceiverConfig.DEFAULT_RECEIVING_ADDRESS, false, true, new StandardMessageExtractor<Object>(new MatlabByteConverter()));
		config2.setSocketType(ZMQ.PULL);
		Receiver<Object> receiver2 = new BasicReceiver(config2);
		AtomicLong mainHeaderCounter2 = new AtomicLong();
		AtomicLong dataHeaderCounter2 = new AtomicLong();
		AtomicLong valCounter2 = new AtomicLong();
		receiver2.addMainHeaderHandler(header -> mainHeaderCounter2.incrementAndGet());
		receiver2.addDataHeaderHandler(header -> dataHeaderCounter2.incrementAndGet());
		receiver2.addValueHandler(values -> valCounter2.incrementAndGet());
		receiver2.connect();

		TimeUnit.MILLISECONDS.sleep(500);
		ExecutorService executor = Executors.newSingleThreadExecutor();
		executor.execute(() -> {
			try {
				sender.send();
				TimeUnit.MILLISECONDS.sleep(100);
				sender.send();
				TimeUnit.MILLISECONDS.sleep(100);
				sender.send();
				TimeUnit.MILLISECONDS.sleep(100);
				sender.send();
				TimeUnit.MILLISECONDS.sleep(100);
				sender.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		});

		Message<Object> message = receiver1.receive();
		assertNotNull(message);
		assertEquals(1, dataHeaderCounter1.getAndSet(0));
		assertEquals(1, mainHeaderCounter1.getAndSet(0));
		assertEquals(1, valCounter1.getAndSet(0));

		message = receiver2.receive();
		assertNotNull(message);
		assertEquals(1, dataHeaderCounter2.getAndSet(0));
		assertEquals(1, mainHeaderCounter2.getAndSet(0));
		assertEquals(1, valCounter2.getAndSet(0));

		message = receiver1.receive();
		assertNotNull(message);
		assertEquals(0, dataHeaderCounter1.getAndSet(0));
		assertEquals(1, mainHeaderCounter1.getAndSet(0));
		assertEquals(1, valCounter1.getAndSet(0));

		message = receiver2.receive();
		assertNotNull(message);
		assertEquals(0, dataHeaderCounter2.getAndSet(0));
		assertEquals(1, mainHeaderCounter2.getAndSet(0));
		assertEquals(1, valCounter2.getAndSet(0));

		message = receiver1.receive();
		assertNull(message);
		assertEquals(0, dataHeaderCounter1.getAndSet(0));
		assertEquals(0, mainHeaderCounter1.getAndSet(0));
		assertEquals(0, valCounter1.getAndSet(0));

		message = receiver2.receive();
		assertNull(message);
		assertEquals(0, dataHeaderCounter2.getAndSet(0));
		assertEquals(0, mainHeaderCounter2.getAndSet(0));
		assertEquals(0, valCounter2.getAndSet(0));

		// should not be necessary but check for exceptions etc.
		receiver1.close();
		receiver2.close();

		executor.shutdown();
	}

	@Test
	public void testStop_03_PUB_SUB() throws Exception {
		SenderConfig senderConfig = new SenderConfig(
				SenderConfig.DEFAULT_SENDING_ADDRESS,
				new StandardPulseIdProvider(),
				new TimeProvider() {

					@Override
					public Timestamp getTime(long pulseId) {
						return new Timestamp(pulseId, 0L);
					}
				},
				new MatlabByteConverter());
		senderConfig.setSocketType(ZMQ.PUB);
		senderConfig.setMonitor(new ConnectionCounterMonitor());
		Sender sender = new Sender(senderConfig);

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

		ReceiverConfig<Object> config1 = new ReceiverConfig<Object>(ReceiverConfig.DEFAULT_RECEIVING_ADDRESS, false, true, new StandardMessageExtractor<Object>(new MatlabByteConverter()));
		config1.setSocketType(ZMQ.SUB);
		Receiver<Object> receiver1 = new BasicReceiver(config1);
		AtomicLong mainHeaderCounter1 = new AtomicLong();
		AtomicLong dataHeaderCounter1 = new AtomicLong();
		AtomicLong valCounter1 = new AtomicLong();
		receiver1.addMainHeaderHandler(header -> mainHeaderCounter1.incrementAndGet());
		receiver1.addDataHeaderHandler(header -> dataHeaderCounter1.incrementAndGet());
		receiver1.addValueHandler(values -> valCounter1.incrementAndGet());
		receiver1.connect();

		ReceiverConfig<Object> config2 = new ReceiverConfig<Object>(ReceiverConfig.DEFAULT_RECEIVING_ADDRESS, false, true, new StandardMessageExtractor<Object>(new MatlabByteConverter()));
		config2.setSocketType(ZMQ.SUB);
		Receiver<Object> receiver2 = new BasicReceiver(config2);
		AtomicLong mainHeaderCounter2 = new AtomicLong();
		AtomicLong dataHeaderCounter2 = new AtomicLong();
		AtomicLong valCounter2 = new AtomicLong();
		receiver2.addMainHeaderHandler(header -> mainHeaderCounter2.incrementAndGet());
		receiver2.addDataHeaderHandler(header -> dataHeaderCounter2.incrementAndGet());
		receiver2.addValueHandler(values -> valCounter2.incrementAndGet());
		receiver2.connect();

		TimeUnit.MILLISECONDS.sleep(500);
		ExecutorService executor = Executors.newSingleThreadExecutor();
		executor.execute(() -> {
			try {
				sender.send();
				TimeUnit.MILLISECONDS.sleep(100);
				sender.send();
				TimeUnit.MILLISECONDS.sleep(100);
				sender.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		});

		Message<Object> message = receiver1.receive();
		assertNotNull(message);
		assertEquals(1, dataHeaderCounter1.getAndSet(0));
		assertEquals(1, mainHeaderCounter1.getAndSet(0));
		assertEquals(1, valCounter1.getAndSet(0));

		message = receiver2.receive();
		assertNotNull(message);
		assertEquals(1, dataHeaderCounter2.getAndSet(0));
		assertEquals(1, mainHeaderCounter2.getAndSet(0));
		assertEquals(1, valCounter2.getAndSet(0));

		message = receiver1.receive();
		assertNotNull(message);
		assertEquals(0, dataHeaderCounter1.getAndSet(0));
		assertEquals(1, mainHeaderCounter1.getAndSet(0));
		assertEquals(1, valCounter1.getAndSet(0));

		message = receiver2.receive();
		assertNotNull(message);
		assertEquals(0, dataHeaderCounter2.getAndSet(0));
		assertEquals(1, mainHeaderCounter2.getAndSet(0));
		assertEquals(1, valCounter2.getAndSet(0));

		message = receiver1.receive();
		assertNull(message);
		assertEquals(0, dataHeaderCounter1.getAndSet(0));
		assertEquals(0, mainHeaderCounter1.getAndSet(0));
		assertEquals(0, valCounter1.getAndSet(0));

		message = receiver2.receive();
		assertNull(message);
		assertEquals(0, dataHeaderCounter2.getAndSet(0));
		assertEquals(0, mainHeaderCounter2.getAndSet(0));
		assertEquals(0, valCounter2.getAndSet(0));

		// should not be necessary but check for exceptions etc.
		receiver1.close();
		receiver2.close();

		executor.shutdown();
	}

	private void setMainHeader(MainHeader header) {
		this.hookMainHeader = header;
		this.hookMainHeaderCalled = true;
	}

	private void setDataHeader(DataHeader header) {
		this.hookDataHeader = header;
		this.hookDataHeaderCalled = true;

		this.channelConfigs.clear();
		for (ChannelConfig chConf : header.getChannels()) {
			this.channelConfigs.put(chConf.getName(), chConf);
		}
	}

	public void setValues(Map<String, Value<ByteBuffer>> values) {
		this.hookValues = values;
		this.hookValuesCalled = true;
	}
}
