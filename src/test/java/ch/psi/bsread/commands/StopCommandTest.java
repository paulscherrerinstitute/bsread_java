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

import org.junit.Test;

import ch.psi.bsread.DataChannel;
import ch.psi.bsread.Receiver;
import ch.psi.bsread.ReceiverConfig;
import ch.psi.bsread.Sender;
import ch.psi.bsread.SenderConfig;
import ch.psi.bsread.TimeProvider;
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

		Receiver<ByteBuffer> receiver = new Receiver<ByteBuffer>(new ReceiverConfig<ByteBuffer>(false, true, new StandardMessageExtractor<ByteBuffer>()));
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

		Receiver<ByteBuffer> receiver = new Receiver<ByteBuffer>(new ReceiverConfig<ByteBuffer>(true, true, new StandardMessageExtractor<ByteBuffer>()));
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
