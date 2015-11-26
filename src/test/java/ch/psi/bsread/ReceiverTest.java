package ch.psi.bsread;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import ch.psi.bsread.converter.ByteConverter;
import ch.psi.bsread.converter.MatlabByteConverter;
import ch.psi.bsread.impl.StandardPulseIdProvider;
import ch.psi.bsread.message.ChannelConfig;
import ch.psi.bsread.message.DataHeader;
import ch.psi.bsread.message.MainHeader;
import ch.psi.bsread.message.Message;
import ch.psi.bsread.message.Timestamp;
import ch.psi.bsread.message.Type;
import ch.psi.bsread.message.Value;

public class ReceiverTest {
	private ByteConverter byteConverter = new MatlabByteConverter();
	private MainHeader hookMainHeader;
	private boolean hookMainHeaderCalled;
	private DataHeader hookDataHeader;
	private boolean hookDataHeaderCalled;
	private Map<String, Value<ByteBuffer>> hookValues;
	private boolean hookValuesCalled;
	private Map<String, ChannelConfig> channelConfigs = new HashMap<>();

	protected Receiver<ByteBuffer> getReceiver() {
		return new Receiver<ByteBuffer>();
	}

	@Test
	public void testSenderOneChannel10Hz() {
		Sender sender = new Sender(
				new StandardPulseIdProvider(),
				new TimeProvider() {

					@Override
					public Timestamp getTime(long pulseId) {
						return new Timestamp(pulseId, 0L);
					}
				},
				new MatlabByteConverter()
				);

		// Register data sources ...
		sender.addSource(new DataChannel<Double>(new ChannelConfig("ABC", Type.Float64, 10, 0)) {
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

		Receiver<ByteBuffer> receiver = getReceiver();
		// Optional - register callbacks
		receiver.addMainHeaderHandler(header -> setMainHeader(header));
		receiver.addDataHeaderHandler(header -> setDataHeader(header));
		receiver.addValueHandler(values -> setValues(values));
		receiver.connect();

		// We schedule faster as we want to have the testcase execute faster
		ScheduledFuture<?> sendFuture =
				Executors.newScheduledThreadPool(1).scheduleAtFixedRate(() -> sender.send(), 0, 1, TimeUnit.MILLISECONDS);

		// Receive data
		Message<ByteBuffer> message = null;
		for (int i = 0; i < 22; ++i) {
			hookMainHeaderCalled = false;
			hookDataHeaderCalled = false;
			hookValuesCalled = false;

			message = receiver.receive();

			assertTrue("Main header hook should always be called.", hookMainHeaderCalled);
			assertEquals("Data header hook should only be called the first time.", i == 0, hookDataHeaderCalled);
			assertTrue("Value hook should always be called.", hookValuesCalled);

			// should be the same instance
			assertSame(hookMainHeader, message.getMainHeader());
			assertSame(hookDataHeader, message.getDataHeader());
			assertSame(hookValues, message.getValues());

			assertTrue("Is a 10Hz Channel", hookMainHeader.getPulseId() % 10 == 0);
			if (hookDataHeaderCalled) {
				assertEquals(hookDataHeader.getChannels().size(), 1);
				ChannelConfig channelConfig = hookDataHeader.getChannels().get(0);
				assertEquals("ABC", channelConfig.getName());
				assertEquals(10, channelConfig.getModulo());
				assertEquals(0, channelConfig.getOffset());
				assertEquals(Type.Float64, channelConfig.getType());
				assertArrayEquals(new int[] { 1 }, channelConfig.getShape());
			}

			String channelName;
			ChannelConfig chConf;
			Value<ByteBuffer> value;
			Double javaVal;

			assertEquals(hookValues.size(), 1);
			assertEquals(i * 10, hookMainHeader.getPulseId());

			channelName = "ABC";
			chConf = this.channelConfigs.get(channelName);
			assertTrue(hookValues.containsKey(channelName));
			value = hookValues.get(channelName);
			javaVal = byteConverter.getValue(value.getValue(), chConf, hookMainHeader, null);
			assertEquals(Double.valueOf(hookMainHeader.getPulseId()), javaVal, 0.00000000001);
			assertEquals(hookMainHeader.getPulseId(), value.getTimestamp().getEpoch());
			assertEquals(0, value.getTimestamp().getNs());
			assertEquals(hookMainHeader.getPulseId(), hookMainHeader.getGlobalTimestamp().getEpoch());
			assertEquals(0, hookMainHeader.getGlobalTimestamp().getNs());
		}

		sendFuture.cancel(true);
		receiver.close();
		sender.close();
	}

	@Test
	public void testSenderOneChannel01Hz() {
		Sender sender = new Sender(
				new StandardPulseIdProvider(),
				new TimeProvider() {

					@Override
					public Timestamp getTime(long pulseId) {
						return new Timestamp(pulseId, 0L);
					}
				},
				new MatlabByteConverter()
				);

		// Register data sources ...
		sender.addSource(new DataChannel<Double>(new ChannelConfig("ABC", Type.Float64, 1000, 0)) {
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

		Receiver<ByteBuffer> receiver = getReceiver();
		// Optional - register callbacks
		receiver.addMainHeaderHandler(header -> setMainHeader(header));
		receiver.addDataHeaderHandler(header -> setDataHeader(header));
		receiver.addValueHandler(values -> setValues(values));
		receiver.connect();

		// We schedule faster as we want to have the testcase execute faster
		ScheduledFuture<?> sendFuture =
				Executors.newScheduledThreadPool(1).scheduleAtFixedRate(() -> sender.send(), 0, 1, TimeUnit.MILLISECONDS);

		// Receive data
		Message<ByteBuffer> message = null;
		for (int i = 0; i < 5; ++i) {
			hookMainHeaderCalled = false;
			hookDataHeaderCalled = false;
			hookValuesCalled = false;

			message = receiver.receive();

			assertTrue("Main header hook should always be called.", hookMainHeaderCalled);
			assertEquals("Data header hook should only be called the first time.", i == 0, hookDataHeaderCalled);
			assertTrue("Value hook should always be called.", hookValuesCalled);

			// should be the same instance
			assertSame(hookMainHeader, message.getMainHeader());
			assertSame(hookDataHeader, message.getDataHeader());
			assertSame(hookValues, message.getValues());

			assertTrue("Is a 0.1Hz Channel", hookMainHeader.getPulseId() % 1000 == 0);
			if (hookDataHeaderCalled) {
				assertEquals(hookDataHeader.getChannels().size(), 1);
				ChannelConfig channelConfig = hookDataHeader.getChannels().get(0);
				assertEquals("ABC", channelConfig.getName());
				assertEquals(1000, channelConfig.getModulo());
				assertEquals(0, channelConfig.getOffset());
				assertEquals(Type.Float64, channelConfig.getType());
				assertArrayEquals(new int[] { 1 }, channelConfig.getShape());
			}

			String channelName;
			ChannelConfig chConf;
			Value<ByteBuffer> value;
			Double javaVal;

			assertEquals(hookValues.size(), 1);
			assertEquals(i * 1000, hookMainHeader.getPulseId());

			channelName = "ABC";
			chConf = this.channelConfigs.get(channelName);
			assertTrue(hookValues.containsKey(channelName));
			value = hookValues.get(channelName);
			javaVal = byteConverter.getValue(value.getValue(), chConf, hookMainHeader, null);
			assertEquals(Double.valueOf(hookMainHeader.getPulseId()), javaVal, 0.00000000001);
			assertEquals(hookMainHeader.getPulseId(), value.getTimestamp().getEpoch());
			assertEquals(0, value.getTimestamp().getNs());
			assertEquals(hookMainHeader.getPulseId(), hookMainHeader.getGlobalTimestamp().getEpoch());
			assertEquals(0, hookMainHeader.getGlobalTimestamp().getNs());
		}

		sendFuture.cancel(true);
		receiver.close();
		sender.close();
	}

	@Test
	public void testSenderOneChannel10HzOffset() {
		Sender sender = new Sender(
				new StandardPulseIdProvider(),
				new TimeProvider() {

					@Override
					public Timestamp getTime(long pulseId) {
						return new Timestamp(pulseId, 0L);
					}
				},
				new MatlabByteConverter()
				);

		// Register data sources ...
		sender.addSource(new DataChannel<Double>(new ChannelConfig("ABC", Type.Float64, 10, 1)) {
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

		Receiver<ByteBuffer> receiver = getReceiver();
		// Optional - register callbacks
		receiver.addMainHeaderHandler(header -> setMainHeader(header));
		receiver.addDataHeaderHandler(header -> setDataHeader(header));
		receiver.addValueHandler(values -> setValues(values));
		receiver.connect();

		// We schedule faster as we want to have the testcase execute faster
		ScheduledFuture<?> sendFuture =
				Executors.newScheduledThreadPool(1).scheduleAtFixedRate(() -> sender.send(), 0, 1, TimeUnit.MILLISECONDS);

		// Receive data
		Message<ByteBuffer> message = null;
		for (int i = 0; i < 22; ++i) {
			hookMainHeaderCalled = false;
			hookDataHeaderCalled = false;
			hookValuesCalled = false;

			message = receiver.receive();

			assertTrue("Main header hook should always be called.", hookMainHeaderCalled);
			assertEquals("Data header hook should only be called the first time.", i == 0, hookDataHeaderCalled);
			assertTrue("Value hook should always be called.", hookValuesCalled);

			// should be the same instance
			assertSame(hookMainHeader, message.getMainHeader());
			assertSame(hookDataHeader, message.getDataHeader());
			assertSame(hookValues, message.getValues());

			assertTrue("Is a 10Hz Channel with offset 1", (hookMainHeader.getPulseId() - 1) % 10 == 0);
			if (hookDataHeaderCalled) {
				assertEquals(hookDataHeader.getChannels().size(), 1);
				ChannelConfig channelConfig = hookDataHeader.getChannels().get(0);
				assertEquals("ABC", channelConfig.getName());
				assertEquals(10, channelConfig.getModulo(), 0.00000000001);
				assertEquals(1, channelConfig.getOffset());
				assertEquals(Type.Float64, channelConfig.getType());
				assertArrayEquals(new int[] { 1 }, channelConfig.getShape());
			}

			String channelName;
			ChannelConfig chConf;
			Value<ByteBuffer> value;
			Double javaVal;

			assertEquals(hookValues.size(), 1);
			assertEquals((i * 10) + 1, hookMainHeader.getPulseId());

			channelName = "ABC";
			chConf = this.channelConfigs.get(channelName);
			assertTrue(hookValues.containsKey(channelName));
			value = hookValues.get(channelName);
			javaVal = byteConverter.getValue(value.getValue(), chConf, hookMainHeader, null);
			assertEquals(Double.valueOf(hookMainHeader.getPulseId()), javaVal, 0.00000000001);
			assertEquals(hookMainHeader.getPulseId(), value.getTimestamp().getEpoch());
			assertEquals(0, value.getTimestamp().getNs());
			assertEquals(hookMainHeader.getPulseId(), hookMainHeader.getGlobalTimestamp().getEpoch());
			assertEquals(0, hookMainHeader.getGlobalTimestamp().getNs());
		}

		sendFuture.cancel(true);
		receiver.close();
		sender.close();
	}

	@Test
	public void testSenderTwoChannel100HzAnd10Hz() {
		Sender sender = new Sender(
				new StandardPulseIdProvider(),
				new TimeProvider() {

					@Override
					public Timestamp getTime(long pulseId) {
						return new Timestamp(pulseId, 0L);
					}
				},
				new MatlabByteConverter()
				);

		// Register data sources ...
		sender.addSource(new DataChannel<Double>(new ChannelConfig("ABC_10", Type.Float64, 10, 0)) {
			@Override
			public Double getValue(long pulseId) {
				return (double) pulseId;
			}

			@Override
			public Timestamp getTime(long pulseId) {
				return new Timestamp(pulseId, 0L);
			}
		});
		sender.addSource(new DataChannel<Double>(new ChannelConfig("ABC_100", Type.Float64, 1, 0)) {
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

		Receiver<ByteBuffer> receiver = getReceiver();
		// Optional - register callbacks
		receiver.addMainHeaderHandler(header -> setMainHeader(header));
		receiver.addDataHeaderHandler(header -> setDataHeader(header));
		receiver.addValueHandler(values -> setValues(values));
		receiver.connect();

		// We schedule faster as we want to have the testcase execute faster
		ScheduledFuture<?> sendFuture =
				Executors.newScheduledThreadPool(1).scheduleAtFixedRate(() -> sender.send(), 0, 1, TimeUnit.MILLISECONDS);

		// Receive data
		Message<ByteBuffer> message = null;
		for (int i = 0; i < 22; ++i) {
			hookMainHeaderCalled = false;
			hookDataHeaderCalled = false;
			hookValuesCalled = false;

			message = receiver.receive();

			assertTrue("Main header hook should always be called.", hookMainHeaderCalled);
			assertEquals("Data header hook should only be called the first time.", i == 0, hookDataHeaderCalled);
			assertTrue("Value hook should always be called.", hookValuesCalled);

			// should be the same instance
			assertSame(hookMainHeader, message.getMainHeader());
			assertSame(hookDataHeader, message.getDataHeader());
			assertSame(hookValues, message.getValues());

			if (hookDataHeaderCalled) {
				assertEquals(hookDataHeader.getChannels().size(), 2);
				ChannelConfig channelConfig = hookDataHeader.getChannels().get(0);
				assertEquals("ABC_10", channelConfig.getName());
				assertEquals(10, channelConfig.getModulo());
				assertEquals(0, channelConfig.getOffset());
				assertEquals(Type.Float64, channelConfig.getType());
				assertArrayEquals(new int[] { 1 }, channelConfig.getShape());

				channelConfig = hookDataHeader.getChannels().get(1);
				assertEquals("ABC_100", channelConfig.getName());
				assertEquals(1, channelConfig.getModulo());
				assertEquals(0, channelConfig.getOffset());
				assertEquals(Type.Float64, channelConfig.getType());
				assertArrayEquals(new int[] { 1 }, channelConfig.getShape());
			}

			// 10Hz -> both channels should have values
			String channelName;
			ChannelConfig chConf;
			Value<ByteBuffer> value;
			Double javaVal;
			if (hookMainHeader.getPulseId() % 10 == 0) {
				assertEquals(2, hookValues.size());
				assertEquals(i, hookMainHeader.getPulseId());

				channelName = "ABC_10";
				chConf = this.channelConfigs.get(channelName);
				assertTrue(hookValues.containsKey(channelName));
				value = hookValues.get(channelName);
				javaVal = byteConverter.getValue(value.getValue(), chConf, hookMainHeader, null);
				assertEquals(Double.valueOf(hookMainHeader.getPulseId()), javaVal, 0.00000000001);
				assertEquals(hookMainHeader.getPulseId(), value.getTimestamp().getEpoch());
				assertEquals(0, value.getTimestamp().getNs());
				assertEquals(hookMainHeader.getPulseId(), hookMainHeader.getGlobalTimestamp().getEpoch());
				assertEquals(0, hookMainHeader.getGlobalTimestamp().getNs());

				channelName = "ABC_100";
				chConf = this.channelConfigs.get(channelName);
				assertTrue(hookValues.containsKey(channelName));
				assertEquals(i, hookMainHeader.getPulseId());
				value = hookValues.get(channelName);
				javaVal = byteConverter.getValue(value.getValue(), chConf, hookMainHeader, null);
				assertEquals(Double.valueOf(hookMainHeader.getPulseId()), javaVal, 0.00000000001);
				assertEquals(hookMainHeader.getPulseId(), value.getTimestamp().getEpoch());
				assertEquals(0, value.getTimestamp().getNs());
				assertEquals(hookMainHeader.getPulseId(), hookMainHeader.getGlobalTimestamp().getEpoch());
				assertEquals(0, hookMainHeader.getGlobalTimestamp().getNs());
			} else {
				assertEquals(1, hookValues.size());
				assertEquals(i, hookMainHeader.getPulseId());

				channelName = "ABC_100";
				chConf = this.channelConfigs.get(channelName);
				assertTrue(hookValues.containsKey(channelName));
				value = hookValues.get(channelName);
				javaVal = byteConverter.getValue(value.getValue(), chConf, hookMainHeader, null);
				assertEquals(Double.valueOf(hookMainHeader.getPulseId()), javaVal, 0.00000000001);
				assertEquals(hookMainHeader.getPulseId(), value.getTimestamp().getEpoch());
				assertEquals(0, value.getTimestamp().getNs());
				assertEquals(hookMainHeader.getPulseId(), hookMainHeader.getGlobalTimestamp().getEpoch());
				assertEquals(0, hookMainHeader.getGlobalTimestamp().getNs());
			}
		}

		sendFuture.cancel(true);
		receiver.close();
		sender.close();
	}

	@Test
	public void testSenderEncoding() {
		ByteConverter byteConverter = new MatlabByteConverter();

		Sender sender = new Sender(
				new StandardPulseIdProvider(),
				new TimeProvider() {

					@Override
					public Timestamp getTime(long pulseId) {
						return new Timestamp(pulseId, 0);
					}

				},
				byteConverter);

		// Register data sources ...
		sender.addSource(new DataChannel<Long>(new ChannelConfig("ABC", Type.Int64, new int[] { 1 }, 10, 0,
				ChannelConfig.ENCODING_LITTLE_ENDIAN)) {
			@Override
			public Long getValue(long pulseId) {
				return pulseId;
			}

			@Override
			public Timestamp getTime(long pulseId) {
				return new Timestamp(pulseId, pulseId);
			}
		});
		sender.addSource(new DataChannel<Long>(new ChannelConfig("ABCD", Type.Int64, new int[] { 1 }, 10, 0,
				ChannelConfig.ENCODING_BIG_ENDIAN)) {
			@Override
			public Long getValue(long pulseId) {
				return pulseId + 1;
			}

			@Override
			public Timestamp getTime(long pulseId) {
				return new Timestamp(pulseId + 1, pulseId + 1);
			}
		});
		sender.bind();

		Receiver<ByteBuffer> receiver = getReceiver();
		// Optional - register callbacks
		receiver.addMainHeaderHandler(header -> setMainHeader(header));
		receiver.addDataHeaderHandler(header -> setDataHeader(header));
		receiver.addValueHandler(values -> setValues(values));
		receiver.connect();

		// We schedule faster as we want to have the testcase execute faster
		ScheduledFuture<?> sendFuture =
				Executors.newScheduledThreadPool(1).scheduleAtFixedRate(() -> sender.send(), 0, 1, TimeUnit.MILLISECONDS);

		// Receive data
		Message<ByteBuffer> message = null;
		for (int i = 0; i < 22; ++i) {
			hookMainHeaderCalled = false;
			hookDataHeaderCalled = false;
			hookValuesCalled = false;

			message = receiver.receive();

			assertTrue("Main header hook should always be called.", hookMainHeaderCalled);
			assertEquals("Data header hook should only be called the first time.", i == 0, hookDataHeaderCalled);
			assertTrue("Value hook should always be called.", hookValuesCalled);

			// should be the same instance
			assertSame(hookMainHeader, message.getMainHeader());
			assertSame(hookDataHeader, message.getDataHeader());
			assertSame(hookValues, message.getValues());

			assertTrue("Is a 10Hz Channel", hookMainHeader.getPulseId() % 10 == 0);
			Timestamp globalTimestamp = hookMainHeader.getGlobalTimestamp();
			assertEquals(hookMainHeader.getPulseId(), globalTimestamp.getEpoch());
			assertEquals(0, globalTimestamp.getNs());

			if (hookDataHeaderCalled) {
				assertEquals(hookDataHeader.getChannels().size(), 2);

				ChannelConfig channelConfig = hookDataHeader.getChannels().get(0);
				assertEquals("ABC", channelConfig.getName());
				assertEquals(10, channelConfig.getModulo());
				assertEquals(0, channelConfig.getOffset());
				assertEquals(Type.Int64, channelConfig.getType());
				assertEquals(ChannelConfig.ENCODING_LITTLE_ENDIAN, channelConfig.getEncoding());
				assertArrayEquals(new int[] { 1 }, channelConfig.getShape());

				channelConfig = hookDataHeader.getChannels().get(1);
				assertEquals("ABCD", channelConfig.getName());
				assertEquals(10, channelConfig.getModulo());
				assertEquals(0, channelConfig.getOffset());
				assertEquals(Type.Int64, channelConfig.getType());
				assertEquals(ChannelConfig.ENCODING_BIG_ENDIAN, channelConfig.getEncoding());
				assertArrayEquals(new int[] { 1 }, channelConfig.getShape());
			}

			for (int j = 0; j < hookDataHeader.getChannels().size(); ++j) {
				ChannelConfig channelConfig = hookDataHeader.getChannels().get(j);

				Value<ByteBuffer> value = hookValues.get(channelConfig.getName());
				Timestamp iocTimestamp = value.getTimestamp();
				assertEquals(hookMainHeader.getPulseId() + j, iocTimestamp.getEpoch());
				assertEquals(hookMainHeader.getPulseId() + j, iocTimestamp.getNs());
				Number val = this.byteConverter.getValue(
						value.getValue(),
						channelConfig, 
						hookMainHeader,
						null);
				assertEquals(hookMainHeader.getPulseId() + j, val.longValue());
			}
		}

		sendFuture.cancel(true);
		receiver.close();
		sender.close();
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
