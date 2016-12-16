package ch.psi.bsread;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import ch.psi.bsread.allocator.HeaderReservingMsgAllocator;
import ch.psi.bsread.common.allocator.ByteBufferAllocator;
import ch.psi.bsread.converter.ByteConverter;
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

public class ReceiverTest_3 {
	private ByteConverter byteConverter = new MatlabByteConverter();
	private MainHeader hookMainHeader;
	private boolean hookMainHeaderCalled;
	private DataHeader hookDataHeader;
	private boolean hookDataHeaderCalled;
	private Map<String, Value<ByteBuffer>> hookValues;
	private boolean hookValuesCalled;
	private Map<String, ChannelConfig> channelConfigs = new HashMap<>();
	private long initialDelay = 200;
	private long period = 1;

	protected Receiver<ByteBuffer> getReceiver() {
		ReceiverConfig<ByteBuffer> config =
				new ReceiverConfig<>(
						ReceiverConfig.DEFAULT_RECEIVING_ADDRESS,
						true,
						false,
						new StandardMessageExtractor<ByteBuffer>(),
						new HeaderReservingMsgAllocator(
								12,
								ByteBufferAllocator.DEFAULT_ALLOCATOR)
				);
		return new Receiver<ByteBuffer>(config);
	}

	@Test
	public void testSenderOneChannel10Hz() {
		ScheduledSender sender = new ScheduledSender(
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

		Receiver<ByteBuffer> receiver = getReceiver();
		// Optional - register callbacks
		receiver.addMainHeaderHandler(header -> setMainHeader(header));
		receiver.addDataHeaderHandler(header -> setDataHeader(header));
		receiver.addValueHandler(values -> setValues(values));

		try {
			receiver.connect();
			// We schedule faster as we want to have the testcase execute faster
			sender.bind();
			sender.sendAtFixedRate(initialDelay, period, TimeUnit.MILLISECONDS);

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
					ChannelConfig channelConfig = hookDataHeader.getChannels().iterator().next();
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
				javaVal = byteConverter.getValue(hookMainHeader, hookDataHeader, chConf, value.getValue(), null);
				assertEquals(Double.valueOf(hookMainHeader.getPulseId()), javaVal, 0.00000000001);
				assertEquals(hookMainHeader.getPulseId(), value.getTimestamp().getSec());
				assertEquals(0, value.getTimestamp().getNs());
				assertEquals(hookMainHeader.getPulseId(), hookMainHeader.getGlobalTimestamp().getSec());
				assertEquals(0, hookMainHeader.getGlobalTimestamp().getNs());
			}
		} finally {
			receiver.close();
			sender.close();
		}
	}

	@Test
	public void testSenderOneChannel01Hz() {
		ScheduledSender sender = new ScheduledSender(
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

		Receiver<ByteBuffer> receiver = getReceiver();
		// Optional - register callbacks
		receiver.addMainHeaderHandler(header -> setMainHeader(header));
		receiver.addDataHeaderHandler(header -> setDataHeader(header));
		receiver.addValueHandler(values -> setValues(values));

		try {
			receiver.connect();
			// We schedule faster as we want to have the testcase execute faster
			sender.bind();
			sender.sendAtFixedRate(initialDelay, period, TimeUnit.MILLISECONDS);

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
					ChannelConfig channelConfig = hookDataHeader.getChannels().iterator().next();
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
				javaVal = byteConverter.getValue(hookMainHeader, hookDataHeader, chConf, value.getValue(), null);
				assertEquals(Double.valueOf(hookMainHeader.getPulseId()), javaVal, 0.00000000001);
				assertEquals(hookMainHeader.getPulseId(), value.getTimestamp().getSec());
				assertEquals(0, value.getTimestamp().getNs());
				assertEquals(hookMainHeader.getPulseId(), hookMainHeader.getGlobalTimestamp().getSec());
				assertEquals(0, hookMainHeader.getGlobalTimestamp().getNs());
			}
		} finally {
			receiver.close();
			sender.close();
		}
	}

	@Test
	public void testSenderOneChannel10HzOffset() {
		ScheduledSender sender = new ScheduledSender(
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

		Receiver<ByteBuffer> receiver = getReceiver();
		// Optional - register callbacks
		receiver.addMainHeaderHandler(header -> setMainHeader(header));
		receiver.addDataHeaderHandler(header -> setDataHeader(header));
		receiver.addValueHandler(values -> setValues(values));

		try {
			receiver.connect();
			// We schedule faster as we want to have the testcase execute faster
			sender.bind();
			sender.sendAtFixedRate(initialDelay, period, TimeUnit.MILLISECONDS);

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
					ChannelConfig channelConfig = hookDataHeader.getChannels().iterator().next();
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
				javaVal = byteConverter.getValue(hookMainHeader, hookDataHeader, chConf, value.getValue(), null);
				assertEquals(Double.valueOf(hookMainHeader.getPulseId()), javaVal, 0.00000000001);
				assertEquals(hookMainHeader.getPulseId(), value.getTimestamp().getSec());
				assertEquals(0, value.getTimestamp().getNs());
				assertEquals(hookMainHeader.getPulseId(), hookMainHeader.getGlobalTimestamp().getSec());
				assertEquals(0, hookMainHeader.getGlobalTimestamp().getNs());
			}
		} finally {
			receiver.close();
			sender.close();
		}
	}

	@Test
	public void testSenderTwoChannel100HzAnd10Hz() {
		ScheduledSender sender = new ScheduledSender(
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

		Receiver<ByteBuffer> receiver = getReceiver();
		// Optional - register callbacks
		receiver.addMainHeaderHandler(header -> setMainHeader(header));
		receiver.addDataHeaderHandler(header -> setDataHeader(header));
		receiver.addValueHandler(values -> setValues(values));

		try {
			receiver.connect();
			// We schedule faster as we want to have the testcase execute faster
			sender.bind();
			sender.sendAtFixedRate(initialDelay, period, TimeUnit.MILLISECONDS);

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
					Iterator<ChannelConfig> configIter = hookDataHeader.getChannels().iterator();
					ChannelConfig channelConfig = configIter.next();
					assertEquals("ABC_10", channelConfig.getName());
					assertEquals(10, channelConfig.getModulo());
					assertEquals(0, channelConfig.getOffset());
					assertEquals(Type.Float64, channelConfig.getType());
					assertArrayEquals(new int[] { 1 }, channelConfig.getShape());

					channelConfig = configIter.next();
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
					javaVal = byteConverter.getValue(hookMainHeader, hookDataHeader, chConf, value.getValue(), null);
					assertEquals(Double.valueOf(hookMainHeader.getPulseId()), javaVal, 0.00000000001);
					assertEquals(hookMainHeader.getPulseId(), value.getTimestamp().getSec());
					assertEquals(0, value.getTimestamp().getNs());
					assertEquals(hookMainHeader.getPulseId(), hookMainHeader.getGlobalTimestamp().getSec());
					assertEquals(0, hookMainHeader.getGlobalTimestamp().getNs());

					channelName = "ABC_100";
					chConf = this.channelConfigs.get(channelName);
					assertTrue(hookValues.containsKey(channelName));
					assertEquals(i, hookMainHeader.getPulseId());
					value = hookValues.get(channelName);
					javaVal = byteConverter.getValue(hookMainHeader, hookDataHeader, chConf, value.getValue(), null);
					assertEquals(Double.valueOf(hookMainHeader.getPulseId()), javaVal, 0.00000000001);
					assertEquals(hookMainHeader.getPulseId(), value.getTimestamp().getSec());
					assertEquals(0, value.getTimestamp().getNs());
					assertEquals(hookMainHeader.getPulseId(), hookMainHeader.getGlobalTimestamp().getSec());
					assertEquals(0, hookMainHeader.getGlobalTimestamp().getNs());
				} else {
					assertEquals(1, hookValues.size());
					assertEquals(i, hookMainHeader.getPulseId());

					channelName = "ABC_100";
					chConf = this.channelConfigs.get(channelName);
					assertTrue(hookValues.containsKey(channelName));
					value = hookValues.get(channelName);
					javaVal = byteConverter.getValue(hookMainHeader, hookDataHeader, chConf, value.getValue(), null);
					assertEquals(Double.valueOf(hookMainHeader.getPulseId()), javaVal, 0.00000000001);
					assertEquals(hookMainHeader.getPulseId(), value.getTimestamp().getSec());
					assertEquals(0, value.getTimestamp().getNs());
					assertEquals(hookMainHeader.getPulseId(), hookMainHeader.getGlobalTimestamp().getSec());
					assertEquals(0, hookMainHeader.getGlobalTimestamp().getNs());
				}
			}
		} finally {
			receiver.close();
			sender.close();
		}
	}

	@Test
	public void testSenderEncoding() {
		ByteConverter byteConverter = new MatlabByteConverter();

		ScheduledSender sender = new ScheduledSender(
				new SenderConfig(
						SenderConfig.DEFAULT_SENDING_ADDRESS,
						new StandardPulseIdProvider(),
						new TimeProvider() {

							@Override
							public Timestamp getTime(long pulseId) {
								return new Timestamp(pulseId, 0);
							}

						},
						byteConverter)
				);

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

		Receiver<ByteBuffer> receiver = getReceiver();
		// Optional - register callbacks
		receiver.addMainHeaderHandler(header -> setMainHeader(header));
		receiver.addDataHeaderHandler(header -> setDataHeader(header));
		receiver.addValueHandler(values -> setValues(values));

		try {
			receiver.connect();
			// We schedule faster as we want to have the testcase execute faster
			sender.bind();
			sender.sendAtFixedRate(initialDelay, period, TimeUnit.MILLISECONDS);

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
				assertEquals(hookMainHeader.getPulseId(), globalTimestamp.getSec());
				assertEquals(0, globalTimestamp.getNs());

				if (hookDataHeaderCalled) {
					assertEquals(hookDataHeader.getChannels().size(), 2);
					Iterator<ChannelConfig> configIter = hookDataHeader.getChannels().iterator();
					ChannelConfig channelConfig = configIter.next();
					assertEquals("ABC", channelConfig.getName());
					assertEquals(10, channelConfig.getModulo());
					assertEquals(0, channelConfig.getOffset());
					assertEquals(Type.Int64, channelConfig.getType());
					assertEquals(ChannelConfig.ENCODING_LITTLE_ENDIAN, channelConfig.getEncoding());
					assertArrayEquals(new int[] { 1 }, channelConfig.getShape());

					channelConfig = configIter.next();
					assertEquals("ABCD", channelConfig.getName());
					assertEquals(10, channelConfig.getModulo());
					assertEquals(0, channelConfig.getOffset());
					assertEquals(Type.Int64, channelConfig.getType());
					assertEquals(ChannelConfig.ENCODING_BIG_ENDIAN, channelConfig.getEncoding());
					assertArrayEquals(new int[] { 1 }, channelConfig.getShape());
				}

				int j = 0;
				for (ChannelConfig channelConfig : hookDataHeader.getChannels()) {
					Value<ByteBuffer> value = hookValues.get(channelConfig.getName());
					Timestamp iocTimestamp = value.getTimestamp();
					assertEquals(hookMainHeader.getPulseId() + j, iocTimestamp.getSec());
					assertEquals(hookMainHeader.getPulseId() + j, iocTimestamp.getNs());
					Number val = this.byteConverter.getValue(
							hookMainHeader,
							hookDataHeader,
							channelConfig,
							value.getValue(),
							null);
					assertEquals(hookMainHeader.getPulseId() + j, val.longValue());

					++j;
				}
			}
		} finally {
			receiver.close();
			sender.close();
		}
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
