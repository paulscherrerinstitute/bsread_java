package ch.psi.bsread;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Array;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;
import java.util.stream.IntStream;

import org.junit.Test;

import ch.psi.bsread.common.allocator.ByteBufferAllocator;
import ch.psi.bsread.compression.Compression;
import ch.psi.bsread.compression.Compressor;
import ch.psi.bsread.converter.AbstractByteConverter;
import ch.psi.bsread.converter.MatlabByteConverter;
import ch.psi.bsread.converter.ValueConverter;
import ch.psi.bsread.impl.StandardMessageExtractor;
import ch.psi.bsread.impl.StandardPulseIdProvider;
import ch.psi.bsread.message.ChannelConfig;
import ch.psi.bsread.message.DataHeader;
import ch.psi.bsread.message.MainHeader;
import ch.psi.bsread.message.Message;
import ch.psi.bsread.message.Timestamp;
import ch.psi.bsread.message.Type;
import ch.psi.bsread.message.Value;

public class CompressionTest {
	private MainHeader hookMainHeader;
	private boolean hookMainHeaderCalled;
	private DataHeader hookDataHeader;
	private boolean hookDataHeaderCalled;
	private Map<String, Value<ByteBuffer>> hookValues;
	private boolean hookValuesCalled;
	private Map<String, ChannelConfig> channelConfigs = new HashMap<>();
	private MatlabByteConverter byteConverter = new MatlabByteConverter();

	protected <V> Receiver<V> getReceiver() {
		return new Receiver<V>(new ReceiverConfig<V>(new StandardMessageExtractor<V>(new MatlabByteConverter())));
	}

	@Test
	public void testDataHeaderCompressionOneChannel10Hz() throws InterruptedException {
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
						new MatlabByteConverter(),
						Compression.lz4)
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
		TimeUnit.MILLISECONDS.sleep(100);

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
				ChannelConfig channelConfig = hookDataHeader.getChannels().iterator().next();
				assertEquals("ABC", channelConfig.getName());
				assertEquals(10, channelConfig.getModulo());
				assertEquals(0, channelConfig.getOffset());
				assertEquals(Type.Float64, channelConfig.getType());
				assertArrayEquals(new int[] { 1 }, channelConfig.getShape());
			}

			String channelName;
			Value<ByteBuffer> value;
			Double javaVal;

			assertEquals(hookValues.size(), 1);
			assertEquals(i * 10, hookMainHeader.getPulseId());

			channelName = "ABC";
			assertTrue(hookValues.containsKey(channelName));
			value = hookValues.get(channelName);
			javaVal = value.getValue(Double.class);
			assertEquals(Double.valueOf(hookMainHeader.getPulseId()), javaVal, 0.00000000001);
			assertEquals(hookMainHeader.getPulseId(), value.getTimestamp().getSec());
			assertEquals(0, value.getTimestamp().getNs());
			assertEquals(hookMainHeader.getPulseId(), hookMainHeader.getGlobalTimestamp().getSec());
			assertEquals(0, hookMainHeader.getGlobalTimestamp().getNs());
		}

		sendFuture.cancel(true);
		receiver.close();
		sender.close();
	}

	@Test
	public void testDataCompressionOneChannel10Hz() throws InterruptedException {
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
		sender.addSource(new DataChannel<Double>(new ChannelConfig("ABC", Type.Float64, new int[] { 1 }, 10, 0,
				ChannelConfig.DEFAULT_ENCODING, Compression.lz4)) {
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
		TimeUnit.MILLISECONDS.sleep(100);

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
				ChannelConfig channelConfig = hookDataHeader.getChannels().iterator().next();
				assertEquals("ABC", channelConfig.getName());
				assertEquals(10, channelConfig.getModulo());
				assertEquals(0, channelConfig.getOffset());
				assertEquals(Type.Float64, channelConfig.getType());
				assertArrayEquals(new int[] { 1 }, channelConfig.getShape());
			}

			String channelName;
			Value<ByteBuffer> value;
			Double javaVal;

			assertEquals(hookValues.size(), 1);
			assertEquals(i * 10, hookMainHeader.getPulseId());

			channelName = "ABC";
			assertTrue(hookValues.containsKey(channelName));
			value = hookValues.get(channelName);
			javaVal = value.getValue(Double.class);
			assertEquals(Double.valueOf(hookMainHeader.getPulseId()), javaVal, 0.00000000001);
			assertEquals(hookMainHeader.getPulseId(), value.getTimestamp().getSec());
			assertEquals(0, value.getTimestamp().getNs());
			assertEquals(hookMainHeader.getPulseId(), hookMainHeader.getGlobalTimestamp().getSec());
			assertEquals(0, hookMainHeader.getGlobalTimestamp().getNs());
		}

		sendFuture.cancel(true);
		receiver.close();
		sender.close();
	}

	@Test
	public void testDataHeaderCompressionTwoChannel100HzAnd10Hz() throws InterruptedException {
		ByteOrder[] byteOrders = new ByteOrder[] { ByteOrder.LITTLE_ENDIAN, ByteOrder.BIG_ENDIAN };
		for (ByteOrder byteOrder : byteOrders) {
			for (Compression compression : Compression.values()) {
				testDataHeaderCompressionTwoChannel100HzAnd10Hz(byteOrder, compression);
			}
		}
	}

	protected void testDataHeaderCompressionTwoChannel100HzAnd10Hz(ByteOrder byteOrder, Compression compression) throws InterruptedException {
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
						new MatlabByteConverter(),
						compression)
				);

		// Register data sources ...
		sender.addSource(new DataChannel<Double>(new ChannelConfig("ABC_10", Type.Float64, new int[] { 1 }, 10, 0,
				ChannelConfig.getEncoding(byteOrder))) {
			@Override
			public Double getValue(long pulseId) {
				return (double) pulseId;
			}

			@Override
			public Timestamp getTime(long pulseId) {
				return new Timestamp(pulseId, 0L);
			}
		});
		sender.addSource(new DataChannel<Double>(new ChannelConfig("ABC_100", Type.Float64, new int[] { 1 }, 1, 0,
				ChannelConfig.getEncoding(byteOrder))) {
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
		TimeUnit.MILLISECONDS.sleep(100);

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
			Value<ByteBuffer> value;
			Double javaVal;
			if (hookMainHeader.getPulseId() % 10 == 0) {
				assertEquals(2, hookValues.size());
				assertEquals(i, hookMainHeader.getPulseId());

				channelName = "ABC_10";
				assertTrue(hookValues.containsKey(channelName));
				value = hookValues.get(channelName);
				javaVal = value.getValue(Double.class);
				assertEquals(Double.valueOf(hookMainHeader.getPulseId()), javaVal, 0.00000000001);
				assertEquals(hookMainHeader.getPulseId(), value.getTimestamp().getSec());
				assertEquals(0, value.getTimestamp().getNs());
				assertEquals(hookMainHeader.getPulseId(), hookMainHeader.getGlobalTimestamp().getSec());
				assertEquals(0, hookMainHeader.getGlobalTimestamp().getNs());

				channelName = "ABC_100";
				assertTrue(hookValues.containsKey(channelName));
				assertEquals(i, hookMainHeader.getPulseId());
				value = hookValues.get(channelName);
				javaVal = value.getValue(Double.class);
				assertEquals(Double.valueOf(hookMainHeader.getPulseId()), javaVal, 0.00000000001);
				assertEquals(hookMainHeader.getPulseId(), value.getTimestamp().getSec());
				assertEquals(0, value.getTimestamp().getNs());
				assertEquals(hookMainHeader.getPulseId(), hookMainHeader.getGlobalTimestamp().getSec());
				assertEquals(0, hookMainHeader.getGlobalTimestamp().getNs());
			} else {
				assertEquals(1, hookValues.size());
				assertEquals(i, hookMainHeader.getPulseId());

				channelName = "ABC_100";
				assertTrue(hookValues.containsKey(channelName));
				value = hookValues.get(channelName);
				javaVal = value.getValue(Double.class);
				assertEquals(Double.valueOf(hookMainHeader.getPulseId()), javaVal, 0.00000000001);
				assertEquals(hookMainHeader.getPulseId(), value.getTimestamp().getSec());
				assertEquals(0, value.getTimestamp().getNs());
				assertEquals(hookMainHeader.getPulseId(), hookMainHeader.getGlobalTimestamp().getSec());
				assertEquals(0, hookMainHeader.getGlobalTimestamp().getNs());
			}
		}

		sendFuture.cancel(true);
		receiver.close();
		sender.close();
	}

	@Test
	public void testDataCompressionTwoChannel100HzAnd10Hz() throws InterruptedException {
		ByteOrder[] byteOrders = new ByteOrder[] { ByteOrder.LITTLE_ENDIAN, ByteOrder.BIG_ENDIAN };
		for (ByteOrder byteOrder : byteOrders) {
			for (Compression compression : Compression.values()) {
				testDataCompressionTwoChannel100HzAnd10Hz(byteOrder, compression);
			}
		}
	}

	protected void testDataCompressionTwoChannel100HzAnd10Hz(ByteOrder byteOrder, Compression compression) throws InterruptedException {
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
		sender.addSource(new DataChannel<Double>(new ChannelConfig("ABC_10", Type.Float64, new int[] { 1 }, 10, 0,
				ChannelConfig.getEncoding(byteOrder), compression)) {
			@Override
			public Double getValue(long pulseId) {
				return (double) pulseId;
			}

			@Override
			public Timestamp getTime(long pulseId) {
				return new Timestamp(pulseId, 0L);
			}
		});
		sender.addSource(new DataChannel<Double>(new ChannelConfig("ABC_100", Type.Float64, new int[] { 1 }, 1, 0,
				ChannelConfig.getEncoding(byteOrder), compression)) {
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
		TimeUnit.MILLISECONDS.sleep(100);

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
			Value<ByteBuffer> value;
			Double javaVal;
			if (hookMainHeader.getPulseId() % 10 == 0) {
				assertEquals(2, hookValues.size());
				assertEquals(i, hookMainHeader.getPulseId());

				channelName = "ABC_10";
				assertTrue(hookValues.containsKey(channelName));
				value = hookValues.get(channelName);
				javaVal = value.getValue(Double.class);
				assertEquals(Double.valueOf(hookMainHeader.getPulseId()), javaVal, 0.00000000001);
				assertEquals(hookMainHeader.getPulseId(), value.getTimestamp().getSec());
				assertEquals(0, value.getTimestamp().getNs());
				assertEquals(hookMainHeader.getPulseId(), hookMainHeader.getGlobalTimestamp().getSec());
				assertEquals(0, hookMainHeader.getGlobalTimestamp().getNs());

				channelName = "ABC_100";
				assertTrue(hookValues.containsKey(channelName));
				assertEquals(i, hookMainHeader.getPulseId());
				value = hookValues.get(channelName);
				javaVal = value.getValue(Double.class);
				assertEquals(Double.valueOf(hookMainHeader.getPulseId()), javaVal, 0.00000000001);
				assertEquals(hookMainHeader.getPulseId(), value.getTimestamp().getSec());
				assertEquals(0, value.getTimestamp().getNs());
				assertEquals(hookMainHeader.getPulseId(), hookMainHeader.getGlobalTimestamp().getSec());
				assertEquals(0, hookMainHeader.getGlobalTimestamp().getNs());
			} else {
				assertEquals(1, hookValues.size());
				assertEquals(i, hookMainHeader.getPulseId());

				channelName = "ABC_100";
				assertTrue(hookValues.containsKey(channelName));
				value = hookValues.get(channelName);
				javaVal = value.getValue(Double.class);
				assertEquals(Double.valueOf(hookMainHeader.getPulseId()), javaVal, 0.00000000001);
				assertEquals(hookMainHeader.getPulseId(), value.getTimestamp().getSec());
				assertEquals(0, value.getTimestamp().getNs());
				assertEquals(hookMainHeader.getPulseId(), hookMainHeader.getGlobalTimestamp().getSec());
				assertEquals(0, hookMainHeader.getGlobalTimestamp().getNs());
			}
		}

		sendFuture.cancel(true);
		receiver.close();
		sender.close();
	}

	@Test
	public void testDataCompressionTwoArrayChannel100HzAnd10Hz() throws InterruptedException {
		ByteOrder[] byteOrders = new ByteOrder[] { ByteOrder.LITTLE_ENDIAN, ByteOrder.BIG_ENDIAN };
		for (ByteOrder byteOrder : byteOrders) {
			for (Compression compression : Compression.values()) {
				testDataCompressionTwoArrayChannel100HzAnd10Hz(byteOrder, compression);
			}
		}
	}

	protected void testDataCompressionTwoArrayChannel100HzAnd10Hz(ByteOrder byteOrder, Compression compression) throws InterruptedException {
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
		sender.addSource(new DataChannel<double[]>(new ChannelConfig("ABC_10", Type.Float64, new int[] { 3 }, 10, 0,
				ChannelConfig.getEncoding(byteOrder), compression)) {
			@Override
			public double[] getValue(long pulseId) {
				return new double[] { (double) pulseId, 0, (double) pulseId - 1 };
			}

			@Override
			public Timestamp getTime(long pulseId) {
				return new Timestamp(pulseId, 0L);
			}
		});
		sender.addSource(new DataChannel<double[]>(new ChannelConfig("ABC_100", Type.Float64, new int[] { 3 }, 1, 0,
				ChannelConfig.getEncoding(byteOrder), compression)) {
			@Override
			public double[] getValue(long pulseId) {
				return new double[] { (double) pulseId + 1, 0, (double) pulseId };
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
		TimeUnit.MILLISECONDS.sleep(100);

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
				Iterator<ChannelConfig> configIter = hookDataHeader.getChannels().iterator();
				ChannelConfig channelConfig = configIter.next();
				assertEquals("ABC_10", channelConfig.getName());
				assertEquals(10, channelConfig.getModulo());
				assertEquals(0, channelConfig.getOffset());
				assertEquals(Type.Float64, channelConfig.getType());
				assertArrayEquals(new int[] { 3 }, channelConfig.getShape());

				channelConfig = configIter.next();
				assertEquals("ABC_100", channelConfig.getName());
				assertEquals(1, channelConfig.getModulo());
				assertEquals(0, channelConfig.getOffset());
				assertEquals(Type.Float64, channelConfig.getType());
				assertArrayEquals(new int[] { 3 }, channelConfig.getShape());
			}

			// 10Hz -> both channels should have values
			String channelName;
			Value<ByteBuffer> value;
			double[] javaVal;
			if (hookMainHeader.getPulseId() % 10 == 0) {
				assertEquals(2, hookValues.size());
				assertEquals(i, hookMainHeader.getPulseId());

				channelName = "ABC_10";
				assertTrue(hookValues.containsKey(channelName));
				value = hookValues.get(channelName);
				javaVal = value.getValue(double[].class);
				assertArrayEquals(
						new double[] { (double) hookMainHeader.getPulseId(), 0, (double) hookMainHeader.getPulseId() - 1 },
						javaVal, 0.00000000001);
				assertEquals(hookMainHeader.getPulseId(), value.getTimestamp().getSec());
				assertEquals(0, value.getTimestamp().getNs());
				assertEquals(hookMainHeader.getPulseId(), hookMainHeader.getGlobalTimestamp().getSec());
				assertEquals(0, hookMainHeader.getGlobalTimestamp().getNs());

				channelName = "ABC_100";
				assertTrue(hookValues.containsKey(channelName));
				assertEquals(i, hookMainHeader.getPulseId());
				value = hookValues.get(channelName);
				javaVal = value.getValue(double[].class);
				assertArrayEquals(
						new double[] { (double) hookMainHeader.getPulseId() + 1, 0, (double) hookMainHeader.getPulseId() },
						javaVal, 0.00000000001);
				assertEquals(hookMainHeader.getPulseId(), value.getTimestamp().getSec());
				assertEquals(0, value.getTimestamp().getNs());
				assertEquals(hookMainHeader.getPulseId(), hookMainHeader.getGlobalTimestamp().getSec());
				assertEquals(0, hookMainHeader.getGlobalTimestamp().getNs());
			} else {
				assertEquals(1, hookValues.size());
				assertEquals(i, hookMainHeader.getPulseId());

				channelName = "ABC_100";
				assertTrue(hookValues.containsKey(channelName));
				value = hookValues.get(channelName);
				javaVal = value.getValue(double[].class);
				assertArrayEquals(
						new double[] { (double) hookMainHeader.getPulseId() + 1, 0, (double) hookMainHeader.getPulseId() },
						javaVal, 0.00000000001);
				assertEquals(hookMainHeader.getPulseId(), value.getTimestamp().getSec());
				assertEquals(0, value.getTimestamp().getNs());
				assertEquals(hookMainHeader.getPulseId(), hookMainHeader.getGlobalTimestamp().getSec());
				assertEquals(0, hookMainHeader.getGlobalTimestamp().getNs());
			}
		}

		sendFuture.cancel(true);
		receiver.close();
		sender.close();
	}

	@Test
	public void testAllCompressors_Scalar() {
		testAllCompressors(1);
	}

	@Test
	public void testAllCompressors_Array() {
		testAllCompressors(2048);
	}

	protected void testAllCompressors(int elements) {
		IntFunction<ByteBuffer> allocator = new ByteBufferAllocator();

		int offset;
		int position;
		int limit;
		int oldSize;
		ByteOrder[] orders = new ByteOrder[] { ByteOrder.LITTLE_ENDIAN, ByteOrder.BIG_ENDIAN };
		Type[] types = Type.values();
		Compression[] compressions = Compression.values();
		int runs = 20;
		int warmup = 5;
		long startTime;

		for (Compression compression : compressions) {
			for (ByteOrder byteOrder : orders) {
				for (Type type : types) {
					ChannelConfig channelConfig =
							new ChannelConfig(type.getKey() + "CompressionTest", type, new int[] { elements }, 1, 0,
									ChannelConfig.getEncoding(byteOrder), compression);
					ChannelConfig decompressedChannelConfig = new ChannelConfig(channelConfig);
					decompressedChannelConfig.setCompression(Compression.none);
					Random rand = new Random(0);

					long totRunTime = 0;
					long totBytes = 0;
					long totCompBytes = 0;

					for (int i = 0; i < runs; ++i) {

						Object value = getInitialValue(rand, channelConfig);

						ByteBuffer valueBytes = byteConverter.getBytes(value, type, byteOrder, allocator);

						offset = 0;
						Compressor compressor = compression.getCompressor();

						position = valueBytes.position();
						limit = valueBytes.limit();

						startTime = System.nanoTime();
						ByteBuffer compressed =
								compressor.compressData(valueBytes, valueBytes.position(), valueBytes.remaining(), offset,
										allocator, type.getBytes());
						if (i > warmup) {
							totRunTime += System.nanoTime() - startTime;
							totBytes += valueBytes.remaining();
							totCompBytes += compressed.remaining();
						}
						assertEquals(byteOrder, compressed.order());

						// compressors must not modify original ByteBuffers
						assertEquals(position, valueBytes.position());
						assertEquals(limit, valueBytes.limit());

						int decompressedSize = compressor.getDecompressedDataSize(compressed, offset);
						if (decompressedSize != Compressor.DOES_NOT_PROVIDE_UNCOMPRESSED_SIZE) {
							if (type.getBytes() != ValueConverter.DYNAMIC_NUMBER_OF_BYTES) {
								assertEquals(elements * type.getBytes(), decompressedSize);
							}
						}

						position = compressed.position();
						limit = compressed.limit();

						ByteBuffer decompressed =
								compressor.decompressData(compressed, offset, allocator, type.getBytes());
						assertEquals(byteOrder, decompressed.order());

						// compressors must not modify original ByteBuffers
						assertEquals(position, compressed.position());
						assertEquals(limit, compressed.limit());

						Object newValue = byteConverter.getValue(decompressed, decompressedChannelConfig, null, null);
						assertObjectArrayEquals(value, newValue, compression, byteOrder, type);

						position = valueBytes.position();
						limit = valueBytes.limit();
						offset = 7;
						oldSize = compressed.remaining();
						compressed =
								compressor.compressData(valueBytes, valueBytes.position(), valueBytes.remaining(), offset,
										allocator, type.getBytes());
						assertEquals(byteOrder, compressed.order());

						// compressors must not modify original ByteBuffers
						assertEquals(position, valueBytes.position());
						assertEquals(limit, valueBytes.limit());

						assertEquals(oldSize + offset, compressed.remaining());
						decompressedSize = compressor.getDecompressedDataSize(compressed, offset);
						if (decompressedSize != Compressor.DOES_NOT_PROVIDE_UNCOMPRESSED_SIZE) {
							if (type.getBytes() != ValueConverter.DYNAMIC_NUMBER_OF_BYTES) {
								assertEquals(elements * type.getBytes(), decompressedSize);
							}
						}

						position = compressed.position();
						limit = compressed.limit();

						decompressed = compressor.decompressData(compressed, offset, allocator, type.getBytes());
						assertEquals(byteOrder, decompressed.order());

						// compressors must not modify original ByteBuffers
						assertEquals(position, compressed.position());
						assertEquals(limit, compressed.limit());

						newValue = byteConverter.getValue(decompressed, decompressedChannelConfig, null, null);
						assertObjectArrayEquals(value, newValue, compression, byteOrder, type);
					}

					// System.out.println(compression + " " +
					// channelConfig.getType() + " "
					// + Arrays.toString(channelConfig.getShape()) + " "
					// + channelConfig.getEncoding());
					// System.out.println("\tTime: " + totRunTime + " avg: " +
					// (totRunTime / (runs - warmup)));
					// System.out.println("\tCompression: " + ((double)
					// totCompBytes / totBytes));
				}
			}
		}
	}

	private void assertObjectArrayEquals(Object value, Object newValue, Compression compression, ByteOrder byteOrder,
			Type type) {
		if (value.getClass().isArray()) {
			int length = Array.getLength(value);
			assertEquals(length, Array.getLength(newValue));
			assertEquals(value.getClass().getComponentType(), newValue.getClass().getComponentType());

			for (int i = 0; i < length; ++i) {
				assertEquals("Test " + compression + " " + byteOrder + " " + type, Array.get(value, i),
						Array.get(newValue, i));
			}
		} else {
			assertEquals(value, newValue);
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

	private Object getInitialValue(Random rand, ChannelConfig channelConfig) {
		int arrayLength = AbstractByteConverter.getArrayLength(channelConfig.getShape());
		boolean isArray = AbstractByteConverter.isArray(channelConfig.getShape());

		switch (channelConfig.getType()) {
		case Bool:
			if (isArray) {
				boolean[] vals = new boolean[arrayLength];
				IntStream.range(0, arrayLength)
						.forEach(i -> vals[i] = rand.nextBoolean());
				return vals;
			}
			else {
				return rand.nextBoolean();
			}
		case Int8:
			if (isArray) {
				byte[] vals = new byte[arrayLength];
				rand.nextBytes(vals);
				return vals;
			}
			else {
				byte[] vals = new byte[1];
				rand.nextBytes(vals);
				return vals[0];
			}
		case UInt8:
			if (isArray) {
				short[] vals = new short[arrayLength];
				IntStream.range(0, arrayLength)
						.forEach(i -> vals[i] = (short) (((byte) rand.nextInt()) & 0xff));
				return vals;
			}
			else {
				return (short) (((byte) rand.nextInt()) & 0xff);
			}
		case Int16:
			if (isArray) {
				short[] vals = new short[arrayLength];
				IntStream.range(0, arrayLength)
						.forEach(i -> vals[i] = (short) rand.nextInt());
				return vals;
			}
			else {
				return (short) rand.nextInt();
			}
		case UInt16:
			if (isArray) {
				int[] vals = new int[arrayLength];
				IntStream.range(0, arrayLength)
						.forEach(i -> vals[i] = ((short) rand.nextInt()) & 0xffff);
				return vals;
			}
			else {
				return ((short) rand.nextInt()) & 0xffff;
			}
		case Int32:
			if (isArray) {
				return rand.ints(arrayLength).toArray();
			}
			else {
				return rand.nextInt();
			}
		case UInt32:
			if (isArray) {
				long[] vals = new long[arrayLength];
				IntStream.range(0, arrayLength)
						.forEach(i -> vals[i] = rand.nextInt() & 0xffffffffL);
				return vals;
			}
			else {
				return rand.nextInt() & 0xffffffffL;
			}
		case Int64:
			if (isArray) {
				return rand.longs(arrayLength).toArray();
			}
			else {
				return rand.nextLong();
			}
		case UInt64:
			if (isArray) {
				BigInteger[] vals = new BigInteger[arrayLength];
				IntStream.range(0, arrayLength)
						.forEach(i -> {
							long value = rand.nextLong();
							BigInteger bigInt = BigInteger.valueOf(value & 0x7fffffffffffffffL);
							if (value < 0) {
								bigInt = bigInt.setBit(Long.SIZE - 1);
							}
							vals[i] = bigInt;
						});

				return vals;
			}
			else {
				long value = rand.nextLong();
				BigInteger bigInt = BigInteger.valueOf(value & 0x7fffffffffffffffL);
				if (value < 0) {
					bigInt = bigInt.setBit(Long.SIZE - 1);
				}
				return bigInt;
			}
		case Float32:
			if (isArray) {
				float[] vals = new float[arrayLength];
				IntStream.range(0, arrayLength)
						.forEach(i -> vals[i] = rand.nextFloat());
				return vals;
			}
			else {
				return rand.nextFloat();
			}
		case Float64:
			if (isArray) {
				return rand.doubles(arrayLength).toArray();
			}
			else {
				return rand.nextDouble();
			}
		case String:
			if (isArray) {
				StringBuilder buf = new StringBuilder();
				for (int i = 0; i < arrayLength; ++i) {
					buf.append(rand.nextInt());
				}
				return buf.toString();
			} else {
				return "" + rand.nextInt();
			}
		default:
			throw new RuntimeException("Type " + channelConfig.getType() + " not supported");
		}
	}
}
