package ch.psi.bsread;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;
import org.zeromq.ZMQ;

import ch.psi.bsread.basic.BasicReceiver;
import ch.psi.bsread.compression.Compression;
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

public class ReceiverTest_2 {
	private MainHeader hookMainHeader;
	private boolean hookMainHeaderCalled;
	private DataHeader hookDataHeader;
	private boolean hookDataHeaderCalled;
	private Map<String, Value<Object>> hookValues;
	private boolean hookValuesCalled;
	private Map<String, ChannelConfig> channelConfigs = new HashMap<>();

	@Test
	public void testReceiver() {
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

		int size = 2048;
		Random rand = new Random(0);
		// Register data sources ...
		sender.addSource(new DataChannel<double[]>(new ChannelConfig("ABC", Type.Float64, new int[] { size }, 1, 0, ChannelConfig.DEFAULT_ENCODING, Compression.bitshuffle_lz4)) {
			@Override
			public double[] getValue(long pulseId) {
				double[] val = new double[size];
				for (int i = 0; i < size; ++i) {
					val[i] = rand.nextDouble();
				}

				return val;
			}

			@Override
			public Timestamp getTime(long pulseId) {
				return new Timestamp(pulseId, 0L);
			}
		});
		sender.addSource(new DataChannel<double[]>(new ChannelConfig("ABB", Type.Float64, new int[] { size }, 1, 0, ChannelConfig.DEFAULT_ENCODING, Compression.bitshuffle_lz4)) {
			@Override
			public double[] getValue(long pulseId) {
				double[] val = new double[size];
				for (int i = 0; i < size; ++i) {
					val[i] = rand.nextDouble();
				}

				return val;
			}

			@Override
			public Timestamp getTime(long pulseId) {
				return new Timestamp(pulseId, 0L);
			}
		});
		sender.bind();

		Receiver<Object> receiver = new BasicReceiver();
		// Optional - register callbacks
		receiver.addMainHeaderHandler(header -> setMainHeader(header));
		receiver.addDataHeaderHandler(header -> setDataHeader(header));
		receiver.addValueHandler(values -> setValues(values));
		receiver.connect();

		// We schedule faster as we want to have the testcase execute faster
		ScheduledFuture<?> sendFuture =
				Executors.newScheduledThreadPool(1).scheduleAtFixedRate(() -> sender.send(), 0, 1, TimeUnit.MILLISECONDS);

		// Receive data
		Message<Object> message = null;
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

			assertTrue(hookMainHeader.getPulseId() == i);
			if (hookDataHeaderCalled) {
				assertEquals(hookDataHeader.getChannels().size(), 2);
				ChannelConfig channelConfig = hookDataHeader.getChannels().get(0);
				assertEquals("ABC", channelConfig.getName());
				assertEquals(1, channelConfig.getModulo());
				assertEquals(0, channelConfig.getOffset());
				assertEquals(Type.Float64, channelConfig.getType());
				assertArrayEquals(new int[] { size }, channelConfig.getShape());

				channelConfig = hookDataHeader.getChannels().get(1);
				assertEquals("ABB", channelConfig.getName());
				assertEquals(1, channelConfig.getModulo());
				assertEquals(0, channelConfig.getOffset());
				assertEquals(Type.Float64, channelConfig.getType());
				assertArrayEquals(new int[] { size }, channelConfig.getShape());
			}

			String channelName;
			Value<Object> value;
			double[] javaVal;

			assertEquals(hookValues.size(), 2);
			assertEquals(i, hookMainHeader.getPulseId());

			channelName = "ABC";
			assertTrue(hookValues.containsKey(channelName));
			value = hookValues.get(channelName);
			javaVal = value.getValue(double[].class);
			assertEquals(size, javaVal.length);
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
	public void testReceiver_Push_Pull() throws Exception {
		SenderConfig senderConfig = new SenderConfig(
				new StandardPulseIdProvider(),
				new TimeProvider() {

					@Override
					public Timestamp getTime(long pulseId) {
						return new Timestamp(pulseId, 0L);
					}
				},
				new MatlabByteConverter());
		senderConfig.setSocketType(ZMQ.PUSH);
		Sender sender = new Sender(senderConfig);

		int size = 2048;
		Random rand = new Random(0);
		// Register data sources ...
		sender.addSource(new DataChannel<double[]>(new ChannelConfig("ABC", Type.Float64, new int[] { size }, 1, 0, ChannelConfig.DEFAULT_ENCODING, Compression.bitshuffle_lz4)) {
			@Override
			public double[] getValue(long pulseId) {
				double[] val = new double[size];
				for (int i = 0; i < size; ++i) {
					val[i] = rand.nextDouble();
				}

				return val;
			}

			@Override
			public Timestamp getTime(long pulseId) {
				return new Timestamp(pulseId, 0L);
			}
		});
		sender.addSource(new DataChannel<double[]>(new ChannelConfig("ABB", Type.Float64, new int[] { size }, 1, 0, ChannelConfig.DEFAULT_ENCODING, Compression.bitshuffle_lz4)) {
			@Override
			public double[] getValue(long pulseId) {
				double[] val = new double[size];
				for (int i = 0; i < size; ++i) {
					val[i] = rand.nextDouble();
				}

				return val;
			}

			@Override
			public Timestamp getTime(long pulseId) {
				return new Timestamp(pulseId, 0L);
			}
		});
		sender.bind();

		ReceiverConfig<Object> config1 = new ReceiverConfig<Object>(new StandardMessageExtractor<Object>(new MatlabByteConverter()));
		config1.setSocketType(ZMQ.PULL);
		Receiver<Object> receiver1 = new BasicReceiver(config1);
		AtomicLong mainHeaderCounter1 = new AtomicLong();
		AtomicLong dataHeaderCounter1 = new AtomicLong();
		AtomicLong valCounter1 = new AtomicLong();
		AtomicLong loopCounter1 = new AtomicLong();
		receiver1.addMainHeaderHandler(header -> mainHeaderCounter1.incrementAndGet());
		receiver1.addDataHeaderHandler(header -> dataHeaderCounter1.incrementAndGet());
		receiver1.addValueHandler(values -> valCounter1.incrementAndGet());
		receiver1.connect();

		ReceiverConfig<Object> config2 = new ReceiverConfig<Object>(new StandardMessageExtractor<Object>(new MatlabByteConverter()));
		config2.setSocketType(ZMQ.PULL);
		Receiver<Object> receiver2 = new BasicReceiver(config2);
		AtomicLong mainHeaderCounter2 = new AtomicLong();
		AtomicLong dataHeaderCounter2 = new AtomicLong();
		AtomicLong valCounter2 = new AtomicLong();
		AtomicLong loopCounter2 = new AtomicLong();
		receiver2.addMainHeaderHandler(header -> mainHeaderCounter2.incrementAndGet());
		receiver2.addDataHeaderHandler(header -> dataHeaderCounter2.incrementAndGet());
		receiver2.addValueHandler(values -> valCounter2.incrementAndGet());
		receiver2.connect();

		ExecutorService receiverService = Executors.newFixedThreadPool(2);
		receiverService.execute(() -> {
			try {
				while (receiver1.receive() != null && !Thread.currentThread().isInterrupted()) {
					loopCounter1.incrementAndGet();
				}
			} catch (Throwable t) {
				System.out.println("Receiver1 executor: " + t.getMessage());
			}
		});
		receiverService.execute(() -> {
			try {
				while (receiver2.receive() != null && !Thread.currentThread().isInterrupted()) {
					loopCounter2.incrementAndGet();
				}
			} catch (Throwable t) {
				System.out.println("Receiver2 executor: " + t.getMessage());
			}
		});

		TimeUnit.SECONDS.sleep(1);
		// send/receive data
		int sendCount = 40;
		for (int i = 0; i < sendCount; ++i) {
			sender.send();
			TimeUnit.MILLISECONDS.sleep(1);
		}
		TimeUnit.SECONDS.sleep(1);

		receiverService.shutdown();

		assertEquals(1, dataHeaderCounter1.get());
		assertEquals(sendCount / 2, mainHeaderCounter1.get());
		assertEquals(sendCount / 2, valCounter1.get());
		assertEquals(sendCount / 2, loopCounter1.get());
		receiver1.close();

		assertEquals(1, dataHeaderCounter2.get());
		assertEquals(sendCount / 2, mainHeaderCounter2.get());
		assertEquals(sendCount / 2, valCounter2.get());
		assertEquals(sendCount / 2, loopCounter2.get());
		receiver2.close();

		sender.close();
	}

	@Test
	public void testReceiver_Pub_Sub() throws Exception {
		SenderConfig senderConfig = new SenderConfig(
				new StandardPulseIdProvider(),
				new TimeProvider() {

					@Override
					public Timestamp getTime(long pulseId) {
						return new Timestamp(pulseId, 0L);
					}
				},
				new MatlabByteConverter());
		senderConfig.setSocketType(ZMQ.PUB);
		Sender sender = new Sender(senderConfig);

		int size = 2048;
		Random rand = new Random(0);
		// Register data sources ...
		sender.addSource(new DataChannel<double[]>(new ChannelConfig("ABC", Type.Float64, new int[] { size }, 1, 0, ChannelConfig.DEFAULT_ENCODING, Compression.bitshuffle_lz4)) {
			@Override
			public double[] getValue(long pulseId) {
				double[] val = new double[size];
				for (int i = 0; i < size; ++i) {
					val[i] = rand.nextDouble();
				}

				return val;
			}

			@Override
			public Timestamp getTime(long pulseId) {
				return new Timestamp(pulseId, 0L);
			}
		});
		sender.addSource(new DataChannel<double[]>(new ChannelConfig("ABB", Type.Float64, new int[] { size }, 1, 0, ChannelConfig.DEFAULT_ENCODING, Compression.bitshuffle_lz4)) {
			@Override
			public double[] getValue(long pulseId) {
				double[] val = new double[size];
				for (int i = 0; i < size; ++i) {
					val[i] = rand.nextDouble();
				}

				return val;
			}

			@Override
			public Timestamp getTime(long pulseId) {
				return new Timestamp(pulseId, 0L);
			}
		});
		sender.bind();

		ReceiverConfig<Object> config1 = new ReceiverConfig<Object>(new StandardMessageExtractor<Object>(new MatlabByteConverter()));
		config1.setSocketType(ZMQ.SUB);
		Receiver<Object> receiver1 = new BasicReceiver(config1);
		AtomicLong mainHeaderCounter1 = new AtomicLong();
		AtomicLong dataHeaderCounter1 = new AtomicLong();
		AtomicLong valCounter1 = new AtomicLong();
		AtomicLong loopCounter1 = new AtomicLong();
		receiver1.addMainHeaderHandler(header -> mainHeaderCounter1.incrementAndGet());
		receiver1.addDataHeaderHandler(header -> dataHeaderCounter1.incrementAndGet());
		receiver1.addValueHandler(values -> valCounter1.incrementAndGet());
		receiver1.connect();

		ReceiverConfig<Object> config2 = new ReceiverConfig<Object>(new StandardMessageExtractor<Object>(new MatlabByteConverter()));
		config2.setSocketType(ZMQ.SUB);
		Receiver<Object> receiver2 = new BasicReceiver(config2);
		AtomicLong mainHeaderCounter2 = new AtomicLong();
		AtomicLong dataHeaderCounter2 = new AtomicLong();
		AtomicLong valCounter2 = new AtomicLong();
		AtomicLong loopCounter2 = new AtomicLong();
		receiver2.addMainHeaderHandler(header -> mainHeaderCounter2.incrementAndGet());
		receiver2.addDataHeaderHandler(header -> dataHeaderCounter2.incrementAndGet());
		receiver2.addValueHandler(values -> valCounter2.incrementAndGet());
		receiver2.connect();

		ExecutorService receiverService = Executors.newFixedThreadPool(2);
		receiverService.execute(() -> {
			try {
				while (receiver1.receive() != null && !Thread.currentThread().isInterrupted()) {
					loopCounter1.incrementAndGet();
				}
			} catch (Throwable t) {
				System.out.println("Receiver1 executor: " + t.getMessage());
			}
		});
		receiverService.execute(() -> {
			try {
				while (receiver2.receive() != null && !Thread.currentThread().isInterrupted()) {
					loopCounter2.incrementAndGet();
				}
			} catch (Throwable t) {
				System.out.println("Receiver2 executor: " + t.getMessage());
			}
		});

		TimeUnit.SECONDS.sleep(1);
		// send/receive data
		int sendCount = 40;
		for (int i = 0; i < sendCount; ++i) {
			sender.send();
			TimeUnit.MILLISECONDS.sleep(1);
		}
		TimeUnit.SECONDS.sleep(1);

		receiverService.shutdown();

		assertEquals(1, dataHeaderCounter1.get());
		assertEquals(sendCount, mainHeaderCounter1.get());
		assertEquals(sendCount, valCounter1.get());
		assertEquals(sendCount, loopCounter1.get());
		receiver1.close();

		assertEquals(1, dataHeaderCounter2.get());
		assertEquals(sendCount, mainHeaderCounter2.get());
		assertEquals(sendCount, valCounter2.get());
		assertEquals(sendCount, loopCounter2.get());
		receiver2.close();

		sender.close();
	}

	private static final int nrOfReceivers = 15;//2 * Runtime.getRuntime().availableProcessors();

//	@Test
	public void testManyReceivers_Push_Pull() throws InterruptedException {
		ByteConverter byteConverter = new MatlabByteConverter();
		SenderConfig senderConfig = new SenderConfig(
				new StandardPulseIdProvider(),
				new TimeProvider() {

					@Override
					public Timestamp getTime(long pulseId) {
						return new Timestamp(pulseId, 0);
					}

				},
				byteConverter);
		senderConfig.setSocketType(ZMQ.PUSH);

		Sender sender = new Sender(senderConfig);

		// Register data sources ...
		sender.addSource(new DataChannel<Long>(new ChannelConfig("ABC", Type.Int64, new int[] { 1 }, 1, 0,
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
		sender.addSource(new DataChannel<Long>(new ChannelConfig("ABCD", Type.Int64, new int[] { 1 }, 1, 0,
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

		AtomicBoolean sending = new AtomicBoolean(true);
		AtomicBoolean disconnected = new AtomicBoolean(false);
		AtomicLong receiveCounter = new AtomicLong();
		AtomicLong sendCounter = new AtomicLong();
		AtomicLong disconnectedSendCounter = new AtomicLong();
		ExecutorService receiverExecutor = Executors.newFixedThreadPool(nrOfReceivers);
		List<Receiver<Object>> receivers = new ArrayList<>(nrOfReceivers);
		for (int i = 0; i < nrOfReceivers; ++i) {
			final int j = i;
			System.out.println("Create Receiver " + j);
			ReceiverConfig<Object> config = new ReceiverConfig<Object>(new StandardMessageExtractor<Object>(new MatlabByteConverter()));
			config.setSocketType(ZMQ.PULL);
			Receiver<Object> receiver = new BasicReceiver(config);
			receivers.add(receiver);
			receiver.connect();

			receiverExecutor.execute(() -> {
				while (!disconnected.get())
					try {
						receiver.receive();
						if (!disconnected.get()) {
							System.out.println("Receiver " + j + " receives");
							receiveCounter.incrementAndGet();
						}
					} catch (Exception e) {
						System.out.println("Receiver " + j + " " + e.getMessage());
					}
			});
		}

		// We schedule faster as we want to have the testcase execute faster
		ScheduledExecutorService scheduledExecutor = Executors.newScheduledThreadPool(1);
		ScheduledFuture<?> sendFuture =
				scheduledExecutor.scheduleAtFixedRate(() -> {
					if (sending.get()) {
						sender.send();
						if (!disconnected.get()) {
							sendCounter.incrementAndGet();
						} else {
							disconnectedSendCounter.incrementAndGet();
						}
					}
				}, 0, 100, TimeUnit.MILLISECONDS);

		// Receive data
		TimeUnit.SECONDS.sleep(10);
		sending.set(false);
		TimeUnit.SECONDS.sleep(1);
		disconnected.set(true);
		int counter = 0;
		for (Receiver<Object> receiver : receivers) {
			System.out.println("Close receiver "+(counter++));
			receiver.close();
		}
		sending.set(true);
		TimeUnit.SECONDS.sleep(10);
		receiverExecutor.shutdown();
		TimeUnit.SECONDS.sleep(1);
		sendFuture.cancel(true);
		scheduledExecutor.shutdown();
		sender.close();

		assertEquals(sendCounter.get(), receiveCounter.get());
		assertTrue(disconnectedSendCounter.get() > 0);
	}
	
//	@Test
	public void testManyReceivers_Pub_Sub() throws InterruptedException {
		ByteConverter byteConverter = new MatlabByteConverter();
		SenderConfig senderConfig = new SenderConfig(
				new StandardPulseIdProvider(),
				new TimeProvider() {

					@Override
					public Timestamp getTime(long pulseId) {
						return new Timestamp(pulseId, 0);
					}

				},
				byteConverter);
		senderConfig.setSocketType(ZMQ.PUB);

		Sender sender = new Sender(senderConfig);

		// Register data sources ...
		sender.addSource(new DataChannel<Long>(new ChannelConfig("ABC", Type.Int64, new int[] { 1 }, 1, 0,
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
		sender.addSource(new DataChannel<Long>(new ChannelConfig("ABCD", Type.Int64, new int[] { 1 }, 1, 0,
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

		AtomicBoolean sending = new AtomicBoolean(true);
		AtomicBoolean disconnected = new AtomicBoolean(false);
		AtomicLong receiveCounter = new AtomicLong();
		AtomicLong sendCounter = new AtomicLong();
		AtomicLong disconnectedSendCounter = new AtomicLong();
		ExecutorService receiverExecutor = Executors.newFixedThreadPool(nrOfReceivers);
		List<Receiver<Object>> receivers = new ArrayList<>(nrOfReceivers);
		for (int i = 0; i < nrOfReceivers; ++i) {
			final int j = i;
			System.out.println("Create Receiver " + j);
			ReceiverConfig<Object> config = new ReceiverConfig<Object>(new StandardMessageExtractor<Object>(new MatlabByteConverter()));
			config.setSocketType(ZMQ.SUB);
			Receiver<Object> receiver = new BasicReceiver(config);
			receivers.add(receiver);
			receiver.connect();

			receiverExecutor.execute(() -> {
				while (!disconnected.get())
					try {
						receiver.receive();
						if (!disconnected.get()) {
							System.out.println("Receiver " + j + " receives");
							receiveCounter.incrementAndGet();
						}
					} catch (Exception e) {
						System.out.println("Receiver " + j + " " + e.getMessage());
					}
			});
		}

		// We schedule faster as we want to have the testcase execute faster
		ScheduledExecutorService scheduledExecutor = Executors.newScheduledThreadPool(1);
		ScheduledFuture<?> sendFuture =
				scheduledExecutor.scheduleAtFixedRate(() -> {
					if (sending.get()) {
						sender.send();
						if (!disconnected.get()) {
							sendCounter.incrementAndGet();
						} else {
							disconnectedSendCounter.incrementAndGet();
						}
					}
				}, 0, 100, TimeUnit.MILLISECONDS);

		// Receive data
		TimeUnit.SECONDS.sleep(10);
		sending.set(false);
		TimeUnit.SECONDS.sleep(1);
		disconnected.set(true);
		int counter = 0;
		for (Receiver<Object> receiver : receivers) {
			System.out.println("Close receiver "+(counter++));
			receiver.close();
		}
		sending.set(true);
		TimeUnit.SECONDS.sleep(10);
		receiverExecutor.shutdown();
		TimeUnit.SECONDS.sleep(1);
		sendFuture.cancel(true);
		scheduledExecutor.shutdown();
		sender.close();

		assertEquals(sendCounter.get() * nrOfReceivers, receiveCounter.get());
		assertTrue(disconnectedSendCounter.get() > 0);
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

	public void setValues(Map<String, Value<Object>> values) {
		this.hookValues = values;
		this.hookValuesCalled = true;
	}
}
