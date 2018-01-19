package ch.psi.bsread.monitors;

import static org.junit.Assert.assertEquals;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;
import org.zeromq.ZMQ;

import ch.psi.bsread.DataChannel;
import ch.psi.bsread.Receiver;
import ch.psi.bsread.ReceiverConfig;
import ch.psi.bsread.ScheduledSender;
import ch.psi.bsread.SenderConfig;
import ch.psi.bsread.TimeProvider;
import ch.psi.bsread.basic.BasicReceiver;
import ch.psi.bsread.compression.Compression;
import ch.psi.bsread.converter.MatlabByteConverter;
import ch.psi.bsread.impl.StandardMessageExtractor;
import ch.psi.bsread.impl.StandardPulseIdProvider;
import ch.psi.bsread.message.ChannelConfig;
import ch.psi.bsread.message.Timestamp;
import ch.psi.bsread.message.Type;

public class ConnectionCounterMonitorTest {
	private long waitTime = 100;

	@Test
	public void testReceiver_Push_Pull() throws Exception {
		SenderConfig senderConfig = new SenderConfig(
				SenderConfig.DEFAULT_ADDRESS,
				new StandardPulseIdProvider(),
				new TimeProvider() {

					@Override
					public Timestamp getTime(long pulseId) {
						return new Timestamp(pulseId, 0L);
					}
				},
				new MatlabByteConverter());
		senderConfig.setSocketType(ZMQ.PUSH);
		ConnectionCounterMonitor connectionMonitor = new ConnectionCounterMonitor();
		AtomicInteger connectionCounter = new AtomicInteger();
		connectionMonitor.addHandler((count) -> connectionCounter.set(count));
		senderConfig.setMonitor(connectionMonitor);

		ScheduledSender sender = new ScheduledSender(senderConfig);

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

		try {
			assertEquals(0, connectionMonitor.getConnectionCount());
			assertEquals(0, connectionCounter.get());

			sender.connect();
			TimeUnit.MILLISECONDS.sleep(waitTime);
			assertEquals(0, connectionMonitor.getConnectionCount());
			assertEquals(0, connectionCounter.get());

			receiver1.connect();
			TimeUnit.MILLISECONDS.sleep(waitTime);
			assertEquals(1, connectionMonitor.getConnectionCount());
			assertEquals(1, connectionCounter.get());

			receiver2.connect();
			TimeUnit.MILLISECONDS.sleep(waitTime);
			assertEquals(2, connectionMonitor.getConnectionCount());
			assertEquals(2, connectionCounter.get());

			ExecutorService receiverService = Executors.newFixedThreadPool(2);
			receiverService.execute(() -> {
				try {
					while (receiver1.receive() != null) {
						loopCounter1.incrementAndGet();
					}
				} catch (Throwable t) {
					System.out.println("Receiver1 executor: " + t.getMessage());
				}
			});
			receiverService.execute(() -> {
				try {
					while (receiver2.receive() != null) {
						loopCounter2.incrementAndGet();
					}
				} catch (Throwable t) {
					System.out.println("Receiver2 executor: " + t.getMessage());
				}
			});

			TimeUnit.MILLISECONDS.sleep(waitTime);
			// send/receive data
			int sendCount = 40;
			for (int i = 0; i < sendCount; ++i) {
				sender.send();
				TimeUnit.MILLISECONDS.sleep(1);
			}
			TimeUnit.MILLISECONDS.sleep(waitTime);

			assertEquals(1, dataHeaderCounter1.get());
			assertEquals(sendCount / 2, mainHeaderCounter1.get());
			assertEquals(sendCount / 2, valCounter1.get());
			assertEquals(sendCount / 2, loopCounter1.get());
			receiver1.close();
			TimeUnit.MILLISECONDS.sleep(waitTime);
			assertEquals(1, connectionMonitor.getConnectionCount());
			assertEquals(1, connectionCounter.get());

			assertEquals(1, dataHeaderCounter2.get());
			assertEquals(sendCount / 2, mainHeaderCounter2.get());
			assertEquals(sendCount / 2, valCounter2.get());
			assertEquals(sendCount / 2, loopCounter2.get());
			receiver2.close();
			TimeUnit.MILLISECONDS.sleep(waitTime);
			assertEquals(0, connectionMonitor.getConnectionCount());
			assertEquals(0, connectionCounter.get());

			connectionCounter.set(10000);
			sender.close();
			// ensure stopped
			TimeUnit.MILLISECONDS.sleep(waitTime);
			assertEquals(0, connectionCounter.get());

			receiverService.shutdown();

		} finally {
			receiver1.close();
			receiver2.close();
			sender.close();
		}
	}

	@Test
	public void testReceiver_Pub_Sub() throws Exception {
		SenderConfig senderConfig = new SenderConfig(
				SenderConfig.DEFAULT_ADDRESS,
				new StandardPulseIdProvider(),
				new TimeProvider() {

					@Override
					public Timestamp getTime(long pulseId) {
						return new Timestamp(pulseId, 0L);
					}
				},
				new MatlabByteConverter());
		senderConfig.setSocketType(ZMQ.PUB);
		ConnectionCounterMonitor connectionMonitor = new ConnectionCounterMonitor();
		AtomicInteger connectionCounter = new AtomicInteger();
		connectionMonitor.addHandler((count) -> connectionCounter.set(count));
		senderConfig.setMonitor(connectionMonitor);
		ScheduledSender sender = new ScheduledSender(senderConfig);

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

		try {

			assertEquals(0, connectionMonitor.getConnectionCount());
			assertEquals(0, connectionCounter.get());

			sender.connect();
			TimeUnit.MILLISECONDS.sleep(waitTime);
			assertEquals(0, connectionMonitor.getConnectionCount());
			assertEquals(0, connectionCounter.get());

			receiver1.connect();
			TimeUnit.MILLISECONDS.sleep(waitTime);
			assertEquals(1, connectionMonitor.getConnectionCount());
			assertEquals(1, connectionCounter.get());

			receiver2.connect();
			TimeUnit.MILLISECONDS.sleep(waitTime);
			assertEquals(2, connectionMonitor.getConnectionCount());
			assertEquals(2, connectionCounter.get());

			ExecutorService receiverService = Executors.newFixedThreadPool(2);
			receiverService.execute(() -> {
				try {
					while (receiver1.receive() != null) {
						loopCounter1.incrementAndGet();
					}
				} catch (Throwable t) {
					System.out.println("Receiver1 executor: " + t.getMessage());
				}
			});
			receiverService.execute(() -> {
				try {
					while (receiver2.receive() != null) {
						loopCounter2.incrementAndGet();
					}
				} catch (Throwable t) {
					System.out.println("Receiver2 executor: " + t.getMessage());
				}
			});

			TimeUnit.MILLISECONDS.sleep(waitTime);
			// send/receive data
			int sendCount = 40;
			for (int i = 0; i < sendCount; ++i) {
				sender.send();
				TimeUnit.MILLISECONDS.sleep(1);
			}
			TimeUnit.MILLISECONDS.sleep(waitTime);

			assertEquals(1, dataHeaderCounter1.get());
			assertEquals(sendCount, mainHeaderCounter1.get());
			assertEquals(sendCount, valCounter1.get());
			assertEquals(sendCount, loopCounter1.get());
			receiver1.close();
			TimeUnit.MILLISECONDS.sleep(waitTime);
			assertEquals(1, connectionMonitor.getConnectionCount());
			assertEquals(1, connectionCounter.get());

			assertEquals(1, dataHeaderCounter2.get());
			assertEquals(sendCount, mainHeaderCounter2.get());
			assertEquals(sendCount, valCounter2.get());
			assertEquals(sendCount, loopCounter2.get());
			receiver2.close();
			TimeUnit.MILLISECONDS.sleep(waitTime);
			assertEquals(0, connectionMonitor.getConnectionCount());
			assertEquals(0, connectionCounter.get());

			connectionCounter.set(10000);
			sender.close();
			// ensure stopped
			TimeUnit.MILLISECONDS.sleep(waitTime);
			assertEquals(0, connectionCounter.get());

			receiverService.shutdown();

		} finally {
			receiver1.close();
			receiver2.close();
			sender.close();
		}
	}
}
