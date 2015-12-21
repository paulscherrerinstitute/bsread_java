package ch.psi.bsread.stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import org.junit.Test;

import ch.psi.bsread.DataChannel;
import ch.psi.bsread.Receiver;
import ch.psi.bsread.Sender;
import ch.psi.bsread.SenderConfig;
import ch.psi.bsread.TimeProvider;
import ch.psi.bsread.converter.MatlabByteConverter;
import ch.psi.bsread.impl.StandardPulseIdProvider;
import ch.psi.bsread.message.ChannelConfig;
import ch.psi.bsread.message.Message;
import ch.psi.bsread.message.Timestamp;
import ch.psi.bsread.message.Type;

public class MessageStreamerTest {

	// TODO: When ForkJoinPool.commonPool() is used (and all test of the project
	// are executed) tests
	// fail because runable is not executed (not enough threads available?)
	private static ExecutorService EXECUTOR = Executors.newCachedThreadPool();

	@Test
	public void test_01() throws InterruptedException {
		String channelName = "ABC";
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
		sender.addSource(new DataChannel<Long>(new ChannelConfig(channelName, Type.Int64, 1, 0)) {
			@Override
			public Long getValue(long pulseId) {
				return pulseId;
			}

			@Override
			public Timestamp getTime(long pulseId) {
				return new Timestamp(pulseId, 0L);
			}
		});
		sender.bind();

		AtomicBoolean exited = new AtomicBoolean(false);
		try (MessageStreamer<Long, Message<Long>> messageStreamer =
				new MessageStreamer<>(Receiver.DEFAULT_RECEIVING_ADDRESS, 0, 0, new MatlabByteConverter(),
						Function.identity())) {

			// construct to receive messages
			ValueHandler<StreamSection<Message<Long>>> valueHandler = new ValueHandler<>();
			EXECUTOR.execute(() -> {
				// should block here until MessageStreamer gets closed
				messageStreamer.getStream()
						.forEach(value -> {
							valueHandler.accept(value);
						});

				exited.set(true);
			});

			// send 0
			sender.send();
			valueHandler.awaitUpdate();
			assertFalse(exited.get());
			StreamSection<Message<Long>> streamSection = valueHandler.getValue();
			Message<Long> message = streamSection.getCurrent();
			assertEquals(Long.valueOf(0), message.getValues().get(channelName).getValue());
			assertFalse(streamSection.getPast(true).iterator().hasNext());
			assertFalse(streamSection.getFuture(true).iterator().hasNext());

			valueHandler.resetAwaitBarrier();
			// send 1
			sender.send();
			valueHandler.awaitUpdate();
			assertFalse(exited.get());
			streamSection = valueHandler.getValue();
			message = streamSection.getCurrent();
			assertEquals(Long.valueOf(1), message.getValues().get(channelName).getValue());
			assertFalse(streamSection.getPast(true).iterator().hasNext());
			assertFalse(streamSection.getFuture(true).iterator().hasNext());

			valueHandler.resetAwaitBarrier();
			// send 2
			sender.send();
			valueHandler.awaitUpdate();
			assertFalse(exited.get());
			streamSection = valueHandler.getValue();
			message = streamSection.getCurrent();
			assertEquals(Long.valueOf(2), message.getValues().get(channelName).getValue());
			assertFalse(streamSection.getPast(true).iterator().hasNext());
			assertFalse(streamSection.getFuture(true).iterator().hasNext());

			valueHandler.resetAwaitBarrier();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			TimeUnit.MILLISECONDS.sleep(500);
			assertTrue(exited.get());
		}

		sender.close();
	}

	@Test
	public void test_02() throws InterruptedException {
		String channelName = "ABC";
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
		sender.addSource(new DataChannel<Long>(new ChannelConfig(channelName, Type.Int64, 1, 0)) {
			@Override
			public Long getValue(long pulseId) {
				return pulseId;
			}

			@Override
			public Timestamp getTime(long pulseId) {
				return new Timestamp(pulseId, 0L);
			}
		});
		sender.bind();

		AtomicBoolean exited = new AtomicBoolean(false);
		try (MessageStreamer<Long, Message<Long>> messageStreamer =
				new MessageStreamer<>(Receiver.DEFAULT_RECEIVING_ADDRESS, 3, 2, new MatlabByteConverter(),
						Function.identity())) {

			// construct to receive messages
			ValueHandler<StreamSection<Message<Long>>> valueHandler = new ValueHandler<>();
			EXECUTOR.execute(() -> {
				// should block here until MessageStreamer gets closed
				messageStreamer.getStream()
						.forEach(value -> {
							valueHandler.accept(value);
						});

				exited.set(true);
			});

			// send 0
			sender.send();
			// send 1
			sender.send();
			// send 2
			sender.send();
			// send 3
			sender.send();
			// send 4
			sender.send();
			// send 5
			sender.send();
			valueHandler.awaitUpdate();
			assertFalse(exited.get());
			StreamSection<Message<Long>> streamSection = valueHandler.getValue();
			Message<Long> message = streamSection.getCurrent();
			assertEquals(Long.valueOf(3), message.getValues().get(channelName).getValue());
			Iterator<Message<Long>> iter = streamSection.getPast(true).iterator();
			assertTrue(iter.hasNext());
			assertEquals(Long.valueOf(0), iter.next().getValues().get(channelName).getValue());
			assertTrue(iter.hasNext());
			assertEquals(Long.valueOf(1), iter.next().getValues().get(channelName).getValue());
			assertTrue(iter.hasNext());
			assertEquals(Long.valueOf(2), iter.next().getValues().get(channelName).getValue());
			assertFalse(iter.hasNext());
			iter = streamSection.getFuture(true).iterator();
			assertTrue(iter.hasNext());
			assertEquals(Long.valueOf(4), iter.next().getValues().get(channelName).getValue());
			assertTrue(iter.hasNext());
			assertEquals(Long.valueOf(5), iter.next().getValues().get(channelName).getValue());
			assertFalse(iter.hasNext());

			valueHandler.resetAwaitBarrier();
			// send 6
			sender.send();
			valueHandler.awaitUpdate();
			assertFalse(exited.get());
			streamSection = valueHandler.getValue();
			message = streamSection.getCurrent();
			assertEquals(Long.valueOf(4), message.getValues().get(channelName).getValue());
			iter = streamSection.getPast(true).iterator();
			assertTrue(iter.hasNext());
			assertEquals(Long.valueOf(1), iter.next().getValues().get(channelName).getValue());
			assertTrue(iter.hasNext());
			assertEquals(Long.valueOf(2), iter.next().getValues().get(channelName).getValue());
			assertTrue(iter.hasNext());
			assertEquals(Long.valueOf(3), iter.next().getValues().get(channelName).getValue());
			assertFalse(iter.hasNext());
			iter = streamSection.getFuture(true).iterator();
			assertTrue(iter.hasNext());
			assertEquals(Long.valueOf(5), iter.next().getValues().get(channelName).getValue());
			assertTrue(iter.hasNext());
			assertEquals(Long.valueOf(6), iter.next().getValues().get(channelName).getValue());
			assertFalse(iter.hasNext());

			valueHandler.resetAwaitBarrier();
			// send 7
			sender.send();
			valueHandler.awaitUpdate();
			assertFalse(exited.get());
			streamSection = valueHandler.getValue();
			message = streamSection.getCurrent();
			assertEquals(Long.valueOf(5), message.getValues().get(channelName).getValue());
			iter = streamSection.getPast(true).iterator();
			assertTrue(iter.hasNext());
			assertEquals(Long.valueOf(2), iter.next().getValues().get(channelName).getValue());
			assertTrue(iter.hasNext());
			assertEquals(Long.valueOf(3), iter.next().getValues().get(channelName).getValue());
			assertTrue(iter.hasNext());
			assertEquals(Long.valueOf(4), iter.next().getValues().get(channelName).getValue());
			assertFalse(iter.hasNext());
			iter = streamSection.getFuture(true).iterator();
			assertTrue(iter.hasNext());
			assertEquals(Long.valueOf(6), iter.next().getValues().get(channelName).getValue());
			assertTrue(iter.hasNext());
			assertEquals(Long.valueOf(7), iter.next().getValues().get(channelName).getValue());
			assertFalse(iter.hasNext());

			valueHandler.resetAwaitBarrier();

		} catch (Exception e) {
			e.printStackTrace();
		}

		TimeUnit.MILLISECONDS.sleep(500);
		sender.close();
		assertTrue(exited.get());
	}

	@Test
	public void test_03_NoBackpressure() throws InterruptedException {
		test_03(AsyncTransferSpliterator.DEFAULT_BACKPRESSURE_SIZE);
	}

	@Test
	public void test_03_WithBackpressure() throws InterruptedException {
		test_03(1);
	}

	public void test_03(int backpressure) throws InterruptedException {
		String channelName = "ABC";
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
		sender.addSource(new DataChannel<Long>(new ChannelConfig(channelName, Type.Int64, 1, 0)) {
			@Override
			public Long getValue(long pulseId) {
				return pulseId;
			}

			@Override
			public Timestamp getTime(long pulseId) {
				return new Timestamp(pulseId, 0L);
			}
		});
		sender.bind();

		int pastElements = 3;
		int futureElements = 2;
		long sleepTimeMillis = 1;
		AtomicLong sentValues = new AtomicLong();
		CountDownLatch latch = new CountDownLatch(1);

		try (MessageStreamer<Long, Message<Long>> messageStreamer =
				new MessageStreamer<>(Receiver.DEFAULT_RECEIVING_ADDRESS, pastElements, futureElements, backpressure,
						new MatlabByteConverter(), Function.identity())) {

			// first value based on MessageStreamer config
			AtomicReference<Long> lastValue = new AtomicReference<Long>(Long.valueOf(pastElements - 1));
			EXECUTOR
					.execute(() -> {
						// should block here until MessageStreamer gets closed
						messageStreamer.getStream()
								.forEach(
										streamSection -> {
											long currentValue = lastValue.get().longValue() + 1;

											Message<Long> message = streamSection.getCurrent();
											assertEquals(Long.valueOf(currentValue), message.getValues().get(channelName)
													.getValue());

											int nrOfElements = 0;
											currentValue -= pastElements;
											Iterator<Message<Long>> iter = streamSection.getPast(true).iterator();
											while (iter.hasNext()) {
												assertEquals(Long.valueOf(currentValue++),
														iter.next().getValues().get(channelName).getValue());
												++nrOfElements;
											}
											assertEquals(nrOfElements, pastElements);

											nrOfElements = 0;
											currentValue += 1;
											iter = streamSection.getFuture(true).iterator();

											while (iter.hasNext()) {
												assertEquals(Long.valueOf(currentValue++),
														iter.next().getValues().get(channelName).getValue());
												++nrOfElements;
											}
											assertEquals(nrOfElements, futureElements);

											lastValue.set(lastValue.get() + 1);

											try {
												TimeUnit.MICROSECONDS.sleep(sleepTimeMillis);
											} catch (Exception e) {
												e.printStackTrace();
											}
										});

						latch.countDown();
					});

			ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
			Future<?> future =
					executorService.scheduleAtFixedRate(() -> {
						sender.send();
						sentValues.incrementAndGet();
					}, 10, sleepTimeMillis, TimeUnit.MILLISECONDS);

			TimeUnit.SECONDS.sleep(5);
			future.cancel(true);
			executorService.shutdown();

			System.out.println("Values send: " + sentValues.get() + " values received: "
					+ (lastValue.get() - pastElements));

		} catch (Exception e) {
			e.printStackTrace();
		}

		TimeUnit.MILLISECONDS.sleep(1000);
		sender.close();

		try {
			assertTrue(latch.await(5000, TimeUnit.MILLISECONDS));
		} catch (Exception e) {
			e.printStackTrace();
			assertTrue(false);
		}
	}

	@Test
	public void test_04_NoBackpressure() throws InterruptedException {
		test_04(AsyncTransferSpliterator.DEFAULT_BACKPRESSURE_SIZE);
	}

	@Test
	public void test_04_WithBackpressure() throws InterruptedException {
		test_04(1);
	}

	public void test_04(int backpressure) throws InterruptedException {
		String channelName = "ABC";
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
		sender.addSource(new DataChannel<Long>(new ChannelConfig(channelName, Type.Int64, 1, 0)) {
			@Override
			public Long getValue(long pulseId) {
				return pulseId;
			}

			@Override
			public Timestamp getTime(long pulseId) {
				return new Timestamp(pulseId, 0L);
			}
		});
		sender.bind();

		CountDownLatch latch = new CountDownLatch(1);
		int pastElements = 3;
		int futureElements = 2;
		AtomicLong nrOfValues = new AtomicLong();
		long sleepTimeMillis = 1;
		AtomicLong sentValues = new AtomicLong();

		try (MessageStreamer<Long, Message<Long>> messageStreamer =
				new MessageStreamer<>(Receiver.DEFAULT_RECEIVING_ADDRESS, pastElements, futureElements, backpressure,
						new MatlabByteConverter(), Function.identity())) {

			// StringBuilder output = new StringBuilder();
			EXECUTOR.execute(() -> {
				// should block here until MessageStreamer gets closed
				messageStreamer
						.getStream()
						.parallel()
						.forEach(streamSection -> {
							Message<Long> message = streamSection.getCurrent();
							long currentValue = message.getValues().get(channelName).getValue();

							// StringBuilder pastBuf = new StringBuilder();
							// StringBuilder futureBuf = new StringBuilder();
							//
							// long pastElmCnt = 0;
							// Iterator<Message<Long>> iter =
							// streamSection.getPast(true).iterator();
							// while(iter.hasNext()){
							// pastElmCnt++;
							// pastBuf.append(iter.next().getValues().get(channelName).getValue()).append(" ");
							// }
							//
							// long futureElmCnt = 0;
							// iter = streamSection.getFuture(true).iterator();
							// while(iter.hasNext()){
							// futureElmCnt++;
							// futureBuf.append(iter.next().getValues().get(channelName).getValue()).append(" ");
							// }
							//
							// output.append("Value: "+currentValue +
							// " past "+pastElmCnt + " ("
							// +pastBuf.toString() +") future "+ futureElmCnt +
							// " ("+futureBuf.toString()
							// +")\n"+messageStreamer.toString() +"\n");

								int nrOfElements = 0;
								currentValue -= pastElements;
								Iterator<Message<Long>> iter = streamSection.getPast(true).iterator();
								while (iter.hasNext()) {
									assertEquals(Long.valueOf(currentValue++), iter.next().getValues().get(channelName)
											.getValue());
									++nrOfElements;
								}
								assertEquals(nrOfElements, pastElements);

								nrOfElements = 0;
								currentValue += 1;
								iter = streamSection.getFuture(true).iterator();

								while (iter.hasNext()) {
									assertEquals(Long.valueOf(currentValue++), iter.next().getValues().get(channelName)
											.getValue());
									++nrOfElements;
								}
								assertEquals(nrOfElements, futureElements);

								nrOfValues.incrementAndGet();

								try {
									TimeUnit.MICROSECONDS.sleep(sleepTimeMillis);
								} catch (Exception e) {
									e.printStackTrace();
								}
							});

				latch.countDown();

				// System.out.println(output.toString());
			});

			ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
			Future<?> future =
					executorService.scheduleAtFixedRate(() -> {
						sender.send();
						sentValues.incrementAndGet();
					}, 10, sleepTimeMillis, TimeUnit.MILLISECONDS);

			TimeUnit.SECONDS.sleep(5);
			future.cancel(true);
			executorService.shutdown();

			System.out.println("Values send: " + sentValues.get() + " values received: " + nrOfValues.get());

		} catch (Exception e) {
			e.printStackTrace();
		}

		sender.close();

		try {
			assertTrue(latch.await(5000, TimeUnit.MILLISECONDS));
		} catch (Exception e) {
			e.printStackTrace();
			assertTrue(false);
		}
	}

	private class ValueHandler<V> implements Consumer<V> {
		private V value;
		private CountDownLatch updatedSignal = new CountDownLatch(1);
		private CountDownLatch valueProcessedSignal = new CountDownLatch(1);

		@Override
		public void accept(V value) {
			this.value = value;
			updatedSignal.countDown();

			try {
				// wait here until elements have been checked (otherwise
				// returning process will already delete elements before they
				// can be checked)
				valueProcessedSignal.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		public void awaitUpdate() throws InterruptedException {
			this.awaitUpdate(0, TimeUnit.MICROSECONDS);
		}

		public void awaitUpdate(long timeout, TimeUnit unit) throws InterruptedException {
			if (timeout <= 0) {
				updatedSignal.await();
			} else {
				updatedSignal.await(timeout, unit);
			}
		}

		public void resetAwaitBarrier() {
			updatedSignal = new CountDownLatch(1);

			CountDownLatch newLatch = new CountDownLatch(1);
			valueProcessedSignal.countDown();
			// this might cause problems
			valueProcessedSignal = newLatch;
		}

		public V getValue() {
			return this.value;
		}
	}

}
