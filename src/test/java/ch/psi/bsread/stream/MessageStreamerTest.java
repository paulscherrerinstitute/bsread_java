package ch.psi.bsread.stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import org.junit.Test;

import ch.psi.bsread.DataChannel;
import ch.psi.bsread.Receiver;
import ch.psi.bsread.Sender;
import ch.psi.bsread.TimeProvider;
import ch.psi.bsread.converter.MatlabByteConverter;
import ch.psi.bsread.impl.StandardPulseIdProvider;
import ch.psi.bsread.message.ChannelConfig;
import ch.psi.bsread.message.Message;
import ch.psi.bsread.message.Timestamp;
import ch.psi.bsread.message.Type;

public class MessageStreamerTest {

	@Test
	public void test_01() throws InterruptedException {
		String channelName = "ABC";
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
		sender.addSource(new DataChannel<Long>(new ChannelConfig(channelName, Type.Long, 1, 0)) {
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
		try (MessageStreamer<Message<Long>, Long> messageStreamer = new MessageStreamer<>(Receiver.DEFAULT_RECEIVING_ADDRESS, 0, 0, Function.identity(), new MatlabByteConverter())) {

			// construct to receive messages
			ValueHandler<StreamSection<Message<Long>>> valueHandler = new ValueHandler<>();
			ForkJoinPool.commonPool().execute(() -> {
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
			Message<Long> message = streamSection.getCurrentValue();
			assertEquals(Long.valueOf(0), message.getValues().get(channelName).getValue());
			assertFalse(streamSection.getPastValues(true).iterator().hasNext());
			assertFalse(streamSection.getFutureValues(true).iterator().hasNext());

			valueHandler.resetAwaitBarrier();
			// send 1
			sender.send();
			valueHandler.awaitUpdate();
			assertFalse(exited.get());
			streamSection = valueHandler.getValue();
			message = streamSection.getCurrentValue();
			assertEquals(Long.valueOf(1), message.getValues().get(channelName).getValue());
			assertFalse(streamSection.getPastValues(true).iterator().hasNext());
			assertFalse(streamSection.getFutureValues(true).iterator().hasNext());

			valueHandler.resetAwaitBarrier();
			// send 2
			sender.send();
			valueHandler.awaitUpdate();
			assertFalse(exited.get());
			streamSection = valueHandler.getValue();
			message = streamSection.getCurrentValue();
			assertEquals(Long.valueOf(2), message.getValues().get(channelName).getValue());
			assertFalse(streamSection.getPastValues(true).iterator().hasNext());
			assertFalse(streamSection.getFutureValues(true).iterator().hasNext());

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
		sender.addSource(new DataChannel<Long>(new ChannelConfig(channelName, Type.Long, 1, 0)) {
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
		try (MessageStreamer<Message<Long>, Long> messageStreamer = new MessageStreamer<>(Receiver.DEFAULT_RECEIVING_ADDRESS, 3, 2, Function.identity(), new MatlabByteConverter())) {

			// construct to receive messages
			ValueHandler<StreamSection<Message<Long>>> valueHandler = new ValueHandler<>();
			ForkJoinPool.commonPool().execute(() -> {
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
			Message<Long> message = streamSection.getCurrentValue();
			assertEquals(Long.valueOf(3), message.getValues().get(channelName).getValue());
			Iterator<Message<Long>> iter = streamSection.getPastValues(true).iterator();
			assertTrue(iter.hasNext());
			assertEquals(Long.valueOf(0), iter.next().getValues().get(channelName).getValue());
			assertTrue(iter.hasNext());
			assertEquals(Long.valueOf(1), iter.next().getValues().get(channelName).getValue());
			assertTrue(iter.hasNext());
			assertEquals(Long.valueOf(2), iter.next().getValues().get(channelName).getValue());
			assertFalse(iter.hasNext());
			iter = streamSection.getFutureValues(true).iterator();
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
			message = streamSection.getCurrentValue();
			assertEquals(Long.valueOf(4), message.getValues().get(channelName).getValue());
			iter = streamSection.getPastValues(true).iterator();
			assertTrue(iter.hasNext());
			assertEquals(Long.valueOf(1), iter.next().getValues().get(channelName).getValue());
			assertTrue(iter.hasNext());
			assertEquals(Long.valueOf(2), iter.next().getValues().get(channelName).getValue());
			assertTrue(iter.hasNext());
			assertEquals(Long.valueOf(3), iter.next().getValues().get(channelName).getValue());
			assertFalse(iter.hasNext());
			iter = streamSection.getFutureValues(true).iterator();
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
			message = streamSection.getCurrentValue();
			assertEquals(Long.valueOf(5), message.getValues().get(channelName).getValue());
			iter = streamSection.getPastValues(true).iterator();
			assertTrue(iter.hasNext());
			assertEquals(Long.valueOf(2), iter.next().getValues().get(channelName).getValue());
			assertTrue(iter.hasNext());
			assertEquals(Long.valueOf(3), iter.next().getValues().get(channelName).getValue());
			assertTrue(iter.hasNext());
			assertEquals(Long.valueOf(4), iter.next().getValues().get(channelName).getValue());
			assertFalse(iter.hasNext());
			iter = streamSection.getFutureValues(true).iterator();
			assertTrue(iter.hasNext());
			assertEquals(Long.valueOf(6), iter.next().getValues().get(channelName).getValue());
			assertTrue(iter.hasNext());
			assertEquals(Long.valueOf(7), iter.next().getValues().get(channelName).getValue());
			assertFalse(iter.hasNext());

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
	public void test_03() throws InterruptedException {
		String channelName = "ABC";
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
		sender.addSource(new DataChannel<Long>(new ChannelConfig(channelName, Type.Long, 1, 0)) {
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
		int pastElements = 3;
		int futureElements = 2;
		try (MessageStreamer<Message<Long>, Long> messageStreamer = new MessageStreamer<>(Receiver.DEFAULT_RECEIVING_ADDRESS, pastElements, futureElements, Function.identity(),
				new MatlabByteConverter())) {

			// first value based on MessageStreamer config
			AtomicReference<Long> lastValue = new AtomicReference<Long>(Long.valueOf(pastElements - 1));
			ForkJoinPool.commonPool().execute(() -> {
				// should block here until MessageStreamer gets closed
					messageStreamer.getStream()
							.forEach(streamSection -> {
								long currentValue = lastValue.get().longValue() + 1;

								Message<Long> message = streamSection.getCurrentValue();
								assertEquals(Long.valueOf(currentValue), message.getValues().get(channelName).getValue());

								int nrOfElements = 0;
								currentValue -= pastElements;
								Iterator<Message<Long>> iter = streamSection.getPastValues(true).iterator();
								while (iter.hasNext()) {
									assertEquals(Long.valueOf(currentValue++), iter.next().getValues().get(channelName).getValue());
									++nrOfElements;
								}
								assertEquals(nrOfElements, pastElements);

								nrOfElements = 0;
								currentValue += 1;
								iter = streamSection.getFutureValues(true).iterator();

								while (iter.hasNext()) {
									assertEquals(Long.valueOf(currentValue++), iter.next().getValues().get(channelName).getValue());
									++nrOfElements;
								}
								assertEquals(nrOfElements, futureElements);

								lastValue.set(lastValue.get() + 1);
							});

					exited.set(true);
				});

			ScheduledExecutorService executorService = Executors.newScheduledThreadPool(2);
			ScheduledFuture<?> future = executorService.scheduleAtFixedRate(() -> sender.send(), 10, 10, TimeUnit.MILLISECONDS);

			TimeUnit.SECONDS.sleep(5);
			future.cancel(true);
			executorService.shutdown();

			System.out.println("Nr of values checked: " + (lastValue.get() - pastElements));

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			TimeUnit.MILLISECONDS.sleep(500);
			assertTrue(exited.get());
		}

		sender.close();
	}

	@Test
	public void test_04() throws InterruptedException {
		String channelName = "ABC";
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
		sender.addSource(new DataChannel<Long>(new ChannelConfig(channelName, Type.Long, 1, 0)) {
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
		int pastElements = 3;
		int futureElements = 2;
		try (MessageStreamer<Message<Long>, Long> messageStreamer = new MessageStreamer<>(Receiver.DEFAULT_RECEIVING_ADDRESS, pastElements, futureElements, Function.identity(),
				new MatlabByteConverter())) {

//			StringBuilder output = new StringBuilder();
			ExecutorService executors = Executors.newFixedThreadPool(2);
			executors.execute(() -> {
				// should block here until MessageStreamer gets closed
					messageStreamer.getStream()
							.parallel()
							.forEach(streamSection -> {
								Message<Long> message = streamSection.getCurrentValue();
								long currentValue = message.getValues().get(channelName).getValue();
								
//								StringBuilder pastBuf = new StringBuilder();
//								StringBuilder futureBuf = new StringBuilder();
//								
//								long pastElmCnt = 0;
//								Iterator<Message<Long>> iter = streamSection.getPastValues(true).iterator();
//								while(iter.hasNext()){
//									pastElmCnt++;
//									pastBuf.append(iter.next().getValues().get(channelName).getValue()).append(" ");
//								}
//								
//								long futureElmCnt = 0;
//								iter = streamSection.getFutureValues(true).iterator();
//								while(iter.hasNext()){
//									futureElmCnt++;
//									futureBuf.append(iter.next().getValues().get(channelName).getValue()).append(" ");
//								}
//								
//								output.append("Value: "+currentValue + " past "+pastElmCnt + " (" +pastBuf.toString() +") future "+ futureElmCnt + " ("+futureBuf.toString() +")\n"+messageStreamer.toString() +"\n");

								int nrOfElements = 0;
								currentValue -= pastElements;
								Iterator<Message<Long>> iter = streamSection.getPastValues(true).iterator();
								while (iter.hasNext()) {
									assertEquals(Long.valueOf(currentValue++), iter.next().getValues().get(channelName).getValue());
									++nrOfElements;
								}
								assertEquals(nrOfElements, pastElements);

								nrOfElements = 0;
								currentValue += 1;
								iter = streamSection.getFutureValues(true).iterator();

								while (iter.hasNext()) {
									assertEquals(Long.valueOf(currentValue++), iter.next().getValues().get(channelName).getValue());
									++nrOfElements;
								}
								assertEquals(nrOfElements, futureElements);
							});

					exited.set(true);
					
//					System.out.println(output.toString());
				});

			ScheduledExecutorService executorService = Executors.newScheduledThreadPool(2);
			ScheduledFuture<?> future = executorService.scheduleAtFixedRate(() -> sender.send(), 10, 10, TimeUnit.MILLISECONDS);

			TimeUnit.SECONDS.sleep(1);
			future.cancel(true);
			executorService.shutdown();
			
			executors.shutdown();

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			TimeUnit.MILLISECONDS.sleep(500);
			assertTrue(exited.get());
		}

		sender.close();
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
