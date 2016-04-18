package ch.psi.bsread;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import ch.psi.bsread.configuration.Channel;
import ch.psi.bsread.converter.MatlabByteConverter;
import ch.psi.bsread.impl.StandardPulseIdProvider;
import ch.psi.bsread.message.ChannelConfig;
import ch.psi.bsread.message.Message;
import ch.psi.bsread.message.Timestamp;
import ch.psi.bsread.message.Type;

public class ReceiverTestFilter {

	@Test
	public void testTwoChannelFilter() {
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

		String channel_01 = "Channel_01";
		String channel_02 = "Channel_02";
		// Register data sources ...
		sender.addSource(new DataChannel<Double>(new ChannelConfig(channel_01, Type.Float64, 1, 0)) {
			@Override
			public Double getValue(long pulseId) {
				return (double) pulseId;
			}

			@Override
			public Timestamp getTime(long pulseId) {
				return new Timestamp(pulseId, 0L);
			}
		});
		sender.addSource(new DataChannel<Double>(new ChannelConfig(channel_02, Type.Float64, 1, 0)) {
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

		Receiver<ByteBuffer> receiver = new Receiver<ByteBuffer>();
		receiver.getReceiverConfig().addRequestedChannel(new Channel(channel_01, 10, 0));
		receiver.getReceiverConfig().addRequestedChannel(new Channel(channel_02, 100, 0));
		receiver.connect();

		// We schedule faster as we want to have the testcase execute faster
		ScheduledExecutorService scheduledExecutor = Executors.newScheduledThreadPool(1);
		ScheduledFuture<?> sendFuture =
				scheduledExecutor.scheduleAtFixedRate(() -> {
					sender.send();
				}, 0, 1, TimeUnit.MILLISECONDS);

		// Receive data
		Message<ByteBuffer> message = null;
		for (int i = 0; i < 100; ++i) {
			message = receiver.receive();

			if (message.getMainHeader().getPulseId() % 100 == 0) {
				assertEquals(2, message.getValues().size());
			} else if (message.getMainHeader().getPulseId() % 10 == 0) {
				assertEquals(1, message.getValues().size());
			} else {
				// these messages should be filtered
				assertEquals(0, message.getValues().size());
			}
		}

		sendFuture.cancel(true);
		scheduledExecutor.shutdown();
		receiver.close();
		sender.close();
	}
	
	@Test
	public void testTwoChannelFilterOffset() {
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

		String channel_01 = "Channel_01";
		String channel_02 = "Channel_02";
		// Register data sources ...
		sender.addSource(new DataChannel<Double>(new ChannelConfig(channel_01, Type.Float64, 1, 0)) {
			@Override
			public Double getValue(long pulseId) {
				return (double) pulseId;
			}

			@Override
			public Timestamp getTime(long pulseId) {
				return new Timestamp(pulseId, 0L);
			}
		});
		sender.addSource(new DataChannel<Double>(new ChannelConfig(channel_02, Type.Float64, 1, 0)) {
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

		Receiver<ByteBuffer> receiver = new Receiver<ByteBuffer>();
		receiver.getReceiverConfig().addRequestedChannel(new Channel(channel_01, 10, 0));
		receiver.getReceiverConfig().addRequestedChannel(new Channel(channel_02, 100, 50));
		receiver.connect();

		// We schedule faster as we want to have the testcase execute faster
		ScheduledExecutorService scheduledExecutor = Executors.newScheduledThreadPool(1);
		ScheduledFuture<?> sendFuture =
				scheduledExecutor.scheduleAtFixedRate(() -> {
					sender.send();
				}, 0, 1, TimeUnit.MILLISECONDS);

		// Receive data
		Message<ByteBuffer> message = null;
		for (int i = 0; i < 100; ++i) {
			message = receiver.receive();

			if ((message.getMainHeader().getPulseId() + 50) % 100 == 0) {
				assertEquals(2, message.getValues().size());
			} else if (message.getMainHeader().getPulseId() % 10 == 0) {
				assertEquals(1, message.getValues().size());
			} else {
				// these messages should be filtered
				assertEquals(0, message.getValues().size());
			}
		}

		sendFuture.cancel(true);
		scheduledExecutor.shutdown();
		receiver.close();
		sender.close();
	}
}
