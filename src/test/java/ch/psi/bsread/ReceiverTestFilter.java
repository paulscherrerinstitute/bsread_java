package ch.psi.bsread;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
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
	private long initialDelay = 200;
	private long period = 1;

	@Test
	public void testTwoChannelFilter() {
		ScheduledSender sender = new ScheduledSender(
				new SenderConfig(
						SenderConfig.DEFAULT_ADDRESS,
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

		Receiver<ByteBuffer> receiver = new Receiver<ByteBuffer>();
		receiver.getReceiverConfig().addRequestedChannel(new Channel(channel_01, 10, 0));
		receiver.getReceiverConfig().addRequestedChannel(new Channel(channel_02, 100, 0));

		try {
			receiver.connect();
			// We schedule faster as we want to have the testcase execute faster
			sender.connect();
			sender.sendAtFixedRate(initialDelay, period, TimeUnit.MILLISECONDS);

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
		} finally {
			receiver.close();
			sender.close();
		}
	}

	@Test
	public void testTwoChannelFilterOffset() {
		ScheduledSender sender = new ScheduledSender(
				new SenderConfig(
						SenderConfig.DEFAULT_ADDRESS,
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

		Receiver<ByteBuffer> receiver = new Receiver<ByteBuffer>();
		receiver.getReceiverConfig().addRequestedChannel(new Channel(channel_01, 10, 0));
		receiver.getReceiverConfig().addRequestedChannel(new Channel(channel_02, 100, 50));

		try {
			receiver.connect();
			// We schedule faster as we want to have the testcase execute faster
			sender.connect();
			sender.sendAtFixedRate(initialDelay, period, TimeUnit.MILLISECONDS);

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
		} finally {
			receiver.close();
			sender.close();
		}
	}
}
