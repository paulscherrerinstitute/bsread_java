package ch.psi.bsread;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import ch.psi.bsread.basic.BasicReceiver;
import ch.psi.bsread.compression.Compression;
import ch.psi.bsread.converter.MatlabByteConverter;
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
				new StandardPulseIdProvider(),
				new TimeProvider() {

					@Override
					public Timestamp getTime(long pulseId) {
						return new Timestamp(pulseId, 0L);
					}
				},
				new MatlabByteConverter()
				);

		int size = 2048;
		Random rand = new Random(0);
		// Register data sources ...
		sender.addSource(new DataChannel<double[]>(new ChannelConfig("ABC", Type.Float64, new int[]{size}, 1, 0, ChannelConfig.DEFAULT_ENCODING, Compression.bitshuffle_lz4)) {
			@Override
			public double[] getValue(long pulseId) {
				double[] val = new double[size];
				for(int i = 0; i < size; ++i){
				   val[i] = rand.nextDouble();
				}
				
				return val;
			}

			@Override
			public Timestamp getTime(long pulseId) {
				return new Timestamp(pulseId, 0L);
			}
		});
	      sender.addSource(new DataChannel<double[]>(new ChannelConfig("ABB", Type.Float64, new int[]{size}, 1, 0, ChannelConfig.DEFAULT_ENCODING, Compression.bitshuffle_lz4)) {
	            @Override
	            public double[] getValue(long pulseId) {
	                double[] val = new double[size];
	                for(int i = 0; i < size; ++i){
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
			assertEquals(hookMainHeader.getPulseId(), value.getTimestamp().getEpoch());
			assertEquals(0, value.getTimestamp().getNs());
			assertEquals(hookMainHeader.getPulseId(), hookMainHeader.getGlobalTimestamp().getEpoch());
			assertEquals(0, hookMainHeader.getGlobalTimestamp().getNs());
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

	public void setValues(Map<String, Value<Object>> values) {
		this.hookValues = values;
		this.hookValuesCalled = true;
	}
}
