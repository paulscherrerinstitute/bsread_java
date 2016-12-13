package ch.psi.bsread;


import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
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
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class ReceiverTest_4 {
	private ByteConverter byteConverter = new MatlabByteConverter();
	private MainHeader hookMainHeader;
	private boolean hookMainHeaderCalled;
	private DataHeader hookDataHeader;
	private boolean hookDataHeaderCalled;
	private Map<String, Value<ByteBuffer>> hookValues;
	private boolean hookValuesCalled;
	private Map<String, ChannelConfig> channelConfigs = new HashMap<>();

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
        
    Thread thread;
    Receiver<ByteBuffer> receiver;
    public void startReceiver() throws IOException, InterruptedException {
        closeReceiver();
        thread = new Thread(() -> {   
            try {
                
              receiver = getReceiver();
	      receiver.connect();


		// Receive data
		Message<ByteBuffer> message = null;
		while (true) {
		    message = receiver.receive();
                    if (message == null){
                        System.out.println("Closed receiver");
                        break;
                    }
                    System.out.println("->" );
                }

            } catch (Exception ex) {
                System.err.println(ex);
            } finally {     
                closeReceiver();
            }
        });
        thread.setName("Stream receiver");
        thread.setDaemon(true);
        thread.start();        
    }
    
    volatile AtomicBoolean closing = new AtomicBoolean(false);
      void closeReceiver(){
        if (closing.compareAndSet(false, true)){
            try{
                if (receiver!=null){
                    try {
                        receiver.close();   
                        //thread.interrupt();
                        System.err.println("Closed in thread " + Thread.currentThread().getName());
                    } catch (Exception ex) {
                        System.err.println(ex);
                    }
                    receiver = null;
                }
            } finally {
               closing.compareAndSet( true, false);      
            }
        } else {
            System.err.println("Didn't close in thread " + Thread.currentThread().getName());            
        }
    }      
     
     Sender getSender(){
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
                return sender;
     }
    

	@Test
	public void testSenderOneChannel10Hz() throws IOException, InterruptedException {
                Sender sender = getSender();
		// We schedule faster as we want to have the testcase execute faster
		ScheduledFuture<?> sendFuture=Executors.newScheduledThreadPool(1).scheduleAtFixedRate(() -> sender.send(), 0, 1, TimeUnit.MILLISECONDS);

                                
                
                
                for (int i=0; i<5; i++){
                    System.out.println(i);
                    startReceiver();

                    Thread.sleep(1000);
                    //thread.interrupt();
                    closeReceiver();

                    long start = System.currentTimeMillis();
                    while( thread.isAlive()){
                        assert (System.currentTimeMillis() - start  < 20000 );
                       Thread.sleep(10); 
                    }
                }
                
                
                sendFuture.cancel(true);
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
