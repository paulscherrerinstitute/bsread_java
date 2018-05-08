package ch.psi.bsread;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;

import com.fasterxml.jackson.databind.ObjectMapper;

import ch.psi.bsread.command.Command;
import ch.psi.bsread.command.PolymorphicCommandMixIn;
import ch.psi.bsread.common.concurrent.singleton.Deferred;
import ch.psi.bsread.configuration.Channel;
import ch.psi.bsread.impl.StandardMessageExtractor;

import zmq.MsgAllocator;

public class ReceiverConfig<V> {
   public static final String DEFAULT_ADDRESS = "tcp://localhost:9999";
   public static final int DEFAULT_HIGH_WATER_MARK = 100;
   public static final int CLIENT_HIGH_WATER_MARK = 10000;
   public static final int DEFAULT_RECEIVE_BUFFER_SIZE = 0;
   // drop pending messages immediately when socket is closed
   public static final int DEFAULT_LINGER = 100;
   public static final int DEFAULT_IDLE_CONNECTION_TIMEOUT = Integer.MAX_VALUE;
   public static final int DEFAULT_RECEIVE_TIMEOUT = (int) TimeUnit.SECONDS.toMillis(1);

   // share context due to "too many open files" issue
   // (http://stackoverflow.com/questions/25380162/jeromq-maximum-socket-opened-issue/25478590#25478590)
   // TODO: use Runtime.getRuntime().availableProcessors() ?
   public static final Deferred<Context> DEFERRED_CONTEXT = new Deferred<>(() -> ZMQ.context(1));

   private Context context;
   private boolean keepListeningOnStop;
   private boolean parallelHandlerProcessing;
   private int highWaterMark = DEFAULT_HIGH_WATER_MARK;
   private int linger = DEFAULT_LINGER;
   private int receiveBufferSize = DEFAULT_RECEIVE_BUFFER_SIZE;
   private MessageExtractor<V> messageExtractor;
   private ObjectMapper objectMapper;
   private final MsgAllocator msgAllocator;
   private int socketType = ZMQ.PULL;
   private String address = DEFAULT_ADDRESS;
   private int receiveTimeout = DEFAULT_RECEIVE_TIMEOUT;
   private int idleConnectionTimeout = ReceiverConfig.DEFAULT_IDLE_CONNECTION_TIMEOUT;
   private IdleConnectionTimeoutBehavior idleConnectionTimeoutBehavior = IdleConnectionTimeoutBehavior.RECONNECT;
   private Collection<Channel> requestedChannels;

   public ReceiverConfig() {
      this(DEFAULT_ADDRESS);
   }

   public ReceiverConfig(MessageExtractor<V> messageExtractor) {
      this(DEFAULT_ADDRESS, messageExtractor);
   }

   public ReceiverConfig(String address) {
      this(address, new StandardMessageExtractor<V>());
   }

   public ReceiverConfig(String address, MessageExtractor<V> messageExtractor) {
      this(address, true, false, messageExtractor);
   }

   public ReceiverConfig(String address, boolean keepListeningOnStop, boolean parallelHandlerProcessing,
         MessageExtractor<V> messageExtractor) {
      this(address, keepListeningOnStop, parallelHandlerProcessing, messageExtractor, null);
   }

   public ReceiverConfig(String address, boolean keepListeningOnStop, boolean parallelHandlerProcessing,
         MessageExtractor<V> messageExtractor, MsgAllocator msgAllocator) {
      this.address = address;
      this.keepListeningOnStop = keepListeningOnStop;
      this.parallelHandlerProcessing = parallelHandlerProcessing;
      this.msgAllocator = msgAllocator;

      this.setMessageExtractor(messageExtractor);
      this.setObjectMapper(new ObjectMapper());
   }

   public Context getContext() {
      if (this.context != null) {
         return context;
      } else {
         return ReceiverConfig.DEFERRED_CONTEXT.get();
      }
   }

   public void setContext(Context context) {
      this.context = context;
   }

   public boolean isKeepListeningOnStop() {
      return keepListeningOnStop;
   }

   public void setKeepListeningOnStop(boolean keepListeningOnStop) {
      this.keepListeningOnStop = keepListeningOnStop;
   }

   public boolean isParallelHandlerProcessing() {
      return parallelHandlerProcessing;
   }

   public void setParallelHandlerProcessing(boolean parallelHandlerProcessing) {
      this.parallelHandlerProcessing = parallelHandlerProcessing;
   }

   public int getHighWaterMark() {
      return highWaterMark;
   }

   public void setHighWaterMark(int highWaterMark) {
      this.highWaterMark = highWaterMark;
   }

   public int getLinger() {
      return linger;
   }

   public void setLinger(int linger) {
      this.linger = linger;
   }
   
   public int getReceiveBufferSize() {
      return receiveBufferSize;
   }

   public void setReceiveBufferSize(int receiveBufferSize) {
      this.receiveBufferSize = receiveBufferSize;
   }

   public MessageExtractor<V> getMessageExtractor() {
      return messageExtractor;
   }

   public void setMessageExtractor(MessageExtractor<V> messageExtractor) {
      this.messageExtractor = messageExtractor;
   }

   public MsgAllocator getMsgAllocator() {
      return msgAllocator;
   }

   public ObjectMapper getObjectMapper() {
      return objectMapper;
   }

   public void setObjectMapper(ObjectMapper objectMapper) {
      this.objectMapper = objectMapper;

      addObjectMapperMixin(objectMapper);
   }

   public int getSocketType() {
      return socketType;
   }

   public void setSocketType(int socketType) {
      this.socketType = socketType;
   }

   public void setAddress(String address) {
      this.address = address;
   }

   public String getAddress() {
      return address;
   }

   /**
    * Setter for the receive timeout in millis (use -1 for blocking receive - this is not
    * recommended). In case no message is received within this time limit, a reconnect will be
    * triggered.
    * 
    * @param receiveTimeout The receive timeout
    */
   public void setReceiveTimeout(int receiveTimeout) {
      this.receiveTimeout = receiveTimeout;
   }

   /**
    * Getter for the receive timeout in millis.
    * 
    * @return int The receive timeout
    */
   public int getReceiveTimeout() {
      return receiveTimeout;
   }

   /**
    * Getter for the idle connection timeout in millis.
    * 
    * @return int The idle connection timeout
    */
   public int getIdleConnectionTimeout() {
      return idleConnectionTimeout;
   }

   public void setIdleConnectionTimeout(int idleConnectionTimeout) {
      this.idleConnectionTimeout = idleConnectionTimeout;
   }

   public void setIdleConnectionTimeoutBehavior(IdleConnectionTimeoutBehavior idleConnectionTimeoutBehavior) {
      this.idleConnectionTimeoutBehavior = idleConnectionTimeoutBehavior;
   }

   public IdleConnectionTimeoutBehavior getIdleConnectionTimeoutBehavior() {
      return idleConnectionTimeoutBehavior;
   }

   public Collection<Channel> getRequestedChannels() {
      return this.requestedChannels;
   }

   public void setRequestedChannels(Collection<Channel> requestedChannels) {
      this.requestedChannels = requestedChannels;
   }

   public void addRequestedChannel(Channel requestedChannel) {
      if (this.requestedChannels == null) {
         this.requestedChannels = new ArrayList<>();
      }
      this.requestedChannels.add(requestedChannel);
   }

   public static void addObjectMapperMixin(ObjectMapper objectMapper) {
      objectMapper.addMixIn(Command.class, PolymorphicCommandMixIn.class);
   }

   public enum IdleConnectionTimeoutBehavior {
      RECONNECT, KEEP_RUNNING,
      /* Returns null */
      STOP;
   }
}
