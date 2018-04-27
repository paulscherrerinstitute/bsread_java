package ch.psi.bsread;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import com.fasterxml.jackson.core.JsonProcessingException;

import ch.psi.bsread.command.Command;
import ch.psi.bsread.common.helper.ByteBufferHelper;
import ch.psi.bsread.compression.Compression;
import ch.psi.bsread.message.DataHeader;
import ch.psi.bsread.message.MainHeader;
import ch.psi.bsread.message.Timestamp;
import ch.psi.bsread.message.Type;
import ch.psi.bsread.monitors.Monitor;
import ch.psi.bsread.monitors.MonitorConfig;

public class Sender {
   private static final Logger LOGGER = LoggerFactory.getLogger(Sender.class);

   private Socket socket;

   private SenderConfig senderConfig;

   private MainHeader mainHeader;
   private byte[] dataHeaderBytes;
   private String dataHeaderMD5 = "";
   private long sentMessages = 0;

   private List<DataChannel<?>> channels = new ArrayList<>();

   public Sender() {
      this(new SenderConfig());
   }

   public Sender(SenderConfig senderConfig) {
      this.senderConfig = senderConfig;
      this.mainHeader = senderConfig.getMainHeaderSupplier() != null ? senderConfig.getMainHeaderSupplier().get()
            : new MainHeader();
   }

   public void connect() {
      Context context = senderConfig.getContext();
      socket = context.socket(senderConfig.getSocketType());
      socket.setSndHWM(senderConfig.getHighWaterMark());
      socket.setLinger(senderConfig.getLinger());
      socket.setSendBufferSize(senderConfig.getSendBufferSize());

      Monitor monitor = senderConfig.getMonitor();
      if (monitor != null) {
         monitor.start(new MonitorConfig(
               context,
               socket,
               senderConfig.getObjectMapper(),
               senderConfig.getSocketType(),
               senderConfig.isBlockingSend()));
      }

      Utils.connect(socket, senderConfig.getAddress(), senderConfig.getSocketType());

      try {
         // it seems that the socket does sometimes not bind in a timely
         // manner.
         TimeUnit.MILLISECONDS.sleep(100);
      } catch (InterruptedException e) {
         LOGGER.error("Interrupted while sleeping.");
      }
   }

   public void close() {
      Monitor monitor = senderConfig.getMonitor();
      if (monitor != null) {
         monitor.stop(sentMessages);
      }
      if (socket != null) {
         socket.close();
      }

      try {
         // it seems that the socket does sometimes not get closed in a
         // timely manner.
         TimeUnit.MILLISECONDS.sleep(100);
      } catch (InterruptedException e) {
         LOGGER.error("Interrupted while sleeping.");
      }
   }

   public void send() {
      long pulseId = senderConfig.getPulseIdProvider().getNextPulseId();
      boolean isSendNeeded = false;
      DataChannel<?> channel;
      ByteOrder byteOrder;
      // check if it is realy necessary to send something (e.g. if there is
      // only only a 10Hz it should send only every 10th call)
      for (int i = 0; i < channels.size() && !isSendNeeded; ++i) {
         isSendNeeded = isSendNeeded(pulseId, channels.get(i));
      }

      if (isSendNeeded) {
         mainHeader.setPulseId(pulseId);
         mainHeader.setGlobalTimestamp(senderConfig.getGlobalTimeProvider().getTime(pulseId));
         mainHeader.setHash(dataHeaderMD5);
         mainHeader.setDataHeaderCompression(senderConfig.getDataHeaderCompression());
         final int blockingFlag = senderConfig.getBlockingFlag();

         try {
            if (LOGGER.isDebugEnabled()) {
               LOGGER.debug(
                     "Send message for pulse '{}' and channels '{}'.",
                     mainHeader.getPulseId(),
                     channels.stream().map(dataChannel -> dataChannel.getConfig().getName())
                           .collect(Collectors.joining(", ")));
            }

            // Send header
            socket.send(senderConfig.getObjectMapper().writeValueAsBytes(mainHeader), blockingFlag | ZMQ.SNDMORE);

            // Send data header
            socket.send(dataHeaderBytes, blockingFlag | ZMQ.SNDMORE);

            // Send data
            int lastSendMore;
            for (int i = 0; i < channels.size(); ++i) {
               channel = channels.get(i);
               byteOrder = channel.getConfig().getByteOrder();
               lastSendMore = ((i + 1) < channels.size() ? blockingFlag | ZMQ.SNDMORE : blockingFlag);
               isSendNeeded = isSendNeeded(pulseId, channel);

               if (isSendNeeded) {
                  final Object value = channel.getValue(pulseId);

                  // TODO: conversion could be done in parallel as a
                  // pre-step (Important: change allocators to non-reusing
                  // types)
                  ByteBuffer valueBuffer = senderConfig.getByteConverter().getBytes(value,
                        channel.getConfig().getType(), byteOrder, senderConfig.getValueAllocator());
                  valueBuffer = channel
                        .getConfig()
                        .getCompression()
                        .getCompressor()
                        .compressData(valueBuffer, valueBuffer.position(), valueBuffer.remaining(), 0,
                              senderConfig.getCompressedValueAllocator(), channel.getConfig().getType().getBytes());
                  socket.sendByteBuffer(valueBuffer, ZMQ.SNDMORE | blockingFlag);

                  Timestamp timestamp = channel.getTime(pulseId);
                  // c-implementation uses a unsigned long (Json::UInt64,
                  // uint64_t) for time -> decided to ignore this here
                  ByteBuffer timeBuffer = senderConfig.getByteConverter().getBytes(timestamp.getAsLongArray(),
                        Type.Int64, byteOrder, senderConfig.getValueAllocator());
                  socket.sendByteBuffer(timeBuffer, lastSendMore);
               } else {
                  // Send placeholder
                  socket.send((byte[]) null, ZMQ.SNDMORE | blockingFlag);
                  socket.send((byte[]) null, lastSendMore);
               }
            }

            ++sentMessages;
         } catch (JsonProcessingException e) {
            throw new IllegalStateException("Unable to serialize message", e);
         }
      }
   }

   private boolean isSendNeeded(long pulseId, DataChannel<?> channel) {
      // Check if this channel sends data for given pulseId
      return ((pulseId - channel.getConfig().getOffset()) % channel.getConfig().getModulo()) == 0;
   }

   /**
    * (Re)Generate the data header based on the configured data channels
    */
   private void generateDataHeader() {
      DataHeader dataHeader = new DataHeader();

      for (DataChannel<?> channel : channels) {
         dataHeader.addChannel(channel.getConfig());
      }

      try {
         dataHeaderBytes = senderConfig.getObjectMapper().writeValueAsBytes(dataHeader);
         if (!Compression.none.equals(senderConfig.getDataHeaderCompression())) {
            ByteBuffer tmpBuf = senderConfig.getDataHeaderCompression().getCompressor().compressDataHeader(
                  ByteBuffer.wrap(dataHeaderBytes),
                  senderConfig.getCompressedValueAllocator());
            dataHeaderBytes = ByteBufferHelper.copyToByteArray(tmpBuf);
         }
         // decided to compute hash from the bytes that are send to Receivers
         // (allows to check consistency without uncompressing the bytes at
         // receivers side)
         dataHeaderMD5 = Utils.computeMD5(dataHeaderBytes);
      } catch (JsonProcessingException e) {
         throw new RuntimeException("Unable to generate data header", e);
      }
   }

   public void sendCommand(Command command) {
      try {
         socket.send(senderConfig.getObjectMapper().writeValueAsBytes(command), ZMQ.NOBLOCK);
      } catch (JsonProcessingException e) {
         String message = "Could not send command.";
         LOGGER.error(message, e);
         throw new RuntimeException(message, e);
      }
   }

   public void addSource(DataChannel<?> channel) {
      channels.add(channel);
      generateDataHeader();
   }

   public void removeSource(DataChannel<?> channel) {
      channels.remove(channel);
      generateDataHeader();
   }

   /**
    * Returns the currently configured data channels as an unmodifiable list
    * 
    * @return Unmodifiable list of data channels
    */
   public List<DataChannel<?>> getChannels() {
      return Collections.unmodifiableList(channels);
   }

   public SenderConfig getSenderConfig() {
      return senderConfig;
   }
}
