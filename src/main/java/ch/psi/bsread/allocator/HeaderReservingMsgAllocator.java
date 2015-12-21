package ch.psi.bsread.allocator;

import java.nio.ByteBuffer;
import java.util.function.IntFunction;

import zmq.Msg;
import zmq.MsgAllocator;

public class HeaderReservingMsgAllocator implements MsgAllocator {
   private int maxHeaderBytes;
   private IntFunction<ByteBuffer> byteBufferAllocator;

   public HeaderReservingMsgAllocator(int maxHeaderBytes, IntFunction<ByteBuffer> byteBufferAllocator) {
      this.maxHeaderBytes = maxHeaderBytes;
      this.byteBufferAllocator = byteBufferAllocator;
   }

   @Override
   public Msg allocate(int size) {
      if (size > 0) {
         ByteBuffer buf = byteBufferAllocator.apply(maxHeaderBytes + size);
         buf.position(maxHeaderBytes);
         return new Msg(buf);
      } else {
         return new Msg(size);
      }
   }
}
