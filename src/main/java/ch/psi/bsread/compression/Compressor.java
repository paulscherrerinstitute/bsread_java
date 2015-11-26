package ch.psi.bsread.compression;

import java.nio.ByteBuffer;
import java.util.function.IntFunction;

public interface Compressor {
   public static final int DOES_NOT_PROVIDE_UNCOMPRESSED_SIZE = -1;
   public static final int DEFAULT_COMPRESS_THRESHOLD = 64;

   /**
    * Compresses the provided ByteBuffer containing the uncompressed data blob.
    * 
    * @param src The uncompressed source ByteBuffer (make sure position and remaining are set
    *        correctly)
    * @param srcOff The src offset (where the compression should start).
    * @param srcLen The nr of bytes from src to compress
    * @param destOff Number of bytes to reserve in front of the compressed byte blob (e.g. for
    *        additional header information)
    * @param bufferAllocator The allocator for the destination (compressed) ByteBuffer (e.g.
    *        direct/heap ByteBuffer and/or reuse existing ByteBuffer)
    * @param nBytesPerElement The number of bytes to represent one element (e.g. Integer.Bytes for
    *        int)
    * @return ByteBuffer The destination ByteBuffer
    */
   public ByteBuffer compressData(ByteBuffer src, int srcOff, int srcLen, int destOff,
         IntFunction<ByteBuffer> bufferAllocator, int nBytesPerElement);

   /**
    * Decompresses the provided ByteBuffer containing the compressed data blob.
    * 
    * @param src The compressed source ByteBuffer (make sure position and remaining are set
    *        correctly)
    * @param srcOff The src offset (where the decompression should start).
    * @param bufferAllocator The allocator for the destination (decompressed) ByteBuffer (e.g.
    *        direct/heap ByteBuffer and/or reuse existing ByteBuffer)
    * @param nBytesPerElement The number of bytes to represent one element (e.g.
    * @return ByteBuffer The destination ByteBuffer
    */
   public ByteBuffer decompressData(ByteBuffer src, int srcOff, IntFunction<ByteBuffer> bufferAllocator,
         int nBytesPerElement);

   /**
    * Utility method that provides the uncompressed size of a compressed byte blob. This method is
    * optional and can extract the uncompressed size from meta data it might have to store (e.g. lz4
    * stores the uncompressed size as a prefix to the byte blob), i.e., this method should not
    * decompress the byte blob to determine the uncompressed size.
    * 
    * @param src The compressed byte blob.
    * @param srcOff The src offset.
    * @return int The uncompressed size of the byte blob.
    */
   default int getDecompressedDataSize(ByteBuffer src, int srcOff) {
      return DOES_NOT_PROVIDE_UNCOMPRESSED_SIZE;
   }

   /**
    * Compresses the provided ByteBuffer containing the uncompressed data header blob.
    *
    * @param src The uncompressed source ByteBuffer (make sure position and remaining are set
    *        correctly)
    * @param bufferAllocator The allocator for the destination (decompressed) ByteBuffer (e.g.
    *        direct/heap ByteBuffer and/or reuse existing ByteBuffer)
    * @return ByteBuffer The destination ByteBuffer
    */
   public ByteBuffer compressDataHeader(ByteBuffer src, IntFunction<ByteBuffer> bufferAllocator);

   /**
    * Decompresses the provided ByteBuffer containing the compressed data header blob.
    *
    * @param src The uncompressed source ByteBuffer (make sure position and remaining are set
    *        correctly)
    * @param bufferAllocator The allocator for the destination (decompressed) ByteBuffer (e.g.
    *        direct/heap ByteBuffer and/or reuse existing ByteBuffer)
    * @return ByteBuffer The destination ByteBuffer
    */
   public ByteBuffer decompressDataHeader(ByteBuffer src, IntFunction<ByteBuffer> bufferAllocator);
}
