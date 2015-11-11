package ch.psi.bsread.compression;

import java.nio.ByteBuffer;
import java.util.function.IntFunction;

import ch.psi.bsread.message.ChannelConfig;

public interface Compressor {

	/**
	 * Compresses the provided ByteBuffer containing the uncompressed data blob.
	 * 
	 * @param config The ChannelConfig
	 * @param src
	 *            The uncompressed source ByteBuffer (make sure position and
	 *            remaining are set correctly)
	 * @param bufferAllocator
	 *            The allocator for the destination (compressed) ByteBuffer
	 *            (e.g. direct/heap ByteBuffer and/or reuse existing ByteBuffer)
	 * @return ByteBuffer The destination ByteBuffer
	 */
	public ByteBuffer compressData(ChannelConfig config, ByteBuffer src, IntFunction<ByteBuffer> bufferAllocator);

	/**
	 * Decompresses the provided ByteBuffer containing the compressed data blob.
	 * 
	 * @param config The ChannelConfig
	 * @param src
	 *            The compressed source ByteBuffer (make sure position and
	 *            remaining are set correctly)
	 * @param bufferAllocator
	 *            The allocator for the destination (decompressed) ByteBuffer
	 *            (e.g. direct/heap ByteBuffer and/or reuse existing ByteBuffer)
	 * @return ByteBuffer The destination ByteBuffer
	 */
	public ByteBuffer decompressData(ChannelConfig config, ByteBuffer src, IntFunction<ByteBuffer> bufferAllocator);

	/**
	 * Compresses the provided ByteBuffer containing the uncompressed data header blob.
	 *
	 * @param src
	 *            The uncompressed source ByteBuffer (make sure position and
	 *            remaining are set correctly)
	 * @param bufferAllocator
	 *            The allocator for the destination (decompressed) ByteBuffer
	 *            (e.g. direct/heap ByteBuffer and/or reuse existing ByteBuffer)
	 * @return ByteBuffer The destination ByteBuffer
	 */
	public ByteBuffer compressHeader(ByteBuffer src, IntFunction<ByteBuffer> bufferAllocator);

	/**
	 * Decompresses the provided ByteBuffer containing the compressed data header blob.
	 *
	 * @param src
	 *            The uncompressed source ByteBuffer (make sure position and
	 *            remaining are set correctly)
	 * @param bufferAllocator
	 *            The allocator for the destination (decompressed) ByteBuffer
	 *            (e.g. direct/heap ByteBuffer and/or reuse existing ByteBuffer)
	 * @return ByteBuffer The destination ByteBuffer
	 */
	public byte[] decompressHeader(ByteBuffer src, IntFunction<ByteBuffer> bufferAllocator);
}
