package ch.psi.bsread.compression.lz4;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.function.IntFunction;

import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import ch.psi.bsread.compression.Compressor;
import ch.psi.bsread.message.ChannelConfig;

public class LZ4Compressor implements Compressor {

	private static final ThreadLocal<byte[]> TMP_BYTE_ARRAY_PROVIDER = ThreadLocal
			.<byte[]> withInitial(() -> new byte[0]);

	private net.jpountz.lz4.LZ4Compressor compressor;
	private LZ4FastDecompressor decompressor;

	public LZ4Compressor() {
		LZ4Factory factory = LZ4Factory.fastestInstance();
		compressor = factory.fastCompressor();
		decompressor = factory.fastDecompressor();
	}

	protected ByteBuffer compress(boolean prefixUncompressedSize, ByteBuffer src, IntFunction<ByteBuffer> bufferAllocator) {
		int uncompressedSizeBytes = 4;
		int uncompressedSize = src.remaining();
		int maxCompressedSize = compressor.maxCompressedLength(uncompressedSize);
		int totalSize = maxCompressedSize;
		int startCompressedPos = 0;
		if (prefixUncompressedSize) {
			totalSize += uncompressedSizeBytes;
			startCompressedPos = uncompressedSizeBytes;
		}

		ByteBuffer dest = bufferAllocator.apply(totalSize);

		if (prefixUncompressedSize) {
			ByteOrder originalOrder = dest.order();
			dest.order(ByteOrder.BIG_ENDIAN);
			dest.asIntBuffer().put(uncompressedSize);
			dest.order(originalOrder);
		}
		// set position for compressed part (after header info)
		dest.position(startCompressedPos);

		int compressedLength = compressor.compress(src, src.position(), uncompressedSize, dest, startCompressedPos, maxCompressedSize);
		// make buffer ready for read
		dest.position(0);
		dest.limit(startCompressedPos + compressedLength);

		return dest;
	}
	
	protected ByteBuffer decompress(int decompressedLength, ByteBuffer src, IntFunction<ByteBuffer> bufferAllocator){
		ByteBuffer dest = bufferAllocator.apply(decompressedLength);

		decompressor.decompress(src, src.position(), dest, 0, decompressedLength);
		return dest;
	}
	
	
	@Override
	public ByteBuffer compressData(ChannelConfig config, ByteBuffer src, IntFunction<ByteBuffer> bufferAllocator) {
		return compress(false, src, bufferAllocator);
	}
	
	@Override
	public ByteBuffer decompressData(ChannelConfig config, ByteBuffer src, IntFunction<ByteBuffer> bufferAllocator) {
		return decompress(decompressedLength, src, bufferAllocator)
	}

	@Override
	public ByteBuffer compressHeader(ByteBuffer src, IntFunction<ByteBuffer> bufferAllocator) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] decompressHeader(ByteBuffer src, IntFunction<ByteBuffer> bufferAllocator) {
		// TODO Auto-generated method stub
		return null;
	}
	

	@Override
	public ByteBuffer decompress(ByteBuffer src, IntFunction<ByteBuffer> bufferAllocator) {
		int headerBytes = 4;
		int decompressedLength = src.asIntBuffer().get();
		// set position for compressed part (after header info)
		src.position(headerBytes);

		ByteBuffer dest = bufferAllocator.apply(decompressedLength);

		decompressor.decompress(src, headerBytes, dest, 0, decompressedLength);
		return dest;
	}

	@Override
	public byte[] compress(byte[] src, int srcOff, int srcLen, IntFunction<byte[]> arrayAllocator) {
		int headerBytes = 4;
		int maxCompressedLength = compressor.maxCompressedLength(srcLen);

		byte[] tmp = TMP_BYTE_ARRAY_PROVIDER.get();
		if (tmp.length < maxCompressedLength) {
			tmp = new byte[maxCompressedLength];
			TMP_BYTE_ARRAY_PROVIDER.set(tmp);
		}

		int compressedLength =
				compressor
						.compress(src, srcOff, srcLen, tmp, 0, maxCompressedLength);

		byte[] dest = arrayAllocator.apply(headerBytes + compressedLength);
		System.arraycopy(tmp, 0, dest, headerBytes, compressedLength);

		return dest;
	}

	@Override
	public byte[] decompress(byte[] src, int srcOff, int srcLen, int decompressedLength, IntFunction<byte[]> arrayAllocator) {
		byte[] dest = arrayAllocator.apply(decompressedLength);

		decompressor.decompress(src, srcOff, dest, 0, decompressedLength);
		return dest;
	}
}
