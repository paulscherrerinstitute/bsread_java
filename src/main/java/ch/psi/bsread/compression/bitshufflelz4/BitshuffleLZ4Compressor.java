package ch.psi.bsread.compression.bitshufflelz4;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.IntFunction;

import ch.psi.bitshuffle.BitShuffleLZ4Compressor;
import ch.psi.bitshuffle.BitShuffleLZ4Decompressor;
import ch.psi.bitshuffle.BitShuffleLZ4JNICompressor;
import ch.psi.bitshuffle.BitShuffleLZ4JNIDecompressor;
import ch.psi.bsread.compression.Compressor;
import ch.psi.bsread.converter.ValueConverter;

public class BitshuffleLZ4Compressor implements Compressor {

	private BitShuffleLZ4Compressor compressor;
	private BitShuffleLZ4Decompressor decompressor;

	public BitshuffleLZ4Compressor() {
		compressor = new BitShuffleLZ4JNICompressor();
		decompressor = new BitShuffleLZ4JNIDecompressor();
	}

	protected ByteBuffer compress(ByteBuffer src, int srcOff, int srcLen, ByteOrder sizeOrder, int destOff,
			IntFunction<ByteBuffer> bufferAllocator, int nBytesPerElement) {
		if (nBytesPerElement == ValueConverter.DYNAMIC_NUMBER_OF_BYTES) {
			nBytesPerElement = 1;
		}

		int nElements = srcLen - srcOff;
		if (nElements % nBytesPerElement != 0) {
			throw new RuntimeException("The number of bytes does not correspond to the number of elements, i.e. '" + (srcLen - srcOff) + "' is not dividable by '" + nBytesPerElement + "'");
		}
		nElements /= nBytesPerElement;
		int blockSize = 0;

		int uncompressedSize = srcLen;
		int maxCompressedSize = compressor.maxCompressedLength(nElements, nBytesPerElement, blockSize);
		int startCompressedPos = destOff + 8;
		int totalSize = startCompressedPos + maxCompressedSize;

		ByteBuffer dest = bufferAllocator.apply(totalSize);

		dest.order(sizeOrder);
		dest.position(destOff);
		dest.putInt(uncompressedSize);
		dest.position(destOff + 4);
		dest.putInt(blockSize);
		dest.order(src.order());

		// set position for compressed part (after header info)
		dest.position(startCompressedPos);

		int compressedLength =
				compressor.compress(src, srcOff, dest, startCompressedPos, nElements, nBytesPerElement, blockSize);
		// make buffer ready for read
		dest.position(0);
		dest.limit(startCompressedPos + compressedLength);

		return dest;
	}

	protected ByteBuffer decompress(ByteBuffer src, int srcOff, ByteOrder sizeOrder, IntFunction<ByteBuffer> bufferAllocator, int nBytesPerElement) {
		if (nBytesPerElement == ValueConverter.DYNAMIC_NUMBER_OF_BYTES) {
			nBytesPerElement = 1;
		}

		int startCompressedPos = 8;

		// make sure src does not change in any way (also not temporary)
		int uncompressedSize;
		int blockSize;
		if (src.order() == sizeOrder) {
			uncompressedSize = src.getInt(srcOff);
			blockSize = src.getInt(srcOff + 4);
		} else {
			ByteBuffer dub = src.duplicate().order(sizeOrder);
			uncompressedSize = dub.getInt(srcOff);
			blockSize = dub.getInt(srcOff + 4);
		}

		int nElements = uncompressedSize;
		if (nElements % nBytesPerElement != 0) {
			throw new RuntimeException("The number of bytes does not correspond to the number of elements, i.e. '" + uncompressedSize + "' is not dividable by '" + nBytesPerElement + "'");
		}
		nElements /= nBytesPerElement;

		ByteBuffer dest = bufferAllocator.apply(uncompressedSize);
		dest.order(src.order());

		decompressor.decompress(src, srcOff + startCompressedPos, dest, 0, nElements, nBytesPerElement, blockSize);
		dest.position(0);
		dest.limit(uncompressedSize);
		return dest;
	}

	@Override
	public ByteBuffer compressData(ByteBuffer src, int srcOff, int srcLen, int destOff, IntFunction<ByteBuffer> bufferAllocator, int nBytesPerElement) {
		return compress(src, srcOff, srcLen, src.order(), destOff, bufferAllocator, nBytesPerElement);
	}

	@Override
	public ByteBuffer decompressData(ByteBuffer src, int srcOff, IntFunction<ByteBuffer> bufferAllocator, int nBytesPerElement) {
		ByteBuffer dest = decompress(src, srcOff, src.order(), bufferAllocator, nBytesPerElement);
		return dest;
	}

	@Override
	public int getDecompressedDataSize(ByteBuffer src, int srcOff) {
		return src.getInt(srcOff);
	}

	@Override
	public ByteBuffer compressDataHeader(ByteBuffer src, IntFunction<ByteBuffer> bufferAllocator) {
		return compress(src, src.position(), src.remaining(), ByteOrder.BIG_ENDIAN, 0, bufferAllocator, 1);
	}

	@Override
	public ByteBuffer decompressDataHeader(ByteBuffer src, IntFunction<ByteBuffer> bufferAllocator) {
		return decompress(src, src.position(), ByteOrder.BIG_ENDIAN, bufferAllocator, 1);
	}
}
