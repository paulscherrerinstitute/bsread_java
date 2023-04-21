package ch.psi.bsread.compression.lz4;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.IntFunction;

import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import ch.psi.bsread.compression.Compressor;
import java.util.Arrays;

public class LZ4Compressor implements Compressor {

	private net.jpountz.lz4.LZ4Compressor compressor;
	private LZ4FastDecompressor decompressor;

	public LZ4Compressor() {
		LZ4Factory factory = LZ4Factory.fastestInstance();
		compressor = factory.fastCompressor();
		decompressor = factory.fastDecompressor();
	}

	protected ByteBuffer compress(ByteBuffer src, int srcOff, int srcLen, ByteOrder sizeOrder, int destOff,
			IntFunction<ByteBuffer> bufferAllocator) {
		int uncompressedSize = srcLen;
		int maxCompressedSize = compressor.maxCompressedLength(uncompressedSize);
		int startCompressedPos = destOff + 4;
		int totalSize = startCompressedPos + maxCompressedSize;

		ByteBuffer dest = bufferAllocator.apply(totalSize);

		dest.order(sizeOrder);
		dest.position(destOff);
		dest.putInt(uncompressedSize);
		dest.order(src.order());

		// set position for compressed part (after header info)
		dest.position(startCompressedPos);

		int compressedLength =
				compressor.compress(src, srcOff, uncompressedSize, dest, startCompressedPos, maxCompressedSize);
                //compressedLength = ((compressedLength + 7) & ~7);   //Round to next multiple of 8
		// make buffer ready for read
		dest.position(0);
                dest.limit(startCompressedPos + compressedLength);         

      //### Alternativelly fixing bug of ZMQ 0.5.x in EncoderBase
      //dest = dest.slice(0, startCompressedPos + compressedLength); //Only Java>13  
      /* Java<13
        if (dest.hasArray()) {
            dest =  ByteBuffer.wrap(Arrays.copyOf(dest.array(), startCompressedPos + compressedLength));
        } else {
            byte[] arr = new byte[startCompressedPos + compressedLength];
            dest.get(arr, 0, startCompressedPos + compressedLength);            
            dest =  ByteBuffer.wrap(arr);
        }      
        */                
                
                return dest;
	}

	protected ByteBuffer decompress(ByteBuffer src, int srcOff, ByteOrder sizeOrder, IntFunction<ByteBuffer> bufferAllocator) {
		int startCompressedPos = 4;

		// make sure src does not change in any way (also not temporary)
		int uncompressedSize;
		if(src.order() == sizeOrder){
		   uncompressedSize = src.getInt(srcOff);
		}else{
		   uncompressedSize = src.duplicate().order(sizeOrder).getInt(srcOff);
		}

		ByteBuffer dest = bufferAllocator.apply(uncompressedSize);
		dest.order(src.order());

		decompressor.decompress(src, srcOff + startCompressedPos, dest, 0, uncompressedSize);
	    dest.position(0);
	    dest.limit(uncompressedSize);
		return dest;
	}

	@Override
	public ByteBuffer compressData(ByteBuffer src, int srcOff, int srcLen, int destOff, IntFunction<ByteBuffer> bufferAllocator, int nBytesPerElement) {
		return compress(src, srcOff, srcLen, src.order(), destOff, bufferAllocator);
	}

	@Override
	public ByteBuffer decompressData(ByteBuffer src, int srcOff, IntFunction<ByteBuffer> bufferAllocator, int nBytesPerElement) {
		ByteBuffer dest = decompress(src, srcOff, src.order(), bufferAllocator);
		return dest;
	}

	@Override
	public int getDecompressedDataSize(ByteBuffer src, int srcOff) {
		return src.getInt(srcOff);
	}

	@Override
	public ByteBuffer compressDataHeader(ByteBuffer src, IntFunction<ByteBuffer> bufferAllocator) {
		return compress(src, src.position(), src.remaining(), ByteOrder.BIG_ENDIAN, 0, bufferAllocator);
	}

	@Override
	public ByteBuffer decompressDataHeader(ByteBuffer src, IntFunction<ByteBuffer> bufferAllocator) {
		return decompress(src, src.position(), ByteOrder.BIG_ENDIAN, bufferAllocator);
	}
}
