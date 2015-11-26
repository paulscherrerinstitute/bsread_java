package ch.psi.bsread.compression.none;

import java.nio.ByteBuffer;
import java.util.function.IntFunction;

import ch.psi.bsread.compression.Compressor;

public class NoneCompressor implements Compressor {

	@Override
	public ByteBuffer compressData(ByteBuffer src, int srcOff, int srcLen, int destOff, IntFunction<ByteBuffer> bufferAllocator, int nBytesPerElement) {
		if(destOff > 0){
		   ByteBuffer dest = bufferAllocator.apply(destOff + src.remaining()).order(src.order());
		   dest.position(destOff);
		   ByteBuffer dub = src.duplicate().order(src.order());
		   dub.position(srcOff);
		   dub.limit(srcOff + srcLen); 
		   dest.put(dub);
		   dest.position(0);
		   return dest;
		}else{
		   return src.duplicate().order(src.order());
		}
	}

	@Override
	public ByteBuffer decompressData(ByteBuffer src, int srcOff, IntFunction<ByteBuffer> bufferAllocator, int nBytesPerElement) {
	   ByteBuffer ret = src.duplicate().order(src.order());
	   ret.position(srcOff);
	   return ret;
	}

	@Override
	public int getDecompressedDataSize(ByteBuffer src, int srcOff) {
		return src.remaining() - srcOff;
	}

	@Override
	public ByteBuffer compressDataHeader(ByteBuffer src, IntFunction<ByteBuffer> bufferAllocator) {
		return src.duplicate().order(src.order());
	}

	@Override
	public ByteBuffer decompressDataHeader(ByteBuffer src, IntFunction<ByteBuffer> bufferAllocator) {
	   return src.duplicate().order(src.order());
	}
}
