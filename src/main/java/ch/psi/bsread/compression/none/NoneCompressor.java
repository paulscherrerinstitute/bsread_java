package ch.psi.bsread.compression.none;

import java.nio.ByteBuffer;
import java.util.function.IntFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.psi.bsread.compression.Compressor;

public class NoneCompressor implements Compressor {
	private static final Logger LOGGER = LoggerFactory.getLogger(NoneCompressor.class);

	@Override
	public ByteBuffer compressData(ByteBuffer src, int srcOff, int srcLen, int destOff, IntFunction<ByteBuffer> bufferAllocator, int nBytesPerElement) {
		if (destOff <= srcOff) {
			// assume src ByteBuffer was initialized with space for header
			// information. In case this space is large enough, reuse existing
			// ByteBuffer and avoid copying.
			
			ByteBuffer dest = src.duplicate().order(src.order());
			dest.position(srcOff - destOff);
			return dest;
		} else {
			//LOGGER.info("Need to copy received ByteBuffer. This could be avoided with correct Msg header allocation (srcOff '{}' destOff '{}').", srcOff, destOff);

			ByteBuffer dest = bufferAllocator.apply(destOff + src.remaining()).order(src.order());
			dest.position(destOff);
			ByteBuffer dub = src.duplicate().order(src.order());
			dub.position(srcOff);
			dub.limit(srcOff + srcLen);
			dest.put(dub);
			dest.position(0);
			return dest;
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
