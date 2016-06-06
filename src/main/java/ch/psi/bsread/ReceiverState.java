package ch.psi.bsread;

import java.nio.ByteBuffer;
import java.util.function.IntFunction;

import ch.psi.bsread.common.allocator.ByteBufferAllocator;
import ch.psi.bsread.common.allocator.ReuseByteBufferAllocator;
import ch.psi.bsread.message.DataHeader;

public class ReceiverState {
	private String dataHeaderHash = "unknown";
	private DataHeader dataHeader = null;
	private final IntFunction<ByteBuffer> dataHeaderAllocator = new ReuseByteBufferAllocator(ByteBufferAllocator.DEFAULT_ALLOCATOR);

	public ReceiverState() {
	}

	public String getDataHeaderHash() {
		return dataHeaderHash;
	}

	public void setDataHeaderHash(String dataHeaderHash) {
		this.dataHeaderHash = dataHeaderHash;
	}

	public DataHeader getDataHeader() {
		return dataHeader;
	}

	public void setDataHeader(DataHeader dataHeader) {
		this.dataHeader = dataHeader;
	}
	
	public IntFunction<ByteBuffer> getDataHeaderAllocator() {
		return dataHeaderAllocator;
	}
}
