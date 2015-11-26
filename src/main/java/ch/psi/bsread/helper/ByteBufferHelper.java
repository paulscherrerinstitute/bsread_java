package ch.psi.bsread.helper;

import java.nio.ByteBuffer;

/**
 * Copy of ch.psi.daq.common.helper.ByteBufferHelper
 */
public class ByteBufferHelper {

   /**
	 * Copies a ByteBuffer into a newly allocated ByteBuffer
	 * 
	 * @param buffer
	 *            The ByteBuffer to copy
	 * @return ByteBuffer The copy
	 */
	public static ByteBuffer copy(ByteBuffer buffer) {
		ByteBuffer copy;
		if (buffer.isDirect()) {
			copy = ByteBuffer.allocateDirect(buffer.remaining());
		} else {
			copy = ByteBuffer.allocate(buffer.remaining());
		}

		copy.put(buffer.duplicate());
		copy.flip();
		return copy;
	}

	/**
	 * Converts the received ByteBuffer into a direct ByteBuffer (might return
	 * the same reference in case buffer is already direct).
	 * 
	 * @param buffer
	 *            The ByteBuffer
	 * @return Buffer The direct ByteBuffer
	 */
	public static ByteBuffer asDirect(ByteBuffer buffer) {
		if (buffer.isDirect()) {
			return buffer;
		} else {
			ByteBuffer direct = ByteBuffer.allocateDirect(buffer.remaining());

			direct.order(buffer.order());
			direct.put(buffer.duplicate());
			direct.flip();
			return direct;
		}
	}

	/**
	 * Copies a ByteBuffer into a newly allocated byte array (does not use
	 * underlying byte array of some ByteBuffer implementations)
	 * 
	 * @param buffer
	 *            The ByteBuffer to copy
	 * @return byte[] The copy
	 */
	public static byte[] copyToByteArray(ByteBuffer buffer) {
		byte[] copy = new byte[buffer.remaining()];
		buffer.duplicate().get(copy);
		return copy;
	}

	/**
	 * Extracts a byte array that contains the content of the ByteBuffer (might
	 * reuse underlying byte arrays of some ByteBuffer implementations).
	 * 
	 * @param buffer
	 *            The ByteBuffer
	 * @return byte[] The byte array
	 */
	public static byte[] extractByteArray(ByteBuffer buffer) {
		if (buffer.hasArray() && buffer.position() == 0 && buffer.remaining() == buffer.capacity()) {
			return buffer.array();
		} else {
			return copyToByteArray(buffer);
		}
	}
	
	   /**
	    * Generates a string from a ByteBuffer.
	    * 
	    * @param buffer The ByteBuffer
	    * @param delemiter The delemiter of the bytes of the ByteBuffer
	    * @param header An optional header
	    * @return String the String
	    */
	   public static String toString(ByteBuffer buffer,  String delemiter, String header){
	      StringBuilder builder = new StringBuilder();
	      
	      if(header != null && !"".equals(header)){
	         builder.append(header).append("\n");
	      }
	      
	      builder.append(buffer.position()).append(", ");
	      builder.append(buffer.limit()).append(", ");
	      builder.append(buffer.remaining()).append(", ");
	      builder.append(buffer.order());
	      builder.append("\n");
	      
	      for(int i = 0; i < buffer.remaining(); ){
	         builder.append(buffer.get(buffer.position() + i));
	         ++i;
	         if(i < buffer.remaining()){
	            builder.append(delemiter);
	         }
	      }
	      
	      return builder.toString();
	   }
}
