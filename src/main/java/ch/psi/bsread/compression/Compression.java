package ch.psi.bsread.compression;

import ch.psi.bsread.compression.bitshufflelz4.BitshuffleLZ4Compressor;
import ch.psi.bsread.compression.lz4.LZ4Compressor;
import ch.psi.bsread.compression.none.NoneCompressor;

public enum Compression {
   none(new NoneCompressor()),
   lz4(new LZ4Compressor()),
   bitshuffle_lz4(new BitshuffleLZ4Compressor());
   
   public static final Compression DEFAULT = Compression.bitshuffle_lz4;

   private Compressor compressor;

   private Compression(Compressor compressor) {
      this.compressor = compressor;
   }

   public Compressor getCompressor() {
      return compressor;
   }

   public static Compression getCompressionAlgo(String type) {
      for (Compression algo : Compression.values()) {
         if (algo.name().equalsIgnoreCase(type)) {
            return algo;
         }
      }
      throw new IllegalArgumentException("Type does not exist - " + type);
   }
}
