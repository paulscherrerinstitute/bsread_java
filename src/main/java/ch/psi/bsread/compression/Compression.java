package ch.psi.bsread.compression;

import ch.psi.bsread.compression.lz4.LZ4Compressor;

public enum Compression {
   lz4((byte) 0, new LZ4Compressor());

   private byte type;
   private Compressor compressor;

   private Compression(byte type, Compressor compressor) {
      this.type = type;
      this.compressor = compressor;
   }

   public byte getType() {
      return type;
   }

   public Compressor getCompressor() {
      return compressor;
   }

   public static Compression getCompressionAlgo(byte type) {
      for (Compression algo : Compression.values()) {
         if (type == algo.type) {
            return algo;
         }
      }
      throw new IllegalArgumentException("Type does not exist - " + type);
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
