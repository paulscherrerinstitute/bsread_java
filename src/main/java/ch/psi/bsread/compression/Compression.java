package ch.psi.bsread.compression;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.psi.bsread.compression.bitshufflelz4.BitshuffleLZ4Compressor;
import ch.psi.bsread.compression.lz4.LZ4Compressor;
import ch.psi.bsread.compression.none.NoneCompressor;

public enum Compression {
   none((byte) 0, new NoneCompressor()),  
   bitshuffle_lz4((byte) 1, new BitshuffleLZ4Compressor()),
   lz4((byte) 2, new LZ4Compressor());

   public static final Compression DEFAULT = Compression.bitshuffle_lz4;
   private static final Logger LOGGER = LoggerFactory.getLogger(Compression.class);

   private byte id;
   private Compressor compressor;

   private Compression(byte id, Compressor compressor) {
      this.id = id;
      this.compressor = compressor;
   }

   public byte getId() {
      return id;
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

      final String message =
            String.format("There is no compression '%s'. Available compressions '%s'.", type,
                  Arrays.toString(Compression.values()));
      LOGGER.error(message);
      throw new NullPointerException(message);
   }

   public static Compression byId(byte id) {
      for (Compression algo : Compression.values()) {
         if (algo.id == id) {
            return algo;
         }
      }

      final String message =
            String.format("There is no compression with id '%d'. Available compressions '%s'.", id,
                  Arrays.toString(Compression.values()));
      LOGGER.error(message);
      throw new IllegalArgumentException(message);
   }
}
