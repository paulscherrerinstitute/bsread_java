package ch.psi.bsread.sync;

public class SyncChannelImpl implements SyncChannel {
   private static final String TEST_CHANNEL = "TestChannel";
   private String name;
   private long modulo;
   private long offset;

   protected SyncChannelImpl(final long modulo, final long offset) {
      this(TEST_CHANNEL, modulo, offset);
   }

   public SyncChannelImpl(final String name) {
      this(name, 1, 0);
   }

   public SyncChannelImpl(final String name, final long modulo) {
      this(name, modulo, 0);
   }

   public SyncChannelImpl(final SyncChannel syncChannel) {
      this(syncChannel.getName(), syncChannel.getModulo(), syncChannel.getOffset());
   }

   public SyncChannelImpl(final String name, final long modulo, final long offset) {
      this.name = name;
      this.modulo = modulo;
      this.offset = offset;
   }

   @Override
   public String getName() {
      return name;
   }

   public void setName(String name) {
      this.name = name;
   }

   @Override
   public int getModulo() {
      return (int) modulo;
   }

   // @JsonIgnore
   // public long getModulo2() {
   // return modulo;
   // }

   public void setModulo(int modulo) {
      this.modulo = modulo;
   }

   @Override
   public int getOffset() {
      return (int) offset;
   }

   // @JsonIgnore
   // public long getOffset2() {
   // return offset;
   // }

   public void setOffset(int offset) {
      this.offset = offset;
   }
}
