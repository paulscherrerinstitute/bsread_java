package ch.psi.bsread.stream;

import java.util.function.Consumer;

import ch.psi.bsread.message.DataHeader;

public class SynchronizedDataHeaderConsumer implements Consumer<DataHeader> {
   private final Consumer<DataHeader> consumer;
   private volatile DataHeader currentDataHeader;

   public SynchronizedDataHeaderConsumer(final Consumer<DataHeader> consumer) {
      this.consumer = consumer;
   }

   @Override
   public void accept(final DataHeader dataHeader) {
      DataHeader locDataHeader = currentDataHeader;
      if (locDataHeader == null || !locDataHeader.equals(dataHeader)) {
         synchronized (this) {
            locDataHeader = currentDataHeader;
            if (locDataHeader == null || !locDataHeader.equals(dataHeader)) {
               consumer.accept(dataHeader);
               currentDataHeader = dataHeader;
            }
         }
      }
   }
}
