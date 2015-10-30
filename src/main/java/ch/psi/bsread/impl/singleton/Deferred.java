package ch.psi.bsread.impl.singleton;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * Copy of ch.psi.daq.common.concurrent.singleton.Deferred
 */
public final class Deferred<T> {
   private volatile Supplier<T> supplier = null;
   private T object = null;

   public Deferred(Supplier<T> supplier) {
      this.supplier = Objects.requireNonNull(supplier);
   }

   public T get() {
      if (supplier != null) {
         synchronized (this) {
            if (supplier != null) {
               object = supplier.get();
               supplier = null;
            }
         }
      }
      return object;
   }
}