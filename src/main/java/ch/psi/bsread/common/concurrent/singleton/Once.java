package ch.psi.bsread.common.concurrent.singleton;

import java.util.function.Supplier;

/**
 * Lazy initialization of a singleton when the blueprint is only known at creation time.
 */
public final class Once<T> {
   private volatile boolean initialized = false;
   private T object = null;

   public T get(Supplier<T> supplier) {
      if (!initialized) {
         synchronized (this) {
            if (!initialized) {
               object = supplier.get();
               initialized = true;
            }
         }
      }
      return object;
   }
}
