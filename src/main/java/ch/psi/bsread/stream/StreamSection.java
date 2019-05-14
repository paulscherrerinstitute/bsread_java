package ch.psi.bsread.stream;

import java.util.stream.Stream;

import ch.psi.bsread.message.Value;

public interface StreamSection<T> {
   public static final long DEFAULT_TIMEOUT_IN_MILLIS = Value.DEFAULT_TIMEOUT_IN_MILLIS;

   /**
    * Provides the currently active value.
    * 
    * @return the current value
    */
   public T getCurrent();

   /**
    * Provides the value which was the current value in the last iteration.
    * 
    * @return the value or null
    */
   public T getPreviousCurrent();

   /**
    * Provides the value which will become the current value in the next iteration.
    * 
    * @return the value or null
    */
   public T getNextCurrent();

   /**
    * Provides the value which will expire/retire in the next iteration.
    * 
    * @return the expiring value
    */
   public T getExpiring();

   /**
    * Provides the value which joined the section in the current iteration.
    * 
    * @return the joining value
    */
   public T getJoining();

   /**
    * Provides a view on all elements in the section (past, current, and future) in ascending order
    * from oldest to the youngest value.
    * 
    * @return Stream the values
    */
   public Stream<T> getAll();

   /**
    * Provides a view on all elements in the section (past, current, and future)
    * 
    * @param ascending true orders from oldest to the youngest value, false orders
    *        from youngest to the oldest value.
    * @return Stream the values
    */
   public Stream<T> getAll(boolean ascending);

   /**
    * Provides a view on all elements older than the current value in ascending order from oldest to
    * the youngest value.
    * 
    * @return Collection the values
    */
   public Stream<T> getPast();

   /**
    * Provides a view on all elements older than the current value.
    * 
    * @param ascending true orders from oldest to the youngest value, false orders
    *        from youngest to the oldest value.
    * @return Stream the values
    */
   public Stream<T> getPast(boolean ascending);

   /**
    * Provides a view on all elements younger than the current value in ascending order from oldest
    * to the youngest value.
    * 
    * @return Stream the values
    */
   public Stream<T> getFuture();

   /**
    * Provides a view on all elements younger than the current value.
    * 
    * @param ascending true orders from oldest to the youngest value, false orders
    *        from youngest to the oldest value.
    * @return Stream the values
    */
   public Stream<T> getFuture(boolean ascending);
}
