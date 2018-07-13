package ch.psi.bsread.message;

public class IllegalTimeException extends RuntimeException {
   private static final long serialVersionUID = -3177825256950919040L;

   public IllegalTimeException() {
       super();
   }

   public IllegalTimeException(String message) {
       super(message);
   }

   public IllegalTimeException(String message, Throwable cause) {
       super(message, cause);
   }

   public IllegalTimeException(Throwable cause) {
       super(cause);
   }
}
