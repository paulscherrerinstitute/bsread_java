package ch.psi.bsread;

import ch.psi.bsread.basic.BasicReceiver;
import ch.psi.bsread.message.Message;

public class ReceiverExample {

   public static void main(String[] args) {
      IReceiver<Object> receiver = new BasicReceiver();
      // IReceiver<Object> receiver = new Receiver<Object>(new
      // ReceiverConfig<Object>("tcp://localhost:9000", new
      // StandardMessageExtractor<Object>(new MatlabByteConverter())));

      // Terminate program with ctrl+c
      Runtime.getRuntime().addShutdownHook(new Thread(() -> receiver.close()));

      // Its also possible to register callbacks for certain message parts.
      // These callbacks are triggered within the receive() function
      // (within the same thread) it is guaranteed that the sequence is
      // ordered
      // main header, data header, values
      //
      // receiver.addDataHeaderHandler(header -> System.out.println(header));
      // receiver.addMainHeaderHandler(header -> System.out.println(header) );
      // receiver.addValueHandler(data -> System.out.println(data));

      try {
         Message<Object> message;
         // Due to https://github.com/zeromq/jeromq/issues/116 you must not use Thread.interrupt()
         // to stop the receiving thread!
         while ((message = receiver.receive()) != null) {
            System.out.println(message.getMainHeader());
            System.out.println(message.getDataHeader());
            System.out.println(message.getValues());
         }
      } finally {
         // make sure allocated resources get cleaned up (multiple calls to receiver.close() are
         // side effect free - see shutdown hook)
         receiver.close();
      }
   }
}
