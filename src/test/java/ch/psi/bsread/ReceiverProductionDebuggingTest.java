package ch.psi.bsread;

import ch.psi.bsread.basic.BasicReceiver;
import ch.psi.bsread.message.*;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zmq.ZMQ;


public class ReceiverProductionDebuggingTest {
   private static final Logger logger = LoggerFactory.getLogger(ReceiverProductionDebuggingTest.class);

//   @Ignore
   @Test
   public void testReceiver(){
      String sourceAddress = "tcp://SIN-CVME-DBPM0421:9000";
      ReceiverConfig config = new ReceiverConfig(sourceAddress);
//      config.setSocketType(ZMQ.ZMQ_PULL);
      IReceiver<Object> receiver = new BasicReceiver(config);

      try {

         receiver.connect();

         logger.info("Start analysis ...");

         int counter = 0;

         Message<Object> message;
         // TODO need to be able to loop specified number of times / period of time
         while ((message = receiver.receive()) != null) {
            counter++;
            logger.info("{}", message.getMainHeader());

            if(counter > 4){
               break;
            }
         }
      } finally {
         receiver.close();
      }

   }
}
