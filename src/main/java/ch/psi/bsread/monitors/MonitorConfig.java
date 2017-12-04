package ch.psi.bsread.monitors;

import java.util.UUID;

import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import com.fasterxml.jackson.databind.ObjectMapper;

public class MonitorConfig {  
   private Context context;
   private Socket socket;
   private String monitorItentifier;
   private ObjectMapper objectMapper;
   private int socketType;
   private boolean sendStopMessage;
   private boolean blockingSend;

   public MonitorConfig(Context context, Socket socket, ObjectMapper objectMapper, int socketType, boolean blockingSend) {
      this(context, socket, objectMapper, socketType, blockingSend, true, UUID.randomUUID().toString());
   }

   public MonitorConfig(Context context, Socket socket, ObjectMapper objectMapper, int socketType,
         boolean blockingSend, boolean sendStopMessage, String monitorItentifier) {
      this.context = context;
      this.socket = socket;
      this.objectMapper = objectMapper;
      this.socketType = socketType;
      this.blockingSend = blockingSend;
      this.sendStopMessage = sendStopMessage;
      this.monitorItentifier = monitorItentifier;
   }

   public Context getContext() {
      return context;
   }

   public void setContext(Context context) {
      this.context = context;
   }

   public Socket getSocket() {
      return socket;
   }

   public void setSocket(Socket socket) {
      this.socket = socket;
   }

   public String getMonitorItentifier() {
      return monitorItentifier;
   }

   public void setMonitorItentifier(String monitorItentifier) {
      this.monitorItentifier = monitorItentifier;
   }

   public ObjectMapper getObjectMapper() {
      return objectMapper;
   }

   public void setObjectMapper(ObjectMapper objectMapper) {
      this.objectMapper = objectMapper;
   }

   public int getSocketType() {
      return socketType;
   }

   public void setSocketType(int socketType) {
      this.socketType = socketType;
   }
   
   public boolean isBlockingSend() {
      return blockingSend;
   }

   public void setBlockingSend(boolean blockingSend) {
      this.blockingSend = blockingSend;
   }
   
   public boolean isSendStopMessage() {
      return sendStopMessage;
   }

   public void setSendStopMessage(boolean sendStopMessage) {
      this.sendStopMessage = sendStopMessage;
   }
}
