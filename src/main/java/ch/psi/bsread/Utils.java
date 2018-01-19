package ch.psi.bsread;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static java.time.temporal.ChronoField.YEAR;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

import ch.psi.bsread.message.Timestamp;

public class Utils {
   private static final String BIND_IDENTIFIER = "*";
   private static final String DEFULT_TOPIC = "";

   // keep in sync with TimeUtils.ISO_OFFSET_DATE_TIME
   public static final DateTimeFormatter ISO_OFFSET_DATE_TIME;
   static {
      ISO_OFFSET_DATE_TIME = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .appendValue(YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
            .appendLiteral('-')
            .appendValue(MONTH_OF_YEAR, 2)
            .appendLiteral('-')
            .appendValue(DAY_OF_MONTH, 2)
            .appendLiteral('T')
            .appendValue(HOUR_OF_DAY, 2)
            .appendLiteral(':')
            .appendValue(MINUTE_OF_HOUR, 2)
            // .optionalStart() this is different - ensure always full
            // format
            .appendLiteral(':')
            .appendValue(SECOND_OF_MINUTE, 2)
            .optionalStart()
            .appendFraction(NANO_OF_SECOND, 9, 9, true) // ensure always 9
                                                        // decimals
            .appendOffset("+HH:MM", "+00:00")
            .toFormatter();
   }

   /**
    * Generate the hexadecimal MD5 sum string of a string
    * 
    * @param string String to compute MD5 sum
    * @return MD5 sum of string in hex format
    */
   public static String computeMD5(String string) {
      return Utils.computeMD5(string.getBytes(StandardCharsets.UTF_8));
   }

   /**
    * Generate the hexadecimal MD5 sum string of a byte array
    * 
    * @param bytes Bytes to compute MD5 sum
    * @return MD5 sum of bytes in hex format
    */
   public static String computeMD5(byte[] bytes) {
      try {
         MessageDigest md = MessageDigest.getInstance("MD5");
         StringBuffer hexString = new StringBuffer();
         for (byte b : md.digest(bytes)) {
            hexString.append(Integer.toHexString(0xFF & b));
         }
         return hexString.toString();
      } catch (NoSuchAlgorithmException e) {
         throw new RuntimeException(e);
      }
   }

   /**
    * Formats the time in the system's default timezone, using the ISO8601 time format to the nano
    * seconds resolution.
    * 
    * @param time The time
    * @return String The formated time
    */
   public static String format(Timestamp time) {
      return OffsetDateTime.ofInstant(Instant.ofEpochSecond(time.getSec(), time.getNs()),
            ZoneId.systemDefault()).format(ISO_OFFSET_DATE_TIME);
   }

   /**
    * Setup of the connection.
    * 
    * @param socket The Socket
    * @param address The address
    * @param socketType The socket type
    */
   public static void connect(final Socket socket, final String address, final int socketType) {
      connect(socket, address, socketType, DEFULT_TOPIC);
   }

   /**
    * Setup of the connection.
    * 
    * @param socket The Socket
    * @param address The address
    * @param socketType The socket type
    * @param topic The topic to subscribe
    */
   public static void connect(final Socket socket, final String address, final int socketType, final String topic) {
      if (address.contains(BIND_IDENTIFIER)) {
         socket.bind(address);
      } else {
         socket.connect(address);
      }

      if (ZMQ.SUB == socketType) {
         socket.subscribe(topic.getBytes(StandardCharsets.UTF_8));
      }
   }
}
