package ch.psi.bsread.message;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class Timestamp implements Serializable {
   private static final long serialVersionUID = 2481654141864121974L;

   // need to be able to represent sec as millisecs
   private static final long MAX_SEC = Long.MAX_VALUE / 1000L;
   private static final long MAX_NS = 1000000000L - 1L;

   // the UNIX timestamp
   private long sec;
   // the ns
   private long ns;

   public Timestamp() {}

   public Timestamp(long sec, long ns) {
      this.setSec(sec);
      this.setNs(ns);
   }

   public long getSec() {
      return sec;
   }

   public void setSec(long sec) {
      if (sec > MAX_SEC) {
         throw new IllegalArgumentException(
               String.format("Seconds need to be smaller than '%s' but was '%s'", MAX_SEC, sec));
      }
      this.sec = sec;
   }

   public long getNs() {
      return ns;
   }

   public void setNs(long ns) {
      if (ns > MAX_NS || ns < 0) {
         throw new IllegalArgumentException(String.format(
               "Nanoseconds need to be in the range of '0 - %s' but was '%s'", MAX_NS, ns));
      }
      this.ns = ns;
   }

   @JsonIgnore
   public long[] getAsLongArray() {
      return new long[] {sec, ns};
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + (int) (sec ^ (sec >>> 32));
      result = prime * result + (int) (ns ^ (ns >>> 32));
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      Timestamp other = (Timestamp) obj;
      if (sec != other.sec)
         return false;
      if (ns != other.ns)
         return false;
      return true;
   }

   @Override
   public String toString() {
      return sec + "." + String.format("%09d", ns);
   }

   public static Timestamp ofMillis(long millis) {
      long sec = millis / 1000L;
      long ns = (millis - (sec * 1000L)) * 1000000L;
      return new Timestamp(sec, ns);
   }
   
   public static Timestamp ofMillis(long millis, long nsOffset) {
      long sec = millis / 1000L;
      long ns = (millis - (sec * 1000L)) * 1000000L + (nsOffset % 1000000L);
      return new Timestamp(sec, ns);
   }
}
