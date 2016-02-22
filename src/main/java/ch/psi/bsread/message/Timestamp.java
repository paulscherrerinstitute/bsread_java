package ch.psi.bsread.message;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class Timestamp implements Serializable {
   private static final long serialVersionUID = 2481654141864121974L;

   // the UNIX timestamp
   private long sec;
   // the ns
   private long ns;

   public Timestamp() {}

   public Timestamp(long sec, long ns) {
      this.sec = sec;
      this.ns = ns;
   }

   // public Timestamp(long[] values) {
   // this.ms = values[0];
   // this.nsOffset = values[1];
   // }

   public long getSec() {
      return sec;
   }

   public void setSec(long sec) {
      this.sec = sec;
   }

   public long getNs() {
      return ns;
   }

   public void setNs(long ns) {
      this.ns = ns;
   }

   @JsonIgnore
   public long[] getAsLongArray() {
      return new long[] {sec, ns};
   }

//   @JsonIgnore
//   public long getMillis() {
//      // sec into millis + millis part of ns
//      return sec * 1000L + (ns / 1000000L);
//   }
//
//   @JsonIgnore
//   public double getMillisFractional() {
//      long num = ns / 1000000L * 1000000L;
//      return (ns - num) * 0.000001;
//   }
//   
//   @JsonIgnore
//   public double getSecFractional() {
//	   return ns * 0.000000001;
//   }

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
      return sec + "." + ns;
   }
   
   public static Timestamp ofMillis(long millis){
	   long sec = millis / 1000L;
	   long ns = (millis - (sec * 1000L)) * 1000000L;
	   return new Timestamp(sec, ns);
   }
}
