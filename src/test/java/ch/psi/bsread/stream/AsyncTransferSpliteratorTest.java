package ch.psi.bsread.stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

public class AsyncTransferSpliteratorTest {
   
   private ExecutorService executor = Executors.newCachedThreadPool();

   @Test
   public void testPastFutureSize_0() {
      AsyncTransferSpliterator<Long> spliterator = new AsyncTransferSpliterator<Long>(0, 0);

      spliterator.onAvailable(Long.valueOf(0));
      assertEquals(1, spliterator.getSize());

      StreamSection<Long> section = spliterator.getNext(false);
      assertEquals(1, spliterator.getSize());
      assertEquals(Long.valueOf(0), section.getCurrent());
      Iterator<Long> pastIter = section.getPast(true).iterator();
      assertFalse(pastIter.hasNext());

      pastIter = section.getPast(false).iterator();
      assertFalse(pastIter.hasNext());

      Iterator<Long> futureIter = section.getFuture(true).iterator();
      assertFalse(futureIter.hasNext());
      futureIter = section.getFuture(false).iterator();
      assertFalse(futureIter.hasNext());

      spliterator.onAvailable(Long.valueOf(1));
      assertEquals(2, spliterator.getSize());

      section = spliterator.getNext(false);
      assertEquals(1, spliterator.getSize());
      assertEquals(Long.valueOf(1), section.getCurrent());
      pastIter = section.getPast(true).iterator();
      assertFalse(pastIter.hasNext());

      pastIter = section.getPast(false).iterator();
      assertFalse(pastIter.hasNext());

      futureIter = section.getFuture(true).iterator();
      assertFalse(futureIter.hasNext());
      futureIter = section.getFuture(false).iterator();
      assertFalse(futureIter.hasNext());

      spliterator.onAvailable(Long.valueOf(2));
      assertEquals(2, spliterator.getSize());

      section = spliterator.getNext(false);
      assertEquals(1, spliterator.getSize());
      assertEquals(Long.valueOf(2), section.getCurrent());
      pastIter = section.getPast(true).iterator();
      assertFalse(pastIter.hasNext());

      pastIter = section.getPast(false).iterator();
      assertFalse(pastIter.hasNext());

      futureIter = section.getFuture(true).iterator();
      assertFalse(futureIter.hasNext());
      futureIter = section.getFuture(false).iterator();
      assertFalse(futureIter.hasNext());

      spliterator.onAvailable(Long.valueOf(3));
      assertEquals(2, spliterator.getSize());

      section = spliterator.getNext(false);
      assertEquals(1, spliterator.getSize());
      assertEquals(Long.valueOf(3), section.getCurrent());
      pastIter = section.getPast(true).iterator();
      assertFalse(pastIter.hasNext());

      pastIter = section.getPast(false).iterator();
      assertFalse(pastIter.hasNext());

      futureIter = section.getFuture(true).iterator();
      assertFalse(futureIter.hasNext());
      futureIter = section.getFuture(false).iterator();
      assertFalse(futureIter.hasNext());

      spliterator.onAvailable(Long.valueOf(4));
      assertEquals(2, spliterator.getSize());

      section = spliterator.getNext(false);
      assertEquals(1, spliterator.getSize());
      assertEquals(Long.valueOf(4), section.getCurrent());
      pastIter = section.getPast(true).iterator();
      assertFalse(pastIter.hasNext());

      pastIter = section.getPast(false).iterator();
      assertFalse(pastIter.hasNext());

      futureIter = section.getFuture(true).iterator();
      assertFalse(futureIter.hasNext());
      futureIter = section.getFuture(false).iterator();
      assertFalse(futureIter.hasNext());

      CompletableFuture<StreamSection<Long>> future = CompletableFuture.supplyAsync(() -> spliterator.getNext(false));
      try {
         future.get(1, TimeUnit.SECONDS);
         assertTrue(false);
      } catch (TimeoutException e) {
         assertTrue(true);
      } catch (Exception e) {
         assertTrue(false);
      }
   }

   @Test
   public void testPastSize_1() {
      AsyncTransferSpliterator<Long> spliterator = new AsyncTransferSpliterator<Long>(1, 0);

      spliterator.onAvailable(Long.valueOf(0));
      assertEquals(1, spliterator.getSize());

      spliterator.onAvailable(Long.valueOf(1));
      assertEquals(2, spliterator.getSize());

      StreamSection<Long> section = spliterator.getNext(false);
      assertEquals(2, spliterator.getSize());
      assertEquals(Long.valueOf(1), section.getCurrent());
      Iterator<Long> pastIter = section.getPast(true).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(0), pastIter.next());
      assertFalse(pastIter.hasNext());

      pastIter = section.getPast(false).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(0), pastIter.next());
      assertFalse(pastIter.hasNext());

      Iterator<Long> futureIter = section.getFuture(true).iterator();
      assertFalse(futureIter.hasNext());
      futureIter = section.getFuture(false).iterator();
      assertFalse(futureIter.hasNext());

      spliterator.onAvailable(Long.valueOf(2));
      assertEquals(3, spliterator.getSize());

      section = spliterator.getNext(false);
      assertEquals(2, spliterator.getSize());
      assertEquals(Long.valueOf(2), section.getCurrent());
      pastIter = section.getPast(true).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(1), pastIter.next());
      assertFalse(pastIter.hasNext());

      pastIter = section.getPast(false).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(1), pastIter.next());
      assertFalse(pastIter.hasNext());

      futureIter = section.getFuture(true).iterator();
      assertFalse(futureIter.hasNext());
      futureIter = section.getFuture(false).iterator();
      assertFalse(futureIter.hasNext());

      spliterator.onAvailable(Long.valueOf(3));
      assertEquals(3, spliterator.getSize());

      section = spliterator.getNext(false);
      assertEquals(2, spliterator.getSize());
      assertEquals(Long.valueOf(3), section.getCurrent());
      pastIter = section.getPast(true).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(2), pastIter.next());
      assertFalse(pastIter.hasNext());

      pastIter = section.getPast(false).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(2), pastIter.next());
      assertFalse(pastIter.hasNext());

      futureIter = section.getFuture(true).iterator();
      assertFalse(futureIter.hasNext());
      futureIter = section.getFuture(false).iterator();
      assertFalse(futureIter.hasNext());

      spliterator.onAvailable(Long.valueOf(4));
      assertEquals(3, spliterator.getSize());

      section = spliterator.getNext(false);
      assertEquals(2, spliterator.getSize());
      assertEquals(Long.valueOf(4), section.getCurrent());
      pastIter = section.getPast(true).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(3), pastIter.next());
      assertFalse(pastIter.hasNext());

      pastIter = section.getPast(false).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(3), pastIter.next());
      assertFalse(pastIter.hasNext());

      futureIter = section.getFuture(true).iterator();
      assertFalse(futureIter.hasNext());
      futureIter = section.getFuture(false).iterator();
      assertFalse(futureIter.hasNext());

      CompletableFuture<StreamSection<Long>> future = CompletableFuture.supplyAsync(() -> spliterator.getNext(false));
      try {
         future.get(1, TimeUnit.SECONDS);
         assertTrue(false);
      } catch (TimeoutException e) {
         assertTrue(true);
      } catch (Exception e) {
         assertTrue(false);
      }
   }

   @Test
   public void testPastSize_1_Wait() {
      AsyncTransferSpliterator<Long> spliterator = new AsyncTransferSpliterator<Long>(1, 0);

      spliterator.onAvailable(Long.valueOf(0));
      assertEquals(1, spliterator.getSize());

      CompletableFuture<StreamSection<Long>> future = CompletableFuture.supplyAsync(() -> spliterator.getNext(false));
      try {
         future.get(1, TimeUnit.SECONDS);
         assertTrue(false);
      } catch (TimeoutException e) {
         assertTrue(true);
      } catch (Exception e) {
         assertTrue(false);
      }

      spliterator.onAvailable(Long.valueOf(1));

      StreamSection<Long> section = future.join();
      assertEquals(Long.valueOf(1), section.getCurrent());
      Iterator<Long> pastIter = section.getPast(true).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(0), pastIter.next());
      assertFalse(pastIter.hasNext());

      pastIter = section.getPast(false).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(0), pastIter.next());
      assertFalse(pastIter.hasNext());

      Iterator<Long> futureIter = section.getFuture(true).iterator();
      assertFalse(futureIter.hasNext());
      futureIter = section.getFuture(false).iterator();
      assertFalse(futureIter.hasNext());
   }

   @Test
   public void testPastSize_2() {
      AsyncTransferSpliterator<Long> spliterator = new AsyncTransferSpliterator<Long>(2, 0);

      spliterator.onAvailable(Long.valueOf(0));
      assertEquals(1, spliterator.getSize());

      spliterator.onAvailable(Long.valueOf(1));
      assertEquals(2, spliterator.getSize());

      spliterator.onAvailable(Long.valueOf(2));
      assertEquals(3, spliterator.getSize());

      StreamSection<Long> section = spliterator.getNext(false);
      assertEquals(3, spliterator.getSize());
      assertEquals(Long.valueOf(2), section.getCurrent());
      Iterator<Long> pastIter = section.getPast(true).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(0), pastIter.next());
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(1), pastIter.next());
      assertFalse(pastIter.hasNext());

      pastIter = section.getPast(false).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(1), pastIter.next());
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(0), pastIter.next());
      assertFalse(pastIter.hasNext());

      Iterator<Long> futureIter = section.getFuture(true).iterator();
      assertFalse(futureIter.hasNext());
      futureIter = section.getFuture(false).iterator();
      assertFalse(futureIter.hasNext());

      spliterator.onAvailable(Long.valueOf(3));
      assertEquals(4, spliterator.getSize());

      section = spliterator.getNext(false);
      assertEquals(3, spliterator.getSize());
      assertEquals(Long.valueOf(3), section.getCurrent());
      pastIter = section.getPast(true).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(1), pastIter.next());
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(2), pastIter.next());
      assertFalse(pastIter.hasNext());

      pastIter = section.getPast(false).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(2), pastIter.next());
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(1), pastIter.next());
      assertFalse(pastIter.hasNext());

      futureIter = section.getFuture(true).iterator();
      assertFalse(futureIter.hasNext());
      futureIter = section.getFuture(false).iterator();
      assertFalse(futureIter.hasNext());

      spliterator.onAvailable(Long.valueOf(4));
      assertEquals(4, spliterator.getSize());

      section = spliterator.getNext(false);
      assertEquals(3, spliterator.getSize());
      assertEquals(Long.valueOf(4), section.getCurrent());
      pastIter = section.getPast(true).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(2), pastIter.next());
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(3), pastIter.next());
      assertFalse(pastIter.hasNext());

      pastIter = section.getPast(false).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(3), pastIter.next());
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(2), pastIter.next());
      assertFalse(pastIter.hasNext());

      futureIter = section.getFuture(true).iterator();
      assertFalse(futureIter.hasNext());
      futureIter = section.getFuture(false).iterator();
      assertFalse(futureIter.hasNext());

      CompletableFuture<StreamSection<Long>> future = CompletableFuture.supplyAsync(() -> spliterator.getNext(false));
      try {
         future.get(1, TimeUnit.SECONDS);
         assertTrue(false);
      } catch (TimeoutException e) {
         assertTrue(true);
      } catch (Exception e) {
         assertTrue(false);
      }
   }

   @Test
   public void testPastSize_2_Wait() {
      AsyncTransferSpliterator<Long> spliterator = new AsyncTransferSpliterator<Long>(2, 0);

      spliterator.onAvailable(Long.valueOf(0));
      assertEquals(1, spliterator.getSize());

      CompletableFuture<StreamSection<Long>> future = CompletableFuture.supplyAsync(() -> spliterator.getNext(false));
      try {
         future.get(1, TimeUnit.SECONDS);
         assertTrue(false);
      } catch (TimeoutException e) {
         assertTrue(true);
      } catch (Exception e) {
         assertTrue(false);
      }

      spliterator.onAvailable(Long.valueOf(1));
      assertEquals(2, spliterator.getSize());

      CompletableFuture<StreamSection<Long>> future2 = CompletableFuture.supplyAsync(() -> spliterator.getNext(false));
      try {
         future2.get(1, TimeUnit.SECONDS);
         assertTrue(false);
      } catch (TimeoutException e) {
         assertTrue(true);
      } catch (Exception e) {
         assertTrue(false);
      }

      spliterator.onAvailable(Long.valueOf(2));

      StreamSection<Long> section = future.join();
      assertEquals(Long.valueOf(2), section.getCurrent());
      Iterator<Long> pastIter = section.getPast(true).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(0), pastIter.next());
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(1), pastIter.next());
      assertFalse(pastIter.hasNext());

      pastIter = section.getPast(false).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(1), pastIter.next());
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(0), pastIter.next());
      assertFalse(pastIter.hasNext());

      Iterator<Long> futureIter = section.getFuture(true).iterator();
      assertFalse(futureIter.hasNext());
      futureIter = section.getFuture(false).iterator();
      assertFalse(futureIter.hasNext());

      spliterator.onAvailable(Long.valueOf(3));

      section = future2.join();
      assertEquals(Long.valueOf(3), section.getCurrent());
      pastIter = section.getPast(true).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(1), pastIter.next());
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(2), pastIter.next());
      assertFalse(pastIter.hasNext());

      pastIter = section.getPast(false).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(2), pastIter.next());
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(1), pastIter.next());
      assertFalse(pastIter.hasNext());

      futureIter = section.getFuture(true).iterator();
      assertFalse(futureIter.hasNext());
      futureIter = section.getFuture(false).iterator();
      assertFalse(futureIter.hasNext());
   }

   @Test
   public void testFutureSize_1() {
      AsyncTransferSpliterator<Long> spliterator = new AsyncTransferSpliterator<Long>(0, 1);

      spliterator.onAvailable(Long.valueOf(0));
      assertEquals(1, spliterator.getSize());

      spliterator.onAvailable(Long.valueOf(1));
      assertEquals(2, spliterator.getSize());

      StreamSection<Long> section = spliterator.getNext(false);
      assertEquals(2, spliterator.getSize());
      assertEquals(Long.valueOf(0), section.getCurrent());
      Iterator<Long> pastIter = section.getPast(true).iterator();
      assertFalse(pastIter.hasNext());
      pastIter = section.getPast(false).iterator();
      assertFalse(pastIter.hasNext());

      Iterator<Long> futureIter = section.getFuture(true).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(1), futureIter.next());
      assertFalse(futureIter.hasNext());

      futureIter = section.getFuture(false).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(1), futureIter.next());
      assertFalse(futureIter.hasNext());

      spliterator.onAvailable(Long.valueOf(2));
      assertEquals(3, spliterator.getSize());

      section = spliterator.getNext(false);
      assertEquals(2, spliterator.getSize());
      assertEquals(Long.valueOf(1), section.getCurrent());
      pastIter = section.getPast(true).iterator();
      assertFalse(pastIter.hasNext());
      pastIter = section.getPast(false).iterator();
      assertFalse(pastIter.hasNext());

      futureIter = section.getFuture(true).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(2), futureIter.next());
      assertFalse(futureIter.hasNext());

      futureIter = section.getFuture(false).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(2), futureIter.next());
      assertFalse(futureIter.hasNext());

      spliterator.onAvailable(Long.valueOf(3));
      assertEquals(3, spliterator.getSize());

      section = spliterator.getNext(false);
      assertEquals(2, spliterator.getSize());
      assertEquals(Long.valueOf(2), section.getCurrent());
      pastIter = section.getPast(true).iterator();
      assertFalse(pastIter.hasNext());
      pastIter = section.getPast(false).iterator();
      assertFalse(pastIter.hasNext());

      futureIter = section.getFuture(true).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(3), futureIter.next());
      assertFalse(futureIter.hasNext());

      futureIter = section.getFuture(false).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(3), futureIter.next());
      assertFalse(futureIter.hasNext());

      spliterator.onAvailable(Long.valueOf(4));
      assertEquals(3, spliterator.getSize());

      section = spliterator.getNext(false);
      assertEquals(2, spliterator.getSize());
      assertEquals(Long.valueOf(3), section.getCurrent());
      pastIter = section.getPast(true).iterator();
      assertFalse(pastIter.hasNext());

      pastIter = section.getPast(false).iterator();
      assertFalse(pastIter.hasNext());

      futureIter = section.getFuture(true).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(4), futureIter.next());
      assertFalse(futureIter.hasNext());

      futureIter = section.getFuture(false).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(4), futureIter.next());
      assertFalse(futureIter.hasNext());

      CompletableFuture<StreamSection<Long>> future = CompletableFuture.supplyAsync(() -> spliterator.getNext(false));
      try {
         future.get(1, TimeUnit.SECONDS);
         assertTrue(false);
      } catch (TimeoutException e) {
         assertTrue(true);
      } catch (Exception e) {
         assertTrue(false);
      }
   }

   @Test
   public void testFutureSize_1_Wait() {
      AsyncTransferSpliterator<Long> spliterator = new AsyncTransferSpliterator<Long>(0, 1);

      spliterator.onAvailable(Long.valueOf(0));
      assertEquals(1, spliterator.getSize());

      CompletableFuture<StreamSection<Long>> future = CompletableFuture.supplyAsync(() -> spliterator.getNext(false));
      try {
         future.get(1, TimeUnit.SECONDS);
         assertTrue(false);
      } catch (TimeoutException e) {
         assertTrue(true);
      } catch (Exception e) {
         assertTrue(false);
      }

      spliterator.onAvailable(Long.valueOf(1));
      assertEquals(2, spliterator.getSize());

      StreamSection<Long> section = future.join();
      assertEquals(2, spliterator.getSize());
      assertEquals(Long.valueOf(0), section.getCurrent());
      Iterator<Long> pastIter = section.getPast(true).iterator();
      assertFalse(pastIter.hasNext());
      pastIter = section.getPast(false).iterator();
      assertFalse(pastIter.hasNext());

      Iterator<Long> futureIter = section.getFuture(true).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(1), futureIter.next());
      assertFalse(futureIter.hasNext());

      futureIter = section.getFuture(false).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(1), futureIter.next());
      assertFalse(futureIter.hasNext());
   }

   @Test
   public void testFutureSize_2() {
      AsyncTransferSpliterator<Long> spliterator = new AsyncTransferSpliterator<Long>(0, 2);

      spliterator.onAvailable(Long.valueOf(0));
      assertEquals(1, spliterator.getSize());

      spliterator.onAvailable(Long.valueOf(1));
      assertEquals(2, spliterator.getSize());

      spliterator.onAvailable(Long.valueOf(2));
      assertEquals(3, spliterator.getSize());

      StreamSection<Long> section = spliterator.getNext(false);
      assertEquals(3, spliterator.getSize());
      assertEquals(Long.valueOf(0), section.getCurrent());
      Iterator<Long> pastIter = section.getPast(true).iterator();
      assertFalse(pastIter.hasNext());
      pastIter = section.getPast(false).iterator();
      assertFalse(pastIter.hasNext());

      Iterator<Long> futureIter = section.getFuture(true).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(1), futureIter.next());
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(2), futureIter.next());
      assertFalse(futureIter.hasNext());

      futureIter = section.getFuture(false).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(2), futureIter.next());
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(1), futureIter.next());
      assertFalse(futureIter.hasNext());

      spliterator.onAvailable(Long.valueOf(3));
      assertEquals(4, spliterator.getSize());

      section = spliterator.getNext(false);
      assertEquals(3, spliterator.getSize());
      assertEquals(Long.valueOf(1), section.getCurrent());
      pastIter = section.getPast(true).iterator();
      assertFalse(pastIter.hasNext());
      pastIter = section.getPast(false).iterator();
      assertFalse(pastIter.hasNext());

      futureIter = section.getFuture(true).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(2), futureIter.next());
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(3), futureIter.next());
      assertFalse(futureIter.hasNext());

      futureIter = section.getFuture(false).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(3), futureIter.next());
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(2), futureIter.next());
      assertFalse(futureIter.hasNext());

      spliterator.onAvailable(Long.valueOf(4));
      assertEquals(4, spliterator.getSize());

      section = spliterator.getNext(false);
      assertEquals(3, spliterator.getSize());
      assertEquals(Long.valueOf(2), section.getCurrent());
      pastIter = section.getPast(true).iterator();
      assertFalse(pastIter.hasNext());

      pastIter = section.getPast(false).iterator();
      assertFalse(pastIter.hasNext());

      futureIter = section.getFuture(true).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(3), futureIter.next());
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(4), futureIter.next());
      assertFalse(futureIter.hasNext());

      futureIter = section.getFuture(false).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(4), futureIter.next());
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(3), futureIter.next());
      assertFalse(futureIter.hasNext());

      CompletableFuture<StreamSection<Long>> future = CompletableFuture.supplyAsync(() -> spliterator.getNext(false));
      try {
         future.get(1, TimeUnit.SECONDS);
         assertTrue(false);
      } catch (TimeoutException e) {
         assertTrue(true);
      } catch (Exception e) {
         assertTrue(false);
      }
   }

   @Test
   public void testFutureSize_2_Wait() {
      AsyncTransferSpliterator<Long> spliterator = new AsyncTransferSpliterator<Long>(0, 2);

      spliterator.onAvailable(Long.valueOf(0));
      assertEquals(1, spliterator.getSize());

      CompletableFuture<StreamSection<Long>> future = CompletableFuture.supplyAsync(() -> spliterator.getNext(false));
      try {
         future.get(1, TimeUnit.SECONDS);
         assertTrue(false);
      } catch (TimeoutException e) {
         assertTrue(true);
      } catch (Exception e) {
         assertTrue(false);
      }

      spliterator.onAvailable(Long.valueOf(1));
      assertEquals(2, spliterator.getSize());

      CompletableFuture<StreamSection<Long>> future2 = CompletableFuture.supplyAsync(() -> spliterator.getNext(false));
      try {
         future2.get(1, TimeUnit.SECONDS);
         assertTrue(false);
      } catch (TimeoutException e) {
         assertTrue(true);
      } catch (Exception e) {
         assertTrue(false);
      }

      spliterator.onAvailable(Long.valueOf(2));

      StreamSection<Long> section = future.join();
      assertEquals(Long.valueOf(0), section.getCurrent());
      Iterator<Long> pastIter = section.getPast(true).iterator();
      assertFalse(pastIter.hasNext());
      pastIter = section.getPast(false).iterator();
      assertFalse(pastIter.hasNext());

      Iterator<Long> futureIter = section.getFuture(true).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(1), futureIter.next());
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(2), futureIter.next());
      assertFalse(futureIter.hasNext());

      futureIter = section.getFuture(false).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(2), futureIter.next());
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(1), futureIter.next());
      assertFalse(futureIter.hasNext());

      spliterator.onAvailable(Long.valueOf(3));

      section = future2.join();
      assertEquals(Long.valueOf(1), section.getCurrent());
      pastIter = section.getPast(true).iterator();
      assertFalse(pastIter.hasNext());
      pastIter = section.getPast(false).iterator();
      assertFalse(pastIter.hasNext());

      futureIter = section.getFuture(true).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(2), futureIter.next());
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(3), futureIter.next());
      assertFalse(futureIter.hasNext());

      futureIter = section.getFuture(false).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(3), futureIter.next());
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(2), futureIter.next());
      assertFalse(futureIter.hasNext());
   }

   @Test
   public void testPastFutureSize_1() {
      AsyncTransferSpliterator<Long> spliterator = new AsyncTransferSpliterator<Long>(1, 1);

      spliterator.onAvailable(Long.valueOf(0));
      assertEquals(1, spliterator.getSize());

      spliterator.onAvailable(Long.valueOf(1));
      assertEquals(2, spliterator.getSize());

      spliterator.onAvailable(Long.valueOf(2));
      assertEquals(3, spliterator.getSize());

      StreamSection<Long> section = spliterator.getNext(false);
      assertEquals(3, spliterator.getSize());
      assertEquals(Long.valueOf(1), section.getCurrent());
      Iterator<Long> pastIter = section.getPast(true).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(0), pastIter.next());
      assertFalse(pastIter.hasNext());

      pastIter = section.getPast(false).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(0), pastIter.next());
      assertFalse(pastIter.hasNext());

      Iterator<Long> futureIter = section.getFuture(true).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(2), futureIter.next());
      assertFalse(futureIter.hasNext());
      futureIter = section.getFuture(false).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(2), futureIter.next());
      assertFalse(futureIter.hasNext());

      spliterator.onAvailable(Long.valueOf(3));
      assertEquals(4, spliterator.getSize());

      section = spliterator.getNext(false);
      assertEquals(3, spliterator.getSize());
      assertEquals(Long.valueOf(2), section.getCurrent());
      pastIter = section.getPast(true).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(1), pastIter.next());
      assertFalse(pastIter.hasNext());

      pastIter = section.getPast(false).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(1), pastIter.next());
      assertFalse(pastIter.hasNext());

      futureIter = section.getFuture(true).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(3), futureIter.next());
      assertFalse(futureIter.hasNext());
      futureIter = section.getFuture(false).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(3), futureIter.next());
      assertFalse(futureIter.hasNext());

      spliterator.onAvailable(Long.valueOf(4));
      assertEquals(4, spliterator.getSize());

      section = spliterator.getNext(false);
      assertEquals(3, spliterator.getSize());
      assertEquals(Long.valueOf(3), section.getCurrent());
      pastIter = section.getPast(true).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(2), pastIter.next());
      assertFalse(pastIter.hasNext());

      pastIter = section.getPast(false).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(2), pastIter.next());
      assertFalse(pastIter.hasNext());

      futureIter = section.getFuture(true).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(4), futureIter.next());
      assertFalse(futureIter.hasNext());
      futureIter = section.getFuture(false).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(4), futureIter.next());
      assertFalse(futureIter.hasNext());

      spliterator.onAvailable(Long.valueOf(5));
      assertEquals(4, spliterator.getSize());

      section = spliterator.getNext(false);
      assertEquals(3, spliterator.getSize());
      assertEquals(Long.valueOf(4), section.getCurrent());
      pastIter = section.getPast(true).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(3), pastIter.next());
      assertFalse(pastIter.hasNext());

      pastIter = section.getPast(false).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(3), pastIter.next());
      assertFalse(pastIter.hasNext());

      futureIter = section.getFuture(true).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(5), futureIter.next());
      assertFalse(futureIter.hasNext());
      futureIter = section.getFuture(false).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(5), futureIter.next());
      assertFalse(futureIter.hasNext());

      spliterator.onAvailable(Long.valueOf(6));
      assertEquals(4, spliterator.getSize());

      section = spliterator.getNext(false);
      assertEquals(3, spliterator.getSize());
      assertEquals(Long.valueOf(5), section.getCurrent());
      pastIter = section.getPast(true).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(4), pastIter.next());
      assertFalse(pastIter.hasNext());

      pastIter = section.getPast(false).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(4), pastIter.next());
      assertFalse(pastIter.hasNext());

      futureIter = section.getFuture(true).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(6), futureIter.next());
      assertFalse(futureIter.hasNext());
      futureIter = section.getFuture(false).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(6), futureIter.next());
      assertFalse(futureIter.hasNext());

      CompletableFuture<StreamSection<Long>> future = CompletableFuture.supplyAsync(() -> spliterator.getNext(false));
      try {
         future.get(1, TimeUnit.SECONDS);
         assertTrue(false);
      } catch (TimeoutException e) {
         assertTrue(true);
      } catch (Exception e) {
         assertTrue(false);
      }
   }

   @Test
   public void testPastFutureSize_1_Wait() {
      AsyncTransferSpliterator<Long> spliterator = new AsyncTransferSpliterator<Long>(1, 1);

      spliterator.onAvailable(Long.valueOf(0));
      assertEquals(1, spliterator.getSize());

      CompletableFuture<StreamSection<Long>> future = CompletableFuture.supplyAsync(() -> spliterator.getNext(false), executor);
      try {
         future.get(1, TimeUnit.SECONDS);
         assertTrue(false);
      } catch (TimeoutException e) {
         assertTrue(true);
      } catch (Exception e) {
         assertTrue(false);
      }

      spliterator.onAvailable(Long.valueOf(1));
      assertEquals(2, spliterator.getSize());

      CompletableFuture<StreamSection<Long>> future2 = CompletableFuture.supplyAsync(() -> spliterator.getNext(false), executor);
      try {
         future2.get(1, TimeUnit.SECONDS);
         assertTrue(false);
      } catch (TimeoutException e) {
         assertTrue(true);
      } catch (Exception e) {
         assertTrue(false);
      }

      spliterator.onAvailable(Long.valueOf(2));
      assertEquals(3, spliterator.getSize());

      StreamSection<Long> section = future.join();
      assertEquals(Long.valueOf(1), section.getCurrent());
      Iterator<Long> pastIter = section.getPast(true).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(0), pastIter.next());
      assertFalse(pastIter.hasNext());

      pastIter = section.getPast(false).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(0), pastIter.next());
      assertFalse(pastIter.hasNext());

      Iterator<Long> futureIter = section.getFuture(true).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(2), futureIter.next());
      assertFalse(futureIter.hasNext());
      futureIter = section.getFuture(false).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(2), futureIter.next());
      assertFalse(futureIter.hasNext());

      spliterator.onAvailable(Long.valueOf(3));

      section = future2.join();
      assertEquals(Long.valueOf(2), section.getCurrent());
      pastIter = section.getPast(true).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(1), pastIter.next());
      assertFalse(pastIter.hasNext());

      pastIter = section.getPast(false).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(1), pastIter.next());
      assertFalse(pastIter.hasNext());

      futureIter = section.getFuture(true).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(3), futureIter.next());
      assertFalse(futureIter.hasNext());
      futureIter = section.getFuture(false).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(3), futureIter.next());
      assertFalse(futureIter.hasNext());
   }

   @Test
   public void testPastFutureSize_1_2() {
      AsyncTransferSpliterator<Long> spliterator = new AsyncTransferSpliterator<Long>(1, 2);

      spliterator.onAvailable(Long.valueOf(0));
      assertEquals(1, spliterator.getSize());

      spliterator.onAvailable(Long.valueOf(1));
      assertEquals(2, spliterator.getSize());

      spliterator.onAvailable(Long.valueOf(2));
      assertEquals(3, spliterator.getSize());

      spliterator.onAvailable(Long.valueOf(3));
      assertEquals(4, spliterator.getSize());

      StreamSection<Long> section = spliterator.getNext(false);
      assertEquals(4, spliterator.getSize());
      assertEquals(Long.valueOf(1), section.getCurrent());
      Iterator<Long> pastIter = section.getPast(true).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(0), pastIter.next());
      assertFalse(pastIter.hasNext());
      pastIter = section.getPast(false).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(0), pastIter.next());
      assertFalse(pastIter.hasNext());

      Iterator<Long> futureIter = section.getFuture(true).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(2), futureIter.next());
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(3), futureIter.next());
      assertFalse(futureIter.hasNext());
      futureIter = section.getFuture(false).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(3), futureIter.next());
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(2), futureIter.next());
      assertFalse(futureIter.hasNext());

      spliterator.onAvailable(Long.valueOf(4));
      assertEquals(5, spliterator.getSize());

      section = spliterator.getNext(false);
      assertEquals(4, spliterator.getSize());
      assertEquals(Long.valueOf(2), section.getCurrent());
      pastIter = section.getPast(true).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(1), pastIter.next());
      assertFalse(pastIter.hasNext());
      pastIter = section.getPast(false).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(1), pastIter.next());
      assertFalse(pastIter.hasNext());

      futureIter = section.getFuture(true).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(3), futureIter.next());
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(4), futureIter.next());
      assertFalse(futureIter.hasNext());
      futureIter = section.getFuture(false).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(4), futureIter.next());
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(3), futureIter.next());
      assertFalse(futureIter.hasNext());

      spliterator.onAvailable(Long.valueOf(5));
      assertEquals(5, spliterator.getSize());

      section = spliterator.getNext(false);
      assertEquals(4, spliterator.getSize());
      assertEquals(Long.valueOf(3), section.getCurrent());
      pastIter = section.getPast(true).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(2), pastIter.next());
      assertFalse(pastIter.hasNext());
      pastIter = section.getPast(false).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(2), pastIter.next());
      assertFalse(pastIter.hasNext());

      futureIter = section.getFuture(true).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(4), futureIter.next());
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(5), futureIter.next());
      assertFalse(futureIter.hasNext());
      futureIter = section.getFuture(false).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(5), futureIter.next());
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(4), futureIter.next());
      assertFalse(futureIter.hasNext());

      CompletableFuture<StreamSection<Long>> future = CompletableFuture.supplyAsync(() -> spliterator.getNext(false));
      try {
         future.get(1, TimeUnit.SECONDS);
         assertTrue(false);
      } catch (TimeoutException e) {
         assertTrue(true);
      } catch (Exception e) {
         assertTrue(false);
      }
   }

   @Test
   public void testPastFutureSize_2_1() {
      AsyncTransferSpliterator<Long> spliterator = new AsyncTransferSpliterator<Long>(2, 1);

      spliterator.onAvailable(Long.valueOf(0));
      assertEquals(1, spliterator.getSize());

      spliterator.onAvailable(Long.valueOf(1));
      assertEquals(2, spliterator.getSize());

      spliterator.onAvailable(Long.valueOf(2));
      assertEquals(3, spliterator.getSize());

      spliterator.onAvailable(Long.valueOf(3));
      assertEquals(4, spliterator.getSize());

      StreamSection<Long> section = spliterator.getNext(false);
      assertEquals(4, spliterator.getSize());
      assertEquals(Long.valueOf(2), section.getCurrent());
      Iterator<Long> pastIter = section.getPast(true).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(0), pastIter.next());
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(1), pastIter.next());
      assertFalse(pastIter.hasNext());
      pastIter = section.getPast(false).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(1), pastIter.next());
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(0), pastIter.next());
      assertFalse(pastIter.hasNext());

      Iterator<Long> futureIter = section.getFuture(true).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(3), futureIter.next());
      assertFalse(futureIter.hasNext());
      futureIter = section.getFuture(false).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(3), futureIter.next());
      assertFalse(futureIter.hasNext());

      spliterator.onAvailable(Long.valueOf(4));
      assertEquals(5, spliterator.getSize());

      section = spliterator.getNext(false);
      assertEquals(4, spliterator.getSize());
      assertEquals(Long.valueOf(3), section.getCurrent());
      pastIter = section.getPast(true).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(1), pastIter.next());
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(2), pastIter.next());
      assertFalse(pastIter.hasNext());
      pastIter = section.getPast(false).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(2), pastIter.next());
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(1), pastIter.next());
      assertFalse(pastIter.hasNext());

      futureIter = section.getFuture(true).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(4), futureIter.next());
      assertFalse(futureIter.hasNext());
      futureIter = section.getFuture(false).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(4), futureIter.next());
      assertFalse(futureIter.hasNext());

      spliterator.onAvailable(Long.valueOf(5));
      assertEquals(5, spliterator.getSize());

      section = spliterator.getNext(false);
      assertEquals(4, spliterator.getSize());
      assertEquals(Long.valueOf(4), section.getCurrent());
      pastIter = section.getPast(true).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(2), pastIter.next());
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(3), pastIter.next());
      assertFalse(pastIter.hasNext());
      pastIter = section.getPast(false).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(3), pastIter.next());
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(2), pastIter.next());
      assertFalse(pastIter.hasNext());

      futureIter = section.getFuture(true).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(5), futureIter.next());
      assertFalse(futureIter.hasNext());
      futureIter = section.getFuture(false).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(5), futureIter.next());
      assertFalse(futureIter.hasNext());

      spliterator.onAvailable(Long.valueOf(6));
      assertEquals(5, spliterator.getSize());

      section = spliterator.getNext(false);
      assertEquals(4, spliterator.getSize());
      assertEquals(Long.valueOf(5), section.getCurrent());
      pastIter = section.getPast(true).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(3), pastIter.next());
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(4), pastIter.next());
      assertFalse(pastIter.hasNext());
      pastIter = section.getPast(false).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(4), pastIter.next());
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(3), pastIter.next());
      assertFalse(pastIter.hasNext());

      futureIter = section.getFuture(true).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(6), futureIter.next());
      assertFalse(futureIter.hasNext());
      futureIter = section.getFuture(false).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(6), futureIter.next());
      assertFalse(futureIter.hasNext());

      CompletableFuture<StreamSection<Long>> future = CompletableFuture.supplyAsync(() -> spliterator.getNext(false));
      try {
         future.get(1, TimeUnit.SECONDS);
         assertTrue(false);
      } catch (TimeoutException e) {
         assertTrue(true);
      } catch (Exception e) {
         assertTrue(false);
      }
   }

   @Test
   public void testPastFutureSize_2_3() {
      AsyncTransferSpliterator<Long> spliterator = new AsyncTransferSpliterator<Long>(2, 3);

      spliterator.onAvailable(Long.valueOf(0));
      assertEquals(1, spliterator.getSize());

      spliterator.onAvailable(Long.valueOf(1));
      assertEquals(2, spliterator.getSize());

      spliterator.onAvailable(Long.valueOf(2));
      assertEquals(3, spliterator.getSize());

      spliterator.onAvailable(Long.valueOf(3));
      assertEquals(4, spliterator.getSize());

      spliterator.onAvailable(Long.valueOf(4));
      assertEquals(5, spliterator.getSize());

      spliterator.onAvailable(Long.valueOf(5));
      assertEquals(6, spliterator.getSize());

      StreamSection<Long> section = spliterator.getNext(false);
      assertEquals(6, spliterator.getSize());
      assertEquals(Long.valueOf(2), section.getCurrent());
      Iterator<Long> pastIter = section.getPast(true).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(0), pastIter.next());
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(1), pastIter.next());
      assertFalse(pastIter.hasNext());
      pastIter = section.getPast(false).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(1), pastIter.next());
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(0), pastIter.next());
      assertFalse(pastIter.hasNext());

      Iterator<Long> futureIter = section.getFuture(true).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(3), futureIter.next());
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(4), futureIter.next());
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(5), futureIter.next());
      assertFalse(futureIter.hasNext());
      futureIter = section.getFuture(false).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(5), futureIter.next());
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(4), futureIter.next());
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(3), futureIter.next());
      assertFalse(futureIter.hasNext());

      spliterator.onAvailable(Long.valueOf(6));
      assertEquals(7, spliterator.getSize());

      section = spliterator.getNext(false);
      assertEquals(6, spliterator.getSize());
      assertEquals(Long.valueOf(3), section.getCurrent());
      pastIter = section.getPast(true).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(1), pastIter.next());
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(2), pastIter.next());
      assertFalse(pastIter.hasNext());
      pastIter = section.getPast(false).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(2), pastIter.next());
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(1), pastIter.next());
      assertFalse(pastIter.hasNext());

      futureIter = section.getFuture(true).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(4), futureIter.next());
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(5), futureIter.next());
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(6), futureIter.next());
      assertFalse(futureIter.hasNext());
      futureIter = section.getFuture(false).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(6), futureIter.next());
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(5), futureIter.next());
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(4), futureIter.next());
      assertFalse(futureIter.hasNext());

      spliterator.onAvailable(Long.valueOf(7));
      assertEquals(7, spliterator.getSize());

      section = spliterator.getNext(false);
      assertEquals(6, spliterator.getSize());
      assertEquals(Long.valueOf(4), section.getCurrent());
      pastIter = section.getPast(true).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(2), pastIter.next());
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(3), pastIter.next());
      assertFalse(pastIter.hasNext());
      pastIter = section.getPast(false).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(3), pastIter.next());
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(2), pastIter.next());
      assertFalse(pastIter.hasNext());

      futureIter = section.getFuture(true).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(5), futureIter.next());
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(6), futureIter.next());
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(7), futureIter.next());
      assertFalse(futureIter.hasNext());
      futureIter = section.getFuture(false).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(7), futureIter.next());
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(6), futureIter.next());
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(5), futureIter.next());
      assertFalse(futureIter.hasNext());

      CompletableFuture<StreamSection<Long>> future = CompletableFuture.supplyAsync(() -> spliterator.getNext(false));
      try {
         future.get(1, TimeUnit.SECONDS);
         assertTrue(false);
      } catch (TimeoutException e) {
         assertTrue(true);
      } catch (Exception e) {
         assertTrue(false);
      }
   }

   @Test
   public void testPastFutureSize_3_2() {
      AsyncTransferSpliterator<Long> spliterator = new AsyncTransferSpliterator<Long>(3, 2);

      spliterator.onAvailable(Long.valueOf(0));
      assertEquals(1, spliterator.getSize());

      spliterator.onAvailable(Long.valueOf(1));
      assertEquals(2, spliterator.getSize());

      spliterator.onAvailable(Long.valueOf(2));
      assertEquals(3, spliterator.getSize());

      spliterator.onAvailable(Long.valueOf(3));
      assertEquals(4, spliterator.getSize());

      spliterator.onAvailable(Long.valueOf(4));
      assertEquals(5, spliterator.getSize());

      spliterator.onAvailable(Long.valueOf(5));
      assertEquals(6, spliterator.getSize());

      StreamSection<Long> section = spliterator.getNext(false);
      assertEquals(6, spliterator.getSize());
      assertEquals(Long.valueOf(3), section.getCurrent());
      Iterator<Long> pastIter = section.getPast(true).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(0), pastIter.next());
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(1), pastIter.next());
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(2), pastIter.next());
      assertFalse(pastIter.hasNext());
      pastIter = section.getPast(false).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(2), pastIter.next());
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(1), pastIter.next());
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(0), pastIter.next());
      assertFalse(pastIter.hasNext());

      Iterator<Long> futureIter = section.getFuture(true).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(4), futureIter.next());
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(5), futureIter.next());
      assertFalse(futureIter.hasNext());
      futureIter = section.getFuture(false).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(5), futureIter.next());
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(4), futureIter.next());
      assertFalse(futureIter.hasNext());

      spliterator.onAvailable(Long.valueOf(6));
      assertEquals(7, spliterator.getSize());

      section = spliterator.getNext(false);
      assertEquals(6, spliterator.getSize());
      assertEquals(Long.valueOf(4), section.getCurrent());
      pastIter = section.getPast(true).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(1), pastIter.next());
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(2), pastIter.next());
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(3), pastIter.next());
      assertFalse(pastIter.hasNext());
      pastIter = section.getPast(false).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(3), pastIter.next());
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(2), pastIter.next());
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(1), pastIter.next());
      assertFalse(pastIter.hasNext());

      futureIter = section.getFuture(true).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(5), futureIter.next());
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(6), futureIter.next());
      assertFalse(futureIter.hasNext());
      futureIter = section.getFuture(false).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(6), futureIter.next());
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(5), futureIter.next());
      assertFalse(futureIter.hasNext());

      spliterator.onAvailable(Long.valueOf(7));
      assertEquals(7, spliterator.getSize());

      section = spliterator.getNext(false);
      assertEquals(6, spliterator.getSize());
      assertEquals(Long.valueOf(5), section.getCurrent());
      pastIter = section.getPast(true).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(2), pastIter.next());
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(3), pastIter.next());
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(4), pastIter.next());
      assertFalse(pastIter.hasNext());
      pastIter = section.getPast(false).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(4), pastIter.next());
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(3), pastIter.next());
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(2), pastIter.next());
      assertFalse(pastIter.hasNext());

      futureIter = section.getFuture(true).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(6), futureIter.next());
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(7), futureIter.next());
      assertFalse(futureIter.hasNext());
      futureIter = section.getFuture(false).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(7), futureIter.next());
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(6), futureIter.next());
      assertFalse(futureIter.hasNext());

      CompletableFuture<StreamSection<Long>> future = CompletableFuture.supplyAsync(() -> spliterator.getNext(false));
      try {
         future.get(1, TimeUnit.SECONDS);
         assertTrue(false);
      } catch (TimeoutException e) {
         assertTrue(true);
      } catch (Exception e) {
         assertTrue(false);
      }
   }

   @Test
   public void testPastFutureSize_2() {
      AsyncTransferSpliterator<Long> spliterator = new AsyncTransferSpliterator<Long>(2, 2);

      spliterator.onAvailable(Long.valueOf(0));
      assertEquals(1, spliterator.getSize());

      spliterator.onAvailable(Long.valueOf(1));
      assertEquals(2, spliterator.getSize());

      spliterator.onAvailable(Long.valueOf(2));
      assertEquals(3, spliterator.getSize());

      spliterator.onAvailable(Long.valueOf(3));
      assertEquals(4, spliterator.getSize());

      spliterator.onAvailable(Long.valueOf(4));
      assertEquals(5, spliterator.getSize());

      StreamSection<Long> section = spliterator.getNext(false);
      assertEquals(5, spliterator.getSize());
      assertEquals(Long.valueOf(2), section.getCurrent());
      Iterator<Long> pastIter = section.getPast(true).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(0), pastIter.next());
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(1), pastIter.next());
      assertFalse(pastIter.hasNext());

      pastIter = section.getPast(false).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(1), pastIter.next());
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(0), pastIter.next());
      assertFalse(pastIter.hasNext());

      Iterator<Long> futureIter = section.getFuture(true).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(3), futureIter.next());
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(4), futureIter.next());
      assertFalse(futureIter.hasNext());
      futureIter = section.getFuture(false).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(4), futureIter.next());
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(3), futureIter.next());
      assertFalse(futureIter.hasNext());

      spliterator.onAvailable(Long.valueOf(5));
      assertEquals(6, spliterator.getSize());

      section = spliterator.getNext(false);
      assertEquals(5, spliterator.getSize());
      assertEquals(Long.valueOf(3), section.getCurrent());
      pastIter = section.getPast(true).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(1), pastIter.next());
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(2), pastIter.next());
      assertFalse(pastIter.hasNext());

      pastIter = section.getPast(false).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(2), pastIter.next());
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(1), pastIter.next());
      assertFalse(pastIter.hasNext());

      futureIter = section.getFuture(true).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(4), futureIter.next());
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(5), futureIter.next());
      assertFalse(futureIter.hasNext());
      futureIter = section.getFuture(false).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(5), futureIter.next());
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(4), futureIter.next());
      assertFalse(futureIter.hasNext());

      spliterator.onAvailable(Long.valueOf(6));
      assertEquals(6, spliterator.getSize());

      section = spliterator.getNext(false);
      assertEquals(5, spliterator.getSize());
      assertEquals(Long.valueOf(4), section.getCurrent());
      pastIter = section.getPast(true).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(2), pastIter.next());
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(3), pastIter.next());
      assertFalse(pastIter.hasNext());

      pastIter = section.getPast(false).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(3), pastIter.next());
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(2), pastIter.next());
      assertFalse(pastIter.hasNext());

      futureIter = section.getFuture(true).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(5), futureIter.next());
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(6), futureIter.next());
      assertFalse(futureIter.hasNext());
      futureIter = section.getFuture(false).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(6), futureIter.next());
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(5), futureIter.next());
      assertFalse(futureIter.hasNext());

      spliterator.onAvailable(Long.valueOf(7));
      assertEquals(6, spliterator.getSize());

      section = spliterator.getNext(false);
      assertEquals(5, spliterator.getSize());
      assertEquals(Long.valueOf(5), section.getCurrent());
      pastIter = section.getPast(true).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(3), pastIter.next());
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(4), pastIter.next());
      assertFalse(pastIter.hasNext());

      pastIter = section.getPast(false).iterator();
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(4), pastIter.next());
      assertTrue(pastIter.hasNext());
      assertEquals(Long.valueOf(3), pastIter.next());
      assertFalse(pastIter.hasNext());

      futureIter = section.getFuture(true).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(6), futureIter.next());
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(7), futureIter.next());
      assertFalse(futureIter.hasNext());
      futureIter = section.getFuture(false).iterator();
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(7), futureIter.next());
      assertTrue(futureIter.hasNext());
      assertEquals(Long.valueOf(6), futureIter.next());
      assertFalse(futureIter.hasNext());

      CompletableFuture<StreamSection<Long>> future = CompletableFuture.supplyAsync(() -> spliterator.getNext(false));
      try {
         future.get(1, TimeUnit.SECONDS);
         assertTrue(false);
      } catch (TimeoutException e) {
         assertTrue(true);
      } catch (Exception e) {
         assertTrue(false);
      }
   }

   @Test
   public void testBackPreasure_01() {
      AsyncTransferSpliterator<Long> spliterator = new AsyncTransferSpliterator<Long>(0, 0, 1);

      spliterator.onAvailable(Long.valueOf(0));

      CompletableFuture<Void> future = CompletableFuture.supplyAsync(() -> {
         spliterator.onAvailable(Long.valueOf(1));
         return null;
      }, executor);

      try {
         future.get(1, TimeUnit.SECONDS);
         assertTrue(false);
      } catch (TimeoutException e) {
         assertTrue(true);
      } catch (Exception e) {
         assertTrue(false);
      }
   }

   @Test
   public void testBackPreasure_02() {
      AsyncTransferSpliterator<Long> spliterator = new AsyncTransferSpliterator<Long>(0, 0, 1);

      spliterator.onAvailable(Long.valueOf(0));

      spliterator.getNext(false);
      spliterator.onAvailable(Long.valueOf(1));

      CompletableFuture<Void> future = CompletableFuture.supplyAsync(() -> {
         spliterator.onAvailable(Long.valueOf(2));
         return null;
      }, executor);

      try {
         future.get(1, TimeUnit.SECONDS);
         assertTrue(false);
      } catch (TimeoutException e) {
         assertTrue(true);
      } catch (Exception e) {
         assertTrue(false);
      }
   }
   
   @Test
   public void testBackPreasure_03() {
      AsyncTransferSpliterator<Long> spliterator = new AsyncTransferSpliterator<Long>(0, 0, 1);

      spliterator.onAvailable(Long.valueOf(0));

      spliterator.getNext(false);
      spliterator.onAvailable(Long.valueOf(1));

      CompletableFuture<Void> future = CompletableFuture.supplyAsync(() -> {
         spliterator.onAvailable(Long.valueOf(2));
         return null;
      }, executor);

      try {
         future.get(1, TimeUnit.SECONDS);
         assertTrue(false);
      } catch (TimeoutException e) {
         assertTrue(true);
      } catch (Exception e) {
         assertTrue(false);
      }

      CompletableFuture<Void> future2 = CompletableFuture.supplyAsync(() -> {
         spliterator.onAvailable(Long.valueOf(3));
         return null;
      }, executor);

      try {
         future2.get(1, TimeUnit.SECONDS);
         assertTrue(false);
      } catch (TimeoutException e) {
         assertTrue(true);
      } catch (Exception e) {
         assertTrue(false);
      }

      spliterator.getNext(true);
      future.join();

      try {
         future2.get(1, TimeUnit.SECONDS);
         assertTrue(false);
      } catch (TimeoutException e) {
         assertTrue(true);
      } catch (Exception e) {
         assertTrue(false);
      }

      spliterator.getNext(true);
      future2.join();
   }

   @Test
   public void testBackPreasure_04() {
      AsyncTransferSpliterator<Long> spliterator = new AsyncTransferSpliterator<Long>(0, 0, 2);

      spliterator.onAvailable(Long.valueOf(0));
      spliterator.onAvailable(Long.valueOf(1));

      spliterator.getNext(false);
      spliterator.onAvailable(Long.valueOf(2));

      CompletableFuture<Void> future = CompletableFuture.supplyAsync(() -> {
         spliterator.onAvailable(Long.valueOf(3));
         return null;
      }, executor);

      try {
         future.get(1, TimeUnit.SECONDS);
         assertTrue(false);
      } catch (TimeoutException e) {
         assertTrue(true);
      } catch (Exception e) {
         assertTrue(false);
      }

      CompletableFuture<Void> future2 = CompletableFuture.supplyAsync(() -> {
         spliterator.onAvailable(Long.valueOf(4));
         return null;
      }, executor);

      try {
         future2.get(1, TimeUnit.SECONDS);
         assertTrue(false);
      } catch (TimeoutException e) {
         assertTrue(true);
      } catch (Exception e) {
         assertTrue(false);
      }

      spliterator.getNext(true);
      future.join();

      try {
         future2.get(1, TimeUnit.SECONDS);
         assertTrue(false);
      } catch (TimeoutException e) {
         assertTrue(true);
      } catch (Exception e) {
         assertTrue(false);
      }

      spliterator.getNext(true);
      future2.join();
   }

   @Test
   public void testBackPreasure_05_1_1() {
      AsyncTransferSpliterator<Long> spliterator = new AsyncTransferSpliterator<Long>(1, 1, 2);

      spliterator.onAvailable(Long.valueOf(0));
      spliterator.onAvailable(Long.valueOf(1));
      spliterator.onAvailable(Long.valueOf(2));
      spliterator.onAvailable(Long.valueOf(3));
      spliterator.onAvailable(Long.valueOf(4));

      spliterator.getNext(false);
      spliterator.onAvailable(Long.valueOf(5));

      CompletableFuture<Void> future = CompletableFuture.supplyAsync(() -> {
         spliterator.onAvailable(Long.valueOf(6));
         return null;
      }, executor);

      try {
         future.get(1, TimeUnit.SECONDS);
         assertTrue(false);
      } catch (TimeoutException e) {
         assertTrue(true);
      } catch (Exception e) {
         assertTrue(false);
      }

      CompletableFuture<Void> future2 = CompletableFuture.supplyAsync(() -> {
         spliterator.onAvailable(Long.valueOf(7));
         return null;
      }, executor);

      try {
         future2.get(1, TimeUnit.SECONDS);
         assertTrue(false);
      } catch (TimeoutException e) {
         assertTrue(true);
      } catch (Exception e) {
         assertTrue(false);
      }

      spliterator.getNext(true);
      future.join();

      try {
         future2.get(1, TimeUnit.SECONDS);
         assertTrue(false);
      } catch (TimeoutException e) {
         assertTrue(true);
      } catch (Exception e) {
         assertTrue(false);
      }

      spliterator.getNext(true);
      future2.join();
   }

   @Test
   public void testBackPreasure_05_2_1() {
      AsyncTransferSpliterator<Long> spliterator = new AsyncTransferSpliterator<Long>(2, 1, 2);

      spliterator.onAvailable(Long.valueOf(0));
      spliterator.onAvailable(Long.valueOf(1));
      spliterator.onAvailable(Long.valueOf(2));
      spliterator.onAvailable(Long.valueOf(3));
      spliterator.onAvailable(Long.valueOf(4));
      spliterator.onAvailable(Long.valueOf(5));
      spliterator.onAvailable(Long.valueOf(6));

      spliterator.getNext(false);
      spliterator.onAvailable(Long.valueOf(6));

      CompletableFuture<Void> future = CompletableFuture.supplyAsync(() -> {
         spliterator.onAvailable(Long.valueOf(7));
         return null;
      }, executor);

      try {
         future.get(1, TimeUnit.SECONDS);
         assertTrue(false);
      } catch (TimeoutException e) {
         assertTrue(true);
      } catch (Exception e) {
         assertTrue(false);
      }

      CompletableFuture<Void> future2 = CompletableFuture.supplyAsync(() -> {
         spliterator.onAvailable(Long.valueOf(8));
         return null;
      }, executor);

      try {
         future2.get(1, TimeUnit.SECONDS);
         assertTrue(false);
      } catch (TimeoutException e) {
         assertTrue(true);
      } catch (Exception e) {
         assertTrue(false);
      }

      spliterator.getNext(true);
      future.join();

      try {
         future2.get(1, TimeUnit.SECONDS);
         assertTrue(false);
      } catch (TimeoutException e) {
         assertTrue(true);
      } catch (Exception e) {
         assertTrue(false);
      }

      spliterator.getNext(true);
      future2.join();
   }

   @Test
   public void testBackPreasure_05_2_2() {
      AsyncTransferSpliterator<Long> spliterator = new AsyncTransferSpliterator<Long>(2, 2, 2);

      spliterator.onAvailable(Long.valueOf(0));
      spliterator.onAvailable(Long.valueOf(1));
      spliterator.onAvailable(Long.valueOf(2));
      spliterator.onAvailable(Long.valueOf(3));
      spliterator.onAvailable(Long.valueOf(4));
      spliterator.onAvailable(Long.valueOf(5));
      spliterator.onAvailable(Long.valueOf(6));
      spliterator.onAvailable(Long.valueOf(7));

      spliterator.getNext(false);
      spliterator.onAvailable(Long.valueOf(8));

      CompletableFuture<Void> future = CompletableFuture.supplyAsync(() -> {
         spliterator.onAvailable(Long.valueOf(9));
         return null;
      }, executor);

      try {
         future.get(1, TimeUnit.SECONDS);
         assertTrue(false);
      } catch (TimeoutException e) {
         assertTrue(true);
      } catch (Exception e) {
         assertTrue(false);
      }

      CompletableFuture<Void> future2 = CompletableFuture.supplyAsync(() -> {
         spliterator.onAvailable(Long.valueOf(10));
         return null;
      }, executor);

      try {
         future2.get(1, TimeUnit.SECONDS);
         assertTrue(false);
      } catch (TimeoutException e) {
         assertTrue(true);
      } catch (Exception e) {
         assertTrue(false);
      }

      spliterator.getNext(true);
      future.join();

      try {
         future2.get(1, TimeUnit.SECONDS);
         assertTrue(false);
      } catch (TimeoutException e) {
         assertTrue(true);
      } catch (Exception e) {
         assertTrue(false);
      }

      spliterator.getNext(true);
      future2.join();
   }

   @Test
   public void testBackPreasure_05_2_2_3() {
      AsyncTransferSpliterator<Long> spliterator = new AsyncTransferSpliterator<Long>(2, 2, 3);

      spliterator.onAvailable(Long.valueOf(0));
      spliterator.onAvailable(Long.valueOf(1));
      spliterator.onAvailable(Long.valueOf(2));
      spliterator.onAvailable(Long.valueOf(3));
      spliterator.onAvailable(Long.valueOf(4));
      spliterator.onAvailable(Long.valueOf(5));
      spliterator.onAvailable(Long.valueOf(6));
      spliterator.onAvailable(Long.valueOf(7));
      spliterator.onAvailable(Long.valueOf(8));

      spliterator.getNext(false);
      spliterator.onAvailable(Long.valueOf(9));

      CompletableFuture<Void> future = CompletableFuture.supplyAsync(() -> {
         spliterator.onAvailable(Long.valueOf(10));
         return null;
      }, executor);

      try {
         future.get(1, TimeUnit.SECONDS);
         assertTrue(false);
      } catch (TimeoutException e) {
         assertTrue(true);
      } catch (Exception e) {
         assertTrue(false);
      }

      CompletableFuture<Void> future2 = CompletableFuture.supplyAsync(() -> {
         spliterator.onAvailable(Long.valueOf(11));
         return null;
      }, executor);

      try {
         future2.get(1, TimeUnit.SECONDS);
         assertTrue(false);
      } catch (TimeoutException e) {
         assertTrue(true);
      } catch (Exception e) {
         assertTrue(false);
      }

      spliterator.getNext(false);
      future.join();

      try {
         future2.get(1, TimeUnit.SECONDS);
         assertTrue(false);
      } catch (TimeoutException e) {
         assertTrue(true);
      } catch (Exception e) {
         assertTrue(false);
      }

      spliterator.getNext(false);
      future2.join();
   }

   @Test
   public void testUnblock_Producer() throws InterruptedException {
      AsyncTransferSpliterator<Long> spliterator = new AsyncTransferSpliterator<Long>(0, 0, 1);
      AtomicBoolean exited = new AtomicBoolean(false);

      Executors.newSingleThreadExecutor().execute(() -> {
         spliterator.onAvailable(Long.valueOf(0));
         spliterator.onAvailable(Long.valueOf(1));

         // should block here
            spliterator.onAvailable(Long.valueOf(2));
            exited.set(true);
         });

      // wait executor gets blocked
      TimeUnit.MILLISECONDS.sleep(500);
      assertFalse(exited.get());

      // unblock waiting producer
      spliterator.onClose();

      // wait executor gets unblocked
      TimeUnit.MILLISECONDS.sleep(500);
      assertTrue(exited.get());
   }

   @Test
   public void testUnblock_Consumer() throws InterruptedException {
      AsyncTransferSpliterator<Long> spliterator = new AsyncTransferSpliterator<Long>(0, 0);
      AtomicBoolean exited = new AtomicBoolean(false);

      Executors.newSingleThreadExecutor().execute(() -> {
         // should block here
            spliterator.getNext(false);
            exited.set(true);
         });

      // wait executor gets blocked
      TimeUnit.MILLISECONDS.sleep(500);
      assertFalse(exited.get());

      // unblock waiting producer
      spliterator.onClose();

      // wait executor gets unblocked
      TimeUnit.MILLISECONDS.sleep(500);
      assertTrue(exited.get());
   }

   @Test
   public void test_ValueOrder_01() throws InterruptedException {
      AsyncTransferSpliterator<Long> spliterator = new AsyncTransferSpliterator<Long>(0, 0);

      Executors.newSingleThreadExecutor().execute(() -> {
         spliterator.onAvailable(Long.valueOf(0), (value) -> {
            try {
               // simulate costly transform function
               TimeUnit.MILLISECONDS.sleep(500);
            } catch (Exception e) {
               e.printStackTrace();
            }
            return value;
         });
      });

      TimeUnit.MILLISECONDS.sleep(10);
      Executors.newSingleThreadExecutor().execute(() -> {
         spliterator.onAvailable(Long.valueOf(1));
      });

      StreamSection<Long> value = spliterator.getNext(false);
      assertEquals(Long.valueOf(0), value.getCurrent());
      value = spliterator.getNext(false);
      assertEquals(Long.valueOf(1), value.getCurrent());
   }

   @Test
   public void test_ValueOrder_02() throws InterruptedException {
      AsyncTransferSpliterator<Long> spliterator = new AsyncTransferSpliterator<Long>(0, 0);

      Executors.newSingleThreadExecutor().execute(() -> {
         spliterator.onAvailable(Long.valueOf(0));
      });

      TimeUnit.MILLISECONDS.sleep(10);
      Executors.newSingleThreadExecutor().execute(() -> {
         spliterator.onAvailable(Long.valueOf(1), (value) -> {
            try {
               // simulate costly transform function
               TimeUnit.MILLISECONDS.sleep(500);
            } catch (Exception e) {
               e.printStackTrace();
            }
            return value;
         });
      });

      TimeUnit.MILLISECONDS.sleep(10);
      Executors.newSingleThreadExecutor().execute(() -> {
         spliterator.onAvailable(Long.valueOf(2));
      });

      StreamSection<Long> value = spliterator.getNext(false);
      assertEquals(Long.valueOf(0), value.getCurrent());
      value = spliterator.getNext(false);
      assertEquals(Long.valueOf(1), value.getCurrent());
      value = spliterator.getNext(false);
      assertEquals(Long.valueOf(2), value.getCurrent());
   }

   @Test
   public void test_Wait_For_First() throws InterruptedException {
      AsyncTransferSpliterator<Long> spliterator = new AsyncTransferSpliterator<Long>(0, 0);

      CompletableFuture<StreamSection<Long>> future = CompletableFuture.supplyAsync(() -> {
         return spliterator.getNext(false);
      });

      try {
         future.get(1, TimeUnit.SECONDS);
         assertTrue(false);
      } catch (TimeoutException e) {
         assertTrue(true);
      } catch (Exception e) {
         assertTrue(false);
      }

      spliterator.onAvailable(Long.valueOf(0));

      StreamSection<Long> value = future.join();
      assertEquals(Long.valueOf(0), value.getCurrent());
   }
}
