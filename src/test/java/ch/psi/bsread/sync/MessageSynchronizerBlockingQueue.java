package ch.psi.bsread.sync;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MessageSynchronizerBlockingQueue<Msg> implements BlockingQueue<Map<String, Msg>>, Closeable {
   private final BlockingQueue<Map<String, Msg>> queue;
   private final AbstractMessageSynchronizer<Msg> messageSync;
   private final ExecutorService executor;
   private CountDownLatch latch = new CountDownLatch(1);

   public MessageSynchronizerBlockingQueue(int capacity, AbstractMessageSynchronizer<Msg> messageSync) {
      this.queue = new ArrayBlockingQueue<>(capacity);
      this.messageSync = messageSync;
      this.executor = Executors.newSingleThreadExecutor();

      this.executor.execute(() -> {
         Map<String, Msg> msgMap;
         latch.countDown();

         while ((msgMap = this.messageSync.nextMessage()) != null) {
            this.queue.add(msgMap);
            latch.countDown();
         }
      });
   }

   public void initBarrier(int count) {
      latch = new CountDownLatch(count);
   }

   public void await() {
      try {
         latch.await();
      } catch (Exception e) {
         e.printStackTrace();
      }
   }

   public void await(long timeout, TimeUnit unit) {
      try {
         latch.await(timeout, unit);
      } catch (Exception e) {
         e.printStackTrace();
      }
   }

   @Override
   public void close() {
      try {
         messageSync.close();
      } catch (IOException e) {
         e.printStackTrace();
      }
      executor.shutdown();
   }

   @Override
   public Map<String, Msg> remove() {
      return this.queue.remove();
   }

   @Override
   public Map<String, Msg> poll() {
      return this.queue.poll();
   }

   @Override
   public Map<String, Msg> element() {
      return this.queue.element();
   }

   @Override
   public Map<String, Msg> peek() {
      return this.queue.peek();
   }

   @Override
   public int size() {
      return this.queue.size();
   }

   @Override
   public boolean isEmpty() {
      return this.queue.isEmpty();
   }

   @Override
   public Iterator<Map<String, Msg>> iterator() {
      return this.queue.iterator();
   }

   @Override
   public Object[] toArray() {
      return this.queue.toArray();
   }

   @Override
   public <T> T[] toArray(T[] a) {
      return this.queue.toArray(a);
   }

   @Override
   public boolean containsAll(Collection<?> c) {
      return this.queue.containsAll(c);
   }

   @Override
   public boolean addAll(Collection<? extends Map<String, Msg>> c) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean removeAll(Collection<?> c) {
      return this.queue.removeAll(c);
   }

   @Override
   public boolean retainAll(Collection<?> c) {
      return this.queue.retainAll(c);
   }

   @Override
   public void clear() {
      this.queue.clear();
   }

   @Override
   public boolean add(Map<String, Msg> e) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean offer(Map<String, Msg> e) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void put(Map<String, Msg> e) throws InterruptedException {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean offer(Map<String, Msg> e, long timeout, TimeUnit unit) throws InterruptedException {
      throw new UnsupportedOperationException();
   }

   @Override
   public Map<String, Msg> take() throws InterruptedException {
      return this.queue.take();
   }

   @Override
   public Map<String, Msg> poll(long timeout, TimeUnit unit) throws InterruptedException {
      return this.queue.poll(timeout, unit);
   }

   @Override
   public int remainingCapacity() {
      return this.queue.remainingCapacity();
   }

   @Override
   public boolean remove(Object o) {
      return this.queue.remove(o);
   }

   @Override
   public boolean contains(Object o) {
      return this.queue.contains(o);
   }

   @Override
   public int drainTo(Collection<? super Map<String, Msg>> c) {
      return this.queue.drainTo(c);
   }

   @Override
   public int drainTo(Collection<? super Map<String, Msg>> c, int maxElements) {
      return this.queue.drainTo(c, maxElements);
   }

   @Override
   public String toString() {
      return this.queue.toString();
   }
}
