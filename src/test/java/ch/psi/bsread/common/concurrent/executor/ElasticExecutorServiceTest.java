package ch.psi.bsread.common.concurrent.executor;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticExecutorServiceTest {
   private static final Logger LOGGER = LoggerFactory.getLogger(ElasticExecutorServiceTest.class);

   @Before
   public void setUp() throws Exception {}

   @After
   public void tearDown() throws Exception {}

   private ExecutorService getElasticExecutorService(int corePoolSize, int maxPoolSize, int ttlSeconds) {
      return new ElasticExecutorService(corePoolSize, maxPoolSize, this.getClass().getSimpleName(), ttlSeconds);
   }

   @SuppressWarnings("unchecked")
   private <T> Set<T> createSet(final T... values) {
      if (values != null && values.length > 0) {
         final Set<T> set = new HashSet<>(values.length);
         for (T t : values) {
            set.add(t);
         }
         return set;
      } else {
         return Collections.emptySet();
      }
   }

   // @Test
   public void test_00() throws Exception {
      final int maxRun = 5;
      for (int i = 0; i < maxRun; ++i) {
         LOGGER.info("'{}' run '{}/{}'.", getClass().getSimpleName(), i, maxRun);
         LOGGER.info("Run test_01().");
         test_01();
         LOGGER.info("Run test_02().");
         test_02();
         LOGGER.info("Run test_03().");
         test_03();
         LOGGER.info("Run test_04().");
         test_04();
      }
   }


   @Test
   public void test_01() throws Exception {
      final int nrOfTasks = 10000;
      final int concurrentLoads = 100;
      final long sleepMS = 100;
      final Set<Long> sleepNumbers = createSet(Long.valueOf(nrOfTasks / 16), Long.valueOf(nrOfTasks / 8),
            Long.valueOf(nrOfTasks / 4), Long.valueOf(nrOfTasks / 2));
      final CountDownLatch countDown = new CountDownLatch(nrOfTasks);
      final CountDownLatch startLoadingLatch = new CountDownLatch(1);
      final ExecutorService loaderExecutor = Executors.newCachedThreadPool();
      final ExecutorService workExecutor = getElasticExecutorService(1, 100, 1);
      final AtomicLong createdTasks = new AtomicLong();
      final AtomicLong executedTasks = new AtomicLong();
      final Runnable executor = () -> {
         final long taskNr = executedTasks.getAndIncrement();
         if (sleepMS > 0 && sleepNumbers.contains(taskNr)) {
            try {
               TimeUnit.MILLISECONDS.sleep(sleepMS);
            } catch (InterruptedException e) {
               e.printStackTrace();
            }
         }
         countDown.countDown();
      };
      final Runnable loader = () -> {
         try {
            startLoadingLatch.await(5, TimeUnit.SECONDS);
         } catch (final Exception e) {
            e.printStackTrace();
         }

         while (createdTasks.getAndIncrement() < nrOfTasks) {
            workExecutor.execute(executor);
         }
      };

      for (int i = 0; i < concurrentLoads; ++i) {
         loaderExecutor.execute(loader);
      }
      startLoadingLatch.countDown();

      countDown.await(60, TimeUnit.SECONDS);

      assertEquals(nrOfTasks, executedTasks.get());

      loaderExecutor.shutdown();
      workExecutor.shutdown();
   }

   @Test
   public void test_02() throws Exception {
      final int nrOfTasks = 10000;
      final int concurrentLoads = 100;
      final long sleepMS = 100;
      final Set<Long> sleepNumbers = createSet(Long.valueOf(nrOfTasks / 16), Long.valueOf(nrOfTasks / 8),
            Long.valueOf(nrOfTasks / 4), Long.valueOf(nrOfTasks / 2));
      final CountDownLatch countDown = new CountDownLatch(nrOfTasks);
      final CountDownLatch startLoadingLatch = new CountDownLatch(1);
      final ExecutorService loaderExecutor = Executors.newCachedThreadPool();
      final ExecutorService workExecutor = getElasticExecutorService(1, 10, 1);
      final AtomicLong createdTasks = new AtomicLong();
      final AtomicLong executedTasks = new AtomicLong();
      final Runnable executor = () -> {
         final long taskNr = executedTasks.getAndIncrement();
         if (sleepMS > 0 && sleepNumbers.contains(taskNr)) {
            try {
               TimeUnit.MILLISECONDS.sleep(sleepMS);
            } catch (InterruptedException e) {
               e.printStackTrace();
            }
         }
         countDown.countDown();
      };
      final Runnable loader = () -> {
         try {
            startLoadingLatch.await(5, TimeUnit.SECONDS);
         } catch (final Exception e) {
            e.printStackTrace();
         }

         while (createdTasks.getAndIncrement() < nrOfTasks) {
            workExecutor.execute(executor);
         }
      };

      for (int i = 0; i < concurrentLoads; ++i) {
         loaderExecutor.execute(loader);
      }
      startLoadingLatch.countDown();

      countDown.await(60, TimeUnit.SECONDS);

      assertEquals(nrOfTasks, executedTasks.get());

      loaderExecutor.shutdown();
      workExecutor.shutdown();
   }

   @Test
   public void test_03() throws Exception {
      final int nrOfTasks = 10000;
      final int concurrentLoads = 100;
      final long sleepMS = 100;
      final Set<Long> sleepNumbers = createSet(Long.valueOf(nrOfTasks / 16), Long.valueOf(nrOfTasks / 8),
            Long.valueOf(nrOfTasks / 4), Long.valueOf(nrOfTasks / 2));
      final CountDownLatch countDown = new CountDownLatch(nrOfTasks);
      final CountDownLatch startLoadingLatch = new CountDownLatch(1);
      final ExecutorService loaderExecutor = Executors.newCachedThreadPool();
      final ExecutorService workExecutor = getElasticExecutorService(1, 10, 1);
      final AtomicLong createdTasks = new AtomicLong();
      final AtomicLong executedTasks = new AtomicLong();
      final Runnable executor = () -> {
         final long taskNr = executedTasks.getAndIncrement();
         if (sleepMS > 0 && sleepNumbers.contains(taskNr)) {
            try {
               TimeUnit.MILLISECONDS.sleep(sleepMS);
            } catch (InterruptedException e) {
               e.printStackTrace();
            }
         }
         countDown.countDown();
      };
      final Runnable loader = () -> {
         try {
            startLoadingLatch.await(5, TimeUnit.SECONDS);
         } catch (final Exception e) {
            e.printStackTrace();
         }

         while (createdTasks.getAndIncrement() < nrOfTasks) {
            workExecutor.execute(executor);
         }
      };

      for (int i = 0; i < concurrentLoads; ++i) {
         loaderExecutor.execute(loader);
      }
      startLoadingLatch.countDown();

      countDown.await(60, TimeUnit.SECONDS);

      assertEquals(nrOfTasks, executedTasks.get());

      loaderExecutor.shutdown();
      workExecutor.shutdown();
   }

   @Test
   public void test_04() throws Exception {
      final int nrOfTasks = 10000;
      final int concurrentLoads = 100;
      final long sleepMS = 3000;
      final Set<Long> sleepNumbers = createSet(Long.valueOf(nrOfTasks / 16), Long.valueOf(nrOfTasks / 8),
            Long.valueOf(nrOfTasks / 4), Long.valueOf(nrOfTasks / 2));
      final ExecutorService loaderExecutor = Executors.newCachedThreadPool();
      final ExecutorService workExecutor = getElasticExecutorService(3, 10, 1);

      {
         final CountDownLatch countDown = new CountDownLatch(nrOfTasks);
         final CountDownLatch startLoadingLatch = new CountDownLatch(1);
         final AtomicLong createdTasks = new AtomicLong();
         final AtomicLong executedTasks = new AtomicLong();
         final Runnable executor = () -> {
            final long taskNr = executedTasks.getAndIncrement();
            if (sleepMS > 0 && sleepNumbers.contains(taskNr)) {
               try {
                  TimeUnit.MILLISECONDS.sleep(sleepMS);
               } catch (InterruptedException e) {
                  e.printStackTrace();
               }
            }
            countDown.countDown();
         };
         final Runnable loader = () -> {
            try {
               startLoadingLatch.await(5, TimeUnit.SECONDS);
            } catch (final Exception e) {
               e.printStackTrace();
            }

            while (createdTasks.getAndIncrement() < nrOfTasks) {
               workExecutor.execute(executor);
            }
         };

         for (int i = 0; i < concurrentLoads; ++i) {
            loaderExecutor.execute(loader);
         }
         startLoadingLatch.countDown();

         countDown.await(60, TimeUnit.SECONDS);

         assertEquals(nrOfTasks, executedTasks.get());
      }

      TimeUnit.MILLISECONDS.sleep(3 * sleepMS);

      {
         final CountDownLatch countDown = new CountDownLatch(nrOfTasks);
         final CountDownLatch startLoadingLatch = new CountDownLatch(1);
         final AtomicLong createdTasks = new AtomicLong();
         final AtomicLong executedTasks = new AtomicLong();
         final Runnable executor = () -> {
            final long taskNr = executedTasks.getAndIncrement();
            if (sleepMS > 0 && sleepNumbers.contains(taskNr)) {
               try {
                  TimeUnit.MILLISECONDS.sleep(sleepMS);
               } catch (InterruptedException e) {
                  e.printStackTrace();
               }
            }
            countDown.countDown();
         };
         final Runnable loader = () -> {
            try {
               startLoadingLatch.await(5, TimeUnit.SECONDS);
            } catch (final Exception e) {
               e.printStackTrace();
            }

            while (createdTasks.getAndIncrement() < nrOfTasks) {
               workExecutor.execute(executor);
            }
         };

         for (int i = 0; i < concurrentLoads; ++i) {
            loaderExecutor.execute(loader);
         }
         startLoadingLatch.countDown();

         countDown.await(60, TimeUnit.SECONDS);

         assertEquals(nrOfTasks, executedTasks.get());
      }

      TimeUnit.MILLISECONDS.sleep(3 * sleepMS);

      {
         final CountDownLatch countDown = new CountDownLatch(nrOfTasks);
         final CountDownLatch startLoadingLatch = new CountDownLatch(1);
         final AtomicLong createdTasks = new AtomicLong();
         final AtomicLong executedTasks = new AtomicLong();
         final Runnable executor = () -> {
            final long taskNr = executedTasks.getAndIncrement();
            if (sleepMS > 0 && sleepNumbers.contains(taskNr)) {
               try {
                  TimeUnit.MILLISECONDS.sleep(sleepMS);
               } catch (InterruptedException e) {
                  e.printStackTrace();
               }
            }
            countDown.countDown();
         };
         final Runnable loader = () -> {
            try {
               startLoadingLatch.await(5, TimeUnit.SECONDS);
            } catch (final Exception e) {
               e.printStackTrace();
            }

            while (createdTasks.getAndIncrement() < nrOfTasks) {
               workExecutor.execute(executor);
            }
         };

         for (int i = 0; i < concurrentLoads; ++i) {
            loaderExecutor.execute(loader);
         }
         startLoadingLatch.countDown();

         countDown.await(60, TimeUnit.SECONDS);

         assertEquals(nrOfTasks, executedTasks.get());
      }

      TimeUnit.MILLISECONDS.sleep(3 * sleepMS);

      {
         final CountDownLatch countDown = new CountDownLatch(nrOfTasks);
         final CountDownLatch startLoadingLatch = new CountDownLatch(1);
         final AtomicLong createdTasks = new AtomicLong();
         final AtomicLong executedTasks = new AtomicLong();
         final Runnable executor = () -> {
            final long taskNr = executedTasks.getAndIncrement();
            if (sleepMS > 0 && sleepNumbers.contains(taskNr)) {
               try {
                  TimeUnit.MILLISECONDS.sleep(sleepMS);
               } catch (InterruptedException e) {
                  e.printStackTrace();
               }
            }
            countDown.countDown();
         };
         final Runnable loader = () -> {
            try {
               startLoadingLatch.await(5, TimeUnit.SECONDS);
            } catch (final Exception e) {
               e.printStackTrace();
            }

            while (createdTasks.getAndIncrement() < nrOfTasks) {
               workExecutor.execute(executor);
            }
         };

         for (int i = 0; i < concurrentLoads; ++i) {
            loaderExecutor.execute(loader);
         }
         startLoadingLatch.countDown();

         countDown.await(60, TimeUnit.SECONDS);

         assertEquals(nrOfTasks, executedTasks.get());
      }

      TimeUnit.MILLISECONDS.sleep(3 * sleepMS);

      {
         final CountDownLatch countDown = new CountDownLatch(nrOfTasks);
         final CountDownLatch startLoadingLatch = new CountDownLatch(1);
         final AtomicLong createdTasks = new AtomicLong();
         final AtomicLong executedTasks = new AtomicLong();
         final Runnable executor = () -> {
            final long taskNr = executedTasks.getAndIncrement();
            if (sleepMS > 0 && sleepNumbers.contains(taskNr)) {
               try {
                  TimeUnit.MILLISECONDS.sleep(sleepMS);
               } catch (InterruptedException e) {
                  e.printStackTrace();
               }
            }
            countDown.countDown();
         };
         final Runnable loader = () -> {
            try {
               startLoadingLatch.await(5, TimeUnit.SECONDS);
            } catch (final Exception e) {
               e.printStackTrace();
            }

            while (createdTasks.getAndIncrement() < nrOfTasks) {
               workExecutor.execute(executor);
            }
         };

         for (int i = 0; i < concurrentLoads; ++i) {
            loaderExecutor.execute(loader);
         }
         startLoadingLatch.countDown();

         countDown.await(60, TimeUnit.SECONDS);

         assertEquals(nrOfTasks, executedTasks.get());
      }

      loaderExecutor.shutdown();
      workExecutor.shutdown();
   }
}
