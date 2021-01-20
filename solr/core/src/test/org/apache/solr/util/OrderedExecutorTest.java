/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.util;

import java.lang.invoke.MethodHandles;

import java.util.HashMap;
import java.util.Map;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;

import org.apache.solr.SolrTestCase;
import org.apache.solr.common.util.ExecutorUtil;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrderedExecutorTest extends SolrTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Test
  public void testExecutionInOrder() {
    OrderedExecutor orderedExecutor = new OrderedExecutor(10, ExecutorUtil.newMDCAwareCachedThreadPool("executeInOrderTest"));
    IntBox intBox = new IntBox();
    for (int i = 0; i < 100; i++) {
      orderedExecutor.execute(1, () -> intBox.value++);
    }
    orderedExecutor.shutdownAndAwaitTermination();
    assertEquals(intBox.value, 100);
  }

  @Test
  public void testLockWhenQueueIsFull() {
    final ExecutorService controlExecutor = ExecutorUtil.newMDCAwareCachedThreadPool("testLockWhenQueueIsFull_control");
    final OrderedExecutor orderedExecutor = new OrderedExecutor
      (10, ExecutorUtil.newMDCAwareCachedThreadPool("testLockWhenQueueIsFull_test"));
    
    try {
      // AAA and BBB events will both depend on the use of the same lockId
      final BlockingQueue<String> events = new ArrayBlockingQueue<>(2);
      final Integer lockId = 1;
      
      // AAA enters executor first so it should execute first (even though it's waiting on latch)
      final CountDownLatch latchAAA = new CountDownLatch(1);
      orderedExecutor.execute(lockId, () -> {
          try {
            if (latchAAA.await(120, TimeUnit.SECONDS)) {
              events.add("AAA");
            } else {
              events.add("AAA Timed Out");
            }
          } catch (InterruptedException e) {
            log.error("Interrupt in AAA worker", e);
            Thread.currentThread().interrupt();
          }
        });
      // BBB doesn't care about the latch, but because it uses the same lockId, it's blocked on AAA
      // so we execute it in a background thread...
      controlExecutor.execute(() -> {
          orderedExecutor.execute(lockId, () -> {
              events.add("BBB");
            });
        });
      
      // now if we release the latchAAA, AAA should be garunteed to fire first, then BBB
      latchAAA.countDown();
      try {
        assertEquals("AAA", events.poll(120, TimeUnit.SECONDS));
        assertEquals("BBB", events.poll(120, TimeUnit.SECONDS));
      } catch (InterruptedException e) {
        log.error("Interrupt polling event queue", e);
        Thread.currentThread().interrupt();
        fail("interupt while trying to poll event queue");
      }
    } finally {
      ExecutorUtil.shutdownAndAwaitTermination(controlExecutor);
      orderedExecutor.shutdownAndAwaitTermination();
    }
  }

  @Test
  public void testRunInParallel() {
    final int parallelism = atLeast(3);
    
    final ExecutorService controlExecutor = ExecutorUtil.newMDCAwareCachedThreadPool("testRunInParallel_control");
    final OrderedExecutor orderedExecutor = new OrderedExecutor
      (parallelism, ExecutorUtil.newMDCAwareCachedThreadPool("testRunInParallel_test"));

    try {
      // distinct lockIds should be able to be used in parallel, up to the size of the executor,
      // w/o any execute calls blocking... until the test Runables being executed are all
      // waiting on the same cyclic barrier...
      final CyclicBarrier barrier = new CyclicBarrier(parallelism + 1);
      final CountDownLatch preBarrierLatch = new CountDownLatch(parallelism);
      final CountDownLatch postBarrierLatch = new CountDownLatch(parallelism);
      
      for (int i = 0; i < parallelism; i++) {
        final int lockId = i;
        controlExecutor.execute(() -> {
            orderedExecutor.execute(lockId, () -> {
                try {
                  log.info("Worker #{} starting", lockId);
                  preBarrierLatch.countDown();
                  barrier.await(120, TimeUnit.SECONDS);
                  postBarrierLatch.countDown();
                } catch (TimeoutException t) {
                  log.error("Timeout in worker# {} awaiting barrier", lockId, t);
                } catch (BrokenBarrierException b) {
                  log.error("Broken Barrier in worker#{}", lockId, b);
                } catch (InterruptedException e) {
                  log.error("Interrupt in worker#{} awaiting barrier", lockId, e);
                  Thread.currentThread().interrupt();
                }
              });
          });
      }

      if (log.isInfoEnabled()) {
        log.info("main thread: about to wait on pre-barrier latch, barrier={}, post-barrier latch={}",
            barrier.getNumberWaiting(), postBarrierLatch.getCount());
      }
      
      try {
        // this latch should have fully counted down by now
        // (or with a small await for thread scheduling but no other external action)
        assertTrue("Timeout awaiting pre barrier latch",
                   preBarrierLatch.await(120, TimeUnit.SECONDS));
      } catch (InterruptedException e) {
        log.error("Interrupt awwaiting pre barrier latch", e);
        Thread.currentThread().interrupt();
        fail("interupt while trying to await the preBarrierLatch");
      }

      if (log.isInfoEnabled()) {
        log.info("main thread: pre-barrier latch done, barrier={}, post-barrier latch={}",
            barrier.getNumberWaiting(), postBarrierLatch.getCount());
      }
      
      // nothing should have counted down yet on the postBarrierLatch
      assertEquals(parallelism, postBarrierLatch.getCount());

      try {
        // if we now await on the the barrier, it should release
        // (once all other threads get to the barrier as well, but no external action needed)
        barrier.await(120, TimeUnit.SECONDS);

        if (log.isInfoEnabled()) {
          log.info("main thread: barrier has released, post-barrier latch={}",
              postBarrierLatch.getCount());
        }
        
        // and now the post-barrier latch should release immediately
        // (or with a small await for thread scheduling but no other external action)
        assertTrue("Timeout awaiting post barrier latch",
                   postBarrierLatch.await(120, TimeUnit.SECONDS));
      } catch (TimeoutException t) {
        log.error("Timeout awaiting barrier", t);
        fail("barrier timed out");
      } catch (BrokenBarrierException b) {
        log.error("Broken Barrier in main test thread", b);
        fail("broken barrier while trying to release the barrier");
      } catch (InterruptedException e) {
        log.error("Interrupt awwaiting barrier / post barrier latch", e);
        Thread.currentThread().interrupt();
        fail("interupt while trying to release the barrier and await the postBarrierLatch");
      }
    } finally {
      ExecutorUtil.shutdownAndAwaitTermination(controlExecutor);
      orderedExecutor.shutdownAndAwaitTermination();
    }
  }

  @Test
  public void testStress() {
    int N = random().nextInt(50) + 20;
    Map<Integer, Integer> base = new HashMap<>();
    Map<Integer, Integer> run = new HashMap<>();
    for (int i = 0; i < N; i++) {
      base.put(i, i);
      run.put(i, i);
    }
    OrderedExecutor orderedExecutor = new OrderedExecutor(10, ExecutorUtil.newMDCAwareCachedThreadPool("testStress"));
    for (int i = 0; i < 1000; i++) {
      int key = random().nextInt(N);
      base.put(key, base.get(key) + 1);
      orderedExecutor.execute(key, () -> run.put(key, run.get(key) + 1));
    }
    orderedExecutor.shutdownAndAwaitTermination();
    assertTrue(base.equals(run));
  }

  private static class IntBox {
    int value;
  }
}
