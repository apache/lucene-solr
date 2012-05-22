package org.apache.lucene.index;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.index.DocumentsWriterStallControl.MemoryController;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.ThreadInterruptedException;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeaks;

/**
 * Tests for {@link DocumentsWriterStallControl}
 */
@ThreadLeaks(failTestIfLeaking = true)
public class TestDocumentsWriterStallControl extends LuceneTestCase {
  
  public void testSimpleStall() throws InterruptedException {
    DocumentsWriterStallControl ctrl = new DocumentsWriterStallControl();
    SimpleMemCtrl memCtrl = new SimpleMemCtrl();
    memCtrl.limit = 1000;
    memCtrl.netBytes = 1000;
    memCtrl.flushBytes = 20;
    ctrl.updateStalled(memCtrl);
    Thread[] waitThreads = waitThreads(atLeast(1), ctrl);
    start(waitThreads);
    assertFalse(ctrl.hasBlocked());
    assertFalse(ctrl.anyStalledThreads());
    join(waitThreads, 10);
    
    // now stall threads and wake them up again
    memCtrl.netBytes = 1001;
    memCtrl.flushBytes = 100;
    ctrl.updateStalled(memCtrl);
    waitThreads = waitThreads(atLeast(1), ctrl);
    start(waitThreads);
    awaitState(100, Thread.State.WAITING, waitThreads);
    assertTrue(ctrl.hasBlocked());
    assertTrue(ctrl.anyStalledThreads());
    memCtrl.netBytes = 50;
    memCtrl.flushBytes = 0;
    ctrl.updateStalled(memCtrl);
    assertFalse(ctrl.anyStalledThreads());
    join(waitThreads, 500);
  }
  
  public void testRandom() throws InterruptedException {
    final DocumentsWriterStallControl ctrl = new DocumentsWriterStallControl();
    SimpleMemCtrl memCtrl = new SimpleMemCtrl();
    memCtrl.limit = 1000;
    memCtrl.netBytes = 1;
    ctrl.updateStalled(memCtrl);
    Thread[] stallThreads = new Thread[atLeast(3)];
    for (int i = 0; i < stallThreads.length; i++) {
      final int threadId = i;
      stallThreads[i] = new Thread() {
        public void run() {
          int baseBytes = threadId % 2 == 0 ? 500 : 700;
          SimpleMemCtrl memCtrl = new SimpleMemCtrl();
          memCtrl.limit = 1000;
          memCtrl.netBytes = 1;
          memCtrl.flushBytes = 0;

          int iters = atLeast(1000);
          for (int j = 0; j < iters; j++) {
            memCtrl.netBytes = baseBytes + random().nextInt(1000);
            memCtrl.flushBytes = random().nextInt((int)memCtrl.netBytes);
            ctrl.updateStalled(memCtrl);
            if (random().nextInt(5) == 0) { // thread 0 only updates
              ctrl.waitIfStalled();
            }
          }
        }
      };
    }
    start(stallThreads);
    long time = System.currentTimeMillis();
    /*
     * use a 100 sec timeout to make sure we not hang forever. join will fail in
     * that case
     */
    while ((System.currentTimeMillis() - time) < 100 * 1000
        && !terminated(stallThreads)) {
      ctrl.updateStalled(memCtrl);
      if (random().nextBoolean()) {
        Thread.yield();
      } else {
        Thread.sleep(1);
      }
      
    }
    join(stallThreads, 100);
    
  }
  
  public void testAccquireReleaseRace() throws InterruptedException {
    final DocumentsWriterStallControl ctrl = new DocumentsWriterStallControl();
    SimpleMemCtrl memCtrl = new SimpleMemCtrl();
    memCtrl.limit = 1000;
    memCtrl.netBytes = 1;
    memCtrl.flushBytes = 0;
    ctrl.updateStalled(memCtrl);
    final AtomicBoolean stop = new AtomicBoolean(false);
    final AtomicBoolean checkPoint = new AtomicBoolean(true);
    
    int numStallers = atLeast(1);
    int numReleasers = atLeast(1);
    int numWaiters = atLeast(1);
    
    final CountDownLatch[] latches = new CountDownLatch[] {
        new CountDownLatch(numStallers + numReleasers), new CountDownLatch(1),
        new CountDownLatch(numWaiters)};
    Thread[] threads = new Thread[numReleasers + numStallers + numWaiters];
    List<Throwable> exceptions =  Collections.synchronizedList(new ArrayList<Throwable>());
    for (int i = 0; i < numReleasers; i++) {
      threads[i] = new Updater(stop, checkPoint, ctrl, latches, true, exceptions);
    }
    for (int i = numReleasers; i < numReleasers + numStallers; i++) {
      threads[i] = new Updater(stop, checkPoint, ctrl, latches, false, exceptions);
      
    }
    for (int i = numReleasers + numStallers; i < numReleasers + numStallers
        + numWaiters; i++) {
      threads[i] = new Waiter(stop, checkPoint, ctrl, latches, exceptions);
      
    }
    
    start(threads);
    int iters = atLeast(20000);
    for (int i = 0; i < iters; i++) {
      if (checkPoint.get()) {
       
        assertTrue("timed out waiting for update threads - deadlock?", latches[0].await(10, TimeUnit.SECONDS));
        if (!exceptions.isEmpty()) {
          for (Throwable throwable : exceptions) {
            throwable.printStackTrace();
          }
          fail("got exceptions in threads");
        }
        
        if (!ctrl.anyStalledThreads()) {
          assertTrue(
              "control claims no stalled threads but waiter seems to be blocked",
              latches[2].await(10, TimeUnit.SECONDS));
        }
        checkPoint.set(false);
        
        latches[1].countDown();
      }
      assertFalse(checkPoint.get());
      if (random().nextInt(2) == 0) {
        latches[0] = new CountDownLatch(numStallers + numReleasers);
        latches[1] = new CountDownLatch(1);
        latches[2] = new CountDownLatch(numWaiters);
        checkPoint.set(true);
      }
  
    }
    
    stop.set(true);
    latches[1].countDown();
    
    for (int i = 0; i < threads.length; i++) {
      memCtrl.limit = 1000;
      memCtrl.netBytes = 1;
      memCtrl.flushBytes = 0;
      ctrl.updateStalled(memCtrl);
      threads[i].join(2000);
      if (threads[i].isAlive() && threads[i] instanceof Waiter) {
        if (threads[i].getState() == Thread.State.WAITING) {
          fail("waiter is not released - anyThreadsStalled: "
              + ctrl.anyStalledThreads());
        }
      }
    }
  }
  
  public static class Waiter extends Thread {
    private CountDownLatch[] latches;
    private DocumentsWriterStallControl ctrl;
    private AtomicBoolean checkPoint;
    private AtomicBoolean stop;
    private List<Throwable> exceptions;
    
    public Waiter(AtomicBoolean stop, AtomicBoolean checkPoint,
        DocumentsWriterStallControl ctrl, CountDownLatch[] latches,
        List<Throwable> exceptions) {
      this.stop = stop;
      this.checkPoint = checkPoint;
      this.ctrl = ctrl;
      this.latches = latches;
      this.exceptions = exceptions;
    }
    
    public void run() {
      try {
        while (!stop.get()) {
          ctrl.waitIfStalled();
          if (checkPoint.get()) {
            CountDownLatch join = latches[2];
            CountDownLatch wait = latches[1];
            join.countDown();
            try {
              assertTrue(wait.await(10, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
              System.out.println("[Waiter] got interrupted - wait count: " + wait.getCount());
              throw new ThreadInterruptedException(e);
            }
          }
        }
      } catch (Throwable e) {
        e.printStackTrace();
        exceptions.add(e);
      }
    }
  }
  
  public static class Updater extends Thread {
    
    private CountDownLatch[] latches;
    private DocumentsWriterStallControl ctrl;
    private AtomicBoolean checkPoint;
    private AtomicBoolean stop;
    private boolean release;
    private List<Throwable> exceptions;
    
    public Updater(AtomicBoolean stop, AtomicBoolean checkPoint,
        DocumentsWriterStallControl ctrl, CountDownLatch[] latches,
        boolean release, List<Throwable> exceptions) {
      this.stop = stop;
      this.checkPoint = checkPoint;
      this.ctrl = ctrl;
      this.latches = latches;
      this.release = release;
      this.exceptions = exceptions;
    }
    
    public void run() {
      try {
        SimpleMemCtrl memCtrl = new SimpleMemCtrl();
        memCtrl.limit = 1000;
        memCtrl.netBytes = release ? 1 : 2000;
        memCtrl.flushBytes = random().nextInt((int)memCtrl.netBytes);
        while (!stop.get()) {
          int internalIters = release && random().nextBoolean() ? atLeast(5) : 1;
          for (int i = 0; i < internalIters; i++) {
            ctrl.updateStalled(memCtrl);
          }
          if (checkPoint.get()) {
            CountDownLatch join = latches[0];
            CountDownLatch wait = latches[1];
            join.countDown();
            try {
              assertTrue(wait.await(10, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
              System.out.println("[Updater] got interrupted - wait count: " + wait.getCount());
              throw new ThreadInterruptedException(e);
            }
          }
          Thread.yield();
        }
      } catch (Throwable e) {
        e.printStackTrace();
        exceptions.add(e);
      }
    }
    
  }
  
  public static boolean terminated(Thread[] threads) {
    for (Thread thread : threads) {
      if (Thread.State.TERMINATED != thread.getState()) return false;
    }
    return true;
  }
  
  public static void start(Thread[] tostart) throws InterruptedException {
    for (Thread thread : tostart) {
      thread.start();
    }
    Thread.sleep(1); // let them start
  }
  
  public static void join(Thread[] toJoin, long timeout)
      throws InterruptedException {
    for (Thread thread : toJoin) {
      thread.join(timeout);
    }
  }
  
  public static Thread[] waitThreads(int num,
      final DocumentsWriterStallControl ctrl) {
    Thread[] array = new Thread[num];
    for (int i = 0; i < array.length; i++) {
      array[i] = new Thread() {
        public void run() {
          ctrl.waitIfStalled();
        }
      };
    }
    return array;
  }
  
  public static void awaitState(long timeout, Thread.State state,
      Thread... threads) throws InterruptedException {
    long t = System.currentTimeMillis();
    while (System.currentTimeMillis() - t <= timeout) {
      boolean done = true;
      for (Thread thread : threads) {
        if (thread.getState() != state) {
          done = false;
        }
      }
      if (done) {
        return;
      }
      if (random().nextBoolean()) {
        Thread.yield();
      } else {
        Thread.sleep(1);
      }
    }
    fail("timed out waiting for state: " + state + " timeout: " + timeout
        + " ms");
  }
  
  private static class SimpleMemCtrl implements MemoryController {
    long netBytes;
    long limit;
    long flushBytes;
    
    @Override
    public long netBytes() {
      return netBytes;
    }
    
    @Override
    public long stallLimitBytes() {
      return limit;
    }

    @Override
    public long flushBytes() {
      return flushBytes;
    }
    
  }
}
