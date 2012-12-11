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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.ThreadInterruptedException;

/**
 * Tests for {@link DocumentsWriterStallControl}
 */
public class TestDocumentsWriterStallControl extends LuceneTestCase {
  
  public void testSimpleStall() throws InterruptedException {
    DocumentsWriterStallControl ctrl = new DocumentsWriterStallControl();
   
    ctrl.updateStalled(false);
    Thread[] waitThreads = waitThreads(atLeast(1), ctrl);
    start(waitThreads);
    assertFalse(ctrl.hasBlocked());
    assertFalse(ctrl.anyStalledThreads());
    join(waitThreads);
    
    // now stall threads and wake them up again
    ctrl.updateStalled(true);
    waitThreads = waitThreads(atLeast(1), ctrl);
    start(waitThreads);
    awaitState(Thread.State.WAITING, waitThreads);
    assertTrue(ctrl.hasBlocked());
    assertTrue(ctrl.anyStalledThreads());
    ctrl.updateStalled(false);
    assertFalse(ctrl.anyStalledThreads());
    join(waitThreads);
  }
  
  public void testRandom() throws InterruptedException {
    final DocumentsWriterStallControl ctrl = new DocumentsWriterStallControl();
    ctrl.updateStalled(false);
    
    Thread[] stallThreads = new Thread[atLeast(3)];
    for (int i = 0; i < stallThreads.length; i++) {
      final int stallProbability = 1 +random().nextInt(10);
      stallThreads[i] = new Thread() {
        @Override
        public void run() {

          int iters = atLeast(1000);
          for (int j = 0; j < iters; j++) {
            ctrl.updateStalled(random().nextInt(stallProbability) == 0);
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
      ctrl.updateStalled(false);
      if (random().nextBoolean()) {
        Thread.yield();
      } else {
        Thread.sleep(1);
      }
      
    }
    join(stallThreads);
    
  }
  
  public void testAccquireReleaseRace() throws InterruptedException {
    final DocumentsWriterStallControl ctrl = new DocumentsWriterStallControl();
    ctrl.updateStalled(false);
    final AtomicBoolean stop = new AtomicBoolean(false);
    final AtomicBoolean checkPoint = new AtomicBoolean(true);
    
    int numStallers = atLeast(1);
    int numReleasers = atLeast(1);
    int numWaiters = atLeast(1);
    final Synchronizer sync = new Synchronizer(numStallers + numReleasers, numStallers + numReleasers+numWaiters);
    Thread[] threads = new Thread[numReleasers + numStallers + numWaiters];
    List<Throwable> exceptions =  Collections.synchronizedList(new ArrayList<Throwable>());
    for (int i = 0; i < numReleasers; i++) {
      threads[i] = new Updater(stop, checkPoint, ctrl, sync, true, exceptions);
    }
    for (int i = numReleasers; i < numReleasers + numStallers; i++) {
      threads[i] = new Updater(stop, checkPoint, ctrl, sync, false, exceptions);
      
    }
    for (int i = numReleasers + numStallers; i < numReleasers + numStallers
        + numWaiters; i++) {
      threads[i] = new Waiter(stop, checkPoint, ctrl, sync, exceptions);
      
    }
    
    start(threads);
    int iters = atLeast(10000);
    final float checkPointProbability = TEST_NIGHTLY ? 0.5f : 0.1f;
    for (int i = 0; i < iters; i++) {
      if (checkPoint.get()) {
       
        assertTrue("timed out waiting for update threads - deadlock?", sync.updateJoin.await(10, TimeUnit.SECONDS));
        if (!exceptions.isEmpty()) {
          for (Throwable throwable : exceptions) {
            throwable.printStackTrace();
          }
          fail("got exceptions in threads");
        }
        
        if (ctrl.hasBlocked() && ctrl.isHealthy()) {
          assertState(numReleasers, numStallers, numWaiters, threads, ctrl);
          
           
          }
        
        checkPoint.set(false);
        sync.waiter.countDown();
        sync.leftCheckpoint.await();
      }
      assertFalse(checkPoint.get());
      assertEquals(0, sync.waiter.getCount());
      if (checkPointProbability >= random().nextFloat()) {
        sync.reset(numStallers + numReleasers, numStallers + numReleasers
            + numWaiters);
        checkPoint.set(true);
      }
  
    }
    if (!checkPoint.get()) {
      sync.reset(numStallers + numReleasers, numStallers + numReleasers
          + numWaiters);
      checkPoint.set(true);
    }
    
    assertTrue(sync.updateJoin.await(10, TimeUnit.SECONDS));
    assertState(numReleasers, numStallers, numWaiters, threads, ctrl);
    checkPoint.set(false);
    stop.set(true);
    sync.waiter.countDown();
    sync.leftCheckpoint.await();
    
    
    for (int i = 0; i < threads.length; i++) {
      ctrl.updateStalled(false);
      threads[i].join(2000);
      if (threads[i].isAlive() && threads[i] instanceof Waiter) {
        if (threads[i].getState() == Thread.State.WAITING) {
          fail("waiter is not released - anyThreadsStalled: "
              + ctrl.anyStalledThreads());
        }
      }
    }
  }
  
  private void assertState(int numReleasers, int numStallers, int numWaiters, Thread[] threads, DocumentsWriterStallControl ctrl) throws InterruptedException {
    int millisToSleep = 100;
    while (true) {
      if (ctrl.hasBlocked() && ctrl.isHealthy()) {
        for (int n = numReleasers + numStallers; n < numReleasers
            + numStallers + numWaiters; n++) {
          if (ctrl.isThreadQueued(threads[n])) {
            if (millisToSleep < 60000) {
              Thread.sleep(millisToSleep);
              millisToSleep *=2;
              break;
            } else {
              fail("control claims no stalled threads but waiter seems to be blocked ");
            }
          }
        }
        break;
      } else {
        break;
      }
    }
    
  }

  public static class Waiter extends Thread {
    private Synchronizer sync;
    private DocumentsWriterStallControl ctrl;
    private AtomicBoolean checkPoint;
    private AtomicBoolean stop;
    private List<Throwable> exceptions;
    
    public Waiter(AtomicBoolean stop, AtomicBoolean checkPoint,
        DocumentsWriterStallControl ctrl, Synchronizer sync,
        List<Throwable> exceptions) {
      super("waiter");
      this.stop = stop;
      this.checkPoint = checkPoint;
      this.ctrl = ctrl;
      this.sync = sync;
      this.exceptions = exceptions;
    }
    
    @Override
    public void run() {
      try {
        while (!stop.get()) {
          ctrl.waitIfStalled();
          if (checkPoint.get()) {
            try {
              assertTrue(sync.await());
            } catch (InterruptedException e) {
              System.out.println("[Waiter] got interrupted - wait count: " + sync.waiter.getCount());
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
    
    private Synchronizer sync;
    private DocumentsWriterStallControl ctrl;
    private AtomicBoolean checkPoint;
    private AtomicBoolean stop;
    private boolean release;
    private List<Throwable> exceptions;
    
    public Updater(AtomicBoolean stop, AtomicBoolean checkPoint,
        DocumentsWriterStallControl ctrl, Synchronizer sync,
        boolean release, List<Throwable> exceptions) {
      super("updater");
      this.stop = stop;
      this.checkPoint = checkPoint;
      this.ctrl = ctrl;
      this.sync = sync;
      this.release = release;
      this.exceptions = exceptions;
    }
    
    @Override
    public void run() {
      try {
       
        while (!stop.get()) {
          int internalIters = release && random().nextBoolean() ? atLeast(5) : 1;
          for (int i = 0; i < internalIters; i++) {
            ctrl.updateStalled(random().nextBoolean());
          }
          if (checkPoint.get()) {
            sync.updateJoin.countDown();
            try {
              assertTrue(sync.await());
            } catch (InterruptedException e) {
              System.out.println("[Updater] got interrupted - wait count: " + sync.waiter.getCount());
              throw new ThreadInterruptedException(e);
            }
            sync.leftCheckpoint.countDown();
          }
          if (random().nextBoolean()) {
            Thread.yield();
          }
        }
      } catch (Throwable e) {
        e.printStackTrace();
        exceptions.add(e);
      }
      sync.updateJoin.countDown();
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
  
  public static void join(Thread[] toJoin)
      throws InterruptedException {
    for (Thread thread : toJoin) {
      thread.join();
    }
  }
  
  public static Thread[] waitThreads(int num,
      final DocumentsWriterStallControl ctrl) {
    Thread[] array = new Thread[num];
    for (int i = 0; i < array.length; i++) {
      array[i] = new Thread() {
        @Override
        public void run() {
          ctrl.waitIfStalled();
        }
      };
    }
    return array;
  }

  /** Waits for all incoming threads to be in wait()
   *  methods. */
  public static void awaitState(Thread.State state,
      Thread... threads) throws InterruptedException {
    while (true) {
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
  }
  
  private static final class Synchronizer {
    volatile CountDownLatch waiter;
    volatile CountDownLatch updateJoin;
    volatile CountDownLatch leftCheckpoint;
    
    public Synchronizer(int numUpdater, int numThreads) {
      reset(numUpdater, numThreads);
    }
    
    public void reset(int numUpdaters, int numThreads) {
      this.waiter = new CountDownLatch(1);
      this.updateJoin = new CountDownLatch(numUpdaters);
      this.leftCheckpoint = new CountDownLatch(numUpdaters);
    }
    
    public boolean await() throws InterruptedException {
      return waiter.await(10, TimeUnit.SECONDS);
    }
    
  }
}
