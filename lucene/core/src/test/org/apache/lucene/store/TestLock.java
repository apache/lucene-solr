package org.apache.lucene.store;

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


import java.io.IOException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.lucene.util.LuceneTestCase;

public class TestLock extends LuceneTestCase {

    public void testObtain() {
        LockMock lock = new LockMock();
        Lock.LOCK_POLL_INTERVAL = 10;

        try {
            lock.obtain(Lock.LOCK_POLL_INTERVAL);
            fail("Should have failed to obtain lock");
        } catch (IOException e) {
            assertEquals("should attempt to lock more than once", lock.lockAttempts, 2);
        }
    }

    private class LockMock extends Lock {
        public int lockAttempts;

        @Override
        public boolean obtain() {
            lockAttempts++;
            return false;
        }
        @Override
        public void close() {
            // do nothing
        }
        @Override
        public boolean isLocked() {
            return false;
        }
    }

  public void testObtainConcurrently() throws InterruptedException, IOException {
    final Directory directory;
    if (random().nextBoolean()) {
      directory = newDirectory();
    } else {
      LockFactory lf = random().nextBoolean() ? SimpleFSLockFactory.INSTANCE : NativeFSLockFactory.INSTANCE;
      directory = newFSDirectory(createTempDir(), lf);
    }
    final AtomicBoolean running = new AtomicBoolean(true);
    final AtomicInteger atomicCounter = new AtomicInteger(0);
    final ReentrantLock assertingLock = new ReentrantLock();
    int numThreads = 2 + random().nextInt(10);
    final int runs = 500 + random().nextInt(1000);
    CyclicBarrier barrier = new CyclicBarrier(numThreads);
    Thread[] threads = new Thread[numThreads];
    for (int i = 0; i < threads.length; i++) {
      threads[i] = new Thread() {
        @Override
        public void run() {
          try {
            barrier.await();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
          while (running.get()) {
            try (Lock lock = directory.makeLock("foo.lock")) {
              if (lock.isLocked() == false && lock.obtain()) {
                assertTrue(lock.isLocked());
                assertFalse(assertingLock.isLocked());
                if (assertingLock.tryLock()) {
                  assertingLock.unlock();
                } else {
                  fail();
                }
              }
            } catch (IOException ex) {
              //
            }
            if (atomicCounter.incrementAndGet() > runs) {
              running.set(false);
            }
          }
        }
      };
      threads[i].start();
    }

    for (int i = 0; i < threads.length; i++) {
      threads[i].join();
    }
    directory.close();
  }

  public void testSingleInstanceLockFactoryDoubleObtain() throws Exception {
    LockFactory lf = new SingleInstanceLockFactory();
    Directory dir = newFSDirectory(createTempDir(), lf);
    Lock lock = dir.makeLock("foo");
    assertTrue(lock.obtain());
    try {
      lock.obtain();
      fail("did not hit double-obtain failure");
    } catch (LockObtainFailedException lofe) {
      // expected
    }
    lock.close();
    
    lock = dir.makeLock("foo");
    assertTrue(lock.obtain());
    lock.close();
    dir.close();
  }

  public void testSimpleFSLockFactoryDoubleObtain() throws Exception {
    Directory dir = newFSDirectory(createTempDir(), SimpleFSLockFactory.INSTANCE);
    Lock lock = dir.makeLock("foo");
    assertTrue(lock.obtain());
    try {
      lock.obtain();
      fail("did not hit double-obtain failure");
    } catch (LockObtainFailedException lofe) {
      // expected
    }
    lock.close();
    
    lock = dir.makeLock("foo");
    assertTrue(lock.obtain());
    lock.close();
    dir.close();
  }

  public void testNativeFSLockFactoryDoubleObtain() throws Exception {
    Directory dir = newFSDirectory(createTempDir(), NativeFSLockFactory.INSTANCE);
    Lock lock = dir.makeLock("foo");
    assertTrue(lock.obtain());
    try {
      lock.obtain();
      fail("did not hit double-obtain failure");
    } catch (LockObtainFailedException lofe) {
      // expected
    }
    lock.close();
    
    lock = dir.makeLock("foo");
    assertTrue(lock.obtain());
    lock.close();
    dir.close();
  }
}
