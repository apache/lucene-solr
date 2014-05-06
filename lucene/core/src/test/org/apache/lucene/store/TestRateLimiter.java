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

import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.store.RateLimiter.SimpleRateLimiter;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.ThreadInterruptedException;

/**
 * Simple testcase for RateLimiter.SimpleRateLimiter
 */
public final class TestRateLimiter extends LuceneTestCase {

  public void testPause() {
    SimpleRateLimiter limiter = new SimpleRateLimiter(10); // 10 MB / Sec
    limiter.pause(2);//init
    long pause = 0;
    for (int i = 0; i < 3; i++) {
      pause += limiter.pause(4 * 1024 * 1024); // fire up 3 * 4 MB 
    }
    final long convert = TimeUnit.MILLISECONDS.convert(pause, TimeUnit.NANOSECONDS);
    assertTrue("we should sleep less than 2 seconds but did: " + convert + " millis", convert < 2000l); 
    assertTrue("we should sleep at least 1 second but did only: " + convert + " millis", convert > 1000l); 
  }

  public void testThreads() throws Exception {

    double targetMBPerSec = 10.0 + 20 * random().nextDouble();
    final SimpleRateLimiter limiter = new SimpleRateLimiter(targetMBPerSec);

    final CountDownLatch startingGun = new CountDownLatch(1);

    Thread[] threads = new Thread[TestUtil.nextInt(random(), 3, 6)];
    final AtomicLong totBytes = new AtomicLong();
    for(int i=0;i<threads.length;i++) {
      threads[i] = new Thread() {
          @Override
          public void run() {
            try {
              startingGun.await();
            } catch (InterruptedException ie) {
              throw new ThreadInterruptedException(ie);
            }
            long bytesSinceLastPause = 0;
            for(int i=0;i<500;i++) {
              long numBytes = TestUtil.nextInt(random(), 1000, 10000);
              totBytes.addAndGet(numBytes);
              bytesSinceLastPause += numBytes;
              if (bytesSinceLastPause > limiter.getMinPauseCheckBytes()) {
                limiter.pause(bytesSinceLastPause);
                bytesSinceLastPause = 0;
              }
            }
          }
        };
      threads[i].start();
    }

    long startNS = System.nanoTime();
    startingGun.countDown();
    for(Thread thread : threads) {
      thread.join();
    }
    long endNS = System.nanoTime();
    double actualMBPerSec = (totBytes.get()/1024/1024.)/((endNS-startNS)/1000000000.0);

    // TODO: this may false trip .... could be we can only assert that it never exceeds the max, so slow jenkins doesn't trip:
    double ratio = actualMBPerSec/targetMBPerSec;

    // Only enforce that it wasn't too fast; if machine is bogged down (can't schedule threads / sleep properly) then it may falsely be too slow:
    //assertTrue("targetMBPerSec=" + targetMBPerSec + " actualMBPerSec=" + actualMBPerSec, ratio >= 0.9 && ratio <= 1.1);
    assertTrue("targetMBPerSec=" + targetMBPerSec + " actualMBPerSec=" + actualMBPerSec, ratio <= 1.1);
  }
}
