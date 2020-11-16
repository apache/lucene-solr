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

package org.apache.lucene.index;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.Version;

public class TestDocumentsWriterPerThreadPool extends LuceneTestCase {

  public void testLockReleaseAndClose() throws IOException {
    try (Directory directory = newDirectory()) {
      DocumentsWriterPerThreadPool pool = new DocumentsWriterPerThreadPool(() ->
          new DocumentsWriterPerThread(Version.LATEST.major, "", directory, directory,
              newIndexWriterConfig(), new DocumentsWriterDeleteQueue(null), null, new AtomicLong(), false));

      DocumentsWriterPerThread first = pool.getAndLock();
      assertEquals(1, pool.size());
      DocumentsWriterPerThread second = pool.getAndLock();
      assertEquals(2, pool.size());
      pool.marksAsFreeAndUnlock(first);
      assertEquals(2, pool.size());
      DocumentsWriterPerThread third = pool.getAndLock();
      assertSame(first, third);
      assertEquals(2, pool.size());
      pool.checkout(third);
      assertEquals(1, pool.size());

      pool.close();
      assertEquals(1, pool.size());
      pool.marksAsFreeAndUnlock(second);
      assertEquals(1, pool.size());
      for (DocumentsWriterPerThread lastPerThead : pool.filterAndLock(x -> true)) {
        pool.checkout(lastPerThead);
        lastPerThead.unlock();
      }
      assertEquals(0, pool.size());
    }
  }

  public void testCloseWhileNewWritersLocked() throws IOException, InterruptedException {
    try (Directory directory = newDirectory()) {
      DocumentsWriterPerThreadPool pool = new DocumentsWriterPerThreadPool(() ->
          new DocumentsWriterPerThread(Version.LATEST.major, "", directory, directory,
              newIndexWriterConfig(), new DocumentsWriterDeleteQueue(null), null, new AtomicLong(), false));

      DocumentsWriterPerThread first = pool.getAndLock();
      pool.lockNewWriters();
      CountDownLatch latch = new CountDownLatch(1);
      Thread t = new Thread(() -> {
        try {
          latch.countDown();
          pool.getAndLock();
          fail();
        } catch (AlreadyClosedException e) {
          // fine
        }
      });
      t.start();
      latch.await();
      while (t.getState().equals(Thread.State.WAITING) == false) {
        Thread.yield();
      }
      first.unlock();
      pool.close();
      pool.unlockNewWriters();
      for (DocumentsWriterPerThread perThread : pool.filterAndLock(x -> true)) {
        assertTrue(pool.checkout(perThread));
        perThread.unlock();
      }
      assertEquals(0, pool.size());
    }
  }
}
