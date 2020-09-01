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

import java.lang.ref.WeakReference;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.lucene.index.DocumentsWriterDeleteQueue.DeleteSlice;
import org.apache.lucene.index.PrefixCodedTerms.TermIterator;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.ThreadInterruptedException;

/**
 * Unit test for {@link DocumentsWriterDeleteQueue}
 */
public class TestDocumentsWriterDeleteQueue extends LuceneTestCase {


  public void testAdvanceReferencesOriginal() {
    WeakAndNext weakAndNext = new WeakAndNext();
    DocumentsWriterDeleteQueue next = weakAndNext.next;
    assertNotNull(next);
    System.gc();
    assertNull(weakAndNext.weak.get());
  }
  class WeakAndNext {
    final WeakReference<DocumentsWriterDeleteQueue> weak;
    final DocumentsWriterDeleteQueue next;

    WeakAndNext() {
      DocumentsWriterDeleteQueue deleteQueue = new DocumentsWriterDeleteQueue(null);
      weak = new WeakReference<>(deleteQueue);
      next = deleteQueue.advanceQueue(2);
    }
  }


  public void testUpdateDelteSlices() throws Exception {
    DocumentsWriterDeleteQueue queue = new DocumentsWriterDeleteQueue(null);
    final int size = 200 + random().nextInt(500) * RANDOM_MULTIPLIER;
    Integer[] ids = new Integer[size];
    for (int i = 0; i < ids.length; i++) {
      ids[i] = random().nextInt();
    }
    DeleteSlice slice1 = queue.newSlice();
    DeleteSlice slice2 = queue.newSlice();
    BufferedUpdates bd1 = new BufferedUpdates("bd1");
    BufferedUpdates bd2 = new BufferedUpdates("bd2");
    int last1 = 0;
    int last2 = 0;
    Set<Term> uniqueValues = new HashSet<>();
    for (int j = 0; j < ids.length; j++) {
      Integer i = ids[j];
      // create an array here since we compare identity below against tailItem
      Term[] term = new Term[] {new Term("id", i.toString())};
      uniqueValues.add(term[0]);
      queue.addDelete(term);
      if (random().nextInt(20) == 0 || j == ids.length - 1) {
        queue.updateSlice(slice1);
        assertTrue(slice1.isTailItem(term));
        slice1.apply(bd1, j);
        assertAllBetween(last1, j, bd1, ids);
        last1 = j + 1;
      }
      if (random().nextInt(10) == 5 || j == ids.length - 1) {
        queue.updateSlice(slice2);
        assertTrue(slice2.isTailItem(term));
        slice2.apply(bd2, j);
        assertAllBetween(last2, j, bd2, ids);
        last2 = j + 1;
      }
      assertEquals(j+1, queue.numGlobalTermDeletes());
    }
    assertEquals(uniqueValues, bd1.deleteTerms.keySet());
    assertEquals(uniqueValues, bd2.deleteTerms.keySet());
    HashSet<Term> frozenSet = new HashSet<>();
    BytesRefBuilder bytesRef = new BytesRefBuilder();
    TermIterator iter = queue.freezeGlobalBuffer(null).deleteTerms.iterator();
    while (iter.next() != null) {
      bytesRef.copyBytes(iter.bytes);
      frozenSet.add(new Term(iter.field(), bytesRef.toBytesRef()));
    }
    assertEquals(uniqueValues, frozenSet);
    assertEquals("num deletes must be 0 after freeze", 0, queue
        .numGlobalTermDeletes());
  }

  private void assertAllBetween(int start, int end, BufferedUpdates deletes,
      Integer[] ids) {
    for (int i = start; i <= end; i++) {
      assertEquals(Integer.valueOf(end), deletes.deleteTerms.get(new Term("id", ids[i].toString())));
    }
  }
  
  public void testClear() {
    DocumentsWriterDeleteQueue queue = new DocumentsWriterDeleteQueue(null);
    assertFalse(queue.anyChanges());
    queue.clear();
    assertFalse(queue.anyChanges());
    final int size = 200 + random().nextInt(500) * RANDOM_MULTIPLIER;
    for (int i = 0; i < size; i++) {
      Term term = new Term("id", "" + i);
      if (random().nextInt(10) == 0) {
        queue.addDelete(new TermQuery(term));
      } else {
        queue.addDelete(term);
      }
      assertTrue(queue.anyChanges());
      if (random().nextInt(10) == 0) {
        queue.clear();
        queue.tryApplyGlobalSlice();
        assertFalse(queue.anyChanges());
      }
    }
    
  }

  public void testAnyChanges() {
    DocumentsWriterDeleteQueue queue = new DocumentsWriterDeleteQueue(null);
    final int size = 200 + random().nextInt(500) * RANDOM_MULTIPLIER;
    int termsSinceFreeze = 0;
    int queriesSinceFreeze = 0;
    for (int i = 0; i < size; i++) {
      Term term = new Term("id", "" + i);
      if (random().nextInt(10) == 0) {
        queue.addDelete(new TermQuery(term));
        queriesSinceFreeze++;
      } else {
        queue.addDelete(term);
        termsSinceFreeze++;
      }
      assertTrue(queue.anyChanges());
      if (random().nextInt(5) == 0) {
        FrozenBufferedUpdates freezeGlobalBuffer = queue.freezeGlobalBuffer(null);
        assertEquals(termsSinceFreeze, freezeGlobalBuffer.deleteTerms.size());
        assertEquals(queriesSinceFreeze, freezeGlobalBuffer.deleteQueries.length);
        queriesSinceFreeze = 0;
        termsSinceFreeze = 0;
        assertFalse(queue.anyChanges());
      }
    }
  }
  
  public void testPartiallyAppliedGlobalSlice() throws Exception {
    final DocumentsWriterDeleteQueue queue = new DocumentsWriterDeleteQueue(null);
    ReentrantLock lock = queue.globalBufferLock;
    lock.lock();
    Thread t = new Thread() {
      @Override
      public void run() {
        queue.addDelete(new Term("foo", "bar"));
      }
    };
    t.start();
    t.join();
    lock.unlock();
    assertTrue("changes in del queue but not in slice yet", queue.anyChanges());
    queue.tryApplyGlobalSlice();
    assertTrue("changes in global buffer", queue.anyChanges());
    FrozenBufferedUpdates freezeGlobalBuffer = queue.freezeGlobalBuffer(null);
    assertTrue(freezeGlobalBuffer.any());
    assertEquals(1, freezeGlobalBuffer.deleteTerms.size());
    assertFalse("all changes applied", queue.anyChanges());
  }

  public void testStressDeleteQueue() throws Exception {
    DocumentsWriterDeleteQueue queue = new DocumentsWriterDeleteQueue(null);
    Set<Term> uniqueValues = new HashSet<>();
    final int size = 10000 + random().nextInt(500) * RANDOM_MULTIPLIER;
    Integer[] ids = new Integer[size];
    for (int i = 0; i < ids.length; i++) {
      ids[i] = random().nextInt();
      uniqueValues.add(new Term("id", ids[i].toString()));
    }
    CountDownLatch latch = new CountDownLatch(1);
    AtomicInteger index = new AtomicInteger(0);
    final int numThreads = 2 + random().nextInt(5);
    UpdateThread[] threads = new UpdateThread[numThreads];
    for (int i = 0; i < threads.length; i++) {
      threads[i] = new UpdateThread(queue, index, ids, latch);
      threads[i].start();
    }
    latch.countDown();
    for (int i = 0; i < threads.length; i++) {
      threads[i].join();
    }

    for (UpdateThread updateThread : threads) {
      DeleteSlice slice = updateThread.slice;
      queue.updateSlice(slice);
      BufferedUpdates deletes = updateThread.deletes;
      slice.apply(deletes, BufferedUpdates.MAX_INT);
      assertEquals(uniqueValues, deletes.deleteTerms.keySet());
    }
    queue.tryApplyGlobalSlice();
    Set<Term> frozenSet = new HashSet<>();
    BytesRefBuilder builder = new BytesRefBuilder();

    TermIterator iter = queue.freezeGlobalBuffer(null).deleteTerms.iterator();
    while (iter.next() != null) {
      builder.copyBytes(iter.bytes);
      frozenSet.add(new Term(iter.field(), builder.toBytesRef()));
    }

    assertEquals("num deletes must be 0 after freeze", 0, queue
        .numGlobalTermDeletes());
    assertEquals(uniqueValues.size(), frozenSet.size());
    assertEquals(uniqueValues, frozenSet);
  }

  public void testClose() {
    {
      DocumentsWriterDeleteQueue queue = new DocumentsWriterDeleteQueue(null);
      assertTrue(queue.isOpen());
      queue.close();
      if (random().nextBoolean()) {
        queue.close(); // double close
      }
      expectThrows(AlreadyClosedException.class, () -> queue.addDelete(new Term("foo", "bar")));
      expectThrows(AlreadyClosedException.class, () -> queue.freezeGlobalBuffer(null));
      expectThrows(AlreadyClosedException.class, () -> queue.addDelete(new MatchNoDocsQuery()));
      expectThrows(AlreadyClosedException.class,
          () -> queue.addDocValuesUpdates(new DocValuesUpdate.NumericDocValuesUpdate(new Term("foo", "bar"), "foo", 1)));
      expectThrows(AlreadyClosedException.class, () -> queue.add(null));
      assertNull(queue.maybeFreezeGlobalBuffer()); // this is fine
      assertFalse(queue.isOpen());
    }
    {
      DocumentsWriterDeleteQueue queue = new DocumentsWriterDeleteQueue(null);
      queue.addDelete(new Term("foo", "bar"));
      expectThrows(IllegalStateException.class, () -> queue.close());
      assertTrue(queue.isOpen());
      queue.tryApplyGlobalSlice();
      queue.freezeGlobalBuffer(null);
      queue.close();
      assertFalse(queue.isOpen());
    }
  }

  private static class UpdateThread extends Thread {
    final DocumentsWriterDeleteQueue queue;
    final AtomicInteger index;
    final Integer[] ids;
    final DeleteSlice slice;
    final BufferedUpdates deletes;
    final CountDownLatch latch;

    protected UpdateThread(DocumentsWriterDeleteQueue queue,
        AtomicInteger index, Integer[] ids, CountDownLatch latch) {
      this.queue = queue;
      this.index = index;
      this.ids = ids;
      this.slice = queue.newSlice();
      deletes = new BufferedUpdates("deletes");
      this.latch = latch;
    }

    @Override
    public void run() {
      try {
        latch.await();
      } catch (InterruptedException e) {
        throw new ThreadInterruptedException(e);
      }
      int i = 0;
      while ((i = index.getAndIncrement()) < ids.length) {
        Term term = new Term("id", ids[i].toString());
        DocumentsWriterDeleteQueue.Node<Term> termNode = DocumentsWriterDeleteQueue.newNode(term);
        queue.add(termNode, slice);
        assertTrue(slice.isTail(termNode));
        slice.apply(deletes, BufferedUpdates.MAX_INT);
      }
    }
  }

}
