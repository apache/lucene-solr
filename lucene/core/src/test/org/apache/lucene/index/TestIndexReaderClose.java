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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

/**
 */
public class TestIndexReaderClose extends LuceneTestCase {

  public void testCloseUnderException() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(random(), new MockAnalyzer(random())));
    writer.addDocument(new Document());
    writer.commit();
    writer.close();
    final int iters = 1000 +  1 + random().nextInt(20);
    for (int j = 0; j < iters; j++) {
      DirectoryReader open = DirectoryReader.open(dir);
      final boolean throwOnClose = !rarely();
      LeafReader leaf = getOnlyLeafReader(open);
      FilterLeafReader reader = new FilterLeafReader(leaf) {
        @Override
        protected void doClose() throws IOException {
          super.doClose();
          if (throwOnClose) {
           throw new IllegalStateException("BOOM!");
          }
        }
      };
      int listenerCount = random().nextInt(20);
      AtomicInteger count = new AtomicInteger();
      boolean faultySet = false;
      for (int i = 0; i < listenerCount; i++) {
          if (rarely()) {
            faultySet = true;
            reader.addReaderClosedListener(new FaultyListener());
          } else {
            count.incrementAndGet();
            reader.addReaderClosedListener(new CountListener(count));
          }
      }
      if (!faultySet && !throwOnClose) {
        reader.addReaderClosedListener(new FaultyListener());
      }

      IllegalStateException expected = expectThrows(IllegalStateException.class, () -> {
        reader.close();
      });

      if (throwOnClose) {
        assertEquals("BOOM!", expected.getMessage());
      } else {
        assertEquals("GRRRRRRRRRRRR!", expected.getMessage());
      }

      expectThrows(AlreadyClosedException.class, () -> {
        reader.fields();
      });

      if (random().nextBoolean()) {
        reader.close(); // call it again
      }
      assertEquals(0, count.get());
    }
    dir.close();
  }

  public void testCoreListenerOnWrapperWithDifferentCacheKey() throws IOException {
    RandomIndexWriter w = new RandomIndexWriter(random(), newDirectory());
    final int numDocs = TestUtil.nextInt(random(), 1, 5);
    for (int i = 0; i < numDocs; ++i) {
      w.addDocument(new Document());
      if (random().nextBoolean()) {
        w.commit();
      }
    }
    w.forceMerge(1);
    w.commit();
    w.close();

    final IndexReader reader = DirectoryReader.open(w.w.getDirectory());
    // We explicitly define a different cache key
    final Object coreCacheKey = new Object();
    final LeafReader leafReader = new FilterLeafReader(getOnlyLeafReader(reader)) {
      @Override
      public Object getCoreCacheKey() {
        return coreCacheKey;
      }
    };

    final int numListeners = TestUtil.nextInt(random(), 1, 10);
    final List<LeafReader.CoreClosedListener> listeners = new ArrayList<>();
    AtomicInteger counter = new AtomicInteger(numListeners);
    
    for (int i = 0; i < numListeners; ++i) {
      CountCoreListener listener = new CountCoreListener(counter, coreCacheKey);
      listeners.add(listener);
      leafReader.addCoreClosedListener(listener);
    }
    for (int i = 0; i < 100; ++i) {
      leafReader.addCoreClosedListener(listeners.get(random().nextInt(listeners.size())));
    }
    final int removed = random().nextInt(numListeners);
    Collections.shuffle(listeners, random());
    for (int i = 0; i < removed; ++i) {
      leafReader.removeCoreClosedListener(listeners.get(i));
    }
    assertEquals(numListeners, counter.get());
    // make sure listeners are registered on the wrapped reader and that closing any of them has the same effect
    if (random().nextBoolean()) {
      reader.close();
    } else {
      leafReader.close();
    }
    assertEquals(removed, counter.get());
    w.w.getDirectory().close();
  }

  private static final class CountCoreListener implements LeafReader.CoreClosedListener {

    private final AtomicInteger count;
    private final Object coreCacheKey;

    public CountCoreListener(AtomicInteger count, Object coreCacheKey) {
      this.count = count;
      this.coreCacheKey = coreCacheKey;
    }

    @Override
    public void onClose(Object coreCacheKey) {
      assertSame(this.coreCacheKey, coreCacheKey);
      count.decrementAndGet();
    }

  }

  private static final class CountListener implements IndexReader.ReaderClosedListener  {
    private final AtomicInteger count;

    public CountListener(AtomicInteger count) {
      this.count = count;
    }

    @Override
    public void onClose(IndexReader reader) {
      count.decrementAndGet();
    }
  }

  private static final class FaultyListener implements IndexReader.ReaderClosedListener {

    @Override
    public void onClose(IndexReader reader) {
      throw new IllegalStateException("GRRRRRRRRRRRR!");
    }
  }

}
