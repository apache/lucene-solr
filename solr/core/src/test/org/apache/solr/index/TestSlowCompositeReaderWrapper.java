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
package org.apache.solr.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.MultiDocValues.MultiSortedDocValues;
import org.apache.lucene.index.MultiDocValues.MultiSortedSetDocValues;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCase;

public class TestSlowCompositeReaderWrapper extends SolrTestCase {

  public void testCoreListenerOnSlowCompositeReaderWrapper() throws IOException {
    RandomIndexWriter w = new RandomIndexWriter(random(), newDirectory());
    final int numDocs = TestUtil.nextInt(random(), 1, 5);
    for (int i = 0; i < numDocs; ++i) {
      w.addDocument(new Document());
      if (random().nextBoolean()) {
        w.commit();
      }
    }
    w.commit();
    w.close();

    final IndexReader reader = DirectoryReader.open(w.w.getDirectory());
    final LeafReader leafReader = SlowCompositeReaderWrapper.wrap(reader);
    
    final int numListeners = TestUtil.nextInt(random(), 1, 10);
    final List<IndexReader.ClosedListener> listeners = new ArrayList<>();
    AtomicInteger counter = new AtomicInteger(numListeners);
    
    for (int i = 0; i < numListeners; ++i) {
      CountCoreListener listener = new CountCoreListener(counter, leafReader.getCoreCacheHelper().getKey());
      listeners.add(listener);
      leafReader.getCoreCacheHelper().addClosedListener(listener);
    }
    for (int i = 0; i < 100; ++i) {
      leafReader.getCoreCacheHelper().addClosedListener(listeners.get(random().nextInt(listeners.size())));
    }
    assertEquals(numListeners, counter.get());
    // make sure listeners are registered on the wrapped reader and that closing any of them has the same effect
    if (random().nextBoolean()) {
      reader.close();
    } else {
      leafReader.close();
    }
    assertEquals(0, counter.get());
    w.w.getDirectory().close();
  }

  private static final class CountCoreListener implements IndexReader.ClosedListener {

    private final AtomicInteger count;
    private final Object coreCacheKey;

    public CountCoreListener(AtomicInteger count, Object coreCacheKey) {
      this.count = count;
      this.coreCacheKey = coreCacheKey;
    }

    @Override
    public void onClose(IndexReader.CacheKey coreCacheKey) {
      assertSame(this.coreCacheKey, coreCacheKey);
      count.decrementAndGet();
    }

  }

  public void testOrdMapsAreCached() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE));
    Document doc = new Document();
    doc.add(new SortedDocValuesField("sorted", new BytesRef("a")));
    doc.add(new SortedSetDocValuesField("sorted_set", new BytesRef("b")));
    doc.add(new SortedSetDocValuesField("sorted_set", new BytesRef("c")));
    w.addDocument(doc);
    w.getReader().close();
    doc = new Document();
    doc.add(new SortedDocValuesField("sorted", new BytesRef("b")));
    doc.add(new SortedSetDocValuesField("sorted_set", new BytesRef("c")));
    doc.add(new SortedSetDocValuesField("sorted_set", new BytesRef("d")));
    w.addDocument(doc);
    IndexReader reader = w.getReader();
    assertTrue(reader.leaves().size() > 1);
    SlowCompositeReaderWrapper slowWrapper = (SlowCompositeReaderWrapper) SlowCompositeReaderWrapper.wrap(reader);
    assertEquals(0, slowWrapper.cachedOrdMaps.size());
    assertEquals(MultiSortedDocValues.class, slowWrapper.getSortedDocValues("sorted").getClass());
    assertEquals(1, slowWrapper.cachedOrdMaps.size());
    assertEquals(MultiSortedSetDocValues.class, slowWrapper.getSortedSetDocValues("sorted_set").getClass());
    assertEquals(2, slowWrapper.cachedOrdMaps.size());
    reader.close();
    w.close();
    dir.close();
  }

  public void testTermsAreCached() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE));
    Document doc = new Document();
    doc.add(new TextField("text", "hello world", Field.Store.NO));
    w.addDocument(doc);
    w.getReader().close();
    doc = new Document();
    doc.add(new TextField("text", "cruel world", Field.Store.NO));
    w.addDocument(doc);

    IndexReader reader = w.getReader();
    assertTrue(reader.leaves().size() > 1);
    SlowCompositeReaderWrapper slowWrapper = (SlowCompositeReaderWrapper) SlowCompositeReaderWrapper.wrap(reader);
    assertEquals(0, slowWrapper.cachedTerms.size());
    assertEquals(MultiTerms.class, slowWrapper.terms("text").getClass());
    assertEquals(1, slowWrapper.cachedTerms.size());
    assertNull(slowWrapper.terms("bogusField"));
    assertEquals(1, slowWrapper.cachedTerms.size());//bogus field isn't cached
    reader.close();
    w.close();
    dir.close();
  }
}
