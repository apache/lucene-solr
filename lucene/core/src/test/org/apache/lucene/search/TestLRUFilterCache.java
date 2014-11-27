package org.apache.lucene.search;

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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

public class TestLRUFilterCache extends LuceneTestCase {

  private static final FilterCachingPolicy NEVER_CACHE = new FilterCachingPolicy() {

    @Override
    public void onCache(Filter filter) {}

    @Override
    public boolean shouldCache(Filter filter, LeafReaderContext context, DocIdSet set) throws IOException {
      return false;
    }

  };

  public void testConcurrency() throws Throwable {
    final LRUFilterCache filterCache = new LRUFilterCache(1 + random().nextInt(20), 1 + random().nextInt(10000));
    Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    SearcherManager mgr = new SearcherManager(w.w, random().nextBoolean(), new SearcherFactory());
    final AtomicBoolean indexing = new AtomicBoolean(true);
    final AtomicReference<Throwable> error = new AtomicReference<>();
    final int numDocs = atLeast(10000);
    Thread[] threads = new Thread[3];
    threads[0] = new Thread() {
      public void run() {
        Document doc = new Document();
        StringField f = new StringField("color", "", Store.NO);
        doc.add(f);
        for (int i = 0; indexing.get() && i < numDocs; ++i) {
          f.setStringValue(RandomPicks.randomFrom(random(), new String[] {"blue", "red", "yellow"}));
          try {
            w.addDocument(doc);
            if ((i & 63) == 0) {
              mgr.maybeRefresh();
              if (rarely()) {
                filterCache.clear();
              }
              if (rarely()) {
                final String color = RandomPicks.randomFrom(random(), new String[] {"blue", "red", "yellow"});
                w.deleteDocuments(new Term("color", color));
              }
            }
          } catch (Throwable t) {
            error.compareAndSet(null, t);
            break;
          }
        }
        indexing.set(false);
      }
    };
    for (int i = 1; i < threads.length; ++i) {
      threads[i] = new Thread() {
        @Override
        public void run() {
          while (indexing.get()) {
            try {
              final IndexSearcher searcher = mgr.acquire();
              try {
                final String value = RandomPicks.randomFrom(random(), new String[] {"blue", "red", "yellow", "green"});
                final Filter f = new QueryWrapperFilter(new TermQuery(new Term("color", value)));
                final Filter cached = filterCache.doCache(f, MAYBE_CACHE_POLICY);
                TotalHitCountCollector collector = new TotalHitCountCollector();
                searcher.search(new ConstantScoreQuery(cached), collector);
                TotalHitCountCollector collector2 = new TotalHitCountCollector();
                searcher.search(new ConstantScoreQuery(f), collector2);
                assertEquals(collector.getTotalHits(), collector2.getTotalHits());
              } finally {
                mgr.release(searcher);
              }
            } catch (Throwable t) {
              error.compareAndSet(null, t);
            }
          }
        }
      };
    }

    for (Thread thread : threads) {
      thread.start();
    }

    for (Thread thread : threads) {
      thread.join();
    }

    if (error.get() != null) {
      throw error.get();
    }
    filterCache.assertConsistent();
    mgr.close();
    w.close();
    dir.close();
    filterCache.assertConsistent();
  }

  public void testLRUEviction() throws Exception {
    Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    Document doc = new Document();
    StringField f = new StringField("color", "blue", Store.NO);
    doc.add(f);
    w.addDocument(doc);
    f.setStringValue("red");
    w.addDocument(doc);
    f.setStringValue("green");
    w.addDocument(doc);
    final DirectoryReader reader = w.getReader();
    final IndexSearcher searcher = newSearcher(reader);
    final LRUFilterCache filterCache = new LRUFilterCache(2, 100000);

    final Filter blue = new QueryWrapperFilter(new TermQuery(new Term("color", "blue")));
    final Filter red = new QueryWrapperFilter(new TermQuery(new Term("color", "red")));
    final Filter green = new QueryWrapperFilter(new TermQuery(new Term("color", "green")));

    assertEquals(Collections.emptySet(), filterCache.cachedFilters());

    // the filter is not cached on any segment: no changes
    searcher.search(new ConstantScoreQuery(filterCache.doCache(green, NEVER_CACHE)), 1);
    assertEquals(Collections.emptySet(), filterCache.cachedFilters());

    searcher.search(new ConstantScoreQuery(filterCache.doCache(red, FilterCachingPolicy.ALWAYS_CACHE)), 1);
    assertEquals(Collections.singleton(red), filterCache.cachedFilters());

    searcher.search(new ConstantScoreQuery(filterCache.doCache(green, FilterCachingPolicy.ALWAYS_CACHE)), 1);
    assertEquals(new HashSet<>(Arrays.asList(red, green)), filterCache.cachedFilters());

    searcher.search(new ConstantScoreQuery(filterCache.doCache(red, FilterCachingPolicy.ALWAYS_CACHE)), 1);
    assertEquals(new HashSet<>(Arrays.asList(red, green)), filterCache.cachedFilters());

    searcher.search(new ConstantScoreQuery(filterCache.doCache(blue, FilterCachingPolicy.ALWAYS_CACHE)), 1);
    assertEquals(new HashSet<>(Arrays.asList(red, blue)), filterCache.cachedFilters());

    searcher.search(new ConstantScoreQuery(filterCache.doCache(blue, FilterCachingPolicy.ALWAYS_CACHE)), 1);
    assertEquals(new HashSet<>(Arrays.asList(red, blue)), filterCache.cachedFilters());

    searcher.search(new ConstantScoreQuery(filterCache.doCache(green, FilterCachingPolicy.ALWAYS_CACHE)), 1);
    assertEquals(new HashSet<>(Arrays.asList(green, blue)), filterCache.cachedFilters());

    searcher.search(new ConstantScoreQuery(filterCache.doCache(red, NEVER_CACHE)), 1);
    assertEquals(new HashSet<>(Arrays.asList(green, blue)), filterCache.cachedFilters());

    reader.close();
    w.close();
    dir.close();
  }

}
