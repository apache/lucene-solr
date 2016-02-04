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
package org.apache.lucene.search;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.RamUsageTester;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

public class TestLRUFilterCache extends LuceneTestCase {

  private static final FilterCachingPolicy NEVER_CACHE = new FilterCachingPolicy() {

    @Override
    public void onUse(Filter filter) {}

    @Override
    public boolean shouldCache(Filter filter, LeafReaderContext context, DocIdSet set) throws IOException {
      return false;
    }

  };

  private static FilterCachingPolicy MAYBE_CACHE_POLICY = new FilterCachingPolicy() {

    @Override
    public void onUse(Filter filter) {}

    @Override
    public boolean shouldCache(Filter filter, LeafReaderContext context, DocIdSet set) throws IOException {
      return random().nextBoolean();
    }

  };

  public void testFilterRamBytesUsed() {
    final Filter simpleFilter = new QueryWrapperFilter(new TermQuery(new Term("some_field", "some_term")));
    final long actualRamBytesUsed = RamUsageTester.sizeOf(simpleFilter);
    final long ramBytesUsed = LRUFilterCache.FILTER_DEFAULT_RAM_BYTES_USED;
    // we cannot assert exactly that the constant is correct since actual
    // memory usage depends on JVM implementations and settings (eg. UseCompressedOops)
    assertEquals(actualRamBytesUsed, ramBytesUsed, actualRamBytesUsed / 2);
  }

  public void testConcurrency() throws Throwable {
    final LRUFilterCache filterCache = new LRUFilterCache(1 + random().nextInt(20), 1 + random().nextInt(10000));
    Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    final SearcherManager mgr = new SearcherManager(w.w, random().nextBoolean(), new SearcherFactory() {
      @Override
      public IndexSearcher newSearcher(IndexReader reader, IndexReader previousReader) throws IOException {
        IndexSearcher searcher = new IndexSearcher(reader);
        // disable built-in caching
        searcher.setQueryCache(null);
        return searcher;
      }
    });
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
    searcher.setQueryCache(null);
    final LRUFilterCache filterCache = new LRUFilterCache(2, 100000);

    final Filter blue = new QueryWrapperFilter(new TermQuery(new Term("color", "blue")));
    final Filter red = new QueryWrapperFilter(new TermQuery(new Term("color", "red")));
    final Filter green = new QueryWrapperFilter(new TermQuery(new Term("color", "green")));

    assertEquals(Collections.emptyList(), filterCache.cachedFilters());

    // the filter is not cached on any segment: no changes
    searcher.search(new ConstantScoreQuery(filterCache.doCache(green, NEVER_CACHE)), 1);
    assertEquals(Collections.emptyList(), filterCache.cachedFilters());

    searcher.search(new ConstantScoreQuery(filterCache.doCache(red, FilterCachingPolicy.ALWAYS_CACHE)), 1);
    assertEquals(Collections.singletonList(red), filterCache.cachedFilters());

    searcher.search(new ConstantScoreQuery(filterCache.doCache(green, FilterCachingPolicy.ALWAYS_CACHE)), 1);
    assertEquals(Arrays.asList(red, green), filterCache.cachedFilters());

    searcher.search(new ConstantScoreQuery(filterCache.doCache(red, FilterCachingPolicy.ALWAYS_CACHE)), 1);
    assertEquals(Arrays.asList(green, red), filterCache.cachedFilters());

    searcher.search(new ConstantScoreQuery(filterCache.doCache(blue, FilterCachingPolicy.ALWAYS_CACHE)), 1);
    assertEquals(Arrays.asList(red, blue), filterCache.cachedFilters());

    searcher.search(new ConstantScoreQuery(filterCache.doCache(blue, FilterCachingPolicy.ALWAYS_CACHE)), 1);
    assertEquals(Arrays.asList(red, blue), filterCache.cachedFilters());

    searcher.search(new ConstantScoreQuery(filterCache.doCache(green, FilterCachingPolicy.ALWAYS_CACHE)), 1);
    assertEquals(Arrays.asList(blue, green), filterCache.cachedFilters());

    searcher.search(new ConstantScoreQuery(filterCache.doCache(red, NEVER_CACHE)), 1);
    assertEquals(Arrays.asList(blue, green), filterCache.cachedFilters());

    reader.close();
    w.close();
    dir.close();
  }

  public void testCache() throws IOException {
    Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    Document doc = new Document();
    StringField f = new StringField("color", "", Store.NO);
    doc.add(f);
    final int numDocs = atLeast(10);
    for (int i = 0; i < numDocs; ++i) {
      f.setStringValue(RandomPicks.randomFrom(random(), Arrays.asList("blue", "red", "green")));
      w.addDocument(doc);
    }
    final DirectoryReader reader = w.getReader();
    final LeafReaderContext leaf1 = reader.leaves().get(0);

    Filter filter1 = new QueryWrapperFilter(new TermQuery(new Term("color", "blue")));
    // different instance yet equal
    Filter filter2 = new QueryWrapperFilter(new TermQuery(new Term("color", "blue")));

    final LRUFilterCache filterCache = new LRUFilterCache(Integer.MAX_VALUE, Long.MAX_VALUE);
    final Filter cachedFilter1 = filterCache.doCache(filter1, FilterCachingPolicy.ALWAYS_CACHE);
    DocIdSet cached1 = cachedFilter1.getDocIdSet(leaf1, null);

    final Filter cachedFilter2 = filterCache.doCache(filter2, NEVER_CACHE);
    DocIdSet cached2 = cachedFilter2.getDocIdSet(leaf1, null);
    assertSame(cached1, cached2);

    filterCache.assertConsistent();

    reader.close();
    w.close();
    dir.close();
  }

  public void testClearFilter() throws IOException {
    Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    Document doc = new Document();
    StringField f = new StringField("color", "", Store.NO);
    doc.add(f);
    final int numDocs = atLeast(10);
    for (int i = 0; i < numDocs; ++i) {
      f.setStringValue(random().nextBoolean() ? "red" : "blue");
      w.addDocument(doc);
    }
    final DirectoryReader reader = w.getReader();
    final LeafReaderContext leaf1 = reader.leaves().get(0);

    final Filter filter1 = new QueryWrapperFilter(new TermQuery(new Term("color", "blue")));
    // different instance yet equal
    final Filter filter2 = new QueryWrapperFilter(new TermQuery(new Term("color", "blue")));

    final LRUFilterCache filterCache = new LRUFilterCache(Integer.MAX_VALUE, Long.MAX_VALUE);

    final Filter cachedFilter1 = filterCache.doCache(filter1, FilterCachingPolicy.ALWAYS_CACHE);
    cachedFilter1.getDocIdSet(leaf1, null);

    filterCache.clearFilter(filter2);

    assertTrue(filterCache.cachedFilters().isEmpty());
    filterCache.assertConsistent();

    reader.close();
    w.close();
    dir.close();
  }

  // This test makes sure that by making the same assumptions as LRUFilterCache, RAMUsageTester
  // computes the same memory usage.
  public void testRamBytesUsedAgreesWithRamUsageTester() throws IOException {
    final LRUFilterCache filterCache = new LRUFilterCache(1 + random().nextInt(5), 1 + random().nextInt(10000));
    // an accumulator that only sums up memory usage of referenced filters and doc id sets
    final RamUsageTester.Accumulator acc = new RamUsageTester.Accumulator() {
      @Override
      public long accumulateObject(Object o, long shallowSize, Map<Field,Object> fieldValues, Collection<Object> queue) {
        if (o instanceof DocIdSet) {
          return ((DocIdSet) o).ramBytesUsed();
        }
        if (o instanceof Filter) {
          return filterCache.ramBytesUsed((Filter) o);
        }
        if (o.getClass().getSimpleName().equals("SegmentCoreReaders")) {
          // do not take core cache keys into account
          return 0;
        }
        if (o instanceof Map) {
          Map<?,?> map = (Map<?,?>) o;
          queue.addAll(map.keySet());
          queue.addAll(map.values());
          final long sizePerEntry = o instanceof LinkedHashMap
              ? LRUFilterCache.LINKED_HASHTABLE_RAM_BYTES_PER_ENTRY
              : LRUFilterCache.HASHTABLE_RAM_BYTES_PER_ENTRY;
          return sizePerEntry * map.size();
        }
        // follow links to other objects, but ignore their memory usage
        super.accumulateObject(o, shallowSize, fieldValues, queue);
        return  0;
      }
      @Override
      public long accumulateArray(Object array, long shallowSize, List<Object> values, Collection<Object> queue) {
        // follow links to other objects, but ignore their memory usage
        super.accumulateArray(array, shallowSize, values, queue);
        return 0;
      }
    };

    Directory dir = newDirectory();
    // serial merges so that segments do not get closed while we are measuring ram usage
    // with RamUsageTester
    IndexWriterConfig iwc = newIndexWriterConfig().setMergeScheduler(new SerialMergeScheduler());
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);

    final List<String> colors = Arrays.asList("blue", "red", "green", "yellow");

    Document doc = new Document();
    StringField f = new StringField("color", "", Store.NO);
    doc.add(f);
    final int iters = atLeast(5);
    for (int iter = 0; iter < iters; ++iter) {
      final int numDocs = atLeast(10);
      for (int i = 0; i < numDocs; ++i) {
        f.setStringValue(RandomPicks.randomFrom(random(), colors));
        w.addDocument(doc);
      }
      try (final DirectoryReader reader = w.getReader()) {
        final IndexSearcher searcher = new IndexSearcher(reader);
        searcher.setQueryCache(null);
        for (int i = 0; i < 3; ++i) {
          final Filter filter = new QueryWrapperFilter(new TermQuery(new Term("color", RandomPicks.randomFrom(random(), colors))));
          searcher.search(new ConstantScoreQuery(filterCache.doCache(filter, MAYBE_CACHE_POLICY)), 1);
        }
      }
      filterCache.assertConsistent();
      assertEquals(RamUsageTester.sizeOf(filterCache, acc), filterCache.ramBytesUsed());
    }

    w.close();
    dir.close();
  }

  /** A filter that produces empty sets. */
  private static class DummyFilter extends Filter {

    private static final AtomicLong ID_GENERATOR = new AtomicLong();

    private final long id = ID_GENERATOR.getAndIncrement();

    @Override
    public DocIdSet getDocIdSet(LeafReaderContext context, Bits acceptDocs) throws IOException {
      return null;
    }

    @Override
    public String toString(String field) {
      return "DummyFilter";
    }

    @Override
    public int hashCode() {
      return 31 * super.hashCode() + new Long(id).hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      return super.equals(obj) && id == ((DummyFilter) obj).id;
    }

  }

  // Test what happens when the cache contains only filters and doc id sets
  // that require very little memory. In that case most of the memory is taken
  // by the cache itself, not cache entries, and we want to make sure that
  // memory usage is not grossly underestimated.
  public void testRamBytesUsedConstantEntryOverhead() throws IOException {
    final LRUFilterCache filterCache = new LRUFilterCache(1000000, 10000000);

    final RamUsageTester.Accumulator acc = new RamUsageTester.Accumulator() {
      @Override
      public long accumulateObject(Object o, long shallowSize, Map<Field,Object> fieldValues, Collection<Object> queue) {
        if (o instanceof DocIdSet) {
          return ((DocIdSet) o).ramBytesUsed();
        }
        if (o instanceof Filter) {
          return filterCache.ramBytesUsed((Filter) o);
        }
        if (o.getClass().getSimpleName().equals("SegmentCoreReaders")) {
          // do not follow references to core cache keys
          return 0;
        }
        return super.accumulateObject(o, shallowSize, fieldValues, queue);
      }
    };

    Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    final int numDocs = atLeast(100);
    for (int i = 0; i < numDocs; ++i) {
      w.addDocument(doc);
    }
    final DirectoryReader reader = w.getReader();
    final IndexSearcher searcher = new IndexSearcher(reader);
    searcher.setQueryCache(null);

    final int numFilters = atLeast(1000);
    for (int i = 0; i < numFilters; ++i) {
      final Filter filter = new DummyFilter();
      final Filter cached = filterCache.doCache(filter, FilterCachingPolicy.ALWAYS_CACHE);
      searcher.search(new ConstantScoreQuery(cached), 1);
    }

    final long actualRamBytesUsed = RamUsageTester.sizeOf(filterCache, acc);
    final long expectedRamBytesUsed = filterCache.ramBytesUsed();
    // error < 30%
    assertEquals(actualRamBytesUsed, expectedRamBytesUsed, 30 * actualRamBytesUsed / 100);

    reader.close();
    w.close();
    dir.close();
  }

  public void testOnUse() throws IOException {
    final LRUFilterCache filterCache = new LRUFilterCache(1 + random().nextInt(5), 1 + random().nextInt(1000));

    Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    Document doc = new Document();
    StringField f = new StringField("color", "", Store.NO);
    doc.add(f);
    final int numDocs = atLeast(10);
    for (int i = 0; i < numDocs; ++i) {
      f.setStringValue(RandomPicks.randomFrom(random(), Arrays.asList("red", "blue", "green", "yellow")));
      w.addDocument(doc);
      if (random().nextBoolean()) {
        w.getReader().close();
      }
    }
    final DirectoryReader reader = w.getReader();
    final IndexSearcher searcher = new IndexSearcher(reader);
    searcher.setQueryCache(null);

    final Map<Filter, Integer> actualCounts = new HashMap<>();
    final Map<Filter, Integer> expectedCounts = new HashMap<>();

    final FilterCachingPolicy countingPolicy = new FilterCachingPolicy() {

      @Override
      public boolean shouldCache(Filter filter, LeafReaderContext context, DocIdSet set) throws IOException {
        return random().nextBoolean();
      }

      @Override
      public void onUse(Filter filter) {
        expectedCounts.put(filter, 1 + (expectedCounts.containsKey(filter) ? expectedCounts.get(filter) : 0));
      }
    };

    Filter[] filters = new Filter[10 + random().nextInt(10)];
    Filter[] cachedFilters = new Filter[filters.length];
    for (int i = 0; i < filters.length; ++i) {
      filters[i] = new QueryWrapperFilter(new TermQuery(new Term("color", RandomPicks.randomFrom(random(), Arrays.asList("red", "blue", "green", "yellow")))));
      cachedFilters[i] = filterCache.doCache(filters[i], countingPolicy);
    }

    for (int i = 0; i < 20; ++i) {
      final int idx = random().nextInt(filters.length);
      searcher.search(new ConstantScoreQuery(cachedFilters[idx]), 1);
      actualCounts.put(filters[idx], 1 + (actualCounts.containsKey(filters[idx]) ? actualCounts.get(filters[idx]) : 0));
    }

    assertEquals(actualCounts, expectedCounts);

    reader.close();
    w.close();
    dir.close();
  }

  public void testStats() throws IOException {
    final LRUFilterCache filterCache = new LRUFilterCache(1, 10000000);

    Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    final List<String> colors = Arrays.asList("blue", "red", "green", "yellow");

    Document doc = new Document();
    StringField f = new StringField("color", "", Store.NO);
    doc.add(f);
    for (int i = 0; i < 10; ++i) {
      f.setStringValue(RandomPicks.randomFrom(random(), colors));
      w.addDocument(doc);
      if (random().nextBoolean()) {
        w.getReader().close();
      }
    }

    final DirectoryReader reader = w.getReader();
    final int segmentCount = reader.leaves().size();
    final IndexSearcher searcher = new IndexSearcher(reader);
    searcher.setQueryCache(null);
    final Filter filter = new QueryWrapperFilter(new TermQuery(new Term("color", "red")));
    final Filter filter2 = new QueryWrapperFilter(new TermQuery(new Term("color", "blue")));

    // first pass, lookups without caching that all miss
    Filter cached = filterCache.doCache(filter, NEVER_CACHE);
    for (int i = 0; i < 10; ++i) {
      searcher.search(new ConstantScoreQuery(cached), 1);
    }
    assertEquals(10 * segmentCount, filterCache.getTotalCount());
    assertEquals(0, filterCache.getHitCount());
    assertEquals(10 * segmentCount, filterCache.getMissCount());
    assertEquals(0, filterCache.getCacheCount());
    assertEquals(0, filterCache.getEvictionCount());
    assertEquals(0, filterCache.getCacheSize());

    // second pass, lookups + caching, only the first one is a miss
    cached = filterCache.doCache(filter, FilterCachingPolicy.ALWAYS_CACHE);
    for (int i = 0; i < 10; ++i) {
      searcher.search(new ConstantScoreQuery(cached), 1);
    }
    assertEquals(20 * segmentCount, filterCache.getTotalCount());
    assertEquals(9 * segmentCount, filterCache.getHitCount());
    assertEquals(11 * segmentCount, filterCache.getMissCount());
    assertEquals(1 * segmentCount, filterCache.getCacheCount());
    assertEquals(0, filterCache.getEvictionCount());
    assertEquals(1 * segmentCount, filterCache.getCacheSize());

    // third pass lookups without caching, we only have hits
    cached = filterCache.doCache(filter, NEVER_CACHE);
    for (int i = 0; i < 10; ++i) {
      searcher.search(new ConstantScoreQuery(cached), 1);
    }
    assertEquals(30 * segmentCount, filterCache.getTotalCount());
    assertEquals(19 * segmentCount, filterCache.getHitCount());
    assertEquals(11 * segmentCount, filterCache.getMissCount());
    assertEquals(1 * segmentCount, filterCache.getCacheCount());
    assertEquals(0, filterCache.getEvictionCount());
    assertEquals(1 * segmentCount, filterCache.getCacheSize());

    // fourth pass with a different filter which will trigger evictions since the size is 1
    cached = filterCache.doCache(filter2, FilterCachingPolicy.ALWAYS_CACHE);
    for (int i = 0; i < 10; ++i) {
      searcher.search(new ConstantScoreQuery(cached), 1);
    }
    assertEquals(40 * segmentCount, filterCache.getTotalCount());
    assertEquals(28 * segmentCount, filterCache.getHitCount());
    assertEquals(12 * segmentCount, filterCache.getMissCount());
    assertEquals(2 * segmentCount, filterCache.getCacheCount());
    assertEquals(1 * segmentCount, filterCache.getEvictionCount());
    assertEquals(1 * segmentCount, filterCache.getCacheSize());

    // now close, causing evictions due to the closing of segment cores
    reader.close();
    w.close();
    assertEquals(40 * segmentCount, filterCache.getTotalCount());
    assertEquals(28 * segmentCount, filterCache.getHitCount());
    assertEquals(12 * segmentCount, filterCache.getMissCount());
    assertEquals(2 * segmentCount, filterCache.getCacheCount());
    assertEquals(2 * segmentCount, filterCache.getEvictionCount());
    assertEquals(0, filterCache.getCacheSize());

    dir.close();
  }

  public void testFineGrainedStats() throws IOException {
    Directory dir1 = newDirectory();
    final RandomIndexWriter w1 = new RandomIndexWriter(random(), dir1);
    Directory dir2 = newDirectory();
    final RandomIndexWriter w2 = new RandomIndexWriter(random(), dir2);

    final List<String> colors = Arrays.asList("blue", "red", "green", "yellow");

    Document doc = new Document();
    StringField f = new StringField("color", "", Store.NO);
    doc.add(f);
    for (RandomIndexWriter w : Arrays.asList(w1, w2)) {
      for (int i = 0; i < 10; ++i) {
        f.setStringValue(RandomPicks.randomFrom(random(), colors));
        w.addDocument(doc);
        if (random().nextBoolean()) {
          w.getReader().close();
        }
      }
    }

    final DirectoryReader reader1 = w1.getReader();
    final int segmentCount1 = reader1.leaves().size();
    final IndexSearcher searcher1 = new IndexSearcher(reader1);
    searcher1.setQueryCache(null);

    final DirectoryReader reader2 = w2.getReader();
    final int segmentCount2 = reader2.leaves().size();
    final IndexSearcher searcher2 = new IndexSearcher(reader2);
    searcher2.setQueryCache(null);

    final Map<Object, Integer> indexId = new HashMap<>();
    for (LeafReaderContext ctx : reader1.leaves()) {
      indexId.put(ctx.reader().getCoreCacheKey(), 1);
    }
    for (LeafReaderContext ctx : reader2.leaves()) {
      indexId.put(ctx.reader().getCoreCacheKey(), 2);
    }

    final AtomicLong hitCount1 = new AtomicLong();
    final AtomicLong hitCount2 = new AtomicLong();
    final AtomicLong missCount1 = new AtomicLong();
    final AtomicLong missCount2 = new AtomicLong();

    final AtomicLong ramBytesUsage = new AtomicLong();
    final AtomicLong cacheSize = new AtomicLong();

    final LRUFilterCache filterCache = new LRUFilterCache(2, 10000000) {
      @Override
      protected void onHit(Object readerCoreKey, Filter filter) {
        super.onHit(readerCoreKey, filter);
        switch(indexId.get(readerCoreKey).intValue()) {
          case 1:
            hitCount1.incrementAndGet();
            break;
          case 2:
            hitCount2.incrementAndGet();
            break;
          default:
            throw new AssertionError();
        }
      }

      @Override
      protected void onMiss(Object readerCoreKey, Filter filter) {
        super.onMiss(readerCoreKey, filter);
        switch(indexId.get(readerCoreKey).intValue()) {
          case 1:
            missCount1.incrementAndGet();
            break;
          case 2:
            missCount2.incrementAndGet();
            break;
          default:
            throw new AssertionError();
        }
      }

      @Override
      protected void onFilterCache(Filter filter, long ramBytesUsed) {
        super.onFilterCache(filter, ramBytesUsed);
        ramBytesUsage.addAndGet(ramBytesUsed);
      }

      @Override
      protected void onFilterEviction(Filter filter, long ramBytesUsed) {
        super.onFilterEviction(filter, ramBytesUsed);
        ramBytesUsage.addAndGet(-ramBytesUsed);
      }

      @Override
      protected void onDocIdSetCache(Object readerCoreKey, long ramBytesUsed) {
        super.onDocIdSetCache(readerCoreKey, ramBytesUsed);
        ramBytesUsage.addAndGet(ramBytesUsed);
        cacheSize.incrementAndGet();
      }

      @Override
      protected void onDocIdSetEviction(Object readerCoreKey, int numEntries, long sumRamBytesUsed) {
        super.onDocIdSetEviction(readerCoreKey, numEntries, sumRamBytesUsed);
        ramBytesUsage.addAndGet(-sumRamBytesUsed);
        cacheSize.addAndGet(-numEntries);
      }

      @Override
      protected void onClear() {
        super.onClear();
        ramBytesUsage.set(0);
        cacheSize.set(0);
      }
    };

    final Filter filter = new QueryWrapperFilter(new TermQuery(new Term("color", "red")));
    final Filter filter2 = new QueryWrapperFilter(new TermQuery(new Term("color", "blue")));
    final Filter filter3 = new QueryWrapperFilter(new TermQuery(new Term("color", "green")));

    // search on searcher1
    Filter cached = filterCache.doCache(filter, FilterCachingPolicy.ALWAYS_CACHE);
    for (int i = 0; i < 10; ++i) {
      searcher1.search(new ConstantScoreQuery(cached), 1);
    }
    assertEquals(9 * segmentCount1, hitCount1.longValue());
    assertEquals(0, hitCount2.longValue());
    assertEquals(segmentCount1, missCount1.longValue());
    assertEquals(0, missCount2.longValue());

    // then on searcher2
    cached = filterCache.doCache(filter2, FilterCachingPolicy.ALWAYS_CACHE);
    for (int i = 0; i < 20; ++i) {
      searcher2.search(new ConstantScoreQuery(cached), 1);
    }
    assertEquals(9 * segmentCount1, hitCount1.longValue());
    assertEquals(19 * segmentCount2, hitCount2.longValue());
    assertEquals(segmentCount1, missCount1.longValue());
    assertEquals(segmentCount2, missCount2.longValue());

    // now on searcher1 again to trigger evictions
    cached = filterCache.doCache(filter3, FilterCachingPolicy.ALWAYS_CACHE);
    for (int i = 0; i < 30; ++i) {
      searcher1.search(new ConstantScoreQuery(cached), 1);
    }
    assertEquals(segmentCount1, filterCache.getEvictionCount());
    assertEquals(38 * segmentCount1, hitCount1.longValue());
    assertEquals(19 * segmentCount2, hitCount2.longValue());
    assertEquals(2 * segmentCount1, missCount1.longValue());
    assertEquals(segmentCount2, missCount2.longValue());

    // check that the recomputed stats are the same as those reported by the cache
    assertEquals(filterCache.ramBytesUsed(), (segmentCount1 + segmentCount2) * LRUFilterCache.HASHTABLE_RAM_BYTES_PER_ENTRY + ramBytesUsage.longValue());
    assertEquals(filterCache.getCacheSize(), cacheSize.longValue());

    reader1.close();
    reader2.close();
    w1.close();
    w2.close();

    assertEquals(filterCache.ramBytesUsed(), ramBytesUsage.longValue());
    assertEquals(0, cacheSize.longValue());

    filterCache.clear();
    assertEquals(0, ramBytesUsage.longValue());
    assertEquals(0, cacheSize.longValue());

    dir1.close();
    dir2.close();
  }

}
