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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.RoaringDocIdSet;

public class TestCachingWrapperFilter extends LuceneTestCase {

  private static FilterCachingPolicy MAYBE_CACHE_POLICY = new FilterCachingPolicy() {

    @Override
    public void onUse(Filter filter) {}

    @Override
    public boolean shouldCache(Filter filter, LeafReaderContext context, DocIdSet set) throws IOException {
      return random().nextBoolean();
    }

  };

  Directory dir;
  DirectoryReader ir;
  IndexSearcher is;
  RandomIndexWriter iw;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    dir = newDirectory();
    iw = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    Field idField = new StringField("id", "", Field.Store.NO);
    doc.add(idField);
    // add 500 docs with id 0..499
    for (int i = 0; i < 500; i++) {
      idField.setStringValue(Integer.toString(i));
      iw.addDocument(doc);
    }
    // delete 20 of them
    for (int i = 0; i < 20; i++) {
      iw.deleteDocuments(new Term("id", Integer.toString(random().nextInt(iw.maxDoc()))));
    }
    ir = iw.getReader();
    is = newSearcher(ir);
  }

  @Override
  public void tearDown() throws Exception {
    iw.close();
    IOUtils.close(ir, dir);
    super.tearDown();
  }

  private void assertFilterEquals(Filter f1, Filter f2) throws Exception {
    Query query = new MatchAllDocsQuery();
    TopDocs hits1 = is.search(new FilteredQuery(query, f1), ir.maxDoc());
    TopDocs hits2 = is.search(new FilteredQuery(query, f2), ir.maxDoc());
    assertEquals(hits1.totalHits, hits2.totalHits);
    CheckHits.checkEqual(query, hits1.scoreDocs, hits2.scoreDocs);
    // now do it again to confirm caching works
    TopDocs hits3 = is.search(new FilteredQuery(query, f1), ir.maxDoc());
    TopDocs hits4 = is.search(new FilteredQuery(query, f2), ir.maxDoc());
    assertEquals(hits3.totalHits, hits4.totalHits);
    CheckHits.checkEqual(query, hits3.scoreDocs, hits4.scoreDocs);
  }

  /** test null iterator */
  public void testEmpty() throws Exception {
    Query query = new BooleanQuery.Builder().build();
    Filter expected = new QueryWrapperFilter(query);
    Filter actual = new CachingWrapperFilter(expected, MAYBE_CACHE_POLICY);
    assertFilterEquals(expected, actual);
  }

  /** test iterator returns NO_MORE_DOCS */
  public void testEmpty2() throws Exception {
    BooleanQuery.Builder query = new BooleanQuery.Builder();
    query.add(new TermQuery(new Term("id", "0")), BooleanClause.Occur.MUST);
    query.add(new TermQuery(new Term("id", "0")), BooleanClause.Occur.MUST_NOT);
    Filter expected = new QueryWrapperFilter(query.build());
    Filter actual = new CachingWrapperFilter(expected, MAYBE_CACHE_POLICY);
    assertFilterEquals(expected, actual);
  }

  /** test null docidset */
  public void testEmpty3() throws Exception {
    Filter expected = new QueryWrapperFilter(new PrefixQuery(new Term("bogusField", "bogusVal")));
    Filter actual = new CachingWrapperFilter(expected, MAYBE_CACHE_POLICY);
    assertFilterEquals(expected, actual);
  }

  /** test iterator returns single document */
  public void testSingle() throws Exception {
    for (int i = 0; i < 10; i++) {
      int id = random().nextInt(ir.maxDoc());
      Query query = new TermQuery(new Term("id", Integer.toString(id)));
      Filter expected = new QueryWrapperFilter(query);
      Filter actual = new CachingWrapperFilter(expected, MAYBE_CACHE_POLICY);
      assertFilterEquals(expected, actual);
    }
  }

  /** test sparse filters (match single documents) */
  public void testSparse() throws Exception {
    for (int i = 0; i < 10; i++) {
      int id_start = random().nextInt(ir.maxDoc()-1);
      int id_end = id_start + 1;
      Query query = TermRangeQuery.newStringRange("id",
          Integer.toString(id_start), Integer.toString(id_end), true, true);
      Filter expected = new QueryWrapperFilter(query);
      Filter actual = new CachingWrapperFilter(expected, MAYBE_CACHE_POLICY);
      assertFilterEquals(expected, actual);
    }
  }

  /** test dense filters (match entire index) */
  public void testDense() throws Exception {
    Query query = new MatchAllDocsQuery();
    Filter expected = new QueryWrapperFilter(query);
    Filter actual = new CachingWrapperFilter(expected, FilterCachingPolicy.ALWAYS_CACHE);
    assertFilterEquals(expected, actual);
  }

  public void testCachingWorks() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    writer.close();

    IndexReader reader = SlowCompositeReaderWrapper.wrap(DirectoryReader.open(dir));
    LeafReaderContext context = (LeafReaderContext) reader.getContext();
    MockFilter filter = new MockFilter();
    CachingWrapperFilter cacher = new CachingWrapperFilter(filter, FilterCachingPolicy.ALWAYS_CACHE);

    // first time, nested filter is called
    DocIdSet strongRef = cacher.getDocIdSet(context, context.reader().getLiveDocs());
    assertTrue("first time", filter.wasCalled());

    // make sure no exception if cache is holding the wrong docIdSet
    cacher.getDocIdSet(context, context.reader().getLiveDocs());

    // second time, nested filter should not be called
    filter.clear();
    cacher.getDocIdSet(context, context.reader().getLiveDocs());
    assertFalse("second time", filter.wasCalled());

    reader.close();
    dir.close();
  }

  public void testNullDocIdSet() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    writer.close();

    IndexReader reader = SlowCompositeReaderWrapper.wrap(DirectoryReader.open(dir));
    LeafReaderContext context = (LeafReaderContext) reader.getContext();

    final Filter filter = new Filter() {
      @Override
      public DocIdSet getDocIdSet(LeafReaderContext context, Bits acceptDocs) {
        return null;
      }
      @Override
      public String toString(String field) {
        return "nullDocIdSetFilter";
      }
    };
    CachingWrapperFilter cacher = new CachingWrapperFilter(filter, MAYBE_CACHE_POLICY);

    // the caching filter should return the empty set constant
    assertNull(cacher.getDocIdSet(context, context.reader().getLiveDocs()));

    reader.close();
    dir.close();
  }

  public void testNullDocIdSetIterator() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    writer.close();

    IndexReader reader = SlowCompositeReaderWrapper.wrap(DirectoryReader.open(dir));
    LeafReaderContext context = (LeafReaderContext) reader.getContext();

    final Filter filter = new Filter() {
      @Override
      public DocIdSet getDocIdSet(LeafReaderContext context, Bits acceptDocs) {
        return new DocIdSet() {
          @Override
          public DocIdSetIterator iterator() {
            return null;
          }

          @Override
          public long ramBytesUsed() {
            return 0L;
          }
        };
      }
      @Override
      public String toString(String field) {
        return "nullDocIdSetIteratorFilter";
      }
    };
    CachingWrapperFilter cacher = new CachingWrapperFilter(filter, FilterCachingPolicy.ALWAYS_CACHE);

    // the caching filter should return the empty set constant
    assertNull(cacher.getDocIdSet(context, context.reader().getLiveDocs()));

    reader.close();
    dir.close();
  }

  private static void assertDocIdSetCacheable(IndexReader reader, Filter filter, boolean shouldCacheable) throws IOException {
    assertTrue(reader.getContext() instanceof LeafReaderContext);
    LeafReaderContext context = (LeafReaderContext) reader.getContext();
    final CachingWrapperFilter cacher = new CachingWrapperFilter(filter, FilterCachingPolicy.ALWAYS_CACHE);
    final DocIdSet originalSet = filter.getDocIdSet(context, context.reader().getLiveDocs());
    final DocIdSet cachedSet = cacher.getDocIdSet(context, context.reader().getLiveDocs());
    if (originalSet == null) {
      assertNull(cachedSet);
    }
    if (cachedSet == null) {
      assertTrue(originalSet == null || originalSet.iterator() == null);
    } else {
      assertTrue(cachedSet.isCacheable());
      assertEquals(shouldCacheable, originalSet.isCacheable());
      //System.out.println("Original: "+originalSet.getClass().getName()+" -- cached: "+cachedSet.getClass().getName());
      if (originalSet.isCacheable()) {
        assertEquals("Cached DocIdSet must be of same class like uncached, if cacheable", originalSet.getClass(), cachedSet.getClass());
      } else {
        assertTrue("Cached DocIdSet must be a RoaringDocIdSet if the original one was not cacheable", cachedSet instanceof RoaringDocIdSet || cachedSet == null);
      }
    }
  }

  public void testIsCacheAble() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    writer.addDocument(new Document());
    writer.close();

    IndexReader reader = SlowCompositeReaderWrapper.wrap(DirectoryReader.open(dir));

    // not cacheable:
    assertDocIdSetCacheable(reader, new QueryWrapperFilter(new TermQuery(new Term("test","value"))), false);
    // returns default empty docidset, always cacheable:
    assertDocIdSetCacheable(reader, new Filter() {
      @Override
      public DocIdSet getDocIdSet(LeafReaderContext context, Bits acceptDocs) {
        return null;
      }
      @Override
      public String toString(String field) {
        return "cacheableFilter";
      }
    }, true);
    // is cacheable:
    assertDocIdSetCacheable(reader, new QueryWrapperFilter(NumericRangeQuery.newIntRange("test", 10, 20, true, true)), false);
    // a fixedbitset filter is always cacheable
    assertDocIdSetCacheable(reader, new Filter() {
      @Override
      public DocIdSet getDocIdSet(LeafReaderContext context, Bits acceptDocs) {
        return new BitDocIdSet(new FixedBitSet(context.reader().maxDoc()));
      }
      @Override
      public String toString(String field) {
        return "cacheableFilter";
      }
    }, true);

    reader.close();
    dir.close();
  }

  public void testEnforceDeletions() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(
        random(),
        dir,
        newIndexWriterConfig(new MockAnalyzer(random())).
            setMergeScheduler(new SerialMergeScheduler()).
            // asserts below requires no unexpected merges:
            setMergePolicy(newLogMergePolicy(10))
    );

    // NOTE: cannot use writer.getReader because RIW (on
    // flipping a coin) may give us a newly opened reader,
    // but we use .reopen on this reader below and expect to
    // (must) get an NRT reader:
    DirectoryReader reader = DirectoryReader.open(writer.w, true);
    // same reason we don't wrap?
    IndexSearcher searcher = newSearcher(reader, false);

    // add a doc, refresh the reader, and check that it's there
    Document doc = new Document();
    doc.add(newStringField("id", "1", Field.Store.YES));
    writer.addDocument(doc);

    reader = refreshReader(reader);
    searcher = newSearcher(reader, false);

    TopDocs docs = searcher.search(new MatchAllDocsQuery(), 1);
    assertEquals("Should find a hit...", 1, docs.totalHits);

    final Filter startFilter = new QueryWrapperFilter(new TermQuery(new Term("id", "1")));

    CachingWrapperFilter filter = new CachingWrapperFilter(startFilter, FilterCachingPolicy.ALWAYS_CACHE);

    docs = searcher.search(new FilteredQuery(new MatchAllDocsQuery(), filter), 1);
    assertTrue(filter.ramBytesUsed() > 0);

    assertEquals("[query + filter] Should find a hit...", 1, docs.totalHits);

    Query constantScore = new ConstantScoreQuery(filter);
    docs = searcher.search(constantScore, 1);
    assertEquals("[just filter] Should find a hit...", 1, docs.totalHits);

    // make sure we get a cache hit when we reopen reader
    // that had no change to deletions

    // fake delete (deletes nothing):
    writer.deleteDocuments(new Term("foo", "bar"));

    IndexReader oldReader = reader;
    reader = refreshReader(reader);
    assertTrue(reader == oldReader);
    int missCount = filter.missCount;
    docs = searcher.search(constantScore, 1);
    assertEquals("[just filter] Should find a hit...", 1, docs.totalHits);

    // cache hit:
    assertEquals(missCount, filter.missCount);

    // now delete the doc, refresh the reader, and see that it's not there
    writer.deleteDocuments(new Term("id", "1"));

    // NOTE: important to hold ref here so GC doesn't clear
    // the cache entry!  Else the assert below may sometimes
    // fail:
    oldReader = reader;
    reader = refreshReader(reader);

    searcher = newSearcher(reader, false);

    missCount = filter.missCount;
    docs = searcher.search(new FilteredQuery(new MatchAllDocsQuery(), filter), 1);
    assertEquals("[query + filter] Should *not* find a hit...", 0, docs.totalHits);

    // cache hit
    assertEquals(missCount, filter.missCount);
    docs = searcher.search(constantScore, 1);
    assertEquals("[just filter] Should *not* find a hit...", 0, docs.totalHits);

    // apply deletes dynamically:
    filter = new CachingWrapperFilter(startFilter, FilterCachingPolicy.ALWAYS_CACHE);
    writer.addDocument(doc);
    reader = refreshReader(reader);
    searcher = newSearcher(reader, false);

    docs = searcher.search(new FilteredQuery(new MatchAllDocsQuery(), filter), 1);
    assertEquals("[query + filter] Should find a hit...", 1, docs.totalHits);
    missCount = filter.missCount;
    assertTrue(missCount > 0);
    constantScore = new ConstantScoreQuery(filter);
    docs = searcher.search(constantScore, 1);
    assertEquals("[just filter] Should find a hit...", 1, docs.totalHits);
    assertEquals(missCount, filter.missCount);

    writer.addDocument(doc);

    // NOTE: important to hold ref here so GC doesn't clear
    // the cache entry!  Else the assert below may sometimes
    // fail:
    oldReader = reader;

    reader = refreshReader(reader);
    searcher = newSearcher(reader, false);

    docs = searcher.search(new FilteredQuery(new MatchAllDocsQuery(), filter), 1);
    assertEquals("[query + filter] Should find 2 hits...", 2, docs.totalHits);
    assertTrue(filter.missCount > missCount);
    missCount = filter.missCount;

    constantScore = new ConstantScoreQuery(filter);
    docs = searcher.search(constantScore, 1);
    assertEquals("[just filter] Should find a hit...", 2, docs.totalHits);
    assertEquals(missCount, filter.missCount);

    // now delete the doc, refresh the reader, and see that it's not there
    writer.deleteDocuments(new Term("id", "1"));

    reader = refreshReader(reader);
    searcher = newSearcher(reader, false);

    docs = searcher.search(new FilteredQuery(new MatchAllDocsQuery(), filter), 1);
    assertEquals("[query + filter] Should *not* find a hit...", 0, docs.totalHits);
    // CWF reused the same entry (it dynamically applied the deletes):
    assertEquals(missCount, filter.missCount);

    docs = searcher.search(constantScore, 1);
    assertEquals("[just filter] Should *not* find a hit...", 0, docs.totalHits);
    // CWF reused the same entry (it dynamically applied the deletes):
    assertEquals(missCount, filter.missCount);

    // NOTE: silliness to make sure JRE does not eliminate
    // our holding onto oldReader to prevent
    // CachingWrapperFilter's WeakHashMap from dropping the
    // entry:
    assertTrue(oldReader != null);

    reader.close();
    writer.close();
    dir.close();
  }

  private static DirectoryReader refreshReader(DirectoryReader reader) throws IOException {
    DirectoryReader oldReader = reader;
    reader = DirectoryReader.openIfChanged(reader);
    if (reader != null) {
      oldReader.close();
      return reader;
    } else {
      return oldReader;
    }
  }

}
