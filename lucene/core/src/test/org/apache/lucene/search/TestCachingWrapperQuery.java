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
import java.util.concurrent.atomic.AtomicBoolean;

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
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;

public class TestCachingWrapperQuery extends LuceneTestCase {
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

  private void assertQueryEquals(Query f1, Query f2) throws Exception {
    // wrap into CSQ so that scores are not needed
    TopDocs hits1 = is.search(new ConstantScoreQuery(f1), ir.maxDoc());
    TopDocs hits2 = is.search(new ConstantScoreQuery(f2), ir.maxDoc());
    assertEquals(hits1.totalHits, hits2.totalHits);
    CheckHits.checkEqual(f1, hits1.scoreDocs, hits2.scoreDocs);
    // now do it again to confirm caching works
    TopDocs hits3 = is.search(new ConstantScoreQuery(f1), ir.maxDoc());
    TopDocs hits4 = is.search(new ConstantScoreQuery(f2), ir.maxDoc());
    assertEquals(hits3.totalHits, hits4.totalHits);
    CheckHits.checkEqual(f1, hits3.scoreDocs, hits4.scoreDocs);
  }

  /** test null iterator */
  public void testEmpty() throws Exception {
    BooleanQuery.Builder expected = new BooleanQuery.Builder();
    Query cached = new CachingWrapperQuery(expected.build(), MAYBE_CACHE_POLICY);
    assertQueryEquals(expected.build(), cached);
  }

  /** test iterator returns NO_MORE_DOCS */
  public void testEmpty2() throws Exception {
    BooleanQuery.Builder expected = new BooleanQuery.Builder();
    expected.add(new TermQuery(new Term("id", "0")), BooleanClause.Occur.MUST);
    expected.add(new TermQuery(new Term("id", "0")), BooleanClause.Occur.MUST_NOT);
    Query cached = new CachingWrapperQuery(expected.build(), MAYBE_CACHE_POLICY);
    assertQueryEquals(expected.build(), cached);
  }

  /** test iterator returns single document */
  public void testSingle() throws Exception {
    for (int i = 0; i < 10; i++) {
      int id = random().nextInt(ir.maxDoc());
      Query expected = new TermQuery(new Term("id", Integer.toString(id)));
      Query cached = new CachingWrapperQuery(expected, MAYBE_CACHE_POLICY);
      assertQueryEquals(expected, cached);
    }
  }

  /** test sparse filters (match single documents) */
  public void testSparse() throws Exception {
    for (int i = 0; i < 10; i++) {
      int id_start = random().nextInt(ir.maxDoc()-1);
      int id_end = id_start + 1;
      Query expected = TermRangeQuery.newStringRange("id",
          Integer.toString(id_start), Integer.toString(id_end), true, true);
      Query cached = new CachingWrapperQuery(expected, MAYBE_CACHE_POLICY);
      assertQueryEquals(expected, cached);
    }
  }

  /** test dense filters (match entire index) */
  public void testDense() throws Exception {
    Query expected = new MatchAllDocsQuery();
    Query cached = new CachingWrapperQuery(expected, MAYBE_CACHE_POLICY);
    assertQueryEquals(expected, cached);
  }

  private static class MockQuery extends Query {

    private final AtomicBoolean wasCalled = new AtomicBoolean();

    public boolean wasCalled() {
      return wasCalled.get();
    }

    public void clear() {
      wasCalled.set(false);
    }

    @Override
    public String toString(String field) {
      return "Mock";
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
      return new ConstantScoreWeight(this) {
        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
          wasCalled.set(true);
          return new ConstantScoreScorer(this, score(), DocIdSetIterator.all(context.reader().maxDoc()));
        }
      };
    }
  }

  public void testCachingWorks() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    writer.close();

    IndexReader reader = SlowCompositeReaderWrapper.wrap(DirectoryReader.open(dir));
    IndexSearcher searcher = newSearcher(reader);
    LeafReaderContext context = (LeafReaderContext) reader.getContext();
    MockQuery filter = new MockQuery();
    CachingWrapperQuery cacher = new CachingWrapperQuery(filter, QueryCachingPolicy.ALWAYS_CACHE);

    // first time, nested filter is called
    searcher.rewrite(cacher).createWeight(searcher, false).scorer(context);
    assertTrue("first time", filter.wasCalled());

    // make sure no exception if cache is holding the wrong docIdSet
    searcher.rewrite(cacher).createWeight(searcher, false).scorer(context);

    // second time, nested filter should not be called
    filter.clear();
    searcher.rewrite(cacher).createWeight(searcher, false).scorer(context);
    assertFalse("second time", filter.wasCalled());

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

    final Query startQuery = new TermQuery(new Term("id", "1"));

    CachingWrapperQuery query = new CachingWrapperQuery(startQuery, QueryCachingPolicy.ALWAYS_CACHE);

    docs = searcher.search(new ConstantScoreQuery(query), 1);
    assertTrue(query.ramBytesUsed() > 0);

    assertEquals("[query + filter] Should find a hit...", 1, docs.totalHits);

    Query constantScore = new ConstantScoreQuery(query);
    docs = searcher.search(constantScore, 1);
    assertEquals("[just filter] Should find a hit...", 1, docs.totalHits);

    // make sure we get a cache hit when we reopen reader
    // that had no change to deletions

    // fake delete (deletes nothing):
    writer.deleteDocuments(new Term("foo", "bar"));

    IndexReader oldReader = reader;
    reader = refreshReader(reader);
    assertTrue(reader == oldReader);
    int missCount = query.missCount;
    docs = searcher.search(constantScore, 1);
    assertEquals("[just filter] Should find a hit...", 1, docs.totalHits);

    // cache hit:
    assertEquals(missCount, query.missCount);

    // now delete the doc, refresh the reader, and see that it's not there
    writer.deleteDocuments(new Term("id", "1"));

    // NOTE: important to hold ref here so GC doesn't clear
    // the cache entry!  Else the assert below may sometimes
    // fail:
    oldReader = reader;
    reader = refreshReader(reader);

    searcher = newSearcher(reader, false);

    missCount = query.missCount;
    docs = searcher.search(new ConstantScoreQuery(query), 1);
    assertEquals("[query + filter] Should *not* find a hit...", 0, docs.totalHits);

    // cache hit
    assertEquals(missCount, query.missCount);
    docs = searcher.search(constantScore, 1);
    assertEquals("[just filter] Should *not* find a hit...", 0, docs.totalHits);

    // apply deletes dynamically:
    query = new CachingWrapperQuery(startQuery, QueryCachingPolicy.ALWAYS_CACHE);
    writer.addDocument(doc);
    reader = refreshReader(reader);
    searcher = newSearcher(reader, false);

    docs = searcher.search(new ConstantScoreQuery(query), 1);
    assertEquals("[query + filter] Should find a hit...", 1, docs.totalHits);
    missCount = query.missCount;
    assertTrue(missCount > 0);
    constantScore = new ConstantScoreQuery(query);
    docs = searcher.search(constantScore, 1);
    assertEquals("[just filter] Should find a hit...", 1, docs.totalHits);
    assertEquals(missCount, query.missCount);

    writer.addDocument(doc);

    // NOTE: important to hold ref here so GC doesn't clear
    // the cache entry!  Else the assert below may sometimes
    // fail:
    oldReader = reader;

    reader = refreshReader(reader);
    searcher = newSearcher(reader, false);

    docs = searcher.search(new ConstantScoreQuery(query), 1);
    assertEquals("[query + filter] Should find 2 hits...", 2, docs.totalHits);
    assertTrue(query.missCount > missCount);
    missCount = query.missCount;

    constantScore = new ConstantScoreQuery(query);
    docs = searcher.search(constantScore, 1);
    assertEquals("[just filter] Should find a hit...", 2, docs.totalHits);
    assertEquals(missCount, query.missCount);

    // now delete the doc, refresh the reader, and see that it's not there
    writer.deleteDocuments(new Term("id", "1"));

    reader = refreshReader(reader);
    searcher = newSearcher(reader, false);

    docs = searcher.search(new ConstantScoreQuery(query), 1);
    assertEquals("[query + filter] Should *not* find a hit...", 0, docs.totalHits);
    // CWF reused the same entry (it dynamically applied the deletes):
    assertEquals(missCount, query.missCount);

    docs = searcher.search(constantScore, 1);
    assertEquals("[just filter] Should *not* find a hit...", 0, docs.totalHits);
    // CWF reused the same entry (it dynamically applied the deletes):
    assertEquals(missCount, query.missCount);

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
