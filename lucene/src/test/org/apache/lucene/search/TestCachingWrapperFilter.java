package org.apache.lucene.search;

/**
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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.SlowMultiReaderWrapper;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.OpenBitSet;
import org.apache.lucene.util.OpenBitSetDISI;
import org.apache.lucene.util._TestUtil;

public class TestCachingWrapperFilter extends LuceneTestCase {

  public void testCachingWorks() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random, dir);
    writer.close();

    IndexReader reader = new SlowMultiReaderWrapper(IndexReader.open(dir, true));
    AtomicReaderContext context = (AtomicReaderContext) reader.getTopReaderContext();
    MockFilter filter = new MockFilter();
    CachingWrapperFilter cacher = new CachingWrapperFilter(filter);

    // first time, nested filter is called
    cacher.getDocIdSet(context);
    assertTrue("first time", filter.wasCalled());

    // make sure no exception if cache is holding the wrong docIdSet
    cacher.getDocIdSet(context);

    // second time, nested filter should not be called
    filter.clear();
    cacher.getDocIdSet(context);
    assertFalse("second time", filter.wasCalled());

    reader.close();
    dir.close();
  }
  
  public void testNullDocIdSet() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random, dir);
    writer.close();

    IndexReader reader = new SlowMultiReaderWrapper(IndexReader.open(dir, true));
    AtomicReaderContext context = (AtomicReaderContext) reader.getTopReaderContext();

    final Filter filter = new Filter() {
      @Override
      public DocIdSet getDocIdSet(AtomicReaderContext context) {
        return null;
      }
    };
    CachingWrapperFilter cacher = new CachingWrapperFilter(filter);

    // the caching filter should return the empty set constant
    assertSame(DocIdSet.EMPTY_DOCIDSET, cacher.getDocIdSet(context));
    
    reader.close();
    dir.close();
  }
  
  public void testNullDocIdSetIterator() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random, dir);
    writer.close();

    IndexReader reader = new SlowMultiReaderWrapper(IndexReader.open(dir, true));
    AtomicReaderContext context = (AtomicReaderContext) reader.getTopReaderContext();

    final Filter filter = new Filter() {
      @Override
      public DocIdSet getDocIdSet(AtomicReaderContext context) {
        return new DocIdSet() {
          @Override
          public DocIdSetIterator iterator() {
            return null;
          }
        };
      }
    };
    CachingWrapperFilter cacher = new CachingWrapperFilter(filter);

    // the caching filter should return the empty set constant
    assertSame(DocIdSet.EMPTY_DOCIDSET, cacher.getDocIdSet(context));
    
    reader.close();
    dir.close();
  }
  
  private static void assertDocIdSetCacheable(IndexReader reader, Filter filter, boolean shouldCacheable) throws IOException {
    assertTrue(reader.getTopReaderContext().isAtomic);
    AtomicReaderContext context = (AtomicReaderContext) reader.getTopReaderContext();
    final CachingWrapperFilter cacher = new CachingWrapperFilter(filter);
    final DocIdSet originalSet = filter.getDocIdSet(context);
    final DocIdSet cachedSet = cacher.getDocIdSet(context);
    assertTrue(cachedSet.isCacheable());
    assertEquals(shouldCacheable, originalSet.isCacheable());
    //System.out.println("Original: "+originalSet.getClass().getName()+" -- cached: "+cachedSet.getClass().getName());
    if (originalSet.isCacheable()) {
      assertEquals("Cached DocIdSet must be of same class like uncached, if cacheable", originalSet.getClass(), cachedSet.getClass());
    } else {
      assertTrue("Cached DocIdSet must be an OpenBitSet if the original one was not cacheable", cachedSet instanceof OpenBitSetDISI || cachedSet == DocIdSet.EMPTY_DOCIDSET);
    }
  }
  
  public void testIsCacheAble() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random, dir);
    writer.addDocument(new Document());
    writer.close();

    IndexReader reader = new SlowMultiReaderWrapper(IndexReader.open(dir, true));

    // not cacheable:
    assertDocIdSetCacheable(reader, new QueryWrapperFilter(new TermQuery(new Term("test","value"))), false);
    // returns default empty docidset, always cacheable:
    assertDocIdSetCacheable(reader, NumericRangeFilter.newIntRange("test", Integer.valueOf(10000), Integer.valueOf(-10000), true, true), true);
    // is cacheable:
    assertDocIdSetCacheable(reader, FieldCacheRangeFilter.newIntRange("test", Integer.valueOf(10), Integer.valueOf(20), true, true), true);
    // a openbitset filter is always cacheable
    assertDocIdSetCacheable(reader, new Filter() {
      @Override
      public DocIdSet getDocIdSet(AtomicReaderContext context) {
        return new OpenBitSet();
      }
    }, true);

    reader.close();
    dir.close();
  }

  public void testEnforceDeletions() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(
        random,
        dir,
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)).
            setMergeScheduler(new SerialMergeScheduler()).
            // asserts below requires no unexpected merges:
            setMergePolicy(newLogMergePolicy(10))
    );

    // NOTE: cannot use writer.getReader because RIW (on
    // flipping a coin) may give us a newly opened reader,
    // but we use .reopen on this reader below and expect to
    // (must) get an NRT reader:
    IndexReader reader = IndexReader.open(writer.w, true);
    // same reason we don't wrap?
    IndexSearcher searcher = newSearcher(reader, false);

    // add a doc, refresh the reader, and check that its there
    Document doc = new Document();
    doc.add(newField("id", "1", Field.Store.YES, Field.Index.NOT_ANALYZED));
    writer.addDocument(doc);

    reader = refreshReader(reader);
    searcher.close();
    searcher = newSearcher(reader, false);

    TopDocs docs = searcher.search(new MatchAllDocsQuery(), 1);
    assertEquals("Should find a hit...", 1, docs.totalHits);

    final Filter startFilter = new QueryWrapperFilter(new TermQuery(new Term("id", "1")));

    // ignore deletions
    CachingWrapperFilter filter = new CachingWrapperFilter(startFilter, CachingWrapperFilter.DeletesMode.IGNORE);
        
    docs = searcher.search(new MatchAllDocsQuery(), filter, 1);
    assertEquals("[query + filter] Should find a hit...", 1, docs.totalHits);
    ConstantScoreQuery constantScore = new ConstantScoreQuery(filter);
    docs = searcher.search(constantScore, 1);
    assertEquals("[just filter] Should find a hit...", 1, docs.totalHits);

    // now delete the doc, refresh the reader, and see that it's not there
    _TestUtil.keepFullyDeletedSegments(writer.w);
    writer.deleteDocuments(new Term("id", "1"));

    reader = refreshReader(reader);
    searcher.close();
    searcher = newSearcher(reader, false);

    docs = searcher.search(new MatchAllDocsQuery(), filter, 1);
    assertEquals("[query + filter] Should *not* find a hit...", 0, docs.totalHits);

    docs = searcher.search(constantScore, 1);
    assertEquals("[just filter] Should find a hit...", 1, docs.totalHits);


    // force cache to regenerate:
    filter = new CachingWrapperFilter(startFilter, CachingWrapperFilter.DeletesMode.RECACHE);

    writer.addDocument(doc);

    reader = refreshReader(reader);
    searcher.close();
    searcher = newSearcher(reader, false);
        
    docs = searcher.search(new MatchAllDocsQuery(), filter, 1);

    assertEquals("[query + filter] Should find a hit...", 1, docs.totalHits);

    constantScore = new ConstantScoreQuery(filter);
    docs = searcher.search(constantScore, 1);
    assertEquals("[just filter] Should find a hit...", 1, docs.totalHits);

    // NOTE: important to hold ref here so GC doesn't clear
    // the cache entry!  Else the assert below may sometimes
    // fail:
    IndexReader oldReader = reader;

    // make sure we get a cache hit when we reopen reader
    // that had no change to deletions
    reader = refreshReader(reader);
    assertTrue(reader != oldReader);
    searcher.close();
    searcher = newSearcher(reader, false);
    int missCount = filter.missCount;
    docs = searcher.search(constantScore, 1);
    assertEquals("[just filter] Should find a hit...", 1, docs.totalHits);
    assertEquals(missCount, filter.missCount);

    // now delete the doc, refresh the reader, and see that it's not there
    writer.deleteDocuments(new Term("id", "1"));

    reader = refreshReader(reader);
    searcher.close();
    searcher = newSearcher(reader, false);

    missCount = filter.missCount;
    docs = searcher.search(new MatchAllDocsQuery(), filter, 1);
    assertEquals(missCount+1, filter.missCount);
    assertEquals("[query + filter] Should *not* find a hit...", 0, docs.totalHits);
    docs = searcher.search(constantScore, 1);
    assertEquals("[just filter] Should *not* find a hit...", 0, docs.totalHits);


    // apply deletions dynamically
    filter = new CachingWrapperFilter(startFilter, CachingWrapperFilter.DeletesMode.DYNAMIC);

    writer.addDocument(doc);
    reader = refreshReader(reader);
    searcher.close();
    searcher = newSearcher(reader, false);
        
    docs = searcher.search(new MatchAllDocsQuery(), filter, 1);
    assertEquals("[query + filter] Should find a hit...", 1, docs.totalHits);
    constantScore = new ConstantScoreQuery(filter);
    docs = searcher.search(constantScore, 1);
    assertEquals("[just filter] Should find a hit...", 1, docs.totalHits);

    // now delete the doc, refresh the reader, and see that it's not there
    writer.deleteDocuments(new Term("id", "1"));

    reader = refreshReader(reader);
    searcher.close();
    searcher = newSearcher(reader, false);

    docs = searcher.search(new MatchAllDocsQuery(), filter, 1);
    assertEquals("[query + filter] Should *not* find a hit...", 0, docs.totalHits);

    missCount = filter.missCount;
    docs = searcher.search(constantScore, 1);
    assertEquals("[just filter] Should *not* find a hit...", 0, docs.totalHits);

    // doesn't count as a miss
    assertEquals(missCount, filter.missCount);

    // NOTE: silliness to make sure JRE does not optimize
    // away our holding onto oldReader to prevent
    // CachingWrapperFilter's WeakHashMap from dropping the
    // entry:
    assertTrue(oldReader != null);

    searcher.close();
    reader.close();
    writer.close();
    dir.close();
  }

  private static IndexReader refreshReader(IndexReader reader) throws IOException {
    IndexReader oldReader = reader;
    reader = reader.reopen();
    if (reader != oldReader) {
      oldReader.close();
    }
    return reader;
  }
}
