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
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

public class TestCachingWrapperFilter extends LuceneTestCase {
  
  public void testCachingWorks() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    writer.close();

    IndexReader reader = SlowCompositeReaderWrapper.wrap(IndexReader.open(dir));
    AtomicReaderContext context = (AtomicReaderContext) reader.getTopReaderContext();
    MockFilter filter = new MockFilter();
    CachingWrapperFilter cacher = new CachingWrapperFilter(filter);

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

    IndexReader reader = SlowCompositeReaderWrapper.wrap(IndexReader.open(dir));
    AtomicReaderContext context = (AtomicReaderContext) reader.getTopReaderContext();

    final Filter filter = new Filter() {
      @Override
      public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) {
        return null;
      }
    };
    CachingWrapperFilter cacher = new CachingWrapperFilter(filter);

    // the caching filter should return the empty set constant
    assertSame(DocIdSet.EMPTY_DOCIDSET, cacher.getDocIdSet(context, context.reader().getLiveDocs()));
    
    reader.close();
    dir.close();
  }
  
  public void testNullDocIdSetIterator() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    writer.close();

    IndexReader reader = SlowCompositeReaderWrapper.wrap(IndexReader.open(dir));
    AtomicReaderContext context = (AtomicReaderContext) reader.getTopReaderContext();

    final Filter filter = new Filter() {
      @Override
      public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) {
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
    assertSame(DocIdSet.EMPTY_DOCIDSET, cacher.getDocIdSet(context, context.reader().getLiveDocs()));
    
    reader.close();
    dir.close();
  }
  
  private static void assertDocIdSetCacheable(IndexReader reader, Filter filter, boolean shouldCacheable) throws IOException {
    assertTrue(reader.getTopReaderContext() instanceof AtomicReaderContext);
    AtomicReaderContext context = (AtomicReaderContext) reader.getTopReaderContext();
    final CachingWrapperFilter cacher = new CachingWrapperFilter(filter);
    final DocIdSet originalSet = filter.getDocIdSet(context, context.reader().getLiveDocs());
    final DocIdSet cachedSet = cacher.getDocIdSet(context, context.reader().getLiveDocs());
    assertTrue(cachedSet.isCacheable());
    assertEquals(shouldCacheable, originalSet.isCacheable());
    //System.out.println("Original: "+originalSet.getClass().getName()+" -- cached: "+cachedSet.getClass().getName());
    if (originalSet.isCacheable()) {
      assertEquals("Cached DocIdSet must be of same class like uncached, if cacheable", originalSet.getClass(), cachedSet.getClass());
    } else {
      assertTrue("Cached DocIdSet must be an FixedBitSet if the original one was not cacheable", cachedSet instanceof FixedBitSet || cachedSet == DocIdSet.EMPTY_DOCIDSET);
    }
  }
  
  public void testIsCacheAble() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    writer.addDocument(new Document());
    writer.close();

    IndexReader reader = SlowCompositeReaderWrapper.wrap(IndexReader.open(dir));

    // not cacheable:
    assertDocIdSetCacheable(reader, new QueryWrapperFilter(new TermQuery(new Term("test","value"))), false);
    // returns default empty docidset, always cacheable:
    assertDocIdSetCacheable(reader, NumericRangeFilter.newIntRange("test", Integer.valueOf(10000), Integer.valueOf(-10000), true, true), true);
    // is cacheable:
    assertDocIdSetCacheable(reader, FieldCacheRangeFilter.newIntRange("test", Integer.valueOf(10), Integer.valueOf(20), true, true), true);
    // a fixedbitset filter is always cacheable
    assertDocIdSetCacheable(reader, new Filter() {
      @Override
      public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) {
        return new FixedBitSet(context.reader().maxDoc());
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
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).
            setMergeScheduler(new SerialMergeScheduler()).
            // asserts below requires no unexpected merges:
            setMergePolicy(newLogMergePolicy(10))
    );
    _TestUtil.keepFullyDeletedSegments(writer.w);

    // NOTE: cannot use writer.getReader because RIW (on
    // flipping a coin) may give us a newly opened reader,
    // but we use .reopen on this reader below and expect to
    // (must) get an NRT reader:
    DirectoryReader reader = IndexReader.open(writer.w, true);
    // same reason we don't wrap?
    IndexSearcher searcher = newSearcher(reader, false);

    // add a doc, refresh the reader, and check that it's there
    Document doc = new Document();
    doc.add(newField("id", "1", StringField.TYPE_STORED));
    writer.addDocument(doc);

    reader = refreshReader(reader);
    searcher = newSearcher(reader, false);

    TopDocs docs = searcher.search(new MatchAllDocsQuery(), 1);
    assertEquals("Should find a hit...", 1, docs.totalHits);

    final Filter startFilter = new QueryWrapperFilter(new TermQuery(new Term("id", "1")));

    // force cache to regenerate after deletions:
    CachingWrapperFilter filter = new CachingWrapperFilter(startFilter, true);

    docs = searcher.search(new MatchAllDocsQuery(), filter, 1);

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
    docs = searcher.search(new MatchAllDocsQuery(), filter, 1);
    assertEquals("[query + filter] Should *not* find a hit...", 0, docs.totalHits);

    // cache miss, because we asked CWF to recache when
    // deletes changed:
    assertEquals(missCount+1, filter.missCount);
    docs = searcher.search(constantScore, 1);
    assertEquals("[just filter] Should *not* find a hit...", 0, docs.totalHits);

    // apply deletes dynamically:
    filter = new CachingWrapperFilter(startFilter);
    writer.addDocument(doc);
    reader = refreshReader(reader);
    searcher = newSearcher(reader, false);

    docs = searcher.search(new MatchAllDocsQuery(), filter, 1);
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
        
    docs = searcher.search(new MatchAllDocsQuery(), filter, 1);
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

    docs = searcher.search(new MatchAllDocsQuery(), filter, 1);
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
