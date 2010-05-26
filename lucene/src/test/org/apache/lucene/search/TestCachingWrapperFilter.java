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
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.OpenBitSet;
import org.apache.lucene.util.OpenBitSetDISI;

public class TestCachingWrapperFilter extends LuceneTestCase {
  public void testCachingWorks() throws Exception {
    Directory dir = new RAMDirectory();
    IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer()));
    writer.close();

    IndexReader reader = IndexReader.open(dir, true);

    MockFilter filter = new MockFilter();
    CachingWrapperFilter cacher = new CachingWrapperFilter(filter);

    // first time, nested filter is called
    cacher.getDocIdSet(reader);
    assertTrue("first time", filter.wasCalled());

    // make sure no exception if cache is holding the wrong docIdSet
    cacher.getDocIdSet(reader);

    // second time, nested filter should not be called
    filter.clear();
    cacher.getDocIdSet(reader);
    assertFalse("second time", filter.wasCalled());

    reader.close();
  }
  
  public void testNullDocIdSet() throws Exception {
    Directory dir = new RAMDirectory();
    IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer()));
    writer.close();

    IndexReader reader = IndexReader.open(dir, true);

    final Filter filter = new Filter() {
      @Override
      public DocIdSet getDocIdSet(IndexReader reader) {
        return null;
      }
    };
    CachingWrapperFilter cacher = new CachingWrapperFilter(filter);

    // the caching filter should return the empty set constant
    assertSame(DocIdSet.EMPTY_DOCIDSET, cacher.getDocIdSet(reader));
    
    reader.close();
  }
  
  public void testNullDocIdSetIterator() throws Exception {
    Directory dir = new RAMDirectory();
    IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer()));
    writer.close();

    IndexReader reader = IndexReader.open(dir, true);

    final Filter filter = new Filter() {
      @Override
      public DocIdSet getDocIdSet(IndexReader reader) {
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
    assertSame(DocIdSet.EMPTY_DOCIDSET, cacher.getDocIdSet(reader));
    
    reader.close();
  }
  
  private static void assertDocIdSetCacheable(IndexReader reader, Filter filter, boolean shouldCacheable) throws IOException {
    final CachingWrapperFilter cacher = new CachingWrapperFilter(filter);
    final DocIdSet originalSet = filter.getDocIdSet(reader);
    final DocIdSet cachedSet = cacher.getDocIdSet(reader);
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
    Directory dir = new RAMDirectory();
    IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer()));
    writer.close();

    IndexReader reader = IndexReader.open(dir, true);

    // not cacheable:
    assertDocIdSetCacheable(reader, new QueryWrapperFilter(new TermQuery(new Term("test","value"))), false);
    // returns default empty docidset, always cacheable:
    assertDocIdSetCacheable(reader, NumericRangeFilter.newIntRange("test", Integer.valueOf(10000), Integer.valueOf(-10000), true, true), true);
    // is cacheable:
    assertDocIdSetCacheable(reader, FieldCacheRangeFilter.newIntRange("test", Integer.valueOf(10), Integer.valueOf(20), true, true), true);
    // a openbitset filter is always cacheable
    assertDocIdSetCacheable(reader, new Filter() {
      @Override
      public DocIdSet getDocIdSet(IndexReader reader) {
        return new OpenBitSet();
      }
    }, true);

    reader.close();
  }
}
