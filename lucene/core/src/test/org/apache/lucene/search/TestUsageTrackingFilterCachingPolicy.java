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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

public class TestUsageTrackingFilterCachingPolicy extends LuceneTestCase {

  public void testCostlyFilter() {
    assertTrue(UsageTrackingQueryCachingPolicy.isCostly(new PrefixQuery(new Term("field", "prefix"))));
    assertTrue(UsageTrackingQueryCachingPolicy.isCostly(IntPoint.newRangeQuery("intField", 1, 1000)));
    assertFalse(UsageTrackingQueryCachingPolicy.isCostly(new TermQuery(new Term("field", "value"))));
  }

  public void testNeverCacheMatchAll() throws Exception {
    Query q = new MatchAllDocsQuery();
    UsageTrackingQueryCachingPolicy policy = new UsageTrackingQueryCachingPolicy();
    for (int i = 0; i < 1000; ++i) {
      policy.onUse(q);
    }
    assertFalse(policy.shouldCache(q));
  }

  public void testNeverCacheTermFilter() throws IOException {
    Query q = new TermQuery(new Term("foo", "bar"));
    UsageTrackingQueryCachingPolicy policy = new UsageTrackingQueryCachingPolicy();
    for (int i = 0; i < 1000; ++i) {
      policy.onUse(q);
    }
    assertFalse(policy.shouldCache(q));
  }

  public void testBooleanQueries() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    w.addDocument(new Document());
    IndexReader reader = w.getReader();
    w.close();
    
    IndexSearcher searcher = new IndexSearcher(reader);
    UsageTrackingQueryCachingPolicy policy = new UsageTrackingQueryCachingPolicy();
    LRUQueryCache cache = new LRUQueryCache(10, Long.MAX_VALUE, new LRUQueryCache.MinSegmentSizePredicate(1, 0f), Float.POSITIVE_INFINITY);
    searcher.setQueryCache(cache);
    searcher.setQueryCachingPolicy(policy);

    DummyQuery q1 = new DummyQuery(1);
    DummyQuery q2 = new DummyQuery(2);
    BooleanQuery bq = new BooleanQuery.Builder()
        .add(q1, Occur.SHOULD)
        .add(q2, Occur.SHOULD)
        .build();

    for (int i = 0; i < 3; ++i) {
      searcher.count(bq);
    }
    assertEquals(0, cache.getCacheSize()); // nothing cached yet, too early

    searcher.count(bq);
    assertEquals(1, cache.getCacheSize()); // the bq got cached, but not q1 and q2

    for (int i = 0; i < 10; ++i) {
      searcher.count(bq);
    }
    assertEquals(1, cache.getCacheSize()); // q1 and q2 still not cached since we do not pull scorers on them

    searcher.count(q1);
    assertEquals(2, cache.getCacheSize()); // q1 used on its own -> cached

    reader.close();
    dir.close();
  }

  private static class DummyQuery extends Query {

    private final int id;

    DummyQuery(int id) {
      this.id = id;
    }

    @Override
    public String toString(String field) {
      return "dummy";
    }

    @Override
    public boolean equals(Object obj) {
      return sameClassAs(obj) && ((DummyQuery) obj).id == id;
    }

    @Override
    public int hashCode() {
      return id;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
      return new ConstantScoreWeight(DummyQuery.this, boost) {
        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
          return new ConstantScoreScorer(this, score(), scoreMode, DocIdSetIterator.all(1));
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
          return true;
        }
      };
    }

    @Override
    public void visit(QueryVisitor visitor) {

    }

  }

}
