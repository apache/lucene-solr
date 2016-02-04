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
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;

/** This class only tests some basic functionality in CSQ, the main parts are mostly
 * tested by MultiTermQuery tests, explanations seems to be tested in TestExplanations! */
public class TestConstantScoreQuery extends LuceneTestCase {
  
  public void testCSQ() throws Exception {
    final Query q1 = new ConstantScoreQuery(new TermQuery(new Term("a", "b")));
    final Query q2 = new ConstantScoreQuery(new TermQuery(new Term("a", "c")));
    final Query q3 = new ConstantScoreQuery(TermRangeQuery.newStringRange("a", "b", "c", true, true));
    QueryUtils.check(q1);
    QueryUtils.check(q2);
    QueryUtils.checkEqual(q1,q1);
    QueryUtils.checkEqual(q2,q2);
    QueryUtils.checkEqual(q3,q3);
    QueryUtils.checkUnequal(q1,q2);
    QueryUtils.checkUnequal(q2,q3);
    QueryUtils.checkUnequal(q1,q3);
    QueryUtils.checkUnequal(q1, new TermQuery(new Term("a", "b")));
  }
  
  private void checkHits(IndexSearcher searcher, Query q, final float expectedScore, final Class<? extends Scorer> innerScorerClass) throws IOException {
    final int[] count = new int[1];
    searcher.search(q, new SimpleCollector() {
      private Scorer scorer;
    
      @Override
      public void setScorer(Scorer scorer) {
        this.scorer = scorer;
        if (innerScorerClass != null) {
          final FilterScorer innerScorer = (FilterScorer) scorer;
          assertEquals("inner Scorer is implemented by wrong class", innerScorerClass, innerScorer.in.getClass());
        }
      }
      
      @Override
      public void collect(int doc) throws IOException {
        assertEquals("Score differs from expected", expectedScore, this.scorer.score(), 0);
        count[0]++;
      }
      
      @Override
      public boolean needsScores() {
        return true;
      }
    });
    assertEquals("invalid number of results", 1, count[0]);
  }
  
  public void testWrapped2Times() throws Exception {
    Directory directory = null;
    IndexReader reader = null;
    IndexSearcher searcher = null;
    try {
      directory = newDirectory();
      RandomIndexWriter writer = new RandomIndexWriter (random(), directory);

      Document doc = new Document();
      doc.add(newStringField("field", "term", Field.Store.NO));
      writer.addDocument(doc);

      reader = writer.getReader();
      writer.close();
      // we don't wrap with AssertingIndexSearcher in order to have the original scorer in setScorer.
      searcher = newSearcher(reader, true, false);
      searcher.setQueryCache(null); // to assert on scorer impl
      
      // set a similarity that does not normalize our boost away
      searcher.setSimilarity(new DefaultSimilarity() {
        @Override
        public float queryNorm(float sumOfSquaredWeights) {
          return 1.0f;
        }
      });
      
      final BoostQuery csq1 = new BoostQuery(new ConstantScoreQuery(new TermQuery(new Term ("field", "term"))), 2f);
      final BoostQuery csq2 = new BoostQuery(new ConstantScoreQuery(csq1), 5f);
      
      final BooleanQuery.Builder bq = new BooleanQuery.Builder();
      bq.add(csq1, BooleanClause.Occur.SHOULD);
      bq.add(csq2, BooleanClause.Occur.SHOULD);
      
      final BoostQuery csqbq = new BoostQuery(new ConstantScoreQuery(bq.build()), 17f);
      
      checkHits(searcher, csq1, csq1.getBoost(), TermScorer.class);
      checkHits(searcher, csq2, csq2.getBoost(), TermScorer.class);
      
      // for the combined BQ, the scorer should always be BooleanScorer's BucketScorer, because our scorer supports out-of order collection!
      final Class<FakeScorer> bucketScorerClass = FakeScorer.class;
      checkHits(searcher, csqbq, csqbq.getBoost(), bucketScorerClass);
    } finally {
      IOUtils.close(reader, directory);
    }
  }

  // a filter for which other queries don't have special rewrite rules
  private static class FilterWrapper extends Filter {

    private final Filter in;
    
    FilterWrapper(Filter in) {
      this.in = in;
    }
    
    @Override
    public DocIdSet getDocIdSet(LeafReaderContext context, Bits acceptDocs) throws IOException {
      return in.getDocIdSet(context, acceptDocs);
    }

    @Override
    public String toString(String field) {
      return in.toString(field);
    }
    
    @Override
    public boolean equals(Object obj) {
      if (super.equals(obj) == false) {
        return false;
      }
      return in.equals(((FilterWrapper) obj).in);
    }

    @Override
    public int hashCode() {
      return 31 * super.hashCode() + in.hashCode();
    }
  }

  public void testConstantScoreQueryAndFilter() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);
    Document doc = new Document();
    doc.add(newStringField("field", "a", Field.Store.NO));
    w.addDocument(doc);
    doc = new Document();
    doc.add(newStringField("field", "b", Field.Store.NO));
    w.addDocument(doc);
    IndexReader r = w.getReader();
    w.close();

    Filter filterB = new FilterWrapper(new QueryWrapperFilter(new TermQuery(new Term("field", "b"))));
    Query query = new ConstantScoreQuery(filterB);

    IndexSearcher s = newSearcher(r);
    assertEquals(1, s.search(new FilteredQuery(query, filterB), 1).totalHits); // Query for field:b, Filter field:b

    Filter filterA = new FilterWrapper(new QueryWrapperFilter(new TermQuery(new Term("field", "a"))));
    query = new ConstantScoreQuery(filterA);

    assertEquals(0, s.search(new FilteredQuery(query, filterB), 1).totalHits); // Query field:b, Filter field:a

    r.close();
    d.close();
  }

  // LUCENE-5307
  // don't reuse the scorer of filters since they have been created with bulkScorer=false
  public void testQueryWrapperFilter() throws IOException {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);
    Document doc = new Document();
    doc.add(newStringField("field", "a", Field.Store.NO));
    w.addDocument(doc);
    IndexReader r = w.getReader();
    w.close();

    final Query wrapped = AssertingQuery.wrap(random(), new TermQuery(new Term("field", "a")));
    Filter filter = new QueryWrapperFilter(wrapped);
    IndexSearcher s = newSearcher(r);
    assert s instanceof AssertingIndexSearcher;
    // this used to fail
    s.search(new ConstantScoreQuery(filter), new TotalHitCountCollector());
    
    // check the rewrite
    Query rewritten = filter;
    for (Query q = rewritten.rewrite(r); q != rewritten; q = rewritten.rewrite(r)) {
      rewritten = q;
    }
    assertEquals(new BoostQuery(new ConstantScoreQuery(wrapped), 0), rewritten);
    
    r.close();
    d.close();
  }

  public void testPropagatesApproximations() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    Field f = newTextField("field", "a b", Field.Store.NO);
    doc.add(f);
    w.addDocument(doc);
    w.commit();

    DirectoryReader reader = w.getReader();
    final IndexSearcher searcher = newSearcher(reader);
    searcher.setQueryCache(null); // to still have approximations

    PhraseQuery pq = new PhraseQuery("field", "a", "b");

    ConstantScoreQuery q = new ConstantScoreQuery(pq);

    final Weight weight = searcher.createNormalizedWeight(q, true);
    final Scorer scorer = weight.scorer(searcher.getIndexReader().leaves().get(0));
    assertNotNull(scorer.twoPhaseIterator());

    reader.close();
    w.close();
    dir.close();
  }

  public void testExtractTerms() throws Exception {
    final IndexSearcher searcher = newSearcher(new MultiReader());
    final TermQuery termQuery = new TermQuery(new Term("foo", "bar"));
    final ConstantScoreQuery csq = new ConstantScoreQuery(termQuery);

    final Set<Term> scoringTerms = new HashSet<>();
    searcher.createNormalizedWeight(csq, true).extractTerms(scoringTerms);
    assertEquals(Collections.emptySet(), scoringTerms);

    final Set<Term> matchingTerms = new HashSet<>();
    searcher.createNormalizedWeight(csq, false).extractTerms(matchingTerms);
    assertEquals(Collections.singleton(new Term("foo", "bar")), matchingTerms);
  }
}
