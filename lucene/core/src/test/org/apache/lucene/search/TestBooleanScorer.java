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
import java.util.Arrays;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.Weight.DefaultBulkScorer;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestBooleanScorer extends LuceneTestCase {
  private static final String FIELD = "category";
  
  public void testMethod() throws Exception {
    Directory directory = newDirectory();

    String[] values = new String[] { "1", "2", "3", "4" };

    RandomIndexWriter writer = new RandomIndexWriter(random(), directory);
    for (int i = 0; i < values.length; i++) {
      Document doc = new Document();
      doc.add(newStringField(FIELD, values[i], Field.Store.YES));
      writer.addDocument(doc);
    }
    IndexReader ir = writer.getReader();
    writer.close();

    BooleanQuery.Builder booleanQuery1 = new BooleanQuery.Builder();
    booleanQuery1.add(new TermQuery(new Term(FIELD, "1")), BooleanClause.Occur.SHOULD);
    booleanQuery1.add(new TermQuery(new Term(FIELD, "2")), BooleanClause.Occur.SHOULD);

    BooleanQuery.Builder query = new BooleanQuery.Builder();
    query.add(booleanQuery1.build(), BooleanClause.Occur.MUST);
    query.add(new TermQuery(new Term(FIELD, "9")), BooleanClause.Occur.MUST_NOT);

    IndexSearcher indexSearcher = newSearcher(ir);
    ScoreDoc[] hits = indexSearcher.search(query.build(), 1000).scoreDocs;
    assertEquals("Number of matched documents", 2, hits.length);
    ir.close();
    directory.close();
  }

  /** Throws UOE if Weight.scorer is called */
  private static class CrazyMustUseBulkScorerQuery extends Query {

    @Override
    public String toString(String field) {
      return "MustUseBulkScorerQuery";
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
      return new Weight(CrazyMustUseBulkScorerQuery.this) {
        @Override
        public void extractTerms(Set<Term> terms) {
          throw new UnsupportedOperationException();
        }

        @Override
        public Explanation explain(LeafReaderContext context, int doc) {
          throw new UnsupportedOperationException();
        }

        @Override
        public float getValueForNormalization() {
          return 1.0f;
        }

        @Override
        public void normalize(float norm, float topLevelBoost) {
        }

        @Override
        public Scorer scorer(LeafReaderContext context) {
          throw new UnsupportedOperationException();
        }

        @Override
        public BulkScorer bulkScorer(LeafReaderContext context) {
          return new BulkScorer() {
            @Override
            public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
              assert min == 0;
              collector.setScorer(new FakeScorer());
              collector.collect(0);
              return DocIdSetIterator.NO_MORE_DOCS;
            }
            @Override
            public long cost() {
              return 1;
            }
          };
        }
      };
    }
  }

  /** Make sure BooleanScorer can embed another
   *  BooleanScorer. */
  public void testEmbeddedBooleanScorer() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(newTextField("field", "doctors are people who prescribe medicines of which they know little, to cure diseases of which they know less, in human beings of whom they know nothing", Field.Store.NO));
    w.addDocument(doc);
    IndexReader r = w.getReader();
    w.close();

    IndexSearcher s = new IndexSearcher(r);
    BooleanQuery.Builder q1 = new BooleanQuery.Builder();
    q1.add(new TermQuery(new Term("field", "little")), BooleanClause.Occur.SHOULD);
    q1.add(new TermQuery(new Term("field", "diseases")), BooleanClause.Occur.SHOULD);

    BooleanQuery.Builder q2 = new BooleanQuery.Builder();
    q2.add(q1.build(), BooleanClause.Occur.SHOULD);
    q2.add(new CrazyMustUseBulkScorerQuery(), BooleanClause.Occur.SHOULD);

    assertEquals(1, s.search(q2.build(), 10).totalHits);
    r.close();
    dir.close();
  }

  public void testOptimizeTopLevelClauseOrNull() throws IOException {
    // When there is a single non-null scorer, this scorer should be used
    // directly
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new StringField("foo", "bar", Store.NO));
    w.addDocument(doc);
    IndexReader reader = w.getReader();
    IndexSearcher searcher = new IndexSearcher(reader);
    searcher.setQueryCache(null); // so that weights are not wrapped
    final LeafReaderContext ctx = reader.leaves().get(0);
    Query query = new BooleanQuery.Builder()
      .add(new TermQuery(new Term("foo", "bar")), Occur.SHOULD) // existing term
      .add(new TermQuery(new Term("foo", "baz")), Occur.SHOULD) // missing term
      .build();

    // no scores -> term scorer
    Weight weight = searcher.createNormalizedWeight(query, false);
    BulkScorer scorer = ((BooleanWeight) weight).booleanScorer(ctx);
    assertTrue(scorer instanceof DefaultBulkScorer); // term scorer

    // disabled coords -> term scorer
    query = new BooleanQuery.Builder()
      .add(new TermQuery(new Term("foo", "bar")), Occur.SHOULD) // existing term
      .add(new TermQuery(new Term("foo", "baz")), Occur.SHOULD) // missing term
      .setDisableCoord(true)
      .build();
    weight = searcher.createNormalizedWeight(query, true);
    scorer = ((BooleanWeight) weight).booleanScorer(ctx);
    assertTrue(scorer instanceof DefaultBulkScorer); // term scorer

    // enabled coords -> BoostedBulkScorer
    searcher.setSimilarity(new DefaultSimilarity());
    query = new BooleanQuery.Builder()
      .add(new TermQuery(new Term("foo", "bar")), Occur.SHOULD) // existing term
      .add(new TermQuery(new Term("foo", "baz")), Occur.SHOULD) // missing term
      .build();
    weight = searcher.createNormalizedWeight(query, true);
    scorer = ((BooleanWeight) weight).booleanScorer(ctx);
    assertTrue(scorer instanceof BooleanTopLevelScorers.BoostedBulkScorer);

    w.close();
    reader.close();
    dir.close();
  }

  public void testOptimizeProhibitedClauses() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new StringField("foo", "bar", Store.NO));
    doc.add(new StringField("foo", "baz", Store.NO));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new StringField("foo", "baz", Store.NO));
    w.addDocument(doc);
    w.forceMerge(1);
    IndexReader reader = w.getReader();
    IndexSearcher searcher = new IndexSearcher(reader);
    searcher.setQueryCache(null); // so that weights are not wrapped
    final LeafReaderContext ctx = reader.leaves().get(0);

    Query query = new BooleanQuery.Builder()
      .add(new TermQuery(new Term("foo", "baz")), Occur.SHOULD)
      .add(new TermQuery(new Term("foo", "bar")), Occur.MUST_NOT)
      .build();
    Weight weight = searcher.createNormalizedWeight(query, true);
    BulkScorer scorer = ((BooleanWeight) weight).booleanScorer(ctx);
    assertTrue(scorer instanceof ReqExclBulkScorer);

    query = new BooleanQuery.Builder()
        .add(new TermQuery(new Term("foo", "baz")), Occur.SHOULD)
        .add(new MatchAllDocsQuery(), Occur.SHOULD)
        .add(new TermQuery(new Term("foo", "bar")), Occur.MUST_NOT)
        .build();
    weight = searcher.createNormalizedWeight(query, true);
    scorer = ((BooleanWeight) weight).booleanScorer(ctx);
    assertTrue(scorer instanceof ReqExclBulkScorer);

    query = new BooleanQuery.Builder()
        .add(new TermQuery(new Term("foo", "baz")), Occur.MUST)
        .add(new TermQuery(new Term("foo", "bar")), Occur.MUST_NOT)
        .build();
    weight = searcher.createNormalizedWeight(query, true);
    scorer = ((BooleanWeight) weight).booleanScorer(ctx);
    assertTrue(scorer instanceof ReqExclBulkScorer);

    query = new BooleanQuery.Builder()
        .add(new TermQuery(new Term("foo", "baz")), Occur.FILTER)
        .add(new TermQuery(new Term("foo", "bar")), Occur.MUST_NOT)
        .build();
    weight = searcher.createNormalizedWeight(query, true);
    scorer = ((BooleanWeight) weight).booleanScorer(ctx);
    assertTrue(scorer instanceof ReqExclBulkScorer);

    w.close();
    reader.close();
    dir.close();
  }

  public void testSparseClauseOptimization() throws IOException {
    // When some windows have only one scorer that can match, the scorer will
    // directly call the collector in this window
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document emptyDoc = new Document();
    final int numDocs = atLeast(10);
    for (int d = 0; d < numDocs; ++d) {
      for (int i = random().nextInt(5000); i >= 0; --i) {
        w.addDocument(emptyDoc);
      }
      Document doc = new Document();
      for (String value : Arrays.asList("foo", "bar", "baz")) {
        if (random().nextBoolean()) {
          doc.add(new StringField("field", value, Store.NO));
        }
      }
    }
    for (int i = TestUtil.nextInt(random(), 3000, 5000); i >= 0; --i) {
      w.addDocument(emptyDoc);
    }
    if (random().nextBoolean()) {
      w.forceMerge(1);
    }
    IndexReader reader = w.getReader();
    IndexSearcher searcher = newSearcher(reader);

    Query query = new BooleanQuery.Builder()
      .add(new BoostQuery(new TermQuery(new Term("field", "foo")), 3), Occur.SHOULD)
      .add(new BoostQuery(new TermQuery(new Term("field", "bar")), 3), Occur.SHOULD)
      .add(new BoostQuery(new TermQuery(new Term("field", "baz")), 3), Occur.SHOULD)
      .setDisableCoord(random().nextBoolean())
      .build();

    // duel BS1 vs. BS2
    QueryUtils.check(random(), query, searcher);

    reader.close();
    w.close();
    dir.close();
  }
}
