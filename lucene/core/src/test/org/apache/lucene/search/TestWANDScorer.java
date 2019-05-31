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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestWANDScorer extends LuceneTestCase {

  public void testScalingFactor() {
    doTestScalingFactor(1);
    doTestScalingFactor(2);
    doTestScalingFactor(Math.nextDown(1f));
    doTestScalingFactor(Math.nextUp(1f));
    doTestScalingFactor(Float.MIN_VALUE);
    doTestScalingFactor(Math.nextUp(Float.MIN_VALUE));
    doTestScalingFactor(Float.MAX_VALUE);
    doTestScalingFactor(Math.nextDown(Float.MAX_VALUE));
    assertEquals(WANDScorer.scalingFactor(Float.MIN_VALUE) - 1, WANDScorer.scalingFactor(0));
    assertEquals(WANDScorer.scalingFactor(Float.MAX_VALUE) + 1, WANDScorer.scalingFactor(Float.POSITIVE_INFINITY));
  }

  private void doTestScalingFactor(float f) {
    int scalingFactor = WANDScorer.scalingFactor(f);
    float scaled = Math.scalb(f, scalingFactor);
    assertTrue(""+scaled, scaled > 1 << 15);
    assertTrue(""+scaled, scaled <= 1 << 16);
  }

  private Query maybeWrap(Query query) {
    if (random().nextBoolean()) {
      query = new BlockScoreQueryWrapper(query, TestUtil.nextInt(random(), 2, 8));
      query = new AssertingQuery(random(), query);
    }
    return query;
  }

  public void testBasics() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig().setMergePolicy(newLogMergePolicy()));
    for (String[] values : Arrays.asList(
        new String[]{ "A", "B" },       // 0
        new String[]{ "A" },            // 1
        new String[]{ },                // 2
        new String[]{ "A", "B", "C" },  // 3
        new String[]{ "B" },            // 4
        new String[]{ "B", "C" }        // 5
        )) {
      Document doc = new Document();
      for (String value : values) {
        doc.add(new StringField("foo", value, Store.NO));
      }
      w.addDocument(doc);
    }
    w.forceMerge(1);
    w.close();

    IndexReader reader = DirectoryReader.open(dir);
    IndexSearcher searcher = newSearcher(reader);

    Query query = new BooleanQuery.Builder()
        .add(new BoostQuery(new ConstantScoreQuery(new TermQuery(new Term("foo", "A"))), 2), Occur.SHOULD)
        .add(new ConstantScoreQuery(new TermQuery(new Term("foo", "B"))), Occur.SHOULD)
        .add(new BoostQuery(new ConstantScoreQuery(new TermQuery(new Term("foo", "C"))), 3), Occur.SHOULD)
        .build();

    Scorer scorer = searcher
        .createWeight(searcher.rewrite(query), ScoreMode.TOP_SCORES, 1)
        .scorer(searcher.getIndexReader().leaves().get(0));

    assertEquals(0, scorer.iterator().nextDoc());
    assertEquals(2 + 1, scorer.score(), 0);

    assertEquals(1, scorer.iterator().nextDoc());
    assertEquals(2, scorer.score(), 0);

    assertEquals(3, scorer.iterator().nextDoc());
    assertEquals(2 + 1 + 3, scorer.score(), 0);

    assertEquals(4, scorer.iterator().nextDoc());
    assertEquals(1, scorer.score(), 0);

    assertEquals(5, scorer.iterator().nextDoc());
    assertEquals(1 + 3, scorer.score(), 0);

    assertEquals(DocIdSetIterator.NO_MORE_DOCS, scorer.iterator().nextDoc());

    scorer = searcher
        .createWeight(searcher.rewrite(query), ScoreMode.TOP_SCORES, 1)
        .scorer(searcher.getIndexReader().leaves().get(0));
    scorer.setMinCompetitiveScore(4);

    assertEquals(3, scorer.iterator().nextDoc());
    assertEquals(2 + 1 + 3, scorer.score(), 0);

    assertEquals(5, scorer.iterator().nextDoc());
    assertEquals(1 + 3, scorer.score(), 0);

    assertEquals(DocIdSetIterator.NO_MORE_DOCS, scorer.iterator().nextDoc());

    scorer = searcher
        .createWeight(searcher.rewrite(query), ScoreMode.TOP_SCORES, 1)
        .scorer(searcher.getIndexReader().leaves().get(0));

    assertEquals(0, scorer.iterator().nextDoc());
    assertEquals(2 + 1, scorer.score(), 0);

    scorer.setMinCompetitiveScore(10);

    assertEquals(DocIdSetIterator.NO_MORE_DOCS, scorer.iterator().nextDoc());

    //  test a filtered disjunction
    query = new BooleanQuery.Builder()
        .add(
            new BooleanQuery.Builder()
            .add(new BoostQuery(new ConstantScoreQuery(new TermQuery(new Term("foo", "A"))), 2), Occur.SHOULD)
            .add(new ConstantScoreQuery(new TermQuery(new Term("foo", "B"))), Occur.SHOULD)
            .build(), Occur.MUST)
        .add(new TermQuery(new Term("foo", "C")), Occur.FILTER)
        .build();

    scorer = searcher
        .createWeight(searcher.rewrite(query), ScoreMode.TOP_SCORES, 1)
        .scorer(searcher.getIndexReader().leaves().get(0));

    assertEquals(3, scorer.iterator().nextDoc());
    assertEquals(2 + 1, scorer.score(), 0);

    assertEquals(5, scorer.iterator().nextDoc());
    assertEquals(1, scorer.score(), 0);

    assertEquals(DocIdSetIterator.NO_MORE_DOCS, scorer.iterator().nextDoc());

    scorer = searcher
        .createWeight(searcher.rewrite(query), ScoreMode.TOP_SCORES, 1)
        .scorer(searcher.getIndexReader().leaves().get(0));

    scorer.setMinCompetitiveScore(2);

    assertEquals(3, scorer.iterator().nextDoc());
    assertEquals(2 + 1, scorer.score(), 0);

    assertEquals(DocIdSetIterator.NO_MORE_DOCS, scorer.iterator().nextDoc());

    // Now test a filtered disjunction with a MUST_NOT
    query = new BooleanQuery.Builder()
        .add(new BoostQuery(new ConstantScoreQuery(new TermQuery(new Term("foo", "A"))), 2), Occur.SHOULD)
        .add(new ConstantScoreQuery(new TermQuery(new Term("foo", "B"))), Occur.SHOULD)
        .add(new TermQuery(new Term("foo", "C")), Occur.MUST_NOT)
        .build();

    scorer = searcher
        .createWeight(searcher.rewrite(query), ScoreMode.TOP_SCORES, 1)
        .scorer(searcher.getIndexReader().leaves().get(0));

    assertEquals(0, scorer.iterator().nextDoc());
    assertEquals(2 + 1, scorer.score(), 0);

    assertEquals(1, scorer.iterator().nextDoc());
    assertEquals(2, scorer.score(), 0);

    assertEquals(4, scorer.iterator().nextDoc());
    assertEquals(1, scorer.score(), 0);

    assertEquals(DocIdSetIterator.NO_MORE_DOCS, scorer.iterator().nextDoc());

    scorer = searcher
        .createWeight(searcher.rewrite(query), ScoreMode.TOP_SCORES, 1)
        .scorer(searcher.getIndexReader().leaves().get(0));

    scorer.setMinCompetitiveScore(3);

    assertEquals(0, scorer.iterator().nextDoc());
    assertEquals(2 + 1, scorer.score(), 0);

    assertEquals(DocIdSetIterator.NO_MORE_DOCS, scorer.iterator().nextDoc());

    reader.close();
    dir.close();
  }

  public void testRandom() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    int numDocs = atLeast(1000);
    for (int i = 0; i < numDocs; ++i) {
      Document doc = new Document();
      int numValues = random().nextInt(1 << random().nextInt(5));
      int start = random().nextInt(10);
      for (int j = 0; j < numValues; ++j) {
        doc.add(new StringField("foo", Integer.toString(start + j), Store.NO));
      }
      w.addDocument(doc);
    }
    IndexReader reader = DirectoryReader.open(w);
    w.close();
    IndexSearcher searcher = newSearcher(reader);

    for (int iter = 0; iter < 100; ++iter) {
      int start = random().nextInt(10);
      int numClauses = random().nextInt(1 << random().nextInt(5));
      BooleanQuery.Builder builder = new BooleanQuery.Builder();
      for (int i = 0; i < numClauses; ++i) {
        builder.add(maybeWrap(new TermQuery(new Term("foo", Integer.toString(start + i)))), Occur.SHOULD);
      }
      Query query = builder.build();

      CheckHits.checkTopScores(random(), query, searcher);

      int filterTerm = random().nextInt(30);
      Query filteredQuery = new BooleanQuery.Builder()
          .add(query, Occur.MUST)
          .add(new TermQuery(new Term("foo", Integer.toString(filterTerm))), Occur.FILTER)
          .build();

      CheckHits.checkTopScores(random(), filteredQuery, searcher);
    }
    reader.close();
    dir.close();
  }

  public void testRandomWithInfiniteMaxScore() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    int numDocs = atLeast(1000);
    for (int i = 0; i < numDocs; ++i) {
      Document doc = new Document();
      int numValues = random().nextInt(1 << random().nextInt(5));
      int start = random().nextInt(10);
      for (int j = 0; j < numValues; ++j) {
        doc.add(new StringField("foo", Integer.toString(start + j), Store.NO));
      }
      w.addDocument(doc);
    }
    IndexReader reader = DirectoryReader.open(w);
    w.close();
    IndexSearcher searcher = newSearcher(reader);

    for (int iter = 0; iter < 100; ++iter) {
      int start = random().nextInt(10);
      int numClauses = random().nextInt(1 << random().nextInt(5));
      BooleanQuery.Builder builder = new BooleanQuery.Builder();
      for (int i = 0; i < numClauses; ++i) {
        Query query = new TermQuery(new Term("foo", Integer.toString(start + i)));
        if (random().nextBoolean()) {
          query = new InfiniteMaxScoreWrapperQuery(query, numDocs / TestUtil.nextInt(random(), 1, 100));
        }
        builder.add(query, Occur.SHOULD);
      }
      Query query = builder.build();

      CheckHits.checkTopScores(random(), query, searcher);

      int filterTerm = random().nextInt(30);
      Query filteredQuery = new BooleanQuery.Builder()
          .add(query, Occur.MUST)
          .add(new TermQuery(new Term("foo", Integer.toString(filterTerm))), Occur.FILTER)
          .build();

      CheckHits.checkTopScores(random(), filteredQuery, searcher);
    }
    reader.close();
    dir.close();
  }

  private static class InfiniteMaxScoreWrapperScorer extends FilterScorer {

    private final int maxRange;
    private int lastShallowTarget = -1;

    InfiniteMaxScoreWrapperScorer(Scorer scorer, int maxRange) {
      super(scorer);
      this.maxRange = maxRange;
    }

    @Override
    public int advanceShallow(int target) throws IOException {
      lastShallowTarget = target;
      return in.advanceShallow(target);
    }

    @Override
    public float getMaxScore(int upTo) throws IOException {
      if (upTo - Math.max(docID(), lastShallowTarget) >= maxRange) {
        return Float.POSITIVE_INFINITY;
      }
      return in.getMaxScore(upTo);
    }

  }

  private static class InfiniteMaxScoreWrapperQuery extends Query {

    private final Query query;
    private final int maxRange;
    
    /**
     * If asked for the maximum score over a range of doc IDs that is greater
     * than or equal to maxRange, this query will return a maximum score of +Infty
     */
    InfiniteMaxScoreWrapperQuery(Query query, int maxRange) {
      this.query = query;
      this.maxRange = maxRange;
    }
    
    @Override
    public String toString(String field) {
      return query.toString(field);
    }

    @Override
    public boolean equals(Object obj) {
      return sameClassAs(obj) && query.equals(((InfiniteMaxScoreWrapperQuery) obj).query);
    }

    @Override
    public int hashCode() {
      return 31 * classHash() + query.hashCode();
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
      Query rewritten = query.rewrite(reader);
      if (rewritten != query) {
        return new InfiniteMaxScoreWrapperQuery(rewritten, maxRange);
      }
      return super.rewrite(reader);
    }

    @Override
    public void visit(QueryVisitor visitor) {

    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
      return new FilterWeight(query.createWeight(searcher, scoreMode, boost)) {
        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
          Scorer scorer = super.scorer(context);
          if (scorer == null) {
            return null;
          } else {
            return new InfiniteMaxScoreWrapperScorer(scorer, maxRange);
          }
        }

        @Override
        public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
          ScorerSupplier supplier = super.scorerSupplier(context);
          if (supplier == null) {
            return null;
          } else {
            return new ScorerSupplier() {
              
              @Override
              public Scorer get(long leadCost) throws IOException {
                return new InfiniteMaxScoreWrapperScorer(supplier.get(leadCost), maxRange);
              }
              
              @Override
              public long cost() {
                return supplier.cost();
              }
            };
          }
        }
      };
    }
  }

}
