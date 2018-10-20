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

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.TermFrequencyAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

public class TestReqOptSumScorer extends LuceneTestCase {

  public void testBasics() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, newIndexWriterConfig().setMergePolicy(
        // retain doc id order
        newLogMergePolicy(random().nextBoolean())));
    Document doc = new Document();
    doc.add(new StringField("f", "foo", Store.NO));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new StringField("f", "foo", Store.NO));
    doc.add(new StringField("f", "bar", Store.NO));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new StringField("f", "foo", Store.NO));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new StringField("f", "bar", Store.NO));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new StringField("f", "foo", Store.NO));
    doc.add(new StringField("f", "bar", Store.NO));
    w.addDocument(doc);
    w.forceMerge(1);

    IndexReader reader = w.getReader();
    w.close();
    IndexSearcher searcher = newSearcher(reader);
    Query query = new BooleanQuery.Builder()
        .add(new ConstantScoreQuery(new TermQuery(new Term("f", "foo"))), Occur.MUST)
        .add(new ConstantScoreQuery(new TermQuery(new Term("f", "bar"))), Occur.SHOULD)
        .build();
    Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.TOP_SCORES, 1);
    LeafReaderContext context = searcher.getIndexReader().leaves().get(0);

    Scorer scorer = weight.scorer(context);
    assertEquals(0, scorer.iterator().nextDoc());
    assertEquals(1, scorer.iterator().nextDoc());
    assertEquals(2, scorer.iterator().nextDoc());
    assertEquals(4, scorer.iterator().nextDoc());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, scorer.iterator().nextDoc());

    scorer = weight.scorer(context);
    scorer.setMinCompetitiveScore(Math.nextDown(1f));
    assertEquals(0, scorer.iterator().nextDoc());
    assertEquals(1, scorer.iterator().nextDoc());
    assertEquals(2, scorer.iterator().nextDoc());
    assertEquals(4, scorer.iterator().nextDoc());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, scorer.iterator().nextDoc());

    scorer = weight.scorer(context);
    scorer.setMinCompetitiveScore(Math.nextUp(1f));
    assertEquals(1, scorer.iterator().nextDoc());
    assertEquals(4, scorer.iterator().nextDoc());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, scorer.iterator().nextDoc());

    scorer = weight.scorer(context);
    assertEquals(0, scorer.iterator().nextDoc());
    scorer.setMinCompetitiveScore(Math.nextUp(1f));
    assertEquals(1, scorer.iterator().nextDoc());
    assertEquals(4, scorer.iterator().nextDoc());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, scorer.iterator().nextDoc());

    reader.close();
    dir.close();
  }

  public void testMaxBlock() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig().setMergePolicy(newLogMergePolicy()));
    FieldType ft = new FieldType();
    ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
    ft.setTokenized(true);
    ft.freeze();

    for (int i = 0; i < 1024; i++) {
      // create documents with an increasing number of As and one B
      Document doc = new Document();
      doc.add(new Field("foo", new TermFreqTokenStream("a", i+1), ft));
      if (random().nextFloat() < 0.5f) {
        doc.add(new Field("foo", new TermFreqTokenStream("b", 1), ft));
      }
      w.addDocument(doc);
    }
    w.forceMerge(1);
    w.close();
    IndexReader reader = DirectoryReader.open(dir);
    IndexSearcher searcher = newSearcher(reader);
    searcher.setSimilarity(new TestSimilarity.SimpleSimilarity());
    // freq == score
    // searcher.setSimilarity(new TestSimilarity.SimpleSimilarity());
    final Query reqQ = new TermQuery(new Term("foo", "a"));
    final Query optQ = new TermQuery(new Term("foo", "b"));
    final Query boolQ = new BooleanQuery.Builder()
        .add(reqQ, Occur.MUST)
        .add(optQ, Occur.SHOULD)
        .build();
    Scorer actual = reqOptScorer(searcher, reqQ, optQ, true);
    Scorer expected = searcher
        .createWeight(boolQ, ScoreMode.COMPLETE, 1)
        .scorer(searcher.getIndexReader().leaves().get(0));
    actual.setMinCompetitiveScore(Math.nextUp(1));
    // Checks that all blocks are fully visited
    for (int i = 0; i < 1024; i++) {
      assertEquals(i, actual.iterator().nextDoc());
      assertEquals(i, expected.iterator().nextDoc());
      assertEquals(actual.score(),expected.score(), 0);
    }
    reader.close();
    dir.close();
  }

  public void testMaxScoreSegment() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig().setMergePolicy(newLogMergePolicy()));
    for (String[] values : Arrays.asList(
        new String[]{ "A" },            // 0
        new String[]{ "A" },            // 1
        new String[]{ },                // 2
        new String[]{ "A", "B" },       // 3
        new String[]{ "A" },            // 4
        new String[]{ "B" },            // 5
        new String[]{ "A", "B" },       // 6
        new String[]{ "B" }             // 7
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
    final Query reqQ = new ConstantScoreQuery(new TermQuery(new Term("foo", "A")));
    final Query optQ = new ConstantScoreQuery(new TermQuery(new Term("foo", "B")));
    Scorer scorer = reqOptScorer(searcher, reqQ, optQ, false);
    assertEquals(0, scorer.iterator().nextDoc());
    assertEquals(1, scorer.score(), 0);
    assertEquals(1, scorer.iterator().nextDoc());
    assertEquals(1, scorer.score(), 0);
    assertEquals(3, scorer.iterator().nextDoc());
    assertEquals(2, scorer.score(), 0);
    assertEquals(4, scorer.iterator().nextDoc());
    assertEquals(1, scorer.score(), 0);
    assertEquals(6, scorer.iterator().nextDoc());
    assertEquals(2, scorer.score(), 0);
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, scorer.iterator().nextDoc());

    scorer = reqOptScorer(searcher, reqQ, optQ, false);
    scorer.setMinCompetitiveScore(Math.nextDown(1f));
    assertEquals(0, scorer.iterator().nextDoc());
    assertEquals(1, scorer.score(), 0);
    assertEquals(1, scorer.iterator().nextDoc());
    assertEquals(1, scorer.score(), 0);
    assertEquals(3, scorer.iterator().nextDoc());
    assertEquals(2, scorer.score(), 0);
    assertEquals(4, scorer.iterator().nextDoc());
    assertEquals(1, scorer.score(), 0);
    assertEquals(6, scorer.iterator().nextDoc());
    assertEquals(2, scorer.score(), 0);
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, scorer.iterator().nextDoc());

    scorer = reqOptScorer(searcher, reqQ, optQ, false);
    scorer.setMinCompetitiveScore(Math.nextUp(1f));
    assertEquals(3, scorer.iterator().nextDoc());
    assertEquals(2, scorer.score(), 0);
    assertEquals(6, scorer.iterator().nextDoc());
    assertEquals(2, scorer.score(), 0);
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, scorer.iterator().nextDoc());

    scorer = reqOptScorer(searcher, reqQ, optQ, true);
    scorer.setMinCompetitiveScore(Math.nextUp(2f));
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, scorer.iterator().nextDoc());

    reader.close();
    dir.close();
  }

  public void testRandomFrequentOpt() throws IOException {
    doTestRandom(0.5);
  }

  public void testRandomRareOpt() throws IOException {
    doTestRandom(0.05);
  }

  private void doTestRandom(double optFreq) throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, newIndexWriterConfig());
    int numDocs = atLeast(1000);
    for (int i = 0; i < numDocs; ++i) {
      int numAs = random().nextBoolean() ? 0 : 1 + random().nextInt(5);
      int numBs = random().nextDouble() < optFreq ? 0 : 1 + random().nextInt(5);
      Document doc = new Document();
      for (int j = 0; j < numAs; ++j) {
        doc.add(new StringField("f", "A", Store.NO));
      }
      for (int j = 0; j < numBs; ++j) {
        doc.add(new StringField("f", "B", Store.NO));
      }
      if (random().nextBoolean()) {
        doc.add(new StringField("f", "C", Store.NO));
      }
      w.addDocument(doc);
    }
    IndexReader r = w.getReader();
    w.close();
    IndexSearcher searcher = newSearcher(r);

    Query mustTerm = new TermQuery(new Term("f", "A"));
    Query shouldTerm = new TermQuery(new Term("f", "B"));
    Query query = new BooleanQuery.Builder()
        .add(mustTerm, Occur.MUST)
        .add(shouldTerm, Occur.SHOULD)
        .build();

    TopScoreDocCollector coll = TopScoreDocCollector.create(10, null, Integer.MAX_VALUE);
    searcher.search(query, coll);
    ScoreDoc[] expected = coll.topDocs().scoreDocs;

    // Also test a filtered query, since it does not compute the score on all
    // matches.
    query = new BooleanQuery.Builder()
        .add(query, Occur.MUST)
        .add(new TermQuery(new Term("f", "C")), Occur.FILTER)
        .build();

    coll = TopScoreDocCollector.create(10, null, Integer.MAX_VALUE);
    searcher.search(query, coll);
    ScoreDoc[] expectedFiltered = coll.topDocs().scoreDocs;

    CheckHits.checkTopScores(random(), query, searcher);

    {
      Query q = new BooleanQuery.Builder()
          .add(new RandomApproximationQuery(mustTerm, random()), Occur.MUST)
          .add(shouldTerm, Occur.SHOULD)
          .build();

      coll = TopScoreDocCollector.create(10, null, 1);
      searcher.search(q, coll);
      ScoreDoc[] actual = coll.topDocs().scoreDocs;
      CheckHits.checkEqual(query, expected, actual);

      q = new BooleanQuery.Builder()
          .add(mustTerm, Occur.MUST)
          .add(new RandomApproximationQuery(shouldTerm, random()), Occur.SHOULD)
          .build();
      coll = TopScoreDocCollector.create(10, null, 1);
      searcher.search(q, coll);
      actual = coll.topDocs().scoreDocs;
      CheckHits.checkEqual(q, expected, actual);

      q = new BooleanQuery.Builder()
          .add(new RandomApproximationQuery(mustTerm, random()), Occur.MUST)
          .add(new RandomApproximationQuery(shouldTerm, random()), Occur.SHOULD)
          .build();
      coll = TopScoreDocCollector.create(10, null, 1);
      searcher.search(q, coll);
      actual = coll.topDocs().scoreDocs;
      CheckHits.checkEqual(q, expected, actual);
    }

    {
      Query nestedQ = new BooleanQuery.Builder()
          .add(query, Occur.MUST)
          .add(new TermQuery(new Term("f", "C")), Occur.FILTER)
          .build();
      CheckHits.checkTopScores(random(), nestedQ, searcher);

      query = new BooleanQuery.Builder()
          .add(query, Occur.MUST)
          .add(new RandomApproximationQuery(new TermQuery(new Term("f", "C")), random()), Occur.FILTER)
          .build();

      coll = TopScoreDocCollector.create(10, null, 1);
      searcher.search(nestedQ, coll);
      ScoreDoc[] actualFiltered = coll.topDocs().scoreDocs;
      CheckHits.checkEqual(nestedQ, expectedFiltered, actualFiltered);
    }

    {
      query = new BooleanQuery.Builder()
          .add(query, Occur.MUST)
          .add(new TermQuery(new Term("f", "C")), Occur.SHOULD)
          .build();

      CheckHits.checkTopScores(random(), query, searcher);

      query = new BooleanQuery.Builder()
          .add(new TermQuery(new Term("f", "C")), Occur.MUST)
          .add(query, Occur.SHOULD)
          .build();

      CheckHits.checkTopScores(random(), query, searcher);
    }

    r.close();
    dir.close();
  }

  private static Scorer reqOptScorer(IndexSearcher searcher, Query reqQ, Query optQ, boolean withBlockScore) throws IOException {
    Scorer reqScorer = searcher
        .createWeight(reqQ, ScoreMode.TOP_SCORES, 1)
        .scorer(searcher.getIndexReader().leaves().get(0));
    Scorer optScorer = searcher
        .createWeight(optQ, ScoreMode.TOP_SCORES, 1)
        .scorer(searcher.getIndexReader().leaves().get(0));
    if (withBlockScore) {
      return new ReqOptSumScorer(reqScorer, optScorer, ScoreMode.TOP_SCORES);
    } else {
      return new ReqOptSumScorer(reqScorer, optScorer, ScoreMode.TOP_SCORES) {
        @Override
        public float getMaxScore(int upTo) {
          return Float.POSITIVE_INFINITY;
        }
      };
    }
  }

  private static class TermFreqTokenStream extends TokenStream {
    private final String term;
    private final int termFreq;
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final TermFrequencyAttribute termFreqAtt = addAttribute(TermFrequencyAttribute.class);
    private boolean finish;

    public TermFreqTokenStream(String term, int termFreq) {
      this.term = term;
      this.termFreq = termFreq;
    }

    @Override
    public boolean incrementToken() {
      if (finish) {
        return false;
      }

      clearAttributes();

      termAtt.append(term);
      termFreqAtt.setTermFrequency(termFreq);

      finish = true;
      return true;
    }

    @Override
    public void reset() {
      finish = false;
    }
  }
}
