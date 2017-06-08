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
package org.apache.lucene.search.spans;


import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.lucene.analysis.*;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.search.CheckHits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.English;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import junit.framework.Assert;


public class TestSpansTreeQuery extends LuceneTestCase {
  static IndexSearcher searcherClassic;
  static IndexSearcher searcherBM25;
  static IndexReader reader;
  static Directory directory;

  static final int MAX_TEST_DOC = 33;

  @BeforeClass
  public static void beforeClass() throws Exception {
    directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory,
        newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.SIMPLE, true))
            .setMaxBufferedDocs(TestUtil.nextInt(random(), MAX_TEST_DOC, MAX_TEST_DOC + 100))
            .setMergePolicy(newLogMergePolicy()));
    for (int i = 0; i < MAX_TEST_DOC; i++) {
      Document doc = new Document();
      String text;
      if (i < (MAX_TEST_DOC-1)) {
        text = English.intToEnglish(i);
        if ((i % 5) == 0) { // add some multiple occurrences of the same term(s)
          text += " " + text;
        }
      } else { // last doc, for testing distances > 1, and repeating occurrrences of wb
        text = "az a b c d e wa wb wb wc az";
      }
      doc.add(newTextField("field", text, Field.Store.YES));
      writer.addDocument(doc);
    }
    reader = writer.getReader();
    searcherClassic = new IndexSearcher(reader);
    searcherClassic.setSimilarity(new ClassicSimilarity());
    searcherBM25 = new IndexSearcher(reader);
    searcherBM25.setSimilarity(new BM25Similarity());
    writer.close();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    reader.close();
    directory.close();
    searcherClassic = null;
    searcherBM25 = null;
    reader = null;
    directory = null;
  }

  final String FIELD_NAME = "field";

  Term lcnTerm(String term) {
    return new Term(FIELD_NAME, term);
  }

  Term[] lcnTerms(String... terms) {
    Term[] lcnTrms = new Term[terms.length];
    for (int i = 0; i < terms.length; i++) {
      lcnTrms[i] = lcnTerm(terms[i]);
    }
    return lcnTrms;
  }


  TermQuery termQuery(String term) {
    return new TermQuery(lcnTerm(term));
  }

  SpanTermQuery spanTermQuery(String term) {
    return new SpanTermQuery(lcnTerm(term));
  }

  ScoreDoc[] search(IndexSearcher searcher, Query query) throws IOException {
    TopScoreDocCollector collector = TopScoreDocCollector.create(MAX_TEST_DOC);
    searcher.search(query, collector);
    return collector.topDocs().scoreDocs;
  }

  int[] docsFromHits(ScoreDoc[] hits) throws Exception {
    int[] docs = new int[hits.length];
    for (int i = 0; i < hits.length; i++) {
      docs[i] = hits[i].doc;
    }
    return docs;
  }

  void checkEqualDocOrder(Query qexp, Query qact) throws Exception {
    ScoreDoc[] expHits = search(searcherBM25, qexp);
    ScoreDoc[] actHits = search(searcherBM25, qact);
    assertEquals("same nr of hits", expHits.length, actHits.length);
    for (int i = 0; i < expHits.length; i++) {
      assertEquals("same doc at rank " + i, expHits[i].doc, actHits[i].doc);
    }
  }

  void showQueryResults(String message, Query q, ScoreDoc[] hits) {
    System.out.println(message + " results from query " + q);
    for (ScoreDoc hit : hits) {
      System.out.println("doc=" + hit.doc + ", score=" + hit.score);
    }
  }

  void checkEqualScores(Query qexp, Query qact) throws Exception {
    ScoreDoc[] expHits = search(searcherBM25, qexp);
    int[] expDocs = docsFromHits(expHits);
    //showQueryResults("expected BM25", qexp, expHits);

    ScoreDoc[] actHits = search(searcherBM25, qact);
    //showQueryResults("actual BM25", qact, actHits);

    CheckHits.checkHitsQuery(qact, actHits, expHits, expDocs);

    expHits = search(searcherClassic, qexp);
    expDocs = docsFromHits(expHits);
    //showQueryResults("expected Classic", qexp, expHits);

    actHits = search(searcherClassic, qact);
    //showQueryResults("actual Classic", qexp, expHits);
    CheckHits.checkHitsQuery(qact, actHits, expHits, expDocs);
  }

  void checkSpanTerm(String term) throws Exception {
    TermQuery tq = termQuery(term);
    SpanTermQuery stq = spanTermQuery(term);

    checkEqualScores(tq, stq); // test SpanScorer

    checkEqualScores(tq, SpansTreeQuery.wrap(stq)); // test SpanTreeScorer
  }

  public void testSpanTermZero() throws Exception {
    checkSpanTerm("zero");
  }

  public void testSpanTermSeven() throws Exception {
    checkSpanTerm("seven");
  }

  public void testSpanTermFive() throws Exception {
    checkSpanTerm("five");
  }

  SpanTermQuery[] spanTermQueries(String... terms) {
    SpanTermQuery[] stqs = new SpanTermQuery[terms.length];
    for (int i = 0; i < terms.length; i++) {
      stqs[i] = spanTermQuery(terms[i]);
    }
    return stqs;
  }

  SpanOrQuery spanOrQuery(String... terms) {
    return new SpanOrQuery(spanTermQueries(terms));
  }

  SpanOrQuery spanOrNearQuery(int maxDistance, String... terms) {
    return new SpanOrQuery(maxDistance, spanTermQueries(terms));
  }

  BooleanQuery booleanOrQuery(String... terms) {
    BooleanQuery.Builder bqb = new BooleanQuery.Builder();
    for (int i = 0; i < terms.length; i++) {
      bqb.add(termQuery(terms[i]), BooleanClause.Occur.SHOULD);
    }
    return bqb.build();
  }

  void checkSpanOrTerms(String... terms)  throws Exception {
    assertTrue(terms.length >= 1);
    Query boq = SpansTreeQuery.wrap(booleanOrQuery(terms));
    assertTrue(boq instanceof BooleanQuery); // test SpansTreeQuery.wrap
    assertTrue(((BooleanQuery)boq).clauses().get(terms.length-1).getQuery() instanceof TermQuery); // test SpansTreeQuery.wrap
    SpanOrQuery soq = spanOrQuery(terms);
    Query sptroq = SpansTreeQuery.wrap(soq);
    //checkEqualDocOrder(boq, sptroq);
    //checkEqualScores(boq, soq); // test SpanScorer for OR over terms, fails
    checkEqualScores(boq, sptroq); // test SpanTreeScorer for OR over terms
  }

  public void testSpanOrOneTerm1() throws Exception {
    checkSpanOrTerms("zero");
  }

  public void testSpanOrOneTerm2() throws Exception {
    checkSpanOrTerms("thirty");
  }

  public void testSpanOrTwoTerms() throws Exception {
    checkSpanOrTerms("zero", "thirty");
  }

  public void testSpanOrTwoCooccurringTerms() throws Exception {
    checkSpanOrTerms("twenty", "five");
  }

  public void testSpanOrMoreTerms() throws Exception {
    checkSpanOrTerms(
      "zero",
      "one",
      "two",
      "three",
      "four",
      "five",
      "six",
      "seven",
      "twenty",
      "thirty"
      );
  }

  void checkSameHighestScoringDocAndScore(Query exp, Query act) throws Exception {
    ScoreDoc[] expHits = search(searcherBM25, exp);
    int[] expDocs = docsFromHits(expHits);
    //showQueryResults("checkSameHighestScoringDocAndScore expected BM25", exp, expHits);

    ScoreDoc[] actHits = search(searcherBM25, act);
    //showQueryResults("checkSameHighestScoringDocAndScore actual BM25", act, actHits);

    final float scoreTolerance = 1.0e-6f; // from CheckHits.java

    assertEquals("highest scoring docs the same", expHits[0].doc, actHits[0].doc);
    assertTrue("equal scores", Math.abs(expHits[0].score - actHits[0].score) <= scoreTolerance);
  }

  void checkSameHighestScoringDocAndScoreRange(Query exp, Query act, float maxFac, float minFac) throws Exception {
    ScoreDoc[] expHits = search(searcherBM25, exp);
    int[] expDocs = docsFromHits(expHits);
    //showQueryResults("checkSameHighestScoringDocAndScore expected BM25", exp, expHits);

    ScoreDoc[] actHits = search(searcherBM25, act);
    //showQueryResults("checkSameHighestScoringDocAndScore actual BM25", act, actHits);

    final float scoreTolerance = 1.0e-6f; // from CheckHits.java

    assertTrue("at least one expected hit", expHits.length >= 1);
    assertTrue("at least one actual hit", actHits.length >= 1);

    int actDoc = 0; // order may differ when top scores are equal
    while ((actDoc < actHits.length)
            && (actHits[actDoc].doc != expHits[0].doc)
            && (Math.abs(actHits[0].score - actHits[actDoc+1].score) < 1e-6f) ) {
      actDoc++;
    }
    assertEquals("highest scoring docs the same", expHits[0].doc, actHits[actDoc].doc);
    if ( (expHits[0].score * maxFac < actHits[actDoc].score)
      || (expHits[0].score * minFac > actHits[actDoc].score))
    {
      Assert.fail("For highest scoring doc"
                    + ", expHits[0].doc=" + expHits[0].doc
                    + ", score not in expected range: " + (expHits[0].score * minFac)
                    + " <= " + actHits[actDoc].score
                    + " <= " + (expHits[0].score * maxFac));
    }
 }

  public void testSpanAdjacentAllTermsInDocUnordered() throws Exception {
    /* On "twenty five twenty five"
     * unordered "twenty five" should score the same as "twenty" OR "five"
     */
    String t1 = "twenty";
    String t2 = "five";
    SpanNearQuery snq = SpanNearQuery.newUnorderedNearQuery(FIELD_NAME)
      .addClause(spanTermQuery(t1))
      .addClause(spanTermQuery(t2))
      .setSlop(0)
      .build();
    BooleanQuery boq = booleanOrQuery(t1, t2);

    checkSameHighestScoringDocAndScore(boq, SpansTreeQuery.wrap(snq));
  }

  public void testSpanAdjacentAllTermsInDocOrdered1() throws Exception {
    /* On "twenty five twenty five"
     * ordered "twenty five" should score the same as "twenty" OR "five"
     */
    String t1 = "twenty";
    String t2 = "five";
    SpanNearQuery snq = SpanNearQuery.newOrderedNearQuery(FIELD_NAME)
      .addClause(spanTermQuery(t1))
      .addClause(spanTermQuery(t2))
      .setSlop(0)
      .build();
    BooleanQuery boq = booleanOrQuery(t1, t2);

    checkSameHighestScoringDocAndScore(boq, SpansTreeQuery.wrap(snq));
  }

  public void testSpanAdjacentAllTermsInDocOrdered2() throws Exception {
    /* On "twenty five twenty five"
     * ordered "five twenty" should score less, but more than half of "twenty" OR "five"
     */
    String t1 = "five";
    String t2 = "twenty";
    SpanNearQuery snq = SpanNearQuery.newOrderedNearQuery(FIELD_NAME)
      .addClause(spanTermQuery(t1))
      .addClause(spanTermQuery(t2))
      .setSlop(0)
      .build();
    BooleanQuery.Builder bqb = new BooleanQuery.Builder();
    bqb.add(termQuery(t1), BooleanClause.Occur.SHOULD);
    bqb.add(termQuery(t2), BooleanClause.Occur.SHOULD);
    BooleanQuery boq = bqb.build();

    checkSameHighestScoringDocAndScoreRange(boq, SpansTreeQuery.wrap(snq), 0.7f, 0.5f);
  }

  public void testSpanMoreDistanceLessScore() throws Exception {
    String t1 = "a";
    String t2 = "b";
    String t3 = "c";
    SpanNearQuery snq2 = SpanNearQuery.newOrderedNearQuery(FIELD_NAME)
      .addClause(spanTermQuery(t1))
      .addClause(spanTermQuery(t2))
      .setSlop(2)
      .build();
    SpanNearQuery snq3 = SpanNearQuery.newOrderedNearQuery(FIELD_NAME)
      .addClause(spanTermQuery(t1))
      .addClause(spanTermQuery(t3))
      .setSlop(2)
      .build();

    checkSameHighestScoringDocAndScoreRange(SpansTreeQuery.wrap(snq2), SpansTreeQuery.wrap(snq3),
                                            0.50f, 0.49f);
  }

  Query sptrSimpleUnorderedNested(String t1a, String t1b, String t2, int slop) {
    SpanNearQuery snq1 = SpanNearQuery.newUnorderedNearQuery(FIELD_NAME)
      .addClause(spanTermQuery(t1a))
      .addClause(spanTermQuery(t1b))
      .setSlop(slop)
      .build();

    SpanNearQuery snqn = SpanNearQuery.newUnorderedNearQuery(FIELD_NAME)
      .addClause(snq1)
      .addClause(spanTermQuery(t2))
      .setSlop(slop)
      .build();

     return SpansTreeQuery.wrap(snqn);
  }

  public void testSpanNestedMoreDistanceLessScore() throws Exception {
    String t1 = "a";
    String t2 = "b";
    String t3 = "c";
    String t4 = "d";
    String t5 = "e";
    Query sptrq1 = sptrSimpleUnorderedNested(t1, t2, t4, 2);
    Query sptrq2 = sptrSimpleUnorderedNested(t1, t3, t5, 2);

    checkSameHighestScoringDocAndScoreRange(sptrq1, sptrq2, 0.7f, 0.6f);
  }

  public void testNonMatchingPresentTermScore() throws Exception {
    String t1 = "a";
    String t2 = "b";
    String t3 = "c";

    SpanOrQuery soq = new SpanOrQuery(spanTermQuery(t1), spanTermQuery(t2));

    SpanNearQuery snq1 = SpanNearQuery.newUnorderedNearQuery(FIELD_NAME)
      .addClause(soq)
      .addClause(spanTermQuery(t3))
      .setSlop(0)
      .setNonMatchSlop(3)
      .build(); // t1 is present but does not match.

    SpanNearQuery snq2 = SpanNearQuery.newUnorderedNearQuery(FIELD_NAME)
      .addClause(soq)
      .addClause(spanTermQuery(t3))
      .setSlop(0)
      .setNonMatchSlop(4) // t1 scores lower than in snq1
      .build(); // t1 is present but does not match.

    SpansTreeQuery sptrnq1 = new SpansTreeQuery(snq1);
    SpansTreeQuery sptrnq2 = new SpansTreeQuery(snq2);

    checkSameHighestScoringDocAndScoreRange(sptrnq1, sptrnq2, 0.98f, 0.9f);
  }

  public void testSpanNot() throws Exception {
    /* On "twenty five twenty five"
     * "twenty" not preceeded by "five", and followed by "five",
     *  should score less, but more than half of "twenty five"
     */
    String t1 = "five";
    String t2 = "twenty";
    SpanNotQuery sntq = new SpanNotQuery( spanTermQuery(t2), spanTermQuery(t1), 1, 0);

    SpanNearQuery snrq1 = SpanNearQuery.newOrderedNearQuery(FIELD_NAME)
      .addClause(sntq)
      .addClause(spanTermQuery(t1))
      .setSlop(0)
      .build();

    SpanNearQuery snrq2 = SpanNearQuery.newOrderedNearQuery(FIELD_NAME)
      .addClause(spanTermQuery(t2))
      .addClause(spanTermQuery(t1))
      .setSlop(0)
      .build();

    Query sptrnrq1 = SpansTreeQuery.wrap(snrq1);
    Query sptrnrq2 = SpansTreeQuery.wrap(snrq2);

    checkSameHighestScoringDocAndScoreRange(sptrnrq2, sptrnrq1, 0.8f, 0.5f);
  }

  public void testSpanBoost() throws Exception {
    String term = "zero";
    SpanTermQuery stq = spanTermQuery(term);
    SpanBoostQuery sbq = new SpanBoostQuery(stq, 1.1f);

    checkSameHighestScoringDocAndScoreRange(sbq, stq, 0.92f, 0.90f);
    checkSameHighestScoringDocAndScoreRange(SpansTreeQuery.wrap(sbq), stq, 0.92f, 0.90f);
  }

  public void testSpanOrNearZeroDistance() throws Exception {
    String t1 = "a";
    String t2 = "b";
    BooleanQuery boq = booleanOrQuery(t1, t2);
    SpanOrQuery sonq = spanOrNearQuery(0, t1, t2);
    checkEqualScores(boq, SpansTreeQuery.wrap(sonq));
  }

  public void testSpanOrNearMoreDistanceLessScore() throws Exception {
    String t1 = "a";
    String t2 = "b";
    String t3 = "c";
    Query stq1 = SpansTreeQuery.wrap(spanOrNearQuery(4, t1, t2));
    Query stq2 = SpansTreeQuery.wrap(spanOrNearQuery(4, t1, t3));
    checkSameHighestScoringDocAndScoreRange(stq1, stq2, 0.5f, 0.4f);
  }

  public void testSpanOrNearThreeSubqueries() throws Exception {
    String t1 = "a";
    String t2 = "b";
    String t3 = "c";
    BooleanQuery boq = booleanOrQuery(t1, t2, t3);
    SpanOrQuery sonq = spanOrNearQuery(0, t3, t2, t1);
    checkEqualScores(boq, SpansTreeQuery.wrap(sonq));
  }

  public void testSpanOrNearNonMatchingSubQuery() throws Exception {
    String t1 = "a";
    String t2 = "b";
    String t3 = "c";
    String t5 = "e";
    SpanOrQuery sonq1 = spanOrNearQuery(1, t3, t2, t1);
    SpanOrQuery sonq2 = spanOrNearQuery(1, t5, t2, t1);
    checkSameHighestScoringDocAndScoreRange(
                      SpansTreeQuery.wrap(sonq1),
                      SpansTreeQuery.wrap(sonq2),
                      0.9f, 0.8f);
  }

  public void testSpanOrNearSinglePresentSubquery() throws Exception {
    String t1 = "a";
    String t2 = "h";
    SpanQuery q1 = spanTermQuery(t1);
    SpanOrQuery q2 = spanOrNearQuery(1, t2, t1);
    checkSameHighestScoringDocAndScoreRange(
                      SpansTreeQuery.wrap(q1),
                      SpansTreeQuery.wrap(q2),
                      0.51f, 0.49f);
  }

  public void testSpanOrNearRepeatingOccurrences1() throws Exception {
    String t1 = "wa";
    String t2 = "wb";
    BooleanQuery boq = booleanOrQuery(t1, t2);
    SpanOrQuery sonq = spanOrNearQuery(3, t2, t1);
    checkSameHighestScoringDocAndScoreRange(
                      boq,
                      SpansTreeQuery.wrap(sonq),
                      0.9f, 0.8f);
  }

  public void testSpanOrNearRepeatingOccurrences2() throws Exception {
    String t1 = "wb";
    String t2 = "wc";
    BooleanQuery boq = booleanOrQuery(t1, t2);
    SpanOrQuery sonq = spanOrNearQuery(3, t2, t1);
    checkSameHighestScoringDocAndScoreRange(
                      boq,
                      SpansTreeQuery.wrap(sonq),
                      0.9f, 0.8f);
  }

  public void testIncreasingScoreExtraMatchLowSlopFactor() throws Exception {
    String t1 = "az"; // near and far from a
    String t2 = "a";
    SpanNearQuery snq1 = SpanNearQuery.newUnorderedNearQuery(FIELD_NAME)
      .addClause(spanTermQuery(t1))
      .addClause(spanTermQuery(t2))
      .setSlop(0) // does not match far
      .setNonMatchSlop(20) // for consistent non match scoring
      .build();
    SpanNearQuery snq2 = SpanNearQuery.newUnorderedNearQuery(FIELD_NAME)
      .addClause(spanTermQuery(t1))
      .addClause(spanTermQuery(t2))
      .setSlop(8) // also matches far
      .setNonMatchSlop(20) // for consistent non match scoring
      .build();
    checkSameHighestScoringDocAndScoreRange(
                      SpansTreeQuery.wrap(snq2),
                      SpansTreeQuery.wrap(snq1),
                      0.98f, 0.9f);
  }

  SynonymQuery synonymQuery(String... terms) {
    return new SynonymQuery(lcnTerms(terms));
  }

  SpanSynonymQuery spanSynonymQuery(String... terms) {
    return new SpanSynonymQuery(lcnTerms(terms));
  }

  void sortByDoc(ScoreDoc[] scoreDocs) {
    Arrays.sort(scoreDocs, new Comparator<ScoreDoc>() {
        @Override
        public int compare(ScoreDoc sd1, ScoreDoc sd2) {
          return sd1.doc - sd2.doc;
        }
    });
  }

  void checkScoresInRange(Query qexp, Query qact, float maxFac, float minFac) throws Exception {
    ScoreDoc[] expHits = search(searcherBM25, qexp);
    //showQueryResults("checkScoresInRange expected", qexp, expHits);

    ScoreDoc[] actHits = search(searcherBM25, qact);
    //showQueryResults("checkScoresInRange actual", qact, actHits);

    if (expHits.length != actHits.length) {
      Assert.fail("Unequal lengths: expHits="+expHits.length+",actHits="+actHits.length);
    }

    sortByDoc(expHits);
    sortByDoc(actHits);
    for (int i = 0; i < expHits.length; i++) {
      if (expHits[i].doc != actHits[i].doc)
      {
        Assert.fail("At index " + i
                      + ": expHits[i].doc=" + expHits[i].doc
                      + " != actHits[i].doc=" + actHits[i].doc);
      }

      if ( (expHits[i].score * maxFac < actHits[i].score)
        || (expHits[i].score * minFac > actHits[i].score))
      {
        Assert.fail("At index " + i
                      + ", expHits[i].doc=" + expHits[i].doc
                      + ", score not in expected range: " + (expHits[i].score * minFac)
                      + " <= " + actHits[i].score
                      + " <= " + (expHits[i].score * maxFac));
      }
    }
  }

  void checkSynTerms(String... terms)  throws Exception {
    assertTrue(terms.length >= 1);
    SpanOrQuery soq = spanOrQuery(terms);
    SpanSynonymQuery ssq = spanSynonymQuery(terms);
    checkScoresInRange(SpansTreeQuery.wrap(soq), SpansTreeQuery.wrap(ssq), 1.0f, 0.425f);

    SynonymQuery sq = synonymQuery(terms);
    checkEqualScores(SpansTreeQuery.wrap(sq), SpansTreeQuery.wrap(ssq));
  }

  public void testSynTwoTermsNoDocOverlap() throws Exception {
    checkSynTerms("zero", "one");
  }

  public void testSynTwoTermsDocOverlap() throws Exception {
    checkSynTerms("twenty", "one");
  }

  public void testSynNearOrNear() throws Exception {
    // twenty occurs 10 times
    // thirty occurs 2 times
    SpanSynonymQuery ssq2030 = spanSynonymQuery("twenty", "thirty");
    SpanOrQuery soq2030 = spanOrQuery("twenty", "thirty");
    SpanTermQuery stq1 = spanTermQuery("one");

    SpanNearQuery synNear = SpanNearQuery.newOrderedNearQuery(FIELD_NAME)
      .addClause(ssq2030)
      .addClause(stq1)
      .setSlop(0)
      .build();
    SpanNearQuery orNear = SpanNearQuery.newOrderedNearQuery(FIELD_NAME)
      .addClause(soq2030)
      .addClause(stq1)
      .setSlop(0)
      .build();

    checkSameHighestScoringDocAndScoreRange(
                      SpansTreeQuery.wrap(orNear),
                      SpansTreeQuery.wrap(synNear),
                      0.80f, 0.70f);
  }

  public void testRecurringTerms() throws Exception {
    String t1 = "a";
    String t2 = "b";
    String t3 = "c";

    SpanNearQuery snq1 = SpanNearQuery.newUnorderedNearQuery(FIELD_NAME)
      .addClause(spanTermQuery(t1))
      .addClause(spanTermQuery(t2))
      .setSlop(0)
      .build();

    SpanNearQuery snq2 = SpanNearQuery.newUnorderedNearQuery(FIELD_NAME)
      .addClause(spanTermQuery(t2))
      .addClause(spanTermQuery(t3))
      .setSlop(0)
      .build();

    SpanNearQuery snq3 = SpanNearQuery.newUnorderedNearQuery(FIELD_NAME)
      .addClause(spanTermQuery(t1))
      .addClause(spanTermQuery(t3))
      .setSlop(1)
      .build();

    SpanOrQuery soq =  new SpanOrQuery(snq1, snq2, snq3); // should score as bag of words

    BooleanQuery boq = booleanOrQuery(t1, t2, t3); // bag of words

    checkSameHighestScoringDocAndScore(boq, SpansTreeQuery.wrap(soq));
  }

}
