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
import java.util.Locale;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

import junit.framework.Assert;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.LuceneTestCase;

import static junit.framework.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Utility class for asserting expected hits in tests.
 */
public class CheckHits {
    
  /**
   * Tests that all documents up to maxDoc which are *not* in the
   * expected result set, have an explanation which indicates that 
   * the document does not match
   */
  public static void checkNoMatchExplanations(Query q, String defaultFieldName,
                                              IndexSearcher searcher, int[] results)
    throws IOException {

    String d = q.toString(defaultFieldName);
    Set<Integer> ignore = new TreeSet<>();
    for (int i = 0; i < results.length; i++) {
      ignore.add(Integer.valueOf(results[i]));
    }
    
    int maxDoc = searcher.getIndexReader().maxDoc();
    for (int doc = 0; doc < maxDoc; doc++) {
      if (ignore.contains(Integer.valueOf(doc))) continue;

      Explanation exp = searcher.explain(q, doc);
      assertNotNull("Explanation of [["+d+"]] for #"+doc+" is null",
                             exp);
      Assert.assertFalse("Explanation of [["+d+"]] for #"+doc+
                         " doesn't indicate non-match: " + exp.toString(),
                         exp.isMatch());
    }
    
  }
  
  /**
   * Tests that a query matches the an expected set of documents using a
   * HitCollector.
   *
   * <p>
   * Note that when using the HitCollector API, documents will be collected
   * if they "match" regardless of what their score is.
   * </p>
   * @param query the query to test
   * @param searcher the searcher to test the query against
   * @param defaultFieldName used for displaying the query in assertion messages
   * @param results a list of documentIds that must match the query
   * @see #checkHits
   */
  public static void checkHitCollector(Random random, Query query, String defaultFieldName,
                                       IndexSearcher searcher, int[] results)
    throws IOException {

    QueryUtils.check(random,query,searcher);
    
    Set<Integer> correct = new TreeSet<>();
    for (int i = 0; i < results.length; i++) {
      correct.add(Integer.valueOf(results[i]));
    }
    final Set<Integer> actual = new TreeSet<>();
    final Collector c = new SetCollector(actual);

    searcher.search(query, c);
    Assert.assertEquals("Simple: " + query.toString(defaultFieldName), 
                        correct, actual);

    for (int i = -1; i < 2; i++) {
      actual.clear();
      IndexSearcher s = QueryUtils.wrapUnderlyingReader
        (random, searcher, i);
      s.search(query, c);
      Assert.assertEquals("Wrap Reader " + i + ": " +
                          query.toString(defaultFieldName),
                          correct, actual);
    }
  }

  /**
   * Just collects document ids into a set.
   */
  public static class SetCollector extends SimpleCollector {
    final Set<Integer> bag;
    public SetCollector(Set<Integer> bag) {
      this.bag = bag;
    }
    private int base = 0;
    @Override
    public void setScorer(Scorable scorer) throws IOException {}
    @Override
    public void collect(int doc) {
      bag.add(Integer.valueOf(doc + base));
    }
    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
      base = context.docBase;
    }
    
    @Override
    public ScoreMode scoreMode() {
      return ScoreMode.COMPLETE_NO_SCORES;
    }
  }

  /**
   * Tests that a query matches the an expected set of documents using Hits.
   *
   * <p>
   * Note that when using the Hits API, documents will only be returned
   * if they have a positive normalized score.
   * </p>
   * @param query the query to test
   * @param searcher the searcher to test the query against
   * @param defaultFieldName used for displaing the query in assertion messages
   * @param results a list of documentIds that must match the query
   * @see #checkHitCollector
   */
  public static void checkHits(
        Random random,
        Query query,
        String defaultFieldName,
        IndexSearcher searcher,
        int[] results)
          throws IOException {

    ScoreDoc[] hits = searcher.search(query, 1000).scoreDocs;

    Set<Integer> correct = new TreeSet<>();
    for (int i = 0; i < results.length; i++) {
      correct.add(Integer.valueOf(results[i]));
    }

    Set<Integer> actual = new TreeSet<>();
    for (int i = 0; i < hits.length; i++) {
      actual.add(Integer.valueOf(hits[i].doc));
    }

    Assert.assertEquals(query.toString(defaultFieldName), correct, actual);

    QueryUtils.check(random, query,searcher, LuceneTestCase.rarely(random));
  }

  /** Tests that a Hits has an expected order of documents */
  public static void checkDocIds(String mes, int[] results, ScoreDoc[] hits) {
    Assert.assertEquals(mes + " nr of hits", hits.length, results.length);
    for (int i = 0; i < results.length; i++) {
      Assert.assertEquals(mes + " doc nrs for hit " + i, results[i], hits[i].doc);
    }
  }

  /** Tests that two queries have an expected order of documents,
   * and that the two queries have the same score values.
   */
  public static void checkHitsQuery(
        Query query,
        ScoreDoc[] hits1,
        ScoreDoc[] hits2,
        int[] results) {

    checkDocIds("hits1", results, hits1);
    checkDocIds("hits2", results, hits2);
    checkEqual(query, hits1, hits2);
  }

  public static void checkEqual(Query query, ScoreDoc[] hits1, ScoreDoc[] hits2) {
     final float scoreTolerance = 1.0e-6f;
     if (hits1.length != hits2.length) {
       Assert.fail("Unequal lengths: hits1="+hits1.length+",hits2="+hits2.length);
     }
    for (int i = 0; i < hits1.length; i++) {
      if (hits1[i].doc != hits2[i].doc) {
        Assert.fail("Hit " + i + " docnumbers don't match\n"
                + hits2str(hits1, hits2,0,0)
                + "for query:" + query.toString());
      }

      if ((hits1[i].doc != hits2[i].doc)
          || Math.abs(hits1[i].score -  hits2[i].score) > scoreTolerance)
      {
        Assert.fail("Hit " + i + ", doc nrs " + hits1[i].doc + " and " + hits2[i].doc
                      + "\nunequal       : " + hits1[i].score
                      + "\n           and: " + hits2[i].score
                      + "\nfor query:" + query.toString());
      }
    }
  }

  public static String hits2str(ScoreDoc[] hits1, ScoreDoc[] hits2, int start, int end) {
    StringBuilder sb = new StringBuilder();
    int len1=hits1==null ? 0 : hits1.length;
    int len2=hits2==null ? 0 : hits2.length;
    if (end<=0) {
      end = Math.max(len1,len2);
    }

      sb.append("Hits length1=").append(len1).append("\tlength2=").append(len2);

    sb.append('\n');
    for (int i=start; i<end; i++) {
        sb.append("hit=").append(i).append(':');
      if (i<len1) {
          sb.append(" doc").append(hits1[i].doc).append('=').append(hits1[i].score).append(" shardIndex=").append(hits1[i].shardIndex);
      } else {
        sb.append("               ");
      }
      sb.append(",\t");
      if (i<len2) {
        sb.append(" doc").append(hits2[i].doc).append('=').append(hits2[i].score).append(" shardIndex=").append(hits2[i].shardIndex);
      }

      sb.append('\n');
    }
    return sb.toString();
  }


  public static String topdocsString(TopDocs docs, int start, int end) {
    StringBuilder sb = new StringBuilder();
      sb.append("TopDocs totalHits=").append(docs.totalHits).append(" top=").append(docs.scoreDocs.length).append('\n');
    if (end<=0) end=docs.scoreDocs.length;
    else end=Math.min(end,docs.scoreDocs.length);
    for (int i=start; i<end; i++) {
      sb.append('\t');
      sb.append(i);
      sb.append(") doc=");
      sb.append(docs.scoreDocs[i].doc);
      sb.append("\tscore=");
      sb.append(docs.scoreDocs[i].score);
      sb.append('\n');
    }
    return sb.toString();
  }

  /**
   * Asserts that the explanation value for every document matching a
   * query corresponds with the true score. 
   *
   * @see ExplanationAsserter
   * @see #checkExplanations(Query, String, IndexSearcher, boolean) for a
   * "deep" testing of the explanation details.
   *   
   * @param query the query to test
   * @param searcher the searcher to test the query against
   * @param defaultFieldName used for displaing the query in assertion messages
   */
  public static void checkExplanations(Query query,
                                       String defaultFieldName,
                                       IndexSearcher searcher) throws IOException {
    checkExplanations(query, defaultFieldName, searcher, false);
  }

  /**
   * Asserts that the explanation value for every document matching a
   * query corresponds with the true score.  Optionally does "deep" 
   * testing of the explanation details.
   *
   * @see ExplanationAsserter
   * @param query the query to test
   * @param searcher the searcher to test the query against
   * @param defaultFieldName used for displaing the query in assertion messages
   * @param deep indicates whether a deep comparison of sub-Explanation details should be executed
   */
  public static void checkExplanations(Query query,
                                       String defaultFieldName,
                                       IndexSearcher searcher, 
                                       boolean deep) throws IOException {

    searcher.search(query,
                    new ExplanationAsserter
                    (query, defaultFieldName, searcher, deep));

  }

  /**
   * Asserts that the result of calling {@link Weight#matches(LeafReaderContext, int)}
   * for every document matching a query returns a non-null {@link Matches}
   * @param query     the query to test
   * @param searcher  the search to test against
   */
  public static void checkMatches(Query query, IndexSearcher searcher) throws IOException {
    searcher.search(query, new MatchesAsserter(query, searcher));
  }
  
  private static final Pattern COMPUTED_FROM_PATTERN = Pattern.compile(".*, computed as .* from:");

  /** 
   * Assert that an explanation has the expected score, and optionally that its
   * sub-details max/sum/factor match to that score.
   *
   * @param q String representation of the query for assertion messages
   * @param doc Document ID for assertion messages
   * @param score Real score value of doc with query q
   * @param deep indicates whether a deep comparison of sub-Explanation details should be executed
   * @param expl The Explanation to match against score
   */
  public static void verifyExplanation(String q, 
                                       int doc, 
                                       float score,
                                       boolean deep,
                                       Explanation expl) {
    float value = expl.getValue().floatValue();
    // TODO: clean this up if we use junit 5 (the assert message is costly)
    try {
      Assert.assertEquals(score, value, 0d);
    } catch (Exception e) {
      Assert.fail(q+": score(doc="+doc+")="+score+" != explanationScore="+value+" Explanation: "+expl);
    }

    if (!deep) return;

    Explanation detail[] = expl.getDetails();
    // TODO: can we improve this entire method? it's really geared to work only with TF/IDF
    if (expl.getDescription().endsWith("computed from:")) {
      return; // something more complicated.
    }
    String descr = expl.getDescription().toLowerCase(Locale.ROOT);
    if (descr.startsWith("score based on ") && descr.contains("child docs in range")) {
      Assert.assertTrue("Child doc explanations are missing", detail.length > 0);
    }
    if (detail.length > 0) {
      if (detail.length==1 && COMPUTED_FROM_PATTERN.matcher(descr).matches() == false) {
        // simple containment, unless it's a freq of: (which lets a query explain how the freq is calculated), 
        // just verify contained expl has same score
        if (expl.getDescription().endsWith("with freq of:") == false
            // with dismax, even if there is a single sub explanation, its
            // score might be different if the score is negative
            && (score >= 0 || expl.getDescription().endsWith("times others of:") == false)) {
          verifyExplanation(q,doc,score,deep,detail[0]);
        }
      } else {
        // explanation must either:
        // - end with one of: "product of:", "sum of:", "max of:", or
        // - have "max plus <x> times others" (where <x> is float).
        float x = 0;
        boolean productOf = descr.endsWith("product of:");
        boolean sumOf = descr.endsWith("sum of:");
        boolean maxOf = descr.endsWith("max of:");
        boolean computedOf = descr.indexOf("computed as") > 0 && COMPUTED_FROM_PATTERN.matcher(descr).matches();
        boolean maxTimesOthers = false;
        if (!(productOf || sumOf || maxOf || computedOf)) {
          // maybe 'max plus x times others'
          int k1 = descr.indexOf("max plus ");
          if (k1>=0) {
            k1 += "max plus ".length();
            int k2 = descr.indexOf(" ",k1);
            try {
              x = Float.parseFloat(descr.substring(k1,k2).trim());
              if (descr.substring(k2).trim().equals("times others of:")) {
                maxTimesOthers = true;
              }
            } catch (NumberFormatException e) {
            }
          }
        }
        // TODO: this is a TERRIBLE assertion!!!!
        if (false == (productOf || sumOf || maxOf || computedOf || maxTimesOthers)) {
          Assert.fail(
              q+": multi valued explanation description=\""+descr
              +"\" must be 'max of plus x times others', 'computed as x from:' or end with 'product of'"
              +" or 'sum of:' or 'max of:' - "+expl);
        }
        double sum = 0;
        float product = 1;
        float max = Float.NEGATIVE_INFINITY;
        double maxError = 0;
        for (int i=0; i<detail.length; i++) {
          float dval = detail[i].getValue().floatValue();
          verifyExplanation(q,doc,dval,deep,detail[i]);
          product *= dval;
          sum += dval;
          max = Math.max(max,dval);

          if (sumOf) {
            // "sum of" is used by BooleanQuery. Making it accurate is not
            // easy since ReqOptSumScorer casts some intermediate
            // contributions to the score to a float before doing another sum.
            // So we introduce some (reasonable) leniency.
            // TODO: remove this leniency
            maxError += Math.ulp(dval) * 2;
          }
        }
        float combined;
        if (productOf) {
          combined = product;
        } else if (sumOf) {
          combined = (float) sum;
        } else if (maxOf) {
          combined = max;
        } else if (maxTimesOthers) {
          combined = (float) (max + x * (sum - max));
        } else {
          Assert.assertTrue("should never get here!", computedOf);
          combined = value;
        }
        // TODO: clean this up if we use junit 5 (the assert message is costly)
        try {
          Assert.assertEquals(combined, value, maxError);
        } catch (Exception e) {
          Assert.fail(q+": actual subDetails combined=="+combined+
              " != value="+value+" Explanation: "+expl);
        }
      }
    }
  }

  /**
   * an IndexSearcher that implicitly checks hte explanation of every match
   * whenever it executes a search.
   *
   * @see ExplanationAsserter
   */
  public static class ExplanationAssertingSearcher extends IndexSearcher {
    public ExplanationAssertingSearcher(IndexReader r) {
      super(r);
    }
    protected void checkExplanations(Query q) throws IOException {
      super.search(q,
                   new ExplanationAsserter
                   (q, null, this));
    }
    @Override
    public TopFieldDocs search(Query query,
                               int n,
                               Sort sort) throws IOException {
      
      checkExplanations(query);
      return super.search(query,n,sort);
    }
    @Override
    public void search(Query query, Collector results) throws IOException {
      checkExplanations(query);
      super.search(query, results);
    }
    @Override
    public TopDocs search(Query query, int n) throws IOException {

      checkExplanations(query);
      return super.search(query, n);
    }
  }
    
  /**
   * Asserts that the score explanation for every document matching a
   * query corresponds with the true score.
   *
   * NOTE: this HitCollector should only be used with the Query and Searcher
   * specified at when it is constructed.
   *
   * @see CheckHits#verifyExplanation
   */
  public static class ExplanationAsserter extends SimpleCollector {

    Query q;
    IndexSearcher s;
    String d;
    boolean deep;
    
    Scorable scorer;
    private int base = 0;

    /** Constructs an instance which does shallow tests on the Explanation */
    public ExplanationAsserter(Query q, String defaultFieldName, IndexSearcher s) {
      this(q,defaultFieldName,s,false);
    }      
    public ExplanationAsserter(Query q, String defaultFieldName, IndexSearcher s, boolean deep) {
      this.q=q;
      this.s=s;
      this.d = q.toString(defaultFieldName);
      this.deep=deep;
    }      
    
    @Override
    public void setScorer(Scorable scorer) throws IOException {
      this.scorer = scorer;     
    }
    
    @Override
    public void collect(int doc) throws IOException {
      Explanation exp = null;
      doc = doc + base;
      try {
        exp = s.explain(q, doc);
      } catch (IOException e) {
        throw new RuntimeException
          ("exception in hitcollector of [["+d+"]] for #"+doc, e);
      }
      
      assertNotNull("Explanation of [["+d+"]] for #"+doc+" is null", exp);
      verifyExplanation(d,doc,scorer.score(),deep,exp);
      Assert.assertTrue("Explanation of [["+d+"]] for #"+ doc + 
                        " does not indicate match: " + exp.toString(), 
                        exp.isMatch());
    }
    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
      base = context.docBase;
    }
    
    @Override
    public ScoreMode scoreMode() {
      return ScoreMode.COMPLETE;
    }
  }

  /**
   * Asserts that the {@link Matches} from a query is non-null whenever
   * the document its created for is a hit.
   *
   * Also checks that the previous non-matching document has a {@code null} {@link Matches}
   */
  public static class MatchesAsserter extends SimpleCollector {

    private final Weight weight;
    private LeafReaderContext context;
    int lastCheckedDoc = -1;

    public MatchesAsserter(Query query, IndexSearcher searcher) throws IOException {
      this.weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1);
    }

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
      this.context = context;
      this.lastCheckedDoc = -1;
    }

    @Override
    public void collect(int doc) throws IOException {
      Matches matches = this.weight.matches(context, doc);
      assertNotNull("Unexpected null Matches object in doc" + doc + " for query " + this.weight.getQuery(), matches);
      if (lastCheckedDoc != doc - 1) {
        assertNull("Unexpected non-null Matches object in non-matching doc" + doc + " for query " + this.weight.getQuery(),
            this.weight.matches(context, doc - 1));
      }
      lastCheckedDoc = doc;
    }

    @Override
    public ScoreMode scoreMode() {
      return ScoreMode.COMPLETE_NO_SCORES;
    }
  }

  public static void checkTopScores(Random random, Query query, IndexSearcher searcher) throws IOException {
    // Check it computed the top hits correctly
    doCheckTopScores(query, searcher, 1);
    doCheckTopScores(query, searcher, 10);

    // Now check that the exposed max scores and block boundaries are valid
    doCheckMaxScores(random, query, searcher);
  }

  private static void doCheckTopScores(Query query, IndexSearcher searcher, int numHits) throws IOException {
    TopScoreDocCollector collector1 = TopScoreDocCollector.create(numHits, null, Integer.MAX_VALUE); // COMPLETE
    TopScoreDocCollector collector2 = TopScoreDocCollector.create(numHits, null, 1); // TOP_SCORES
    searcher.search(query, collector1);
    searcher.search(query, collector2);
    checkEqual(query, collector1.topDocs().scoreDocs, collector2.topDocs().scoreDocs);
  }

  private static void doCheckMaxScores(Random random, Query query, IndexSearcher searcher) throws IOException {
    query = searcher.rewrite(query);
    Weight w1 = searcher.createWeight(query, ScoreMode.COMPLETE, 1);
    Weight w2 = searcher.createWeight(query, ScoreMode.TOP_SCORES, 1);

    // Check boundaries and max scores when iterating all matches
    for (LeafReaderContext ctx : searcher.getIndexReader().leaves()) {
      Scorer s1 = w1.scorer(ctx);
      Scorer s2 = w2.scorer(ctx);
      if (s1 == null) {
        Assert.assertTrue(s2 == null || s2.iterator().nextDoc() == DocIdSetIterator.NO_MORE_DOCS);
        continue;
      }
      TwoPhaseIterator twoPhase1 = s1.twoPhaseIterator();
      TwoPhaseIterator twoPhase2 = s2.twoPhaseIterator();
      DocIdSetIterator approx1 = twoPhase1 == null ? s1.iterator() : twoPhase1.approximation;
      DocIdSetIterator approx2 = twoPhase2 == null ? s2.iterator() : twoPhase2.approximation;
      int upTo = -1;
      float maxScore = 0;
      float minScore = 0;
      for (int doc2 = approx2.nextDoc(); ; doc2 = approx2.nextDoc()) {
        int doc1;
        for (doc1 = approx1.nextDoc(); doc1 < doc2; doc1 = approx1.nextDoc()) {
          if (twoPhase1 == null || twoPhase1.matches()) {
            Assert.assertTrue(s1.score() < minScore);
          }
        }
        Assert.assertEquals(doc1, doc2);
        if (doc2 == DocIdSetIterator.NO_MORE_DOCS) {
          break;
        }

        if (doc2 > upTo) {
          upTo = s2.advanceShallow(doc2);
          Assert.assertTrue(upTo >= doc2);
          maxScore = s2.getMaxScore(upTo);
        }

        if (twoPhase2 == null || twoPhase2.matches()) {
          Assert.assertTrue(twoPhase1 == null || twoPhase1.matches());
          float score = s2.score();
          Assert.assertEquals(s1.score(), score);
          Assert.assertTrue(score + " > " + maxScore + " up to " + upTo, score <= maxScore);

          if (score >= minScore && random.nextInt(10) == 0) {
            // On some scorers, changing the min score changes the way that docs are iterated
            minScore = score;
            s2.setMinCompetitiveScore(minScore);
          }
        }
      }
    }

    // Now check advancing
    for (LeafReaderContext ctx : searcher.getIndexReader().leaves()) {
      Scorer s1 = w1.scorer(ctx);
      Scorer s2 = w2.scorer(ctx);
      if (s1 == null) {
        Assert.assertTrue(s2 == null || s2.iterator().nextDoc() == DocIdSetIterator.NO_MORE_DOCS);
        continue;
      }
      TwoPhaseIterator twoPhase1 = s1.twoPhaseIterator();
      TwoPhaseIterator twoPhase2 = s2.twoPhaseIterator();
      DocIdSetIterator approx1 = twoPhase1 == null ? s1.iterator() : twoPhase1.approximation;
      DocIdSetIterator approx2 = twoPhase2 == null ? s2.iterator() : twoPhase2.approximation;

      int upTo = -1;
      float minScore = 0;
      float maxScore = 0;
      while (true) {
        int doc2 = s2.docID();
        boolean advance;
        int target;
        if (random.nextBoolean()) {
          advance = false;
          target = doc2 + 1;
        } else {
          advance = true;
          int delta = Math.min(1 + random.nextInt(512), DocIdSetIterator.NO_MORE_DOCS - doc2);
          target = s2.docID() + delta;
        }

        if (target > upTo && random.nextBoolean()) {
          int delta = Math.min(random.nextInt(512), DocIdSetIterator.NO_MORE_DOCS - target);
          upTo = target + delta;
          int m = s2.advanceShallow(target);
          assertTrue(m >= target);
          maxScore = s2.getMaxScore(upTo);
        }

        if (advance) {
          doc2 = approx2.advance(target);
        } else {
          doc2 = approx2.nextDoc();
        }

        int doc1;
        for (doc1 = approx1.advance(target); doc1 < doc2; doc1 = approx1.nextDoc()) {
          if (twoPhase1 == null || twoPhase1.matches()) {
            Assert.assertTrue(s1.score() < minScore);
          }
        }
        assertEquals(doc1, doc2);

        if (doc2 == DocIdSetIterator.NO_MORE_DOCS) {
          break;
        }

        if (twoPhase2 == null || twoPhase2.matches()) {
          Assert.assertTrue(twoPhase1 == null || twoPhase1.matches());
          float score = s2.score();
          Assert.assertEquals(s1.score(), score);

          if (doc2 > upTo) {
            upTo = s2.advanceShallow(doc2);
            Assert.assertTrue(upTo >= doc2);
            maxScore = s2.getMaxScore(upTo);
          }

          Assert.assertTrue(score <= maxScore);

          if (score >= minScore && random.nextInt(10) == 0) {
            // On some scorers, changing the min score changes the way that docs are iterated
            minScore = score;
            s2.setMinCompetitiveScore(minScore);
          }
        }
      }
    }
  }
}
