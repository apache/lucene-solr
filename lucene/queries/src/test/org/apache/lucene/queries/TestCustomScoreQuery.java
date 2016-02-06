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
package org.apache.lucene.queries;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.function.FunctionQuery;
import org.apache.lucene.queries.function.FunctionTestSetup;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.CheckHits;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryUtils;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.TopDocs;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test CustomScoreQuery search.
 */
public class TestCustomScoreQuery extends FunctionTestSetup {

  @BeforeClass
  public static void beforeClass() throws Exception {
    createIndex(true);
  }

  /**
   * Test that CustomScoreQuery of Type.INT returns the expected scores.
   */
  @Test
  public void testCustomScoreInt() throws Exception {
    doTestCustomScore(INT_VALUESOURCE, 1.0);
    doTestCustomScore(INT_VALUESOURCE, 4.0);
  }

  /**
   * Test that CustomScoreQuery of Type.FLOAT returns the expected scores.
   */
  @Test
  public void testCustomScoreFloat() throws Exception {
    doTestCustomScore(FLOAT_VALUESOURCE, 1.0);
    doTestCustomScore(FLOAT_VALUESOURCE, 6.0);
  }

  // must have static class otherwise serialization tests fail
  private static class CustomAddQuery extends CustomScoreQuery {
    // constructor
    CustomAddQuery(Query q, FunctionQuery qValSrc) {
      super(q, qValSrc);
    }

    /*(non-Javadoc) @see org.apache.lucene.search.function.CustomScoreQuery#name() */
    @Override
    public String name() {
      return "customAdd";
    }
    
    @Override
    protected CustomScoreProvider getCustomScoreProvider(LeafReaderContext context) {
      return new CustomScoreProvider(context) {
        @Override
        public float customScore(int doc, float subQueryScore, float valSrcScore) {
          return subQueryScore + valSrcScore;
        }

        @Override
        public Explanation customExplain(int doc, Explanation subQueryExpl, Explanation valSrcExpl) {
          List<Explanation> subs = new ArrayList<>();
          subs.add(subQueryExpl);
          if (valSrcExpl != null) {
            subs.add(valSrcExpl);
          }
          float valSrcScore = valSrcExpl == null ? 0 : valSrcExpl.getValue();
          return Explanation.match(valSrcScore + subQueryExpl.getValue(), "custom score: sum of:", subs);
        }
      };
    }
  }

  // must have static class otherwise serialization tests fail
  private static class CustomMulAddQuery extends CustomScoreQuery {
    // constructor
    CustomMulAddQuery(Query q, FunctionQuery qValSrc1, FunctionQuery qValSrc2) {
      super(q, qValSrc1, qValSrc2);
    }

    /*(non-Javadoc) @see org.apache.lucene.search.function.CustomScoreQuery#name() */
    @Override
    public String name() {
      return "customMulAdd";
    }

    @Override
    protected CustomScoreProvider getCustomScoreProvider(LeafReaderContext context) {
      return new CustomScoreProvider(context) {
        @Override
        public float customScore(int doc, float subQueryScore, float valSrcScores[]) {
          if (valSrcScores.length == 0) {
            return subQueryScore;
          }
          if (valSrcScores.length == 1) {
            return subQueryScore + valSrcScores[0];
            // confirm that skipping beyond the last doc, on the
            // previous reader, hits NO_MORE_DOCS
          }
          return (subQueryScore + valSrcScores[0]) * valSrcScores[1]; // we know there are two
        }

        @Override
        public Explanation customExplain(int doc, Explanation subQueryExpl, Explanation valSrcExpls[]) {
          if (valSrcExpls.length == 0) {
            return subQueryExpl;
          }
          if (valSrcExpls.length == 1) {
            return Explanation.match(valSrcExpls[0].getValue() + subQueryExpl.getValue(), "CustomMulAdd, sum of:", subQueryExpl, valSrcExpls[0]);
          } else {
            Explanation exp = Explanation.match(valSrcExpls[0].getValue() + subQueryExpl.getValue(), "sum of:", subQueryExpl, valSrcExpls[0]);
            return Explanation.match(valSrcExpls[1].getValue() * exp.getValue(), "custom score: product of:", valSrcExpls[1], exp);
          }
        }
      };
    }
  }

  private final class CustomExternalQuery extends CustomScoreQuery {

    @Override
    protected CustomScoreProvider getCustomScoreProvider(LeafReaderContext context) throws IOException {
      final NumericDocValues values = DocValues.getNumeric(context.reader(), INT_FIELD);
      return new CustomScoreProvider(context) {
        @Override
        public float customScore(int doc, float subScore, float valSrcScore) {
          assertTrue(doc <= context.reader().maxDoc());
          return values.get(doc);
        }
      };
    }

    public CustomExternalQuery(Query q) {
      super(q);
    }
  }

  @Test
  public void testCustomExternalQuery() throws Exception {
    BooleanQuery.Builder q1 = new BooleanQuery.Builder();
    q1.add(new TermQuery(new Term(TEXT_FIELD, "first")), BooleanClause.Occur.SHOULD);
    q1.add(new TermQuery(new Term(TEXT_FIELD, "aid")), BooleanClause.Occur.SHOULD);
    q1.add(new TermQuery(new Term(TEXT_FIELD, "text")), BooleanClause.Occur.SHOULD);
    
    final Query q = new CustomExternalQuery(q1.build());
    log(q);

    IndexReader r = DirectoryReader.open(dir);
    IndexSearcher s = newSearcher(r);
    TopDocs hits = s.search(q, 1000);
    assertEquals(N_DOCS, hits.totalHits);
    for(int i=0;i<N_DOCS;i++) {
      final int doc = hits.scoreDocs[i].doc;
      final float score = hits.scoreDocs[i].score;
      assertEquals("doc=" + doc, (float) 1+(4*doc) % N_DOCS, score, 0.0001);
    }
    r.close();
  }
  
  @Test
  public void testRewrite() throws Exception {
    IndexReader r = DirectoryReader.open(dir);
    final IndexSearcher s = newSearcher(r);

    Query q = new TermQuery(new Term(TEXT_FIELD, "first"));
    CustomScoreQuery original = new CustomScoreQuery(q);
    CustomScoreQuery rewritten = (CustomScoreQuery) original.rewrite(s.getIndexReader());
    assertTrue("rewritten query should be identical, as TermQuery does not rewrite", original == rewritten);
    assertTrue("no hits for query", s.search(rewritten,1).totalHits > 0);
    assertEquals(s.search(q,1).totalHits, s.search(rewritten,1).totalHits);

    q = new TermRangeQuery(TEXT_FIELD, null, null, true, true); // everything
    original = new CustomScoreQuery(q);
    rewritten = (CustomScoreQuery) original.rewrite(s.getIndexReader());
    assertTrue("rewritten query should not be identical, as TermRangeQuery rewrites", original != rewritten);
    assertTrue("no hits for query", s.search(rewritten,1).totalHits > 0);
    assertEquals(s.search(q,1).totalHits, s.search(original,1).totalHits);
    assertEquals(s.search(q,1).totalHits, s.search(rewritten,1).totalHits);
    
    r.close();
  }
  
  // Test that FieldScoreQuery returns docs with expected score.
  private void doTestCustomScore(ValueSource valueSource, double dboost) throws Exception {
    float boost = (float) dboost;
    FunctionQuery functionQuery = new FunctionQuery(valueSource);
    IndexReader r = DirectoryReader.open(dir);
    IndexSearcher s = newSearcher(r);

    // regular (boolean) query.
    BooleanQuery.Builder q1b = new BooleanQuery.Builder();
    q1b.add(new TermQuery(new Term(TEXT_FIELD, "first")), BooleanClause.Occur.SHOULD);
    q1b.add(new TermQuery(new Term(TEXT_FIELD, "aid")), BooleanClause.Occur.SHOULD);
    q1b.add(new TermQuery(new Term(TEXT_FIELD, "text")), BooleanClause.Occur.SHOULD);
    Query q1 = q1b.build();
    log(q1);

    // custom query, that should score the same as q1.
    BooleanQuery.Builder q2CustomNeutralB = new BooleanQuery.Builder();
    q2CustomNeutralB.setDisableCoord(true);
    Query q2CustomNeutralInner = new CustomScoreQuery(q1);
    q2CustomNeutralB.add(new BoostQuery(q2CustomNeutralInner, (float)Math.sqrt(dboost)), BooleanClause.Occur.SHOULD);
    // a little tricky: we split the boost across an outer BQ and CustomScoreQuery
    // this ensures boosting is correct across all these functions (see LUCENE-4935)
    Query q2CustomNeutral = q2CustomNeutralB.build();
    q2CustomNeutral = new BoostQuery(q2CustomNeutral, (float)Math.sqrt(dboost));
    log(q2CustomNeutral);

    // custom query, that should (by default) multiply the scores of q1 by that of the field
    Query q3CustomMul;
    {
      CustomScoreQuery csq = new CustomScoreQuery(q1, functionQuery);
      csq.setStrict(true);
      q3CustomMul = csq;
    }
    q3CustomMul = new BoostQuery(q3CustomMul, boost);
    log(q3CustomMul);

    // custom query, that should add the scores of q1 to that of the field
    Query q4CustomAdd;
    {
      CustomScoreQuery csq = new CustomAddQuery(q1, functionQuery);
      csq.setStrict(true);
      q4CustomAdd = csq;
    }
    q4CustomAdd = new BoostQuery(q4CustomAdd, boost);
    log(q4CustomAdd);

    // custom query, that multiplies and adds the field score to that of q1
    Query q5CustomMulAdd;
    {
      CustomScoreQuery csq = new CustomMulAddQuery(q1, functionQuery, functionQuery);
      csq.setStrict(true);
      q5CustomMulAdd = csq;
    }
    q5CustomMulAdd = new BoostQuery(q5CustomMulAdd, boost);
    log(q5CustomMulAdd);

    // do al the searches 
    TopDocs td1 = s.search(q1, 1000);
    TopDocs td2CustomNeutral = s.search(q2CustomNeutral, 1000);
    TopDocs td3CustomMul = s.search(q3CustomMul, 1000);
    TopDocs td4CustomAdd = s.search(q4CustomAdd, 1000);
    TopDocs td5CustomMulAdd = s.search(q5CustomMulAdd, 1000);

    // put results in map so we can verify the scores although they have changed
    Map<Integer,Float> h1               = topDocsToMap(td1);
    Map<Integer,Float> h2CustomNeutral  = topDocsToMap(td2CustomNeutral);
    Map<Integer,Float> h3CustomMul      = topDocsToMap(td3CustomMul);
    Map<Integer,Float> h4CustomAdd      = topDocsToMap(td4CustomAdd);
    Map<Integer,Float> h5CustomMulAdd   = topDocsToMap(td5CustomMulAdd);
    
    verifyResults(boost, s, 
        h1, h2CustomNeutral, h3CustomMul, h4CustomAdd, h5CustomMulAdd,
        q1, q2CustomNeutral, q3CustomMul, q4CustomAdd, q5CustomMulAdd);
    r.close();
  }

  // verify results are as expected.
  private void verifyResults(float boost, IndexSearcher s, 
      Map<Integer,Float> h1, Map<Integer,Float> h2customNeutral, Map<Integer,Float> h3CustomMul, Map<Integer,Float> h4CustomAdd, Map<Integer,Float> h5CustomMulAdd,
      Query q1, Query q2, Query q3, Query q4, Query q5) throws Exception {
    
    // verify numbers of matches
    log("#hits = "+h1.size());
    assertEquals("queries should have same #hits",h1.size(),h2customNeutral.size());
    assertEquals("queries should have same #hits",h1.size(),h3CustomMul.size());
    assertEquals("queries should have same #hits",h1.size(),h4CustomAdd.size());
    assertEquals("queries should have same #hits",h1.size(),h5CustomMulAdd.size());

    QueryUtils.check(random(), q1, s, rarely());
    QueryUtils.check(random(), q2, s, rarely());
    QueryUtils.check(random(), q3, s, rarely());
    QueryUtils.check(random(), q4, s, rarely());
    QueryUtils.check(random(), q5, s, rarely());

    // verify scores ratios
    for (final Integer doc : h1.keySet()) {

      log("doc = "+doc);

      float fieldScore = expectedFieldScore(s.getIndexReader().document(doc).get(ID_FIELD));
      log("fieldScore = " + fieldScore);
      assertTrue("fieldScore should not be 0", fieldScore > 0);

      float score1 = h1.get(doc);
      logResult("score1=", s, q1, doc, score1);
      
      float score2 = h2customNeutral.get(doc);
      logResult("score2=", s, q2, doc, score2);
      assertEquals("same score (just boosted) for neutral", boost * score1, score2, CheckHits.explainToleranceDelta(boost * score1, score2));

      float score3 = h3CustomMul.get(doc);
      logResult("score3=", s, q3, doc, score3);
      assertEquals("new score for custom mul", boost * fieldScore * score1, score3, CheckHits.explainToleranceDelta(boost * fieldScore * score1, score3));
      
      float score4 = h4CustomAdd.get(doc);
      logResult("score4=", s, q4, doc, score4);
      assertEquals("new score for custom add", boost * (fieldScore + score1), score4, CheckHits.explainToleranceDelta(boost * (fieldScore + score1), score4));
      
      float score5 = h5CustomMulAdd.get(doc);
      logResult("score5=", s, q5, doc, score5);
      assertEquals("new score for custom mul add", boost * fieldScore * (score1 + fieldScore), score5, CheckHits.explainToleranceDelta(boost * fieldScore * (score1 + fieldScore), score5));
    }
  }

  private void logResult(String msg, IndexSearcher s, Query q, int doc, float score1) throws IOException {
    log(msg+" "+score1);
    log("Explain by: "+q);
    log(s.explain(q,doc));
  }

  // since custom scoring modifies the order of docs, map results 
  // by doc ids so that we can later compare/verify them 
  private Map<Integer,Float> topDocsToMap(TopDocs td) {
    Map<Integer,Float> h = new HashMap<>();
    for (int i=0; i<td.totalHits; i++) {
      h.put(td.scoreDocs[i].doc, td.scoreDocs[i].score);
    }
    return h;
  }

}
