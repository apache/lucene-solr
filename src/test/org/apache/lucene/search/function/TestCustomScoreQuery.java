package org.apache.lucene.search.function;

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
import java.util.HashMap;
import java.util.Iterator;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryUtils;
import org.apache.lucene.search.TopDocs;

/**
 * Test CustomScoreQuery search.
 */
public class TestCustomScoreQuery extends FunctionTestSetup {

  /* @override constructor */
  public TestCustomScoreQuery(String name) {
    super(name);
  }

  /* @override */
  protected void tearDown() throws Exception {
    super.tearDown();
  }

  /* @override */
  protected void setUp() throws Exception {
    // prepare a small index with just a few documents.  
    super.setUp();
  }

  /** Test that CustomScoreQuery of Type.BYTE returns the expected scores. */
  public void testCustomScoreByte () throws CorruptIndexException, Exception {
    // INT field values are small enough to be parsed as byte
    doTestCustomScore(INT_FIELD,FieldScoreQuery.Type.BYTE,1.0);
    doTestCustomScore(INT_FIELD,FieldScoreQuery.Type.BYTE,2.0);
  }

  /** Test that CustomScoreQuery of Type.SHORT returns the expected scores. */
  public void testCustomScoreShort () throws CorruptIndexException, Exception {
    // INT field values are small enough to be parsed as short
    doTestCustomScore(INT_FIELD,FieldScoreQuery.Type.SHORT,1.0);
    doTestCustomScore(INT_FIELD,FieldScoreQuery.Type.SHORT,3.0);
  }

  /** Test that CustomScoreQuery of Type.INT returns the expected scores. */
  public void testCustomScoreInt () throws CorruptIndexException, Exception {
    doTestCustomScore(INT_FIELD,FieldScoreQuery.Type.INT,1.0);
    doTestCustomScore(INT_FIELD,FieldScoreQuery.Type.INT,4.0);
  }

  /** Test that CustomScoreQuery of Type.FLOAT returns the expected scores. */
  public void testCustomScoreFloat () throws CorruptIndexException, Exception {
    // INT field can be parsed as float
    doTestCustomScore(INT_FIELD,FieldScoreQuery.Type.FLOAT,1.0);
    doTestCustomScore(INT_FIELD,FieldScoreQuery.Type.FLOAT,5.0);
    // same values, but in flot format
    doTestCustomScore(FLOAT_FIELD,FieldScoreQuery.Type.FLOAT,1.0);
    doTestCustomScore(FLOAT_FIELD,FieldScoreQuery.Type.FLOAT,6.0);
  }

  // must have static class otherwise serialization tests fail
  private static class CustomAddQuery extends CustomScoreQuery {
    // constructor
    CustomAddQuery (Query q, ValueSourceQuery qValSrc) {
      super(q,qValSrc);
    }
    /*(non-Javadoc) @see org.apache.lucene.search.function.CustomScoreQuery#name() */
    public String name() {
      return "customAdd";
    }
    /*(non-Javadoc) @see org.apache.lucene.search.function.CustomScoreQuery#customScore(int, float, float) */
    public float customScore(int doc, float subQueryScore, float valSrcScore) {
      return subQueryScore + valSrcScore;
    }
    /* (non-Javadoc)@see org.apache.lucene.search.function.CustomScoreQuery#customExplain(int, org.apache.lucene.search.Explanation, org.apache.lucene.search.Explanation)*/
    public Explanation customExplain(int doc, Explanation subQueryExpl, Explanation valSrcExpl) {
      float valSrcScore = valSrcExpl==null ? 0 : valSrcExpl.getValue();
      Explanation exp = new Explanation( valSrcScore + subQueryExpl.getValue(), "custom score: sum of:");
      exp.addDetail(subQueryExpl);
      if (valSrcExpl != null) {
        exp.addDetail(valSrcExpl);
      }
      return exp;      
    } 
  }
  
  // must have static class otherwise serialization tests fail
  private static class CustomMulAddQuery extends CustomScoreQuery {
    // constructor
    CustomMulAddQuery(Query q, ValueSourceQuery qValSrc1, ValueSourceQuery qValSrc2) {
      super(q,new ValueSourceQuery[]{qValSrc1,qValSrc2});
    }
    /*(non-Javadoc) @see org.apache.lucene.search.function.CustomScoreQuery#name() */
    public String name() {
      return "customMulAdd";
    }
    /*(non-Javadoc) @see org.apache.lucene.search.function.CustomScoreQuery#customScore(int, float, float) */
    public float customScore(int doc, float subQueryScore, float valSrcScores[]) {
      if (valSrcScores.length == 0) {
        return subQueryScore;
      }
      if (valSrcScores.length == 1) {
        return subQueryScore + valSrcScores[0];
      }
      return (subQueryScore + valSrcScores[0]) * valSrcScores[1]; // we know there are two
    } 
    /* (non-Javadoc)@see org.apache.lucene.search.function.CustomScoreQuery#customExplain(int, org.apache.lucene.search.Explanation, org.apache.lucene.search.Explanation)*/
    public Explanation customExplain(int doc, Explanation subQueryExpl, Explanation valSrcExpls[]) {
      if (valSrcExpls.length == 0) {
        return subQueryExpl;
      }
      Explanation exp = new Explanation(valSrcExpls[0].getValue() + subQueryExpl.getValue(), "sum of:");
      exp.addDetail(subQueryExpl);
      exp.addDetail(valSrcExpls[0]);
      if (valSrcExpls.length == 1) {
        exp.setDescription("CustomMulAdd, sum of:");
        return exp;
      }
      Explanation exp2 = new Explanation(valSrcExpls[1].getValue() * exp.getValue(), "custom score: product of:");
      exp2.addDetail(valSrcExpls[1]);
      exp2.addDetail(exp);
      return exp2;      
    } 
  }
  
  // Test that FieldScoreQuery returns docs with expected score.
  private void doTestCustomScore (String field, FieldScoreQuery.Type tp, double dboost) throws CorruptIndexException, Exception {
    float boost = (float) dboost;
    IndexSearcher s = new IndexSearcher(dir);
    FieldScoreQuery qValSrc = new FieldScoreQuery(field,tp); // a query that would score by the field
    QueryParser qp = new QueryParser(TEXT_FIELD,anlzr); 
    String qtxt = "first aid text"; // from the doc texts in FunctionQuerySetup.
    
    // regular (boolean) query.
    Query q1 = qp.parse(qtxt); 
    log(q1);
    
    // custom query, that should score the same as q1.
    CustomScoreQuery q2CustomNeutral = new CustomScoreQuery(q1);
    q2CustomNeutral.setBoost(boost);
    log(q2CustomNeutral);
    
    // custom query, that should (by default) multiply the scores of q1 by that of the field
    CustomScoreQuery q3CustomMul = new CustomScoreQuery(q1,qValSrc);
    q3CustomMul.setStrict(true);
    q3CustomMul.setBoost(boost);
    log(q3CustomMul);
    
    // custom query, that should add the scores of q1 to that of the field
    CustomScoreQuery q4CustomAdd = new CustomAddQuery(q1,qValSrc); 
    q4CustomAdd.setStrict(true);
    q4CustomAdd.setBoost(boost);
    log(q4CustomAdd);

    // custom query, that multiplies and adds the field score to that of q1
    CustomScoreQuery q5CustomMulAdd = new CustomMulAddQuery(q1,qValSrc,qValSrc);
    q5CustomMulAdd.setStrict(true);
    q5CustomMulAdd.setBoost(boost);
    log(q5CustomMulAdd);

    // do al the searches 
    TopDocs td1 = s.search(q1,null,1000);
    TopDocs td2CustomNeutral = s.search(q2CustomNeutral,null,1000);
    TopDocs td3CustomMul = s.search(q3CustomMul,null,1000);
    TopDocs td4CustomAdd = s.search(q4CustomAdd,null,1000);
    TopDocs td5CustomMulAdd = s.search(q5CustomMulAdd,null,1000);
    
    // put results in map so we can verify the scores although they have changed
    HashMap h1 = topDocsToMap(td1);
    HashMap h2CustomNeutral = topDocsToMap(td2CustomNeutral);
    HashMap h3CustomMul = topDocsToMap(td3CustomMul);
    HashMap h4CustomAdd = topDocsToMap(td4CustomAdd);
    HashMap h5CustomMulAdd = topDocsToMap(td5CustomMulAdd);
    
    verifyResults(boost, s, 
        h1, h2CustomNeutral, h3CustomMul, h4CustomAdd, h5CustomMulAdd,
        q1, q2CustomNeutral, q3CustomMul, q4CustomAdd, q5CustomMulAdd);
  }
  
  // verify results are as expected.
  private void verifyResults(float boost, IndexSearcher s, 
      HashMap h1, HashMap h2customNeutral, HashMap h3CustomMul, HashMap h4CustomAdd, HashMap h5CustomMulAdd,
      Query q1, Query q2, Query q3, Query q4, Query q5) throws Exception {
    
    // verify numbers of matches
    log("#hits = "+h1.size());
    assertEquals("queries should have same #hits",h1.size(),h2customNeutral.size());
    assertEquals("queries should have same #hits",h1.size(),h3CustomMul.size());
    assertEquals("queries should have same #hits",h1.size(),h4CustomAdd.size());
    assertEquals("queries should have same #hits",h1.size(),h5CustomMulAdd.size());
    
    // verify scores ratios
    for (Iterator it = h1.keySet().iterator(); it.hasNext();) {
      Integer x = (Integer) it.next();

      int doc =  x.intValue();
      log("doc = "+doc);

      float fieldScore = expectedFieldScore(s.getIndexReader().document(doc).get(ID_FIELD));
      log("fieldScore = "+fieldScore);
      assertTrue("fieldScore should not be 0",fieldScore>0);

      float score1 = ((Float)h1.get(x)).floatValue();
      logResult("score1=", s, q1, doc, score1);
      
      float score2 = ((Float)h2customNeutral.get(x)).floatValue();
      logResult("score2=", s, q2, doc, score2);
      assertEquals("same score (just boosted) for neutral", boost * score1, score2, TEST_SCORE_TOLERANCE_DELTA);

      float score3 = ((Float)h3CustomMul.get(x)).floatValue();
      logResult("score3=", s, q3, doc, score3);
      assertEquals("new score for custom mul", boost * fieldScore * score1, score3, TEST_SCORE_TOLERANCE_DELTA);
      
      float score4 = ((Float)h4CustomAdd.get(x)).floatValue();
      logResult("score4=", s, q4, doc, score4);
      assertEquals("new score for custom add", boost * (fieldScore + score1), score4, TEST_SCORE_TOLERANCE_DELTA);
      
      float score5 = ((Float)h5CustomMulAdd.get(x)).floatValue();
      logResult("score5=", s, q5, doc, score5);
      assertEquals("new score for custom mul add", boost * fieldScore * (score1 + fieldScore), score5, TEST_SCORE_TOLERANCE_DELTA);
    }
  }

  private void logResult(String msg, IndexSearcher s, Query q, int doc, float score1) throws IOException {
    QueryUtils.check(q,s);
    log(msg+" "+score1);
    log("Explain by: "+q);
    log(s.explain(q,doc));
  }

  // since custom scoring modifies the order of docs, map results 
  // by doc ids so that we can later compare/verify them 
  private HashMap topDocsToMap(TopDocs td) {
    HashMap h = new HashMap(); 
    for (int i=0; i<td.totalHits; i++) {
      h.put(new Integer(td.scoreDocs[i].doc), new Float(td.scoreDocs[i].score));
    }
    return h;
  }

}
