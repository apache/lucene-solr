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

import java.util.HashMap;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryUtils;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;

/**
 * Test FieldScoreQuery search.
 * <p>
 * Tests here create an index with a few documents, each having
 * an int value indexed  field and a float value indexed field.
 * The values of these fields are later used for scoring.
 * <p>
 * The rank tests use Hits to verify that docs are ordered (by score) as expected.
 * <p>
 * The exact score tests use TopDocs top to verify the exact score.  
 */
public class TestFieldScoreQuery extends FunctionTestSetup {

  /* @override constructor */
  public TestFieldScoreQuery(String name) {
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

  /** Test that FieldScoreQuery of Type.BYTE returns docs in expected order. */
  public void testRankByte () throws CorruptIndexException, Exception {
    // INT field values are small enough to be parsed as byte
    doTestRank(INT_FIELD,FieldScoreQuery.Type.BYTE);
  }

  /** Test that FieldScoreQuery of Type.SHORT returns docs in expected order. */
  public void testRankShort () throws CorruptIndexException, Exception {
    // INT field values are small enough to be parsed as short
    doTestRank(INT_FIELD,FieldScoreQuery.Type.SHORT);
  }

  /** Test that FieldScoreQuery of Type.INT returns docs in expected order. */
  public void testRankInt () throws CorruptIndexException, Exception {
    doTestRank(INT_FIELD,FieldScoreQuery.Type.INT);
  }

  /** Test that FieldScoreQuery of Type.FLOAT returns docs in expected order. */
  public void testRankFloat () throws CorruptIndexException, Exception {
    // INT field can be parsed as float
    doTestRank(INT_FIELD,FieldScoreQuery.Type.FLOAT);
    // same values, but in flot format
    doTestRank(FLOAT_FIELD,FieldScoreQuery.Type.FLOAT);
  }

  // Test that FieldScoreQuery returns docs in expected order.
  private void doTestRank (String field, FieldScoreQuery.Type tp) throws CorruptIndexException, Exception {
    IndexSearcher s = new IndexSearcher(dir);
    Query q = new FieldScoreQuery(field,tp);
    log("test: "+q);
    QueryUtils.check(q,s);
    ScoreDoc[] h = s.search(q, null, 1000).scoreDocs;
    assertEquals("All docs should be matched!",N_DOCS,h.length);
    String prevID = "ID"+(N_DOCS+1); // greater than all ids of docs in this test
    for (int i=0; i<h.length; i++) {
      String resID = s.doc(h[i].doc).get(ID_FIELD);
      log(i+".   score="+h[i].score+"  -  "+resID);
      log(s.explain(q,h[i].doc));
      assertTrue("res id "+resID+" should be < prev res id "+prevID, resID.compareTo(prevID)<0);
      prevID = resID;
    }
  }

  /** Test that FieldScoreQuery of Type.BYTE returns the expected scores. */
  public void testExactScoreByte () throws CorruptIndexException, Exception {
    // INT field values are small enough to be parsed as byte
    doTestExactScore(INT_FIELD,FieldScoreQuery.Type.BYTE);
  }

  /** Test that FieldScoreQuery of Type.SHORT returns the expected scores. */
  public void testExactScoreShort () throws CorruptIndexException, Exception {
    // INT field values are small enough to be parsed as short
    doTestExactScore(INT_FIELD,FieldScoreQuery.Type.SHORT);
  }

  /** Test that FieldScoreQuery of Type.INT returns the expected scores. */
  public void testExactScoreInt () throws CorruptIndexException, Exception {
    doTestExactScore(INT_FIELD,FieldScoreQuery.Type.INT);
  }

  /** Test that FieldScoreQuery of Type.FLOAT returns the expected scores. */
  public void testExactScoreFloat () throws CorruptIndexException, Exception {
    // INT field can be parsed as float
    doTestExactScore(INT_FIELD,FieldScoreQuery.Type.FLOAT);
    // same values, but in flot format
    doTestExactScore(FLOAT_FIELD,FieldScoreQuery.Type.FLOAT);
  }

  // Test that FieldScoreQuery returns docs with expected score.
  private void doTestExactScore (String field, FieldScoreQuery.Type tp) throws CorruptIndexException, Exception {
    IndexSearcher s = new IndexSearcher(dir);
    Query q = new FieldScoreQuery(field,tp);
    TopDocs td = s.search(q,null,1000);
    assertEquals("All docs should be matched!",N_DOCS,td.totalHits);
    ScoreDoc sd[] = td.scoreDocs;
    for (int i=0; i<sd.length; i++) {
      float score = sd[i].score;
      log(s.explain(q,sd[i].doc));
      String id = s.getIndexReader().document(sd[i].doc).get(ID_FIELD);
      float expectedScore = expectedFieldScore(id); // "ID7" --> 7.0
      assertEquals("score of "+id+" shuould be "+expectedScore+" != "+score, expectedScore, score, TEST_SCORE_TOLERANCE_DELTA);
    }
  }

  /** Test that FieldScoreQuery of Type.BYTE caches/reuses loaded values and consumes the proper RAM resources. */
  public void testCachingByte () throws CorruptIndexException, Exception {
    // INT field values are small enough to be parsed as byte
    doTestCaching(INT_FIELD,FieldScoreQuery.Type.BYTE);
  }

  /** Test that FieldScoreQuery of Type.SHORT caches/reuses loaded values and consumes the proper RAM resources. */
  public void testCachingShort () throws CorruptIndexException, Exception {
    // INT field values are small enough to be parsed as short
    doTestCaching(INT_FIELD,FieldScoreQuery.Type.SHORT);
  }

  /** Test that FieldScoreQuery of Type.INT caches/reuses loaded values and consumes the proper RAM resources. */
  public void testCachingInt () throws CorruptIndexException, Exception {
    doTestCaching(INT_FIELD,FieldScoreQuery.Type.INT);
  }

  /** Test that FieldScoreQuery of Type.FLOAT caches/reuses loaded values and consumes the proper RAM resources. */
  public void testCachingFloat () throws CorruptIndexException, Exception {
    // INT field values can be parsed as float
    doTestCaching(INT_FIELD,FieldScoreQuery.Type.FLOAT);
    // same values, but in flot format
    doTestCaching(FLOAT_FIELD,FieldScoreQuery.Type.FLOAT);
  }

  // Test that values loaded for FieldScoreQuery are cached properly and consumes the proper RAM resources.
  private void doTestCaching (String field, FieldScoreQuery.Type tp) throws CorruptIndexException, Exception {
    // prepare expected array types for comparison
    HashMap expectedArrayTypes = new HashMap();
    expectedArrayTypes.put(FieldScoreQuery.Type.BYTE, new byte[0]);
    expectedArrayTypes.put(FieldScoreQuery.Type.SHORT, new short[0]);
    expectedArrayTypes.put(FieldScoreQuery.Type.INT, new int[0]);
    expectedArrayTypes.put(FieldScoreQuery.Type.FLOAT, new float[0]);
    
    IndexSearcher s = new IndexSearcher(dir);
    Object innerArray = null;

    boolean warned = false; // print warning once.
    for (int i=0; i<10; i++) {
      FieldScoreQuery q = new FieldScoreQuery(field,tp);
      ScoreDoc[] h = s.search(q, null, 1000).scoreDocs;
      assertEquals("All docs should be matched!",N_DOCS,h.length);
      try {
        if (i==0) {
          innerArray = q.valSrc.getValues(s.getIndexReader()).getInnerArray();
          log(i+".  compare: "+innerArray.getClass()+" to "+expectedArrayTypes.get(tp).getClass());
          assertEquals("field values should be cached in the correct array type!", innerArray.getClass(),expectedArrayTypes.get(tp).getClass());
        } else {
          log(i+".  compare: "+innerArray+" to "+q.valSrc.getValues(s.getIndexReader()).getInnerArray());
          assertSame("field values should be cached and reused!", innerArray, q.valSrc.getValues(s.getIndexReader()).getInnerArray());
        }
      } catch (UnsupportedOperationException e) {
        if (!warned) {
          System.err.println("WARNING: "+testName()+" cannot fully test values of "+q);
          warned = true;
        }
      }
    }
    
    // verify new values are reloaded (not reused) for a new reader
    s = new IndexSearcher(dir);
    FieldScoreQuery q = new FieldScoreQuery(field,tp);
    ScoreDoc[] h = s.search(q, null, 1000).scoreDocs;
    assertEquals("All docs should be matched!",N_DOCS,h.length);
    try {
      log("compare: "+innerArray+" to "+q.valSrc.getValues(s.getIndexReader()).getInnerArray());
      assertNotSame("cached field values should not be reused if reader as changed!", innerArray, q.valSrc.getValues(s.getIndexReader()).getInnerArray());
    } catch (UnsupportedOperationException e) {
      if (!warned) {
        System.err.println("WARNING: "+testName()+" cannot fully test values of "+q);
        warned = true;
      }
    }
  }

  private String testName() {
    return getClass().getName()+"."+getName();
  }

}
