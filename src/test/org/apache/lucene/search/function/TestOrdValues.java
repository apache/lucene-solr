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

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryUtils;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;

/**
 * Test search based on OrdFieldSource and ReverseOrdFieldSource.
 * <p>
 * Tests here create an index with a few documents, each having
 * an indexed "id" field.
 * The ord values of this field are later used for scoring.
 * <p>
 * The order tests use Hits to verify that docs are ordered as expected.
 * <p>
 * The exact score tests use TopDocs top to verify the exact score.  
 */
public class TestOrdValues extends FunctionTestSetup {

  /* @override constructor */
  public TestOrdValues(String name) {
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

  /** Test OrdFieldSource */
  public void testOrdFieldRank () throws CorruptIndexException, Exception {
    doTestRank(ID_FIELD,true);
  }

  /** Test ReverseOrdFieldSource */
  public void testReverseOrdFieldRank () throws CorruptIndexException, Exception {
    doTestRank(ID_FIELD,false);
  }

  // Test that queries based on reverse/ordFieldScore scores correctly
  private void doTestRank (String field, boolean inOrder) throws CorruptIndexException, Exception {
    IndexSearcher s = new IndexSearcher(dir);
    ValueSource vs;
    if (inOrder) {
      vs = new OrdFieldSource(field);
    } else {
      vs = new ReverseOrdFieldSource(field);
    }
        
    Query q = new ValueSourceQuery(vs);
    log("test: "+q);
    QueryUtils.check(q,s);
    ScoreDoc[] h = s.search(q, null, 1000).scoreDocs;
    assertEquals("All docs should be matched!",N_DOCS,h.length);
    String prevID = inOrder
      ? "IE"   // greater than all ids of docs in this test ("ID0001", etc.)
      : "IC";  // smaller than all ids of docs in this test ("ID0001", etc.)
          
    for (int i=0; i<h.length; i++) {
      String resID = s.doc(h[i].doc).get(ID_FIELD);
      log(i+".   score="+h[i].score+"  -  "+resID);
      log(s.explain(q,h[i].doc));
      if (inOrder) {
        assertTrue("res id "+resID+" should be < prev res id "+prevID, resID.compareTo(prevID)<0);
      } else {
        assertTrue("res id "+resID+" should be > prev res id "+prevID, resID.compareTo(prevID)>0);
      }
      prevID = resID;
    }
  }

  /** Test exact score for OrdFieldSource */
  public void testOrdFieldExactScore () throws CorruptIndexException, Exception {
    doTestExactScore(ID_FIELD,true);
  }

  /** Test exact score for ReverseOrdFieldSource */
  public void testReverseOrdFieldExactScore () throws CorruptIndexException, Exception {
    doTestExactScore(ID_FIELD,false);
  }

  
  // Test that queries based on reverse/ordFieldScore returns docs with expected score.
  private void doTestExactScore (String field, boolean inOrder) throws CorruptIndexException, Exception {
    IndexSearcher s = new IndexSearcher(dir);
    ValueSource vs;
    if (inOrder) {
      vs = new OrdFieldSource(field);
    } else {
      vs = new ReverseOrdFieldSource(field);
    }
    Query q = new ValueSourceQuery(vs);
    TopDocs td = s.search(q,null,1000);
    assertEquals("All docs should be matched!",N_DOCS,td.totalHits);
    ScoreDoc sd[] = td.scoreDocs;
    for (int i=0; i<sd.length; i++) {
      float score = sd[i].score;
      String id = s.getIndexReader().document(sd[i].doc).get(ID_FIELD);
      log("-------- "+i+". Explain doc "+id);
      log(s.explain(q,sd[i].doc));
      float expectedScore =  N_DOCS-i;
      assertEquals("score of result "+i+" shuould be "+expectedScore+" != "+score, expectedScore, score, TEST_SCORE_TOLERANCE_DELTA);
      String expectedId =  inOrder 
        ? id2String(N_DOCS-i) // in-order ==> larger  values first 
        : id2String(i+1);     // reverse  ==> smaller values first 
      assertTrue("id of result "+i+" shuould be "+expectedId+" != "+score, expectedId.equals(id));
    }
  }
  
  /** Test caching OrdFieldSource */
  public void testCachingOrd () throws CorruptIndexException, Exception {
    doTestCaching(ID_FIELD,true);
  }
  
  /** Test caching for ReverseOrdFieldSource */
  public void tesCachingReverseOrd () throws CorruptIndexException, Exception {
    doTestCaching(ID_FIELD,false);
  }

  // Test that values loaded for FieldScoreQuery are cached properly and consumes the proper RAM resources.
  private void doTestCaching (String field, boolean inOrder) throws CorruptIndexException, Exception {
    IndexSearcher s = new IndexSearcher(dir);
    Object innerArray = null;

    boolean warned = false; // print warning once
    
    for (int i=0; i<10; i++) {
      ValueSource vs;
      if (inOrder) {
        vs = new OrdFieldSource(field);
      } else {
        vs = new ReverseOrdFieldSource(field);
      }
      ValueSourceQuery q = new ValueSourceQuery(vs);
      ScoreDoc[] h = s.search(q, null, 1000).scoreDocs;
      try {
        assertEquals("All docs should be matched!",N_DOCS,h.length);
        if (i==0) {
          innerArray = q.valSrc.getValues(s.getIndexReader()).getInnerArray();
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
    
    ValueSource vs;
    ValueSourceQuery q;
    ScoreDoc[] h;
    
    // verify that different values are loaded for a different field
    String field2 = INT_FIELD;
    assertFalse(field.equals(field2)); // otherwise this test is meaningless.
    if (inOrder) {
      vs = new OrdFieldSource(field2);
    } else {
      vs = new ReverseOrdFieldSource(field2);
    }
    q = new ValueSourceQuery(vs);
    h = s.search(q, null, 1000).scoreDocs;
    assertEquals("All docs should be matched!",N_DOCS,h.length);
    try {
      log("compare (should differ): "+innerArray+" to "+q.valSrc.getValues(s.getIndexReader()).getInnerArray());
      assertNotSame("different values shuold be loaded for a different field!", innerArray, q.valSrc.getValues(s.getIndexReader()).getInnerArray());
    } catch (UnsupportedOperationException e) {
      if (!warned) {
        System.err.println("WARNING: "+testName()+" cannot fully test values of "+q);
        warned = true;
      }
    }

    // verify new values are reloaded (not reused) for a new reader
    s = new IndexSearcher(dir);
    if (inOrder) {
      vs = new OrdFieldSource(field);
    } else {
      vs = new ReverseOrdFieldSource(field);
    }
    q = new ValueSourceQuery(vs);
    h = s.search(q, null, 1000).scoreDocs;
    assertEquals("All docs should be matched!",N_DOCS,h.length);
    try {
      log("compare (should differ): "+innerArray+" to "+q.valSrc.getValues(s.getIndexReader()).getInnerArray());
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
