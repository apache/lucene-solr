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
package org.apache.solr.search.function;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.solr.SolrTestCase;
import org.apache.solr.legacy.LegacyFloatField;
import org.apache.solr.legacy.LegacyIntField;
import org.apache.lucene.queries.function.FunctionQuery;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.FloatFieldSource;
import org.apache.lucene.queries.function.valuesource.IntFieldSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryUtils;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

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
public class TestOrdValues extends SolrTestCase {

  @BeforeClass
  public static void beforeClass() throws Exception {
    createIndex(false);
  }

  /**
   * Test OrdFieldSource
   */
  @Test
  public void testOrdFieldRank() throws Exception {
    doTestRank(ID_FIELD, true);
  }

  /**
   * Test ReverseOrdFieldSource
   */
  @Test
  public void testReverseOrdFieldRank() throws Exception {
    doTestRank(ID_FIELD, false);
  }

  // Test that queries based on reverse/ordFieldScore scores correctly
  private void doTestRank(String field, boolean inOrder) throws Exception {
    IndexReader r = DirectoryReader.open(dir);
    IndexSearcher s = newSearcher(r);
    ValueSource vs;
    if (inOrder) {
      vs = new OrdFieldSource(field);
    } else {
      vs = new ReverseOrdFieldSource(field);
    }

    Query q = new FunctionQuery(vs);
    log("test: " + q);
    QueryUtils.check(random(), q, s);
    ScoreDoc[] h = s.search(q, 1000).scoreDocs;
    assertEquals("All docs should be matched!", N_DOCS, h.length);
    String prevID = inOrder
            ? "IE"   // greater than all ids of docs in this test ("ID0001", etc.)
            : "IC";  // smaller than all ids of docs in this test ("ID0001", etc.)

    for (int i = 0; i < h.length; i++) {
      String resID = s.doc(h[i].doc).get(ID_FIELD);
      log(i + ".   score=" + h[i].score + "  -  " + resID);
      log(s.explain(q, h[i].doc));
      if (inOrder) {
        assertTrue("res id " + resID + " should be < prev res id " + prevID, resID.compareTo(prevID) < 0);
      } else {
        assertTrue("res id " + resID + " should be > prev res id " + prevID, resID.compareTo(prevID) > 0);
      }
      prevID = resID;
    }
    r.close();
  }

  /**
   * Test exact score for OrdFieldSource
   */
  @Test
  public void testOrdFieldExactScore() throws Exception {
    doTestExactScore(ID_FIELD, true);
  }

  /**
   * Test exact score for ReverseOrdFieldSource
   */
  @Test
  public void testReverseOrdFieldExactScore() throws Exception {
    doTestExactScore(ID_FIELD, false);
  }


  // Test that queries based on reverse/ordFieldScore returns docs with expected score.
  private void doTestExactScore(String field, boolean inOrder) throws Exception {
    IndexReader r = DirectoryReader.open(dir);
    IndexSearcher s = newSearcher(r);
    ValueSource vs;
    if (inOrder) {
      vs = new OrdFieldSource(field);
    } else {
      vs = new ReverseOrdFieldSource(field);
    }
    Query q = new FunctionQuery(vs);
    TopDocs td = s.search(q, 1000);
    assertEquals("All docs should be matched!", N_DOCS, td.totalHits.value);
    ScoreDoc sd[] = td.scoreDocs;
    for (int i = 0; i < sd.length; i++) {
      float score = sd[i].score;
      String id = s.getIndexReader().document(sd[i].doc).get(ID_FIELD);
      log("-------- " + i + ". Explain doc " + id);
      log(s.explain(q, sd[i].doc));
      float expectedScore = N_DOCS - i - 1;
      assertEquals("score of result " + i + " should be " + expectedScore + " != " + score, expectedScore, score, TEST_SCORE_TOLERANCE_DELTA);
      String expectedId = inOrder
              ? id2String(N_DOCS - i) // in-order ==> larger  values first
              : id2String(i + 1);     // reverse  ==> smaller values first
      assertTrue("id of result " + i + " should be " + expectedId + " != " + score, expectedId.equals(id));
    }
    r.close();
  }
  
  // LUCENE-1250
  public void testEqualsNull() throws Exception {
    OrdFieldSource ofs = new OrdFieldSource("f");
    assertFalse(ofs.equals(null));
    
    ReverseOrdFieldSource rofs = new ReverseOrdFieldSource("f");
    assertFalse(rofs.equals(null));
  }
  
  /**
   * Actual score computation order is slightly different than assumptios
   * this allows for a small amount of variation
   */
  protected static float TEST_SCORE_TOLERANCE_DELTA = 0.001f;

  protected static final int N_DOCS = 17; // select a primary number > 2

  protected static final String ID_FIELD = "id";
  protected static final String TEXT_FIELD = "text";
  protected static final String INT_FIELD = "iii";
  protected static final String FLOAT_FIELD = "fff";

  protected ValueSource INT_VALUESOURCE = new IntFieldSource(INT_FIELD);
  protected ValueSource FLOAT_VALUESOURCE = new FloatFieldSource(FLOAT_FIELD);

  private static final String DOC_TEXT_LINES[] = {
          "Well, this is just some plain text we use for creating the ",
          "test documents. It used to be a text from an online collection ",
          "devoted to first aid, but if there was there an (online) lawyers ",
          "first aid collection with legal advices, \"it\" might have quite ",
          "probably advised one not to include \"it\"'s text or the text of ",
          "any other online collection in one's code, unless one has money ",
          "that one don't need and one is happy to donate for lawyers ",
          "charity. Anyhow at some point, rechecking the usage of this text, ",
          "it became uncertain that this text is free to use, because ",
          "the web site in the disclaimer of he eBook containing that text ",
          "was not responding anymore, and at the same time, in projGut, ",
          "searching for first aid no longer found that eBook as well. ",
          "So here we are, with a perhaps much less interesting ",
          "text for the test, but oh much much safer. ",
  };

  protected static Directory dir;
  protected static Analyzer anlzr;

  @AfterClass
  public static void afterClassFunctionTestSetup() throws Exception {
    if (null != dir) {
      dir.close();
    }
    dir = null;
    anlzr = null;
  }

  protected static void createIndex(boolean doMultiSegment) throws Exception {
    if (VERBOSE) {
      System.out.println("TEST: setUp");
    }
    // prepare a small index with just a few documents.
    dir = newDirectory();
    anlzr = new MockAnalyzer(random());
    IndexWriterConfig iwc = newIndexWriterConfig(anlzr).setMergePolicy(newLogMergePolicy());
    if (doMultiSegment) {
      iwc.setMaxBufferedDocs(TestUtil.nextInt(random(), 2, 7));
    }
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);
    // add docs not exactly in natural ID order, to verify we do check the order of docs by scores
    int remaining = N_DOCS;
    boolean done[] = new boolean[N_DOCS];
    int i = 0;
    while (remaining > 0) {
      if (done[i]) {
        throw new Exception("to set this test correctly N_DOCS=" + N_DOCS + " must be primary and greater than 2!");
      }
      addDoc(iw, i);
      done[i] = true;
      i = (i + 4) % N_DOCS;
      remaining --;
    }
    if (!doMultiSegment) {
      if (VERBOSE) {
        System.out.println("TEST: setUp full merge");
      }
      iw.forceMerge(1);
    }
    iw.close();
    if (VERBOSE) {
      System.out.println("TEST: setUp done close");
    }
  }

  private static void addDoc(RandomIndexWriter iw, int i) throws Exception {
    Document d = new Document();
    Field f;
    int scoreAndID = i + 1;

    FieldType customType = new FieldType(TextField.TYPE_STORED);
    customType.setTokenized(false);
    customType.setOmitNorms(true);
    
    f = newField(ID_FIELD, id2String(scoreAndID), customType); // for debug purposes
    d.add(f);
    d.add(new SortedDocValuesField(ID_FIELD, new BytesRef(id2String(scoreAndID))));

    FieldType customType2 = new FieldType(TextField.TYPE_NOT_STORED);
    customType2.setOmitNorms(true);
    f = newField(TEXT_FIELD, "text of doc" + scoreAndID + textLine(i), customType2); // for regular search
    d.add(f);

    f = new LegacyIntField(INT_FIELD, scoreAndID, Store.YES); // for function scoring
    d.add(f);
    d.add(new NumericDocValuesField(INT_FIELD, scoreAndID));

    f = new LegacyFloatField(FLOAT_FIELD, scoreAndID, Store.YES); // for function scoring
    d.add(f);
    d.add(new NumericDocValuesField(FLOAT_FIELD, Float.floatToRawIntBits(scoreAndID)));

    iw.addDocument(d);
    log("added: " + d);
  }

  // 17 --> ID00017
  protected static String id2String(int scoreAndID) {
    String s = "000000000" + scoreAndID;
    int n = ("" + N_DOCS).length() + 3;
    int k = s.length() - n;
    return "ID" + s.substring(k);
  }

  // some text line for regular search
  private static String textLine(int docNum) {
    return DOC_TEXT_LINES[docNum % DOC_TEXT_LINES.length];
  }

  // extract expected doc score from its ID Field: "ID7" --> 7.0
  protected static float expectedFieldScore(String docIDFieldVal) {
    return Float.parseFloat(docIDFieldVal.substring(2));
  }

  // debug messages (change DBG to true for anything to print)
  protected static void log(Object o) {
    if (VERBOSE) {
      System.out.println(o.toString());
    }
  }

}
