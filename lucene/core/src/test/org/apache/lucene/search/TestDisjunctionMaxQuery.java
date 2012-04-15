package org.apache.lucene.search;

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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Norm;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;

import java.text.DecimalFormat;
import java.io.IOException;

/**
 * Test of the DisjunctionMaxQuery.
 * 
 */
public class TestDisjunctionMaxQuery extends LuceneTestCase {
  
  /** threshold for comparing floats */
  public static final float SCORE_COMP_THRESH = 0.0000f;
  
  /**
   * Similarity to eliminate tf, idf and lengthNorm effects to isolate test
   * case.
   * 
   * <p>
   * same as TestRankingSimilarity in TestRanking.zip from
   * http://issues.apache.org/jira/browse/LUCENE-323
   * </p>
   */
  private static class TestSimilarity extends DefaultSimilarity {
    
    public TestSimilarity() {}
    
    @Override
    public float tf(float freq) {
      if (freq > 0.0f) return 1.0f;
      else return 0.0f;
    }
    
    @Override
    public void computeNorm(FieldInvertState state, Norm norm) {
      // Disable length norm
      norm.setByte(encodeNormValue(state.getBoost()));
    }
    
    @Override
    public float idf(long docFreq, long numDocs) {
      return 1.0f;
    }
  }
  
  public Similarity sim = new TestSimilarity();
  public Directory index;
  public IndexReader r;
  public IndexSearcher s;
  
  private static final FieldType nonAnalyzedType = new FieldType(TextField.TYPE_STORED);
  static {
    nonAnalyzedType.setTokenized(false);
  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    
    index = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), index,
        newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random()))
                                                     .setSimilarity(sim).setMergePolicy(newLogMergePolicy()));
    
    // hed is the most important field, dek is secondary
    
    // d1 is an "ok" match for: albino elephant
    {
      Document d1 = new Document();
      d1.add(newField("id", "d1", nonAnalyzedType));// Field.Keyword("id",
                                                                               // "d1"));
      d1
          .add(newField("hed", "elephant", TextField.TYPE_STORED));// Field.Text("hed", "elephant"));
      d1
          .add(newField("dek", "elephant", TextField.TYPE_STORED));// Field.Text("dek", "elephant"));
      writer.addDocument(d1);
    }
    
    // d2 is a "good" match for: albino elephant
    {
      Document d2 = new Document();
      d2.add(newField("id", "d2", nonAnalyzedType));// Field.Keyword("id",
                                                                               // "d2"));
      d2
          .add(newField("hed", "elephant", TextField.TYPE_STORED));// Field.Text("hed", "elephant"));
      d2.add(newField("dek", "albino", TextField.TYPE_STORED));// Field.Text("dek",
                                                                                // "albino"));
      d2
          .add(newField("dek", "elephant", TextField.TYPE_STORED));// Field.Text("dek", "elephant"));
      writer.addDocument(d2);
    }
    
    // d3 is a "better" match for: albino elephant
    {
      Document d3 = new Document();
      d3.add(newField("id", "d3", nonAnalyzedType));// Field.Keyword("id",
                                                                               // "d3"));
      d3.add(newField("hed", "albino", TextField.TYPE_STORED));// Field.Text("hed",
                                                                                // "albino"));
      d3
          .add(newField("hed", "elephant", TextField.TYPE_STORED));// Field.Text("hed", "elephant"));
      writer.addDocument(d3);
    }
    
    // d4 is the "best" match for: albino elephant
    {
      Document d4 = new Document();
      d4.add(newField("id", "d4", nonAnalyzedType));// Field.Keyword("id",
                                                                               // "d4"));
      d4.add(newField("hed", "albino", TextField.TYPE_STORED));// Field.Text("hed",
                                                                                // "albino"));
      d4
          .add(newField("hed", "elephant", nonAnalyzedType));// Field.Text("hed", "elephant"));
      d4.add(newField("dek", "albino", TextField.TYPE_STORED));// Field.Text("dek",
                                                                                // "albino"));
      writer.addDocument(d4);
    }
    
    r = SlowCompositeReaderWrapper.wrap(writer.getReader());
    writer.close();
    s = newSearcher(r);
    s.setSimilarity(sim);
  }
  
  @Override
  public void tearDown() throws Exception {
    r.close();
    index.close();
    super.tearDown();
  }
  
  public void testSkipToFirsttimeMiss() throws IOException {
    final DisjunctionMaxQuery dq = new DisjunctionMaxQuery(0.0f);
    dq.add(tq("id", "d1"));
    dq.add(tq("dek", "DOES_NOT_EXIST"));
    
    QueryUtils.check(random(), dq, s);
    assertTrue(s.getTopReaderContext() instanceof AtomicReaderContext);
    final Weight dw = s.createNormalizedWeight(dq);
    AtomicReaderContext context = (AtomicReaderContext)s.getTopReaderContext();
    final Scorer ds = dw.scorer(context, true, false, context.reader().getLiveDocs());
    final boolean skipOk = ds.advance(3) != DocIdSetIterator.NO_MORE_DOCS;
    if (skipOk) {
      fail("firsttime skipTo found a match? ... "
          + r.document(ds.docID()).get("id"));
    }
  }
  
  public void testSkipToFirsttimeHit() throws IOException {
    final DisjunctionMaxQuery dq = new DisjunctionMaxQuery(0.0f);
    dq.add(tq("dek", "albino"));
    dq.add(tq("dek", "DOES_NOT_EXIST"));
    assertTrue(s.getTopReaderContext() instanceof AtomicReaderContext);
    QueryUtils.check(random(), dq, s);
    final Weight dw = s.createNormalizedWeight(dq);
    AtomicReaderContext context = (AtomicReaderContext)s.getTopReaderContext();
    final Scorer ds = dw.scorer(context, true, false, context.reader().getLiveDocs());
    assertTrue("firsttime skipTo found no match",
        ds.advance(3) != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals("found wrong docid", "d4", r.document(ds.docID()).get("id"));
  }
  
  public void testSimpleEqualScores1() throws Exception {
    
    DisjunctionMaxQuery q = new DisjunctionMaxQuery(0.0f);
    q.add(tq("hed", "albino"));
    q.add(tq("hed", "elephant"));
    QueryUtils.check(random(), q, s);
    
    ScoreDoc[] h = s.search(q, null, 1000).scoreDocs;
    
    try {
      assertEquals("all docs should match " + q.toString(), 4, h.length);
      
      float score = h[0].score;
      for (int i = 1; i < h.length; i++) {
        assertEquals("score #" + i + " is not the same", score, h[i].score,
            SCORE_COMP_THRESH);
      }
    } catch (Error e) {
      printHits("testSimpleEqualScores1", h, s);
      throw e;
    }
    
  }
  
  public void testSimpleEqualScores2() throws Exception {
    
    DisjunctionMaxQuery q = new DisjunctionMaxQuery(0.0f);
    q.add(tq("dek", "albino"));
    q.add(tq("dek", "elephant"));
    QueryUtils.check(random(), q, s);
    
    ScoreDoc[] h = s.search(q, null, 1000).scoreDocs;
    
    try {
      assertEquals("3 docs should match " + q.toString(), 3, h.length);
      float score = h[0].score;
      for (int i = 1; i < h.length; i++) {
        assertEquals("score #" + i + " is not the same", score, h[i].score,
            SCORE_COMP_THRESH);
      }
    } catch (Error e) {
      printHits("testSimpleEqualScores2", h, s);
      throw e;
    }
    
  }
  
  public void testSimpleEqualScores3() throws Exception {
    
    DisjunctionMaxQuery q = new DisjunctionMaxQuery(0.0f);
    q.add(tq("hed", "albino"));
    q.add(tq("hed", "elephant"));
    q.add(tq("dek", "albino"));
    q.add(tq("dek", "elephant"));
    QueryUtils.check(random(), q, s);
    
    ScoreDoc[] h = s.search(q, null, 1000).scoreDocs;
    
    try {
      assertEquals("all docs should match " + q.toString(), 4, h.length);
      float score = h[0].score;
      for (int i = 1; i < h.length; i++) {
        assertEquals("score #" + i + " is not the same", score, h[i].score,
            SCORE_COMP_THRESH);
      }
    } catch (Error e) {
      printHits("testSimpleEqualScores3", h, s);
      throw e;
    }
    
  }
  
  public void testSimpleTiebreaker() throws Exception {
    
    DisjunctionMaxQuery q = new DisjunctionMaxQuery(0.01f);
    q.add(tq("dek", "albino"));
    q.add(tq("dek", "elephant"));
    QueryUtils.check(random(), q, s);
    
    ScoreDoc[] h = s.search(q, null, 1000).scoreDocs;
    
    try {
      assertEquals("3 docs should match " + q.toString(), 3, h.length);
      assertEquals("wrong first", "d2", s.doc(h[0].doc).get("id"));
      float score0 = h[0].score;
      float score1 = h[1].score;
      float score2 = h[2].score;
      assertTrue("d2 does not have better score then others: " + score0
          + " >? " + score1, score0 > score1);
      assertEquals("d4 and d1 don't have equal scores", score1, score2,
          SCORE_COMP_THRESH);
    } catch (Error e) {
      printHits("testSimpleTiebreaker", h, s);
      throw e;
    }
  }
  
  public void testBooleanRequiredEqualScores() throws Exception {
    
    BooleanQuery q = new BooleanQuery();
    {
      DisjunctionMaxQuery q1 = new DisjunctionMaxQuery(0.0f);
      q1.add(tq("hed", "albino"));
      q1.add(tq("dek", "albino"));
      q.add(q1, BooleanClause.Occur.MUST);// true,false);
      QueryUtils.check(random(), q1, s);
      
    }
    {
      DisjunctionMaxQuery q2 = new DisjunctionMaxQuery(0.0f);
      q2.add(tq("hed", "elephant"));
      q2.add(tq("dek", "elephant"));
      q.add(q2, BooleanClause.Occur.MUST);// true,false);
      QueryUtils.check(random(), q2, s);
    }
    
    QueryUtils.check(random(), q, s);
    
    ScoreDoc[] h = s.search(q, null, 1000).scoreDocs;
    
    try {
      assertEquals("3 docs should match " + q.toString(), 3, h.length);
      float score = h[0].score;
      for (int i = 1; i < h.length; i++) {
        assertEquals("score #" + i + " is not the same", score, h[i].score,
            SCORE_COMP_THRESH);
      }
    } catch (Error e) {
      printHits("testBooleanRequiredEqualScores1", h, s);
      throw e;
    }
  }
  
  public void testBooleanOptionalNoTiebreaker() throws Exception {
    
    BooleanQuery q = new BooleanQuery();
    {
      DisjunctionMaxQuery q1 = new DisjunctionMaxQuery(0.0f);
      q1.add(tq("hed", "albino"));
      q1.add(tq("dek", "albino"));
      q.add(q1, BooleanClause.Occur.SHOULD);// false,false);
    }
    {
      DisjunctionMaxQuery q2 = new DisjunctionMaxQuery(0.0f);
      q2.add(tq("hed", "elephant"));
      q2.add(tq("dek", "elephant"));
      q.add(q2, BooleanClause.Occur.SHOULD);// false,false);
    }
    QueryUtils.check(random(), q, s);
    
    ScoreDoc[] h = s.search(q, null, 1000).scoreDocs;
    
    try {
      assertEquals("4 docs should match " + q.toString(), 4, h.length);
      float score = h[0].score;
      for (int i = 1; i < h.length - 1; i++) { /* note: -1 */
        assertEquals("score #" + i + " is not the same", score, h[i].score,
            SCORE_COMP_THRESH);
      }
      assertEquals("wrong last", "d1", s.doc(h[h.length - 1].doc).get("id"));
      float score1 = h[h.length - 1].score;
      assertTrue("d1 does not have worse score then others: " + score + " >? "
          + score1, score > score1);
    } catch (Error e) {
      printHits("testBooleanOptionalNoTiebreaker", h, s);
      throw e;
    }
  }
  
  public void testBooleanOptionalWithTiebreaker() throws Exception {
    
    BooleanQuery q = new BooleanQuery();
    {
      DisjunctionMaxQuery q1 = new DisjunctionMaxQuery(0.01f);
      q1.add(tq("hed", "albino"));
      q1.add(tq("dek", "albino"));
      q.add(q1, BooleanClause.Occur.SHOULD);// false,false);
    }
    {
      DisjunctionMaxQuery q2 = new DisjunctionMaxQuery(0.01f);
      q2.add(tq("hed", "elephant"));
      q2.add(tq("dek", "elephant"));
      q.add(q2, BooleanClause.Occur.SHOULD);// false,false);
    }
    QueryUtils.check(random(), q, s);
    
    ScoreDoc[] h = s.search(q, null, 1000).scoreDocs;
    
    try {
      
      assertEquals("4 docs should match " + q.toString(), 4, h.length);
      
      float score0 = h[0].score;
      float score1 = h[1].score;
      float score2 = h[2].score;
      float score3 = h[3].score;
      
      String doc0 = s.doc(h[0].doc).get("id");
      String doc1 = s.doc(h[1].doc).get("id");
      String doc2 = s.doc(h[2].doc).get("id");
      String doc3 = s.doc(h[3].doc).get("id");
      
      assertTrue("doc0 should be d2 or d4: " + doc0, doc0.equals("d2")
          || doc0.equals("d4"));
      assertTrue("doc1 should be d2 or d4: " + doc0, doc1.equals("d2")
          || doc1.equals("d4"));
      assertEquals("score0 and score1 should match", score0, score1,
          SCORE_COMP_THRESH);
      assertEquals("wrong third", "d3", doc2);
      assertTrue("d3 does not have worse score then d2 and d4: " + score1
          + " >? " + score2, score1 > score2);
      
      assertEquals("wrong fourth", "d1", doc3);
      assertTrue("d1 does not have worse score then d3: " + score2 + " >? "
          + score3, score2 > score3);
      
    } catch (Error e) {
      printHits("testBooleanOptionalWithTiebreaker", h, s);
      throw e;
    }
    
  }
  
  public void testBooleanOptionalWithTiebreakerAndBoost() throws Exception {
    
    BooleanQuery q = new BooleanQuery();
    {
      DisjunctionMaxQuery q1 = new DisjunctionMaxQuery(0.01f);
      q1.add(tq("hed", "albino", 1.5f));
      q1.add(tq("dek", "albino"));
      q.add(q1, BooleanClause.Occur.SHOULD);// false,false);
    }
    {
      DisjunctionMaxQuery q2 = new DisjunctionMaxQuery(0.01f);
      q2.add(tq("hed", "elephant", 1.5f));
      q2.add(tq("dek", "elephant"));
      q.add(q2, BooleanClause.Occur.SHOULD);// false,false);
    }
    QueryUtils.check(random(), q, s);
    
    ScoreDoc[] h = s.search(q, null, 1000).scoreDocs;
    
    try {
      
      assertEquals("4 docs should match " + q.toString(), 4, h.length);
      
      float score0 = h[0].score;
      float score1 = h[1].score;
      float score2 = h[2].score;
      float score3 = h[3].score;
      
      String doc0 = s.doc(h[0].doc).get("id");
      String doc1 = s.doc(h[1].doc).get("id");
      String doc2 = s.doc(h[2].doc).get("id");
      String doc3 = s.doc(h[3].doc).get("id");
      
      assertEquals("doc0 should be d4: ", "d4", doc0);
      assertEquals("doc1 should be d3: ", "d3", doc1);
      assertEquals("doc2 should be d2: ", "d2", doc2);
      assertEquals("doc3 should be d1: ", "d1", doc3);
      
      assertTrue("d4 does not have a better score then d3: " + score0 + " >? "
          + score1, score0 > score1);
      assertTrue("d3 does not have a better score then d2: " + score1 + " >? "
          + score2, score1 > score2);
      assertTrue("d3 does not have a better score then d1: " + score2 + " >? "
          + score3, score2 > score3);
      
    } catch (Error e) {
      printHits("testBooleanOptionalWithTiebreakerAndBoost", h, s);
      throw e;
    }
  }
  
  /** macro */
  protected Query tq(String f, String t) {
    return new TermQuery(new Term(f, t));
  }
  
  /** macro */
  protected Query tq(String f, String t, float b) {
    Query q = tq(f, t);
    q.setBoost(b);
    return q;
  }
  
  protected void printHits(String test, ScoreDoc[] h, IndexSearcher searcher)
      throws Exception {
    
    System.err.println("------- " + test + " -------");
    
    DecimalFormat f = new DecimalFormat("0.000000000");
    
    for (int i = 0; i < h.length; i++) {
      Document d = searcher.doc(h[i].doc);
      float score = h[i].score;
      System.err
          .println("#" + i + ": " + f.format(score) + " - " + d.get("id"));
    }
  }
}
