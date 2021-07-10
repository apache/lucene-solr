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
import java.io.StringReader;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

/**
 * Test of the DisjunctionMaxQuery.
 * 
 */
@LuceneTestCase.SuppressCodecs("SimpleText")
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
  private static class TestSimilarity extends ClassicSimilarity {
    
    public TestSimilarity() {}
    
    @Override
    public float tf(float freq) {
      if (freq > 0.0f) return 1.0f;
      else return 0.0f;
    }
    
    @Override
    public float lengthNorm(int length) {
      // Disable length norm
      return 1;
    }
    
    @Override
    public float idf(long docFreq, long docCount) {
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
        newIndexWriterConfig(new MockAnalyzer(random()))
                             .setSimilarity(sim).setMergePolicy(newLogMergePolicy()));
    
    // hed is the most important field, dek is secondary
    
    // d1 is an "ok" match for: albino elephant
    {
      Document d1 = new Document();
      d1.add(newField("id", "d1", nonAnalyzedType));// Field.Keyword("id",
                                                                               // "d1"));
      d1
          .add(newTextField("hed", "elephant", Field.Store.YES));// Field.Text("hed", "elephant"));
      d1
          .add(newTextField("dek", "elephant", Field.Store.YES));// Field.Text("dek", "elephant"));
      writer.addDocument(d1);
    }
    
    // d2 is a "good" match for: albino elephant
    {
      Document d2 = new Document();
      d2.add(newField("id", "d2", nonAnalyzedType));// Field.Keyword("id",
                                                                               // "d2"));
      d2
          .add(newTextField("hed", "elephant", Field.Store.YES));// Field.Text("hed", "elephant"));
      d2.add(newTextField("dek", "albino", Field.Store.YES));// Field.Text("dek",
                                                                                // "albino"));
      d2
          .add(newTextField("dek", "elephant", Field.Store.YES));// Field.Text("dek", "elephant"));
      writer.addDocument(d2);
    }
    
    // d3 is a "better" match for: albino elephant
    {
      Document d3 = new Document();
      d3.add(newField("id", "d3", nonAnalyzedType));// Field.Keyword("id",
                                                                               // "d3"));
      d3.add(newTextField("hed", "albino", Field.Store.YES));// Field.Text("hed",
                                                                                // "albino"));
      d3
          .add(newTextField("hed", "elephant", Field.Store.YES));// Field.Text("hed", "elephant"));
      writer.addDocument(d3);
    }
    
    // d4 is the "best" match for: albino elephant
    {
      Document d4 = new Document();
      d4.add(newField("id", "d4", nonAnalyzedType));// Field.Keyword("id",
                                                                               // "d4"));
      d4.add(newTextField("hed", "albino", Field.Store.YES));// Field.Text("hed",
                                                                                // "albino"));
      d4
          .add(newField("hed", "elephant", nonAnalyzedType));// Field.Text("hed", "elephant"));
      d4.add(newTextField("dek", "albino", Field.Store.YES));// Field.Text("dek",
                                                                                // "albino"));
      writer.addDocument(d4);
    }
    
    writer.forceMerge(1);
    r = getOnlyLeafReader(writer.getReader());
    writer.close();
    s = new IndexSearcher(r);
    s.setSimilarity(sim);
  }
  
  @Override
  public void tearDown() throws Exception {
    r.close();
    index.close();
    super.tearDown();
  }
  
  public void testSkipToFirsttimeMiss() throws IOException {
    final DisjunctionMaxQuery dq = new DisjunctionMaxQuery(
        Arrays.asList(tq("id", "d1"), tq("dek", "DOES_NOT_EXIST")), 0.0f);

    QueryUtils.check(random(), dq, s);
    assertTrue(s.getTopReaderContext() instanceof LeafReaderContext);
    final Weight dw = s.createWeight(s.rewrite(dq), ScoreMode.COMPLETE, 1);
    LeafReaderContext context = (LeafReaderContext)s.getTopReaderContext();
    final Scorer ds = dw.scorer(context);
    final boolean skipOk = ds.iterator().advance(3) != DocIdSetIterator.NO_MORE_DOCS;
    if (skipOk) {
      fail("firsttime skipTo found a match? ... "
          + r.document(ds.docID()).get("id"));
    }
  }
  
  public void testSkipToFirsttimeHit() throws IOException {
    final DisjunctionMaxQuery dq = new DisjunctionMaxQuery(
        Arrays.asList(tq("dek", "albino"), tq("dek", "DOES_NOT_EXIST")), 0.0f);

    assertTrue(s.getTopReaderContext() instanceof LeafReaderContext);
    QueryUtils.check(random(), dq, s);
    final Weight dw = s.createWeight(s.rewrite(dq), ScoreMode.COMPLETE, 1);
    LeafReaderContext context = (LeafReaderContext)s.getTopReaderContext();
    final Scorer ds = dw.scorer(context);
    assertTrue("firsttime skipTo found no match",
        ds.iterator().advance(3) != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals("found wrong docid", "d4", r.document(ds.docID()).get("id"));
  }
  
  public void testSimpleEqualScores1() throws Exception {
    
    DisjunctionMaxQuery q = new DisjunctionMaxQuery(
        Arrays.asList(tq("hed", "albino"), tq("hed", "elephant")),
        0.0f);
    QueryUtils.check(random(), q, s);
    
    ScoreDoc[] h = s.search(q, 1000).scoreDocs;
    
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
    
    DisjunctionMaxQuery q = new DisjunctionMaxQuery(
        Arrays.asList(tq("dek", "albino"), tq("dek", "elephant")),
        0.0f);
    QueryUtils.check(random(), q, s);
    
    ScoreDoc[] h = s.search(q, 1000).scoreDocs;
    
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
    
    DisjunctionMaxQuery q = new DisjunctionMaxQuery(
        Arrays.asList(
            tq("hed", "albino"),
            tq("hed", "elephant"),
            tq("dek", "albino"),
            tq("dek", "elephant")),
        0.0f);
    QueryUtils.check(random(), q, s);
    
    ScoreDoc[] h = s.search(q, 1000).scoreDocs;
    
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
    
    DisjunctionMaxQuery q = new DisjunctionMaxQuery(
        Arrays.asList(tq("dek", "albino"), tq("dek", "elephant")),
        0.01f);
    QueryUtils.check(random(), q, s);
    
    ScoreDoc[] h = s.search(q, 1000).scoreDocs;
    
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
    
    BooleanQuery.Builder q = new BooleanQuery.Builder();
    {
      DisjunctionMaxQuery q1 = new DisjunctionMaxQuery(
          Arrays.asList(tq("hed", "albino"), tq("dek", "albino")),
          0.0f);
      q.add(q1, BooleanClause.Occur.MUST);// true,false);
      QueryUtils.check(random(), q1, s);
      
    }
    {
      DisjunctionMaxQuery q2 = new DisjunctionMaxQuery(
          Arrays.asList(tq("hed", "elephant"), tq("dek", "elephant")),
          0.0f);
      q.add(q2, BooleanClause.Occur.MUST);// true,false);
      QueryUtils.check(random(), q2, s);
    }
    
    QueryUtils.check(random(), q.build(), s);
    
    ScoreDoc[] h = s.search(q.build(), 1000).scoreDocs;
    
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
    
    BooleanQuery.Builder q = new BooleanQuery.Builder();
    {
      DisjunctionMaxQuery q1 = new DisjunctionMaxQuery(
          Arrays.asList(tq("hed", "albino"), tq("dek", "albino")),
          0.0f);
      q.add(q1, BooleanClause.Occur.SHOULD);// false,false);
    }
    {
      DisjunctionMaxQuery q2 = new DisjunctionMaxQuery(
          Arrays.asList(tq("hed", "elephant"), tq("dek", "elephant")),
          0.0f);
      q.add(q2, BooleanClause.Occur.SHOULD);// false,false);
    }
    QueryUtils.check(random(), q.build(), s);
    
    ScoreDoc[] h = s.search(q.build(), 1000).scoreDocs;
    
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
    
    BooleanQuery.Builder q = new BooleanQuery.Builder();
    {
      DisjunctionMaxQuery q1 = new DisjunctionMaxQuery(
          Arrays.asList(tq("hed", "albino"), tq("dek", "albino")),
          0.01f);
      q.add(q1, BooleanClause.Occur.SHOULD);// false,false);
    }
    {
      DisjunctionMaxQuery q2 = new DisjunctionMaxQuery(
          Arrays.asList(tq("hed", "elephant"), tq("dek", "elephant")),
          0.01f);
      q.add(q2, BooleanClause.Occur.SHOULD);// false,false);
    }
    QueryUtils.check(random(), q.build(), s);
    
    ScoreDoc[] h = s.search(q.build(), 1000).scoreDocs;
    
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
    
    BooleanQuery.Builder q = new BooleanQuery.Builder();
    {
      DisjunctionMaxQuery q1 = new DisjunctionMaxQuery(
          Arrays.asList(tq("hed", "albino", 1.5f), tq("dek", "albino")),
          0.01f);
      q.add(q1, BooleanClause.Occur.SHOULD);// false,false);
    }
    {
      DisjunctionMaxQuery q2 = new DisjunctionMaxQuery(
          Arrays.asList(tq("hed", "elephant", 1.5f), tq("dek", "elephant")),
          0.01f);
      q.add(q2, BooleanClause.Occur.SHOULD);// false,false);
    }
    QueryUtils.check(random(), q.build(), s);
    
    ScoreDoc[] h = s.search(q.build(), 1000).scoreDocs;
    
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
  
  // LUCENE-4477 / LUCENE-4401:
  public void testBooleanSpanQuery() throws Exception {
    int hits = 0;
    Directory directory = newDirectory();
    Analyzer indexerAnalyzer = new MockAnalyzer(random());

    IndexWriterConfig config = new IndexWriterConfig(indexerAnalyzer);
    IndexWriter writer = new IndexWriter(directory, config);
    String FIELD = "content";
    Document d = new Document();
    d.add(new TextField(FIELD, "clockwork orange", Field.Store.YES));
    writer.addDocument(d);
    writer.close();

    IndexReader indexReader = DirectoryReader.open(directory);
    IndexSearcher searcher = newSearcher(indexReader);

    DisjunctionMaxQuery query = new DisjunctionMaxQuery(
        Arrays.asList(
            new SpanTermQuery(new Term(FIELD, "clockwork")),
            new SpanTermQuery(new Term(FIELD, "clckwork"))),
        1.0f);
    TopScoreDocCollector collector = TopScoreDocCollector.create(1000, Integer.MAX_VALUE);
    searcher.search(query, collector);
    hits = collector.topDocs().scoreDocs.length;
    for (ScoreDoc scoreDoc : collector.topDocs().scoreDocs){
      System.out.println(scoreDoc.doc);
    }
    indexReader.close();
    assertEquals(hits, 1);
    directory.close();
  }

  public void testRewriteBoolean() throws Exception {
    Query sub1 = tq("hed", "albino");
    Query sub2 = tq("hed", "elephant");
    DisjunctionMaxQuery q = new DisjunctionMaxQuery(
        Arrays.asList(
            sub1, sub2
        ), 1.0f);
    Query rewritten = s.rewrite(q);
    assertTrue(rewritten instanceof BooleanQuery);
    BooleanQuery bq = (BooleanQuery) rewritten;
    assertEquals(bq.clauses().size(), 2);
    assertEquals(bq.clauses().get(0), new BooleanClause(sub1, BooleanClause.Occur.SHOULD));
    assertEquals(bq.clauses().get(1), new BooleanClause(sub2, BooleanClause.Occur.SHOULD));
  }

  public void testRewriteEmpty() throws Exception {
    DisjunctionMaxQuery q = new DisjunctionMaxQuery(Collections.emptyList(), 0.0f);
    Query rewritten = s.rewrite(q);
    Query expected = new MatchNoDocsQuery();
    assertEquals(expected, rewritten);
  }

  public void testRandomTopDocs() throws Exception {
    doTestRandomTopDocs(2, 0.05f, 0.05f);
    doTestRandomTopDocs(2, 1.0f, 0.05f);
    doTestRandomTopDocs(3, 1.0f, 0.5f, 0.05f);
    doTestRandomTopDocs(4, 1.0f, 0.5f, 0.05f, 0f);
    doTestRandomTopDocs(4, 1.0f, 0.5f, 0.05f, 0f);
  }

  private void doTestRandomTopDocs(int numFields, double... freqs) throws IOException {
    assert numFields == freqs.length;
    Directory dir = newDirectory();
    IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
    IndexWriter w = new IndexWriter(dir, config);

    int numDocs = TEST_NIGHTLY ? atLeast(1000) : atLeast(100); // at night, make sure some terms have skip data
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      for (int j = 0; j < numFields; j++) {
        StringBuilder builder = new StringBuilder();
        int numAs = random().nextDouble() < freqs[j] ? 0 : 1 + random().nextInt(5);
        for (int k = 0; k < numAs; k++) {
          if (builder.length() > 0) {
            builder.append(' ');
          }
          builder.append('a');
        }
        if (random().nextBoolean()) {
          doc.add(new StringField("field", "c", Field.Store.NO));
        }
        int numOthers = random().nextBoolean() ? 0 : 1 + random().nextInt(5);
        for (int k = 0; k < numOthers; k++) {
          if (builder.length() > 0) {
            builder.append(' ');
          }
          builder.append(Integer.toString(random().nextInt()));
        }
        doc.add(new TextField(Integer.toString(j), new StringReader(builder.toString())));
      }
      w.addDocument(doc);
    }
    IndexReader reader = DirectoryReader.open(w);
    w.close();
    IndexSearcher searcher = newSearcher(reader);
    for (int i = 0; i < 4; i++) {
      List<Query> clauses = new ArrayList<>();
      for (int j = 0; j < numFields; j++) {
        if (i % 2 == 1) {
          clauses.add(tq(Integer.toString(j), "a"));
        } else {
          float boost = random().nextBoolean() ? 0 : random().nextFloat();
          if (boost > 0) {
            clauses.add(tq(Integer.toString(j), "a", boost));
          } else {
            clauses.add(tq(Integer.toString(j), "a"));
          }
        }
      }
      float tieBreaker = random().nextFloat();
      Query query = new DisjunctionMaxQuery(clauses, tieBreaker);
      CheckHits.checkTopScores(random(), query, searcher);

      query = new BooleanQuery.Builder()
          .add(new DisjunctionMaxQuery(clauses, tieBreaker), BooleanClause.Occur.MUST)
          .add(tq("field", "c"), BooleanClause.Occur.FILTER)
          .build();
      CheckHits.checkTopScores(random(), query, searcher);
    }
    reader.close();
    dir.close();
  }
  
  /** macro */
  protected Query tq(String f, String t) {
    return new TermQuery(new Term(f, t));
  }
  
  /** macro */
  protected Query tq(String f, String t, float b) {
    Query q = tq(f, t);
    return new BoostQuery(q, b);
  }
  
  protected void printHits(String test, ScoreDoc[] h, IndexSearcher searcher)
      throws Exception {
    
    System.err.println("------- " + test + " -------");
    
    DecimalFormat f = new DecimalFormat("0.000000000", DecimalFormatSymbols.getInstance(Locale.ROOT));
    
    for (int i = 0; i < h.length; i++) {
      Document d = searcher.doc(h[i].doc);
      float score = h[i].score;
      System.err
          .println("#" + i + ": " + f.format(score) + " - " + d.get("id"));
    }
  }
}
