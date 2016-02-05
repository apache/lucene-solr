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


import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.store.Directory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import junit.framework.Assert;

public class TestMultiTermConstantScore extends BaseTestRangeFilter {

  /** threshold for comparing floats */
  public static final float SCORE_COMP_THRESH = 1e-6f;

  static Directory small;
  static IndexReader reader;

  static public void assertEquals(String m, int e, int a) {
    Assert.assertEquals(m, e, a);
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    String[] data = new String[] { "A 1 2 3 4 5 6", "Z       4 5 6", null,
        "B   2   4 5 6", "Y     3   5 6", null, "C     3     6",
        "X       4 5 6" };

    small = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), small, 
        newIndexWriterConfig(
            new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false)).setMergePolicy(newLogMergePolicy()));

    FieldType customType = new FieldType(TextField.TYPE_STORED);
    customType.setTokenized(false);
    for (int i = 0; i < data.length; i++) {
      Document doc = new Document();
      doc.add(newField("id", String.valueOf(i), customType));// Field.Keyword("id",String.valueOf(i)));
      doc.add(newField("all", "all", customType));// Field.Keyword("all","all"));
      if (null != data[i]) {
        doc.add(newTextField("data", data[i], Field.Store.YES));// Field.Text("data",data[i]));
      }
      writer.addDocument(doc);
    }

    reader = writer.getReader();
    writer.close();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    reader.close();
    small.close();
    reader = null;
    small = null;
  }

  /** macro for readability */
  public static Query csrq(String f, String l, String h, boolean il, boolean ih) {
    TermRangeQuery query = TermRangeQuery.newStringRange(f, l, h, il, ih);
    query.setRewriteMethod(MultiTermQuery.CONSTANT_SCORE_REWRITE);
    if (VERBOSE) {
      System.out.println("TEST: query=" + query);
    }
    return query;
  }

  public static Query csrq(String f, String l, String h, boolean il, boolean ih, MultiTermQuery.RewriteMethod method) {
    TermRangeQuery query = TermRangeQuery.newStringRange(f, l, h, il, ih);
    query.setRewriteMethod(method);
    if (VERBOSE) {
      System.out.println("TEST: query=" + query + " method=" + method);
    }
    return query;
  }

  /** macro for readability */
  public static Query cspq(Term prefix) {
    PrefixQuery query = new PrefixQuery(prefix);
    query.setRewriteMethod(MultiTermQuery.CONSTANT_SCORE_REWRITE);
    return query;
  }

  /** macro for readability */
  public static Query cswcq(Term wild) {
    WildcardQuery query = new WildcardQuery(wild);
    query.setRewriteMethod(MultiTermQuery.CONSTANT_SCORE_REWRITE);
    return query;
  }

  @Test
  public void testBasics() throws IOException {
    QueryUtils.check(csrq("data", "1", "6", T, T));
    QueryUtils.check(csrq("data", "A", "Z", T, T));
    QueryUtils.checkUnequal(csrq("data", "1", "6", T, T), csrq("data", "A",
        "Z", T, T));

    QueryUtils.check(cspq(new Term("data", "p*u?")));
    QueryUtils.checkUnequal(cspq(new Term("data", "pre*")), cspq(new Term(
        "data", "pres*")));

    QueryUtils.check(cswcq(new Term("data", "p")));
    QueryUtils.checkUnequal(cswcq(new Term("data", "pre*n?t")), cswcq(new Term(
        "data", "pr*t?j")));
  }

  @Test
  public void testEqualScores() throws IOException {
    // NOTE: uses index build in *this* setUp

    IndexSearcher search = newSearcher(reader);

    ScoreDoc[] result;

    // some hits match more terms then others, score should be the same

    result = search.search(csrq("data", "1", "6", T, T), 1000).scoreDocs;
    int numHits = result.length;
    assertEquals("wrong number of results", 6, numHits);
    float score = result[0].score;
    for (int i = 1; i < numHits; i++) {
      assertEquals("score for " + i + " was not the same", score,
          result[i].score, SCORE_COMP_THRESH);
    }

    result = search.search(csrq("data", "1", "6", T, T, MultiTermQuery.CONSTANT_SCORE_BOOLEAN_REWRITE), 1000).scoreDocs;
    numHits = result.length;
    assertEquals("wrong number of results", 6, numHits);
    for (int i = 0; i < numHits; i++) {
      assertEquals("score for " + i + " was not the same", score,
          result[i].score, SCORE_COMP_THRESH);
    }

    result = search.search(csrq("data", "1", "6", T, T, MultiTermQuery.CONSTANT_SCORE_REWRITE), 1000).scoreDocs;
    numHits = result.length;
    assertEquals("wrong number of results", 6, numHits);
    for (int i = 0; i < numHits; i++) {
      assertEquals("score for " + i + " was not the same", score,
          result[i].score, SCORE_COMP_THRESH);
    }
  }

  @Test // Test for LUCENE-5245: Empty MTQ rewrites should have a consistent norm, so always need to return a CSQ!
  public void testEqualScoresWhenNoHits() throws IOException {
    // NOTE: uses index build in *this* setUp

    IndexSearcher search = newSearcher(reader);

    ScoreDoc[] result;

    TermQuery dummyTerm = new TermQuery(new Term("data", "1"));

    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.add(dummyTerm, BooleanClause.Occur.SHOULD); // hits one doc
    bq.add(csrq("data", "#", "#", T, T), BooleanClause.Occur.SHOULD); // hits no docs
    result = search.search(bq.build(), 1000).scoreDocs;
    int numHits = result.length;
    assertEquals("wrong number of results", 1, numHits);
    float score = result[0].score;
    for (int i = 1; i < numHits; i++) {
      assertEquals("score for " + i + " was not the same", score,
          result[i].score, SCORE_COMP_THRESH);
    }

    bq = new BooleanQuery.Builder();
    bq.add(dummyTerm, BooleanClause.Occur.SHOULD); // hits one doc
    bq.add(csrq("data", "#", "#", T, T, MultiTermQuery.CONSTANT_SCORE_BOOLEAN_REWRITE), BooleanClause.Occur.SHOULD); // hits no docs
    result = search.search(bq.build(), 1000).scoreDocs;
    numHits = result.length;
    assertEquals("wrong number of results", 1, numHits);
    for (int i = 0; i < numHits; i++) {
      assertEquals("score for " + i + " was not the same", score,
          result[i].score, SCORE_COMP_THRESH);
    }

    bq = new BooleanQuery.Builder();
    bq.add(dummyTerm, BooleanClause.Occur.SHOULD); // hits one doc
    bq.add(csrq("data", "#", "#", T, T, MultiTermQuery.CONSTANT_SCORE_REWRITE), BooleanClause.Occur.SHOULD); // hits no docs
    result = search.search(bq.build(), 1000).scoreDocs;
    numHits = result.length;
    assertEquals("wrong number of results", 1, numHits);
    for (int i = 0; i < numHits; i++) {
      assertEquals("score for " + i + " was not the same", score,
          result[i].score, SCORE_COMP_THRESH);
    }
  }

  @Test
  public void testBoost() throws IOException {
    // NOTE: uses index build in *this* setUp

    IndexSearcher search = newSearcher(reader);

    // test for correct application of query normalization
    // must use a non score normalizing method for this.
    
    search.setSimilarity(new ClassicSimilarity());
    Query q = csrq("data", "1", "6", T, T);
    search.search(new BoostQuery(q, 100), new SimpleCollector() {
      private int base = 0;
      private Scorer scorer;
      @Override
      public void setScorer(Scorer scorer) {
        this.scorer = scorer;
      }
      @Override
      public void collect(int doc) throws IOException {
        assertEquals("score for doc " + (doc + base) + " was not correct", 1.0f, scorer.score(), SCORE_COMP_THRESH);
      }
      @Override
      protected void doSetNextReader(LeafReaderContext context) throws IOException {
        base = context.docBase;
      }
      
      @Override
      public boolean needsScores() {
        return true;
      }
    });

    //
    // Ensure that boosting works to score one clause of a query higher
    // than another.
    //
    Query q1 = new BoostQuery(csrq("data", "A", "A", T, T), .1f); // matches document #0
    Query q2 = csrq("data", "Z", "Z", T, T); // matches document #1
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.setDisableCoord(true);
    bq.add(q1, BooleanClause.Occur.SHOULD);
    bq.add(q2, BooleanClause.Occur.SHOULD);

    ScoreDoc[] hits = search.search(bq.build(), 1000).scoreDocs;
    Assert.assertEquals(1, hits[0].doc);
    Assert.assertEquals(0, hits[1].doc);
    assertTrue(hits[0].score > hits[1].score);

    q1 = new BoostQuery(csrq("data", "A", "A", T, T, MultiTermQuery.CONSTANT_SCORE_BOOLEAN_REWRITE), .1f); // matches document #0
    q2 = csrq("data", "Z", "Z", T, T, MultiTermQuery.CONSTANT_SCORE_BOOLEAN_REWRITE); // matches document #1
    bq = new BooleanQuery.Builder();
    bq.setDisableCoord(true);
    bq.add(q1, BooleanClause.Occur.SHOULD);
    bq.add(q2, BooleanClause.Occur.SHOULD);

    hits = search.search(bq.build(), 1000).scoreDocs;
    Assert.assertEquals(1, hits[0].doc);
    Assert.assertEquals(0, hits[1].doc);
    assertTrue(hits[0].score > hits[1].score);

    q1 = new BoostQuery(csrq("data", "A", "A", T, T), 10f); // matches document #0
    q2 = csrq("data", "Z", "Z", T, T); // matches document #1
    bq = new BooleanQuery.Builder();
    bq.add(q1, BooleanClause.Occur.SHOULD);
    bq.add(q2, BooleanClause.Occur.SHOULD);

    hits = search.search(bq.build(), 1000).scoreDocs;
    Assert.assertEquals(0, hits[0].doc);
    Assert.assertEquals(1, hits[1].doc);
    assertTrue(hits[0].score > hits[1].score);
  }

  @Test
  public void testBooleanOrderUnAffected() throws IOException {
    // NOTE: uses index build in *this* setUp

    IndexSearcher search = newSearcher(reader);

    // first do a regular TermRangeQuery which uses term expansion so
    // docs with more terms in range get higher scores

    Query rq = TermRangeQuery.newStringRange("data", "1", "4", T, T);

    ScoreDoc[] expected = search.search(rq, 1000).scoreDocs;
    int numHits = expected.length;

    // now do a boolean where which also contains a
    // ConstantScoreRangeQuery and make sure hte order is the same

    BooleanQuery.Builder q = new BooleanQuery.Builder();
    q.add(rq, BooleanClause.Occur.MUST);// T, F);
    q.add(csrq("data", "1", "6", T, T), BooleanClause.Occur.MUST);// T, F);

    ScoreDoc[] actual = search.search(q.build(), 1000).scoreDocs;

    assertEquals("wrong numebr of hits", numHits, actual.length);
    for (int i = 0; i < numHits; i++) {
      assertEquals("mismatch in docid for hit#" + i, expected[i].doc,
          actual[i].doc);
    }
  }

  @Test
  public void testRangeQueryId() throws IOException {
    // NOTE: uses index build in *super* setUp

    IndexReader reader = signedIndexReader;
    IndexSearcher search = newSearcher(reader);

    if (VERBOSE) {
      System.out.println("TEST: reader=" + reader);
    }

    int medId = ((maxId - minId) / 2);

    String minIP = pad(minId);
    String maxIP = pad(maxId);
    String medIP = pad(medId);

    int numDocs = reader.numDocs();

    assertEquals("num of docs", numDocs, 1 + maxId - minId);

    ScoreDoc[] result;

    // test id, bounded on both ends

    result = search.search(csrq("id", minIP, maxIP, T, T), numDocs).scoreDocs;
    assertEquals("find all", numDocs, result.length);

    result = search.search(csrq("id", minIP, maxIP, T, T, MultiTermQuery.CONSTANT_SCORE_REWRITE), numDocs).scoreDocs;
    assertEquals("find all", numDocs, result.length);

    result = search.search(csrq("id", minIP, maxIP, T, F), numDocs).scoreDocs;
    assertEquals("all but last", numDocs - 1, result.length);

    result = search.search(csrq("id", minIP, maxIP, T, F, MultiTermQuery.CONSTANT_SCORE_REWRITE), numDocs).scoreDocs;
    assertEquals("all but last", numDocs - 1, result.length);

    result = search.search(csrq("id", minIP, maxIP, F, T), numDocs).scoreDocs;
    assertEquals("all but first", numDocs - 1, result.length);

    result = search.search(csrq("id", minIP, maxIP, F, T, MultiTermQuery.CONSTANT_SCORE_REWRITE), numDocs).scoreDocs;
    assertEquals("all but first", numDocs - 1, result.length);

    result = search.search(csrq("id", minIP, maxIP, F, F), numDocs).scoreDocs;
    assertEquals("all but ends", numDocs - 2, result.length);

    result = search.search(csrq("id", minIP, maxIP, F, F, MultiTermQuery.CONSTANT_SCORE_REWRITE), numDocs).scoreDocs;
    assertEquals("all but ends", numDocs - 2, result.length);

    result = search.search(csrq("id", medIP, maxIP, T, T), numDocs).scoreDocs;
    assertEquals("med and up", 1 + maxId - medId, result.length);

    result = search.search(csrq("id", medIP, maxIP, T, T, MultiTermQuery.CONSTANT_SCORE_REWRITE), numDocs).scoreDocs;
    assertEquals("med and up", 1 + maxId - medId, result.length);

    result = search.search(csrq("id", minIP, medIP, T, T), numDocs).scoreDocs;
    assertEquals("up to med", 1 + medId - minId, result.length);

    result = search.search(csrq("id", minIP, medIP, T, T, MultiTermQuery.CONSTANT_SCORE_REWRITE), numDocs).scoreDocs;
    assertEquals("up to med", 1 + medId - minId, result.length);

    // unbounded id

    result = search.search(csrq("id", minIP, null, T, F), numDocs).scoreDocs;
    assertEquals("min and up", numDocs, result.length);

    result = search.search(csrq("id", null, maxIP, F, T), numDocs).scoreDocs;
    assertEquals("max and down", numDocs, result.length);

    result = search.search(csrq("id", minIP, null, F, F), numDocs).scoreDocs;
    assertEquals("not min, but up", numDocs - 1, result.length);

    result = search.search(csrq("id", null, maxIP, F, F), numDocs).scoreDocs;
    assertEquals("not max, but down", numDocs - 1, result.length);

    result = search.search(csrq("id", medIP, maxIP, T, F), numDocs).scoreDocs;
    assertEquals("med and up, not max", maxId - medId, result.length);

    result = search.search(csrq("id", minIP, medIP, F, T), numDocs).scoreDocs;
    assertEquals("not min, up to med", medId - minId, result.length);

    // very small sets

    result = search.search(csrq("id", minIP, minIP, F, F), numDocs).scoreDocs;
    assertEquals("min,min,F,F", 0, result.length);

    result = search.search(csrq("id", minIP, minIP, F, F, MultiTermQuery.CONSTANT_SCORE_REWRITE), numDocs).scoreDocs;
    assertEquals("min,min,F,F", 0, result.length);

    result = search.search(csrq("id", medIP, medIP, F, F), numDocs).scoreDocs;
    assertEquals("med,med,F,F", 0, result.length);

    result = search.search(csrq("id", medIP, medIP, F, F, MultiTermQuery.CONSTANT_SCORE_REWRITE), numDocs).scoreDocs;
    assertEquals("med,med,F,F", 0, result.length);

    result = search.search(csrq("id", maxIP, maxIP, F, F), numDocs).scoreDocs;
    assertEquals("max,max,F,F", 0, result.length);

    result = search.search(csrq("id", maxIP, maxIP, F, F, MultiTermQuery.CONSTANT_SCORE_REWRITE), numDocs).scoreDocs;
    assertEquals("max,max,F,F", 0, result.length);

    result = search.search(csrq("id", minIP, minIP, T, T), numDocs).scoreDocs;
    assertEquals("min,min,T,T", 1, result.length);

    result = search.search(csrq("id", minIP, minIP, T, T, MultiTermQuery.CONSTANT_SCORE_REWRITE), numDocs).scoreDocs;
    assertEquals("min,min,T,T", 1, result.length);

    result = search.search(csrq("id", null, minIP, F, T), numDocs).scoreDocs;
    assertEquals("nul,min,F,T", 1, result.length);

    result = search.search(csrq("id", null, minIP, F, T, MultiTermQuery.CONSTANT_SCORE_REWRITE), numDocs).scoreDocs;
    assertEquals("nul,min,F,T", 1, result.length);

    result = search.search(csrq("id", maxIP, maxIP, T, T), numDocs).scoreDocs;
    assertEquals("max,max,T,T", 1, result.length);

    result = search.search(csrq("id", maxIP, maxIP, T, T, MultiTermQuery.CONSTANT_SCORE_REWRITE), numDocs).scoreDocs;
    assertEquals("max,max,T,T", 1, result.length);

    result = search.search(csrq("id", maxIP, null, T, F), numDocs).scoreDocs;
    assertEquals("max,nul,T,T", 1, result.length);

    result = search.search(csrq("id", maxIP, null, T, F, MultiTermQuery.CONSTANT_SCORE_REWRITE), numDocs).scoreDocs;
    assertEquals("max,nul,T,T", 1, result.length);

    result = search.search(csrq("id", medIP, medIP, T, T), numDocs).scoreDocs;
    assertEquals("med,med,T,T", 1, result.length);

    result = search.search(csrq("id", medIP, medIP, T, T, MultiTermQuery.CONSTANT_SCORE_REWRITE), numDocs).scoreDocs;
    assertEquals("med,med,T,T", 1, result.length);
  }

  @Test
  public void testRangeQueryRand() throws IOException {
    // NOTE: uses index build in *super* setUp

    IndexReader reader = signedIndexReader;
    IndexSearcher search = newSearcher(reader);

    String minRP = pad(signedIndexDir.minR);
    String maxRP = pad(signedIndexDir.maxR);

    int numDocs = reader.numDocs();

    assertEquals("num of docs", numDocs, 1 + maxId - minId);

    ScoreDoc[] result;

    // test extremes, bounded on both ends

    result = search.search(csrq("rand", minRP, maxRP, T, T), numDocs).scoreDocs;
    assertEquals("find all", numDocs, result.length);

    result = search.search(csrq("rand", minRP, maxRP, T, F), numDocs).scoreDocs;
    assertEquals("all but biggest", numDocs - 1, result.length);

    result = search.search(csrq("rand", minRP, maxRP, F, T), numDocs).scoreDocs;
    assertEquals("all but smallest", numDocs - 1, result.length);

    result = search.search(csrq("rand", minRP, maxRP, F, F), numDocs).scoreDocs;
    assertEquals("all but extremes", numDocs - 2, result.length);

    // unbounded

    result = search.search(csrq("rand", minRP, null, T, F), numDocs).scoreDocs;
    assertEquals("smallest and up", numDocs, result.length);

    result = search.search(csrq("rand", null, maxRP, F, T), numDocs).scoreDocs;
    assertEquals("biggest and down", numDocs, result.length);

    result = search.search(csrq("rand", minRP, null, F, F), numDocs).scoreDocs;
    assertEquals("not smallest, but up", numDocs - 1, result.length);

    result = search.search(csrq("rand", null, maxRP, F, F), numDocs).scoreDocs;
    assertEquals("not biggest, but down", numDocs - 1, result.length);

    // very small sets

    result = search.search(csrq("rand", minRP, minRP, F, F), numDocs).scoreDocs;
    assertEquals("min,min,F,F", 0, result.length);
    result = search.search(csrq("rand", maxRP, maxRP, F, F), numDocs).scoreDocs;
    assertEquals("max,max,F,F", 0, result.length);

    result = search.search(csrq("rand", minRP, minRP, T, T), numDocs).scoreDocs;
    assertEquals("min,min,T,T", 1, result.length);
    result = search.search(csrq("rand", null, minRP, F, T), numDocs).scoreDocs;
    assertEquals("nul,min,F,T", 1, result.length);

    result = search.search(csrq("rand", maxRP, maxRP, T, T), numDocs).scoreDocs;
    assertEquals("max,max,T,T", 1, result.length);
    result = search.search(csrq("rand", maxRP, null, T, F), numDocs).scoreDocs;
    assertEquals("max,nul,T,T", 1, result.length);
  }
}
