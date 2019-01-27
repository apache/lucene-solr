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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.LuceneTestCase;

public class TestSloppyPhraseQuery extends LuceneTestCase {

  private static final String S_1 = "A A A";
  private static final String S_2 = "A 1 2 3 A 4 5 6 A";

  private static final Document DOC_1 = makeDocument("X " + S_1 + " Y");
  private static final Document DOC_2 = makeDocument("X " + S_2 + " Y");
  private static final Document DOC_3 = makeDocument("X " + S_1 + " A Y");
  private static final Document DOC_1_B = makeDocument("X " + S_1 + " Y N N N N " + S_1 + " Z");
  private static final Document DOC_2_B = makeDocument("X " + S_2 + " Y N N N N " + S_2 + " Z");
  private static final Document DOC_3_B = makeDocument("X " + S_1 + " A Y N N N N " + S_1 + " A Y");
  private static final Document DOC_4 = makeDocument("A A X A X B A X B B A A X B A A");
  private static final Document DOC_5_3 = makeDocument("H H H X X X H H H X X X H H H");
  private static final Document DOC_5_4 = makeDocument("H H H H");

  private static final PhraseQuery QUERY_1 = makePhraseQuery( S_1 );
  private static final PhraseQuery QUERY_2 = makePhraseQuery( S_2 );
  private static final PhraseQuery QUERY_4 = makePhraseQuery( "X A A");
  private static final PhraseQuery QUERY_5_4 = makePhraseQuery( "H H H H");

  /**
   * Test DOC_4 and QUERY_4.
   * QUERY_4 has a fuzzy (len=1) match to DOC_4, so all slop values &gt; 0 should succeed.
   * But only the 3rd sequence of A's in DOC_4 will do.
   */
  public void testDoc4_Query4_All_Slops_Should_match() throws Exception {
    for (int slop=0; slop<30; slop++) {
      int numResultsExpected = slop<1 ? 0 : 1;
      checkPhraseQuery(DOC_4, QUERY_4, slop, numResultsExpected);
    }
  }

  /**
   * Test DOC_1 and QUERY_1.
   * QUERY_1 has an exact match to DOC_1, so all slop values should succeed.
   * Before LUCENE-1310, a slop value of 1 did not succeed.
   */
  public void testDoc1_Query1_All_Slops_Should_match() throws Exception {
    for (int slop=0; slop<30; slop++) {
      float freq1 = checkPhraseQuery(DOC_1, QUERY_1, slop, 1);
      float freq2 = checkPhraseQuery(DOC_1_B, QUERY_1, slop, 1);
      assertTrue("slop="+slop+" freq2="+freq2+" should be greater than score1 "+freq1, freq2>freq1);
    }
  }

  /**
   * Test DOC_2 and QUERY_1.
   * 6 should be the minimum slop to make QUERY_1 match DOC_2.
   * Before LUCENE-1310, 7 was the minimum.
   */
  public void testDoc2_Query1_Slop_6_or_more_Should_match() throws Exception {
    for (int slop=0; slop<30; slop++) {
      int numResultsExpected = slop<6 ? 0 : 1;
      float freq1 = checkPhraseQuery(DOC_2, QUERY_1, slop, numResultsExpected);
      if (numResultsExpected>0) {
        float freq2 = checkPhraseQuery(DOC_2_B, QUERY_1, slop, 1);
        assertTrue("slop="+slop+" freq2="+freq2+" should be greater than freq1 "+freq1, freq2>freq1);
      }
    }
  }

  /**
   * Test DOC_2 and QUERY_2.
   * QUERY_2 has an exact match to DOC_2, so all slop values should succeed.
   * Before LUCENE-1310, 0 succeeds, 1 through 7 fail, and 8 or greater succeeds.
   */
  public void testDoc2_Query2_All_Slops_Should_match() throws Exception {
    for (int slop=0; slop<30; slop++) {
      float freq1 = checkPhraseQuery(DOC_2, QUERY_2, slop, 1);
      float freq2 = checkPhraseQuery(DOC_2_B, QUERY_2, slop, 1);
      assertTrue("slop="+slop+" freq2="+freq2+" should be greater than freq1 "+freq1, freq2>freq1);
    }
  }

  /**
   * Test DOC_3 and QUERY_1.
   * QUERY_1 has an exact match to DOC_3, so all slop values should succeed.
   */
  public void testDoc3_Query1_All_Slops_Should_match() throws Exception {
    for (int slop=0; slop<30; slop++) {
      float freq1 = checkPhraseQuery(DOC_3, QUERY_1, slop, 1);
      float freq2 = checkPhraseQuery(DOC_3_B, QUERY_1, slop, 1);
      assertTrue("slop="+slop+" freq2="+freq2+" should be greater than freq1 "+freq1, freq2>freq1);
    }
  }

  /** LUCENE-3412 */
  public void testDoc5_Query5_Any_Slop_Should_be_consistent() throws Exception {
    int nRepeats = 5;
    for (int slop=0; slop<3; slop++) {
      for (int trial=0; trial<nRepeats; trial++) {
        // should steadily always find this one
        checkPhraseQuery(DOC_5_4, QUERY_5_4, slop, 1);
      }
      for (int trial=0; trial<nRepeats; trial++) {
        // should steadily never find this one
        checkPhraseQuery(DOC_5_3, QUERY_5_4, slop, 0);
      }
    }
  }
  
  private float  checkPhraseQuery(Document doc, PhraseQuery query, int slop, int expectedNumResults) throws Exception {
    PhraseQuery.Builder builder = new PhraseQuery.Builder();
    Term[] terms = query.getTerms();
    int[] positions = query.getPositions();
    for (int i = 0; i < terms.length; ++i) {
      builder.add(terms[i], positions[i]);
    }
    builder.setSlop(slop);
    query = builder.build();

    MockDirectoryWrapper ramDir = new MockDirectoryWrapper(random(), new RAMDirectory());
    RandomIndexWriter writer = new RandomIndexWriter(random(), ramDir, new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false));
    writer.addDocument(doc);

    IndexReader reader = writer.getReader();

    IndexSearcher searcher = newSearcher(reader);
    MaxFreqCollector c = new MaxFreqCollector();
    searcher.search(query, c);
    assertEquals("slop: "+slop+"  query: "+query+"  doc: "+doc+"  Wrong number of hits", expectedNumResults, c.totalHits);

    //QueryUtils.check(query,searcher);
    writer.close();
    reader.close();
    ramDir.close();

    // returns the max Scorer.freq() found, because even though norms are omitted, many index stats are different
    // with these different tokens/distributions/lengths.. otherwise this test is very fragile.
    return c.max; 
  }

  private static Document makeDocument(String docText) {
    Document doc = new Document();
    FieldType customType = new FieldType(TextField.TYPE_NOT_STORED);
    customType.setOmitNorms(true);
    Field f = new Field("f", docText, customType);
    doc.add(f);
    return doc;
  }

  private static PhraseQuery makePhraseQuery(String terms) {
    String[] t = terms.split(" +");
    return new PhraseQuery("f", t);
  }

  static class MaxFreqCollector extends SimpleCollector {
    float max;
    int totalHits;
    Scorer scorer;
    
    @Override
    public void setScorer(Scorable scorer) throws IOException {
      this.scorer = (Scorer) AssertingScorable.unwrap(scorer);
    }

    @Override
    public void collect(int doc) throws IOException {
      totalHits++;
      PhraseScorer ps = (PhraseScorer) scorer;
      float freq = ps.matcher.sloppyWeight();
      while (ps.matcher.nextMatch()) {
        freq += ps.matcher.sloppyWeight();
      }
      max = Math.max(max, freq);
    }
    
    @Override
    public ScoreMode scoreMode() {
      return ScoreMode.COMPLETE;
    }
  }
  
  /** checks that no scores are infinite */
  private void assertSaneScoring(PhraseQuery pq, IndexSearcher searcher) throws Exception {
    searcher.search(pq, new SimpleCollector() {
      Scorer scorer;
      
      @Override
      public void setScorer(Scorable scorer) {
        this.scorer = (Scorer) AssertingScorable.unwrap(scorer);
      }
      
      @Override
      public void collect(int doc) throws IOException {
        assertFalse(Float.isInfinite(scorer.score()));
      }
      
      @Override
      public ScoreMode scoreMode() {
        return ScoreMode.COMPLETE;
      }
    });
    QueryUtils.check(random(), pq, searcher);
  }

  // LUCENE-3215
  public void testSlopWithHoles() throws Exception {  
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    FieldType customType = new FieldType(TextField.TYPE_NOT_STORED);
    customType.setOmitNorms(true);
    Field f = new Field("lyrics", "", customType);
    Document doc = new Document();
    doc.add(f);
    f.setStringValue("drug drug");
    iw.addDocument(doc);
    f.setStringValue("drug druggy drug");
    iw.addDocument(doc);
    f.setStringValue("drug druggy druggy drug");
    iw.addDocument(doc);
    f.setStringValue("drug druggy drug druggy drug");
    iw.addDocument(doc);
    IndexReader ir = iw.getReader();
    iw.close();
    IndexSearcher is = newSearcher(ir);

    PhraseQuery.Builder builder = new PhraseQuery.Builder();
    builder.add(new Term("lyrics", "drug"), 1);
    builder.add(new Term("lyrics", "drug"), 4);
    PhraseQuery pq = builder.build();
    // "drug the drug"~1
    assertEquals(1, is.search(pq, 4).totalHits.value);
    builder.setSlop(1);
    pq = builder.build();
    assertEquals(3, is.search(pq, 4).totalHits.value);
    builder.setSlop(2);
    pq = builder.build();
    assertEquals(4, is.search(pq, 4).totalHits.value);
    ir.close();
    dir.close();
  }

  // LUCENE-3215
  public void testInfiniteFreq1() throws Exception {
    String document = "drug druggy drug drug drug";
    
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(newField("lyrics", document, new FieldType(TextField.TYPE_NOT_STORED)));
    iw.addDocument(doc);
    IndexReader ir = iw.getReader();
    iw.close();
    
    IndexSearcher is = newSearcher(ir);
    PhraseQuery.Builder builder = new PhraseQuery.Builder();
    builder.add(new Term("lyrics", "drug"), 1);
    builder.add(new Term("lyrics", "drug"), 3);
    builder.setSlop(1);
    PhraseQuery pq = builder.build();
    // "drug the drug"~1
    assertSaneScoring(pq, is);
    ir.close();
    dir.close();
  }
  
  // LUCENE-3215
  public void testInfiniteFreq2() throws Exception {
    String document = 
      "So much fun to be had in my head " +
      "No more sunshine " +
      "So much fun just lying in my bed " +
      "No more sunshine " +
      "I can't face the sunlight and the dirt outside " +
      "Wanna stay in 666 where this darkness don't lie " +
      "Drug drug druggy " +
      "Got a feeling sweet like honey " +
      "Drug drug druggy " +
      "Need sensation like my baby " +
      "Show me your scars you're so aware " +
      "I'm not barbaric I just care " +
      "Drug drug drug " +
      "I need a reflection to prove I exist " +
      "No more sunshine " +
      "I am a victim of designer blitz " +
      "No more sunshine " +
      "Dance like a robot when you're chained at the knee " +
      "The C.I.A say you're all they'll ever need " +
      "Drug drug druggy " +
      "Got a feeling sweet like honey " +
      "Drug drug druggy " +
      "Need sensation like my baby " +
      "Snort your lines you're so aware " +
      "I'm not barbaric I just care " +
      "Drug drug druggy " +
      "Got a feeling sweet like honey " +
      "Drug drug druggy " +
      "Need sensation like my baby";
        
     Directory dir = newDirectory();

     RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
     Document doc = new Document();
     doc.add(newField("lyrics", document, new FieldType(TextField.TYPE_NOT_STORED)));
     iw.addDocument(doc);
     IndexReader ir = iw.getReader();
     iw.close();
        
     IndexSearcher is = newSearcher(ir);
     
     PhraseQuery.Builder builder = new PhraseQuery.Builder();
     builder.add(new Term("lyrics", "drug"), 1);
     builder.add(new Term("lyrics", "drug"), 4);
     builder.setSlop(5);
     PhraseQuery pq = builder.build();
     // "drug the drug"~5
     assertSaneScoring(pq, is);
     ir.close();
     dir.close();
  }
}
