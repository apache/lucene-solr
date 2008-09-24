package org.apache.lucene.search.spans;

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

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.CheckHits;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.search.DefaultSimilarity;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockRAMDirectory;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.util.LuceneTestCase;

import java.io.IOException;

public class TestSpans extends LuceneTestCase {
  private IndexSearcher searcher;

  public static final String field = "field";

  public void setUp() throws Exception {
    super.setUp();
    RAMDirectory directory = new RAMDirectory();
    IndexWriter writer= new IndexWriter(directory, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
    for (int i = 0; i < docFields.length; i++) {
      Document doc = new Document();
      doc.add(new Field(field, docFields[i], Field.Store.YES, Field.Index.ANALYZED));
      writer.addDocument(doc);
    }
    writer.close();
    searcher = new IndexSearcher(directory);
  }

  private String[] docFields = {
    "w1 w2 w3 w4 w5",
    "w1 w3 w2 w3",
    "w1 xx w2 yy w3",
    "w1 w3 xx w2 yy w3",
    "u2 u2 u1",
    "u2 xx u2 u1",
    "u2 u2 xx u1",
    "u2 xx u2 yy u1",
    "u2 xx u1 u2",
    "u2 u1 xx u2",
    "u1 u2 xx u2",
    "t1 t2 t1 t3 t2 t3"
  };

  public SpanTermQuery makeSpanTermQuery(String text) {
    return new SpanTermQuery(new Term(field, text));
  }
  
  private void checkHits(Query query, int[] results) throws IOException {
    CheckHits.checkHits(query, field, searcher, results);
  }
  
  private void orderedSlopTest3SQ(
        SpanQuery q1,
        SpanQuery q2,
        SpanQuery q3,
        int slop,
        int[] expectedDocs) throws IOException {
    boolean ordered = true;
    SpanNearQuery snq = new SpanNearQuery( new SpanQuery[]{q1,q2,q3}, slop, ordered);
    checkHits(snq, expectedDocs);
  }
  
  public void orderedSlopTest3(int slop, int[] expectedDocs) throws IOException {
    orderedSlopTest3SQ(
       makeSpanTermQuery("w1"),
       makeSpanTermQuery("w2"),
       makeSpanTermQuery("w3"),
       slop,
       expectedDocs);
  }
  
  public void orderedSlopTest3Equal(int slop, int[] expectedDocs) throws IOException {
    orderedSlopTest3SQ(
       makeSpanTermQuery("w1"),
       makeSpanTermQuery("w3"),
       makeSpanTermQuery("w3"),
       slop,
       expectedDocs);
  }
  
  public void orderedSlopTest1Equal(int slop, int[] expectedDocs) throws IOException {
    orderedSlopTest3SQ(
       makeSpanTermQuery("u2"),
       makeSpanTermQuery("u2"),
       makeSpanTermQuery("u1"),
       slop,
       expectedDocs);
  }
  
  public void testSpanNearOrdered01() throws Exception {
    orderedSlopTest3(0, new int[] {0});
  }

  public void testSpanNearOrdered02() throws Exception {
    orderedSlopTest3(1, new int[] {0,1});
  }

  public void testSpanNearOrdered03() throws Exception {
    orderedSlopTest3(2, new int[] {0,1,2});
  }

  public void testSpanNearOrdered04() throws Exception {
    orderedSlopTest3(3, new int[] {0,1,2,3});
  }

  public void testSpanNearOrdered05() throws Exception {
    orderedSlopTest3(4, new int[] {0,1,2,3});
  }
  
  public void testSpanNearOrderedEqual01() throws Exception {
    orderedSlopTest3Equal(0, new int[] {});
  }

  public void testSpanNearOrderedEqual02() throws Exception {
    orderedSlopTest3Equal(1, new int[] {1});
  }

  public void testSpanNearOrderedEqual03() throws Exception {
    orderedSlopTest3Equal(2, new int[] {1});
  }

  public void testSpanNearOrderedEqual04() throws Exception {
    orderedSlopTest3Equal(3, new int[] {1,3});
  }
  
  public void testSpanNearOrderedEqual11() throws Exception {
    orderedSlopTest1Equal(0, new int[] {4});
  }
  
  public void testSpanNearOrderedEqual12() throws Exception {
    orderedSlopTest1Equal(0, new int[] {4});
  }
  
  public void testSpanNearOrderedEqual13() throws Exception {
    orderedSlopTest1Equal(1, new int[] {4,5,6});
  }
  
  public void testSpanNearOrderedEqual14() throws Exception {
    orderedSlopTest1Equal(2, new int[] {4,5,6,7});
  }

  public void testSpanNearOrderedEqual15() throws Exception {
    orderedSlopTest1Equal(3, new int[] {4,5,6,7});
  }

  public void testSpanNearOrderedOverlap() throws Exception {
    boolean ordered = true;
    int slop = 1;
    SpanNearQuery snq = new SpanNearQuery(
                              new SpanQuery[] {
                                makeSpanTermQuery("t1"),
                                makeSpanTermQuery("t2"),
                                makeSpanTermQuery("t3") },
                              slop,
                              ordered);
    Spans spans = snq.getSpans(searcher.getIndexReader());

    assertTrue("first range", spans.next());
    assertEquals("first doc", 11, spans.doc());
    assertEquals("first start", 0, spans.start());
    assertEquals("first end", 4, spans.end());

    assertTrue("second range", spans.next());
    assertEquals("second doc", 11, spans.doc());
    assertEquals("second start", 2, spans.start());
    assertEquals("second end", 6, spans.end());

    assertFalse("third range", spans.next());
  }


  public void testSpanNearUnOrdered() throws Exception {

    //See http://www.gossamer-threads.com/lists/lucene/java-dev/52270 for discussion about this test
    SpanNearQuery snq;
    snq = new SpanNearQuery(
                              new SpanQuery[] {
                                makeSpanTermQuery("u1"),
                                makeSpanTermQuery("u2") },
                              0,
                              false);
    Spans spans = snq.getSpans(searcher.getIndexReader());
    assertTrue("Does not have next and it should", spans.next());
    assertEquals("doc", 4, spans.doc());
    assertEquals("start", 1, spans.start());
    assertEquals("end", 3, spans.end());

    assertTrue("Does not have next and it should", spans.next());
    assertEquals("doc", 5, spans.doc());
    assertEquals("start", 2, spans.start());
    assertEquals("end", 4, spans.end());

    assertTrue("Does not have next and it should", spans.next());
    assertEquals("doc", 8, spans.doc());
    assertEquals("start", 2, spans.start());
    assertEquals("end", 4, spans.end());

    assertTrue("Does not have next and it should", spans.next());
    assertEquals("doc", 9, spans.doc());
    assertEquals("start", 0, spans.start());
    assertEquals("end", 2, spans.end());

    assertTrue("Does not have next and it should", spans.next());
    assertEquals("doc", 10, spans.doc());
    assertEquals("start", 0, spans.start());
    assertEquals("end", 2, spans.end());
    assertTrue("Has next and it shouldn't: " + spans.doc(), spans.next() == false);

    SpanNearQuery u1u2 = new SpanNearQuery(new SpanQuery[]{makeSpanTermQuery("u1"),
                                makeSpanTermQuery("u2")}, 0, false);
    snq = new SpanNearQuery(
                              new SpanQuery[] {
                                u1u2,
                                makeSpanTermQuery("u2")
                              },
                              1,
                              false);
    spans = snq.getSpans(searcher.getIndexReader());
    assertTrue("Does not have next and it should", spans.next());
    assertEquals("doc", 4, spans.doc());
    assertEquals("start", 0, spans.start());
    assertEquals("end", 3, spans.end());

    assertTrue("Does not have next and it should", spans.next());
    //unordered spans can be subsets
    assertEquals("doc", 4, spans.doc());
    assertEquals("start", 1, spans.start());
    assertEquals("end", 3, spans.end());

    assertTrue("Does not have next and it should", spans.next());
    assertEquals("doc", 5, spans.doc());
    assertEquals("start", 0, spans.start());
    assertEquals("end", 4, spans.end());

    assertTrue("Does not have next and it should", spans.next());
    assertEquals("doc", 5, spans.doc());
    assertEquals("start", 2, spans.start());
    assertEquals("end", 4, spans.end());

    assertTrue("Does not have next and it should", spans.next());
    assertEquals("doc", 8, spans.doc());
    assertEquals("start", 0, spans.start());
    assertEquals("end", 4, spans.end());


    assertTrue("Does not have next and it should", spans.next());
    assertEquals("doc", 8, spans.doc());
    assertEquals("start", 2, spans.start());
    assertEquals("end", 4, spans.end());

    assertTrue("Does not have next and it should", spans.next());
    assertEquals("doc", 9, spans.doc());
    assertEquals("start", 0, spans.start());
    assertEquals("end", 2, spans.end());

    assertTrue("Does not have next and it should", spans.next());
    assertEquals("doc", 9, spans.doc());
    assertEquals("start", 0, spans.start());
    assertEquals("end", 4, spans.end());

    assertTrue("Does not have next and it should", spans.next());
    assertEquals("doc", 10, spans.doc());
    assertEquals("start", 0, spans.start());
    assertEquals("end", 2, spans.end());

    assertTrue("Has next and it shouldn't", spans.next() == false);
  }



  private Spans orSpans(String[] terms) throws Exception {
    SpanQuery[] sqa = new SpanQuery[terms.length];
    for (int i = 0; i < terms.length; i++) {
      sqa[i] = makeSpanTermQuery(terms[i]);
    }
    return (new SpanOrQuery(sqa)).getSpans(searcher.getIndexReader());
  }

  private void tstNextSpans(Spans spans, int doc, int start, int end)
  throws Exception {
    assertTrue("next", spans.next());
    assertEquals("doc", doc, spans.doc());
    assertEquals("start", start, spans.start());
    assertEquals("end", end, spans.end());
  }

  public void testSpanOrEmpty() throws Exception {
    Spans spans = orSpans(new String[0]);
    assertFalse("empty next", spans.next());
  }

  public void testSpanOrSingle() throws Exception {
    Spans spans = orSpans(new String[] {"w5"});
    tstNextSpans(spans, 0, 4, 5);
    assertFalse("final next", spans.next());
  }
  
  public void testSpanOrDouble() throws Exception {
    Spans spans = orSpans(new String[] {"w5", "yy"});
    tstNextSpans(spans, 0, 4, 5);
    tstNextSpans(spans, 2, 3, 4);
    tstNextSpans(spans, 3, 4, 5);
    tstNextSpans(spans, 7, 3, 4);
    assertFalse("final next", spans.next());
  }

  public void testSpanOrDoubleSkip() throws Exception {
    Spans spans = orSpans(new String[] {"w5", "yy"});
    assertTrue("initial skipTo", spans.skipTo(3));
    assertEquals("doc", 3, spans.doc());
    assertEquals("start", 4, spans.start());
    assertEquals("end", 5, spans.end());
    tstNextSpans(spans, 7, 3, 4);
    assertFalse("final next", spans.next());
  }

  public void testSpanOrUnused() throws Exception {
    Spans spans = orSpans(new String[] {"w5", "unusedTerm", "yy"});
    tstNextSpans(spans, 0, 4, 5);
    tstNextSpans(spans, 2, 3, 4);
    tstNextSpans(spans, 3, 4, 5);
    tstNextSpans(spans, 7, 3, 4);
    assertFalse("final next", spans.next());
  }

  public void testSpanOrTripleSameDoc() throws Exception {
    Spans spans = orSpans(new String[] {"t1", "t2", "t3"});
    tstNextSpans(spans, 11, 0, 1);
    tstNextSpans(spans, 11, 1, 2);
    tstNextSpans(spans, 11, 2, 3);
    tstNextSpans(spans, 11, 3, 4);
    tstNextSpans(spans, 11, 4, 5);
    tstNextSpans(spans, 11, 5, 6);
    assertFalse("final next", spans.next());
  }

  public void testSpanScorerZeroSloppyFreq() throws Exception {
    boolean ordered = true;
    int slop = 1;

    final Similarity sim = new DefaultSimilarity() {
      public float sloppyFreq(int distance) {
        return 0.0f;
      }
    };

    SpanNearQuery snq = new SpanNearQuery(
                              new SpanQuery[] {
                                makeSpanTermQuery("t1"),
                                makeSpanTermQuery("t2") },
                              slop,
                              ordered) {
      public Similarity getSimilarity(Searcher s) {
        return sim;
      }
    };

    Scorer spanScorer = snq.weight(searcher).scorer(searcher.getIndexReader());

    assertTrue("first doc", spanScorer.next());
    assertEquals("first doc number", spanScorer.doc(), 11);
    float score = spanScorer.score();
    assertTrue("first doc score should be zero, " + score, score == 0.0f);
    assertTrue("no second doc", ! spanScorer.next());
  }

  // LUCENE-1404
  private void addDoc(IndexWriter writer, String id, String text) throws IOException {
    final Document doc = new Document();
    doc.add( new Field("id", id, Field.Store.YES, Field.Index.UN_TOKENIZED) );
    doc.add( new Field("text", text, Field.Store.YES, Field.Index.TOKENIZED) );
    writer.addDocument(doc);
  }

  // LUCENE-1404
  private int hitCount(Searcher searcher, String word) throws Throwable {
    return searcher.search(new TermQuery(new Term("text", word)), 10).totalHits;
  }

  // LUCENE-1404
  private SpanQuery createSpan(String value) {
    return new SpanTermQuery(new Term("text", value));
  }                     
  
  // LUCENE-1404
  private SpanQuery createSpan(int slop, boolean ordered, SpanQuery[] clauses) {
    return new SpanNearQuery(clauses, slop, ordered);
  }

  // LUCENE-1404
  private SpanQuery createSpan(int slop, boolean ordered, String term1, String term2) {
    return createSpan(slop, ordered, new SpanQuery[] {createSpan(term1), createSpan(term2)});
  }

  // LUCENE-1404
  public void testNPESpanQuery() throws Throwable {
    final Directory dir = new MockRAMDirectory();
    final IndexWriter writer = new IndexWriter(dir, new StandardAnalyzer(new String[0]), IndexWriter.MaxFieldLength.LIMITED);

    // Add documents
    addDoc(writer, "1", "the big dogs went running to the market");
    addDoc(writer, "2", "the cat chased the mouse, then the cat ate the mouse quickly");
    
    // Commit
    writer.close();

    // Get searcher
    final IndexReader reader = IndexReader.open(dir);
    final IndexSearcher searcher = new IndexSearcher(reader);

    // Control (make sure docs indexed)
    assertEquals(2, hitCount(searcher, "the"));
    assertEquals(1, hitCount(searcher, "cat"));
    assertEquals(1, hitCount(searcher, "dogs"));
    assertEquals(0, hitCount(searcher, "rabbit"));

    // This throws exception (it shouldn't)
    assertEquals(1,
                 searcher.search(createSpan(0, true,                                 
                                            new SpanQuery[] {createSpan(4, false, "chased", "cat"),
                                                             createSpan("ate")}), 10).totalHits);
    reader.close();
    dir.close();
  }
}
