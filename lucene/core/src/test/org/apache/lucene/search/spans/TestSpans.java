package org.apache.lucene.search.spans;

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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.CheckHits;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

import java.io.IOException;
import java.util.List;

public class TestSpans extends LuceneTestCase {
  private IndexSearcher searcher;
  private IndexReader reader;
  private Directory directory;
  
  public static final String field = "field";

  @Override
  public void setUp() throws Exception {
    super.setUp();
    directory = newDirectory();
    RandomIndexWriter writer= new RandomIndexWriter(random(), directory, newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));
    for (int i = 0; i < docFields.length; i++) {
      Document doc = new Document();
      doc.add(newTextField(field, docFields[i], Field.Store.YES));
      writer.addDocument(doc);
    }
    reader = writer.getReader();
    writer.close();
    searcher = newSearcher(reader);
  }
  
  @Override
  public void tearDown() throws Exception {
    reader.close();
    directory.close();
    super.tearDown();
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
    "t1 t2 t1 t3 t2 t3",
    "s2 s1 s1 xx xx s2 xx s2 xx s1 xx xx xx xx xx s2 xx"
  };

  public SpanTermQuery makeSpanTermQuery(String text) {
    return new SpanTermQuery(new Term(field, text));
  }
  
  private void checkHits(Query query, int[] results) throws IOException {
    CheckHits.checkHits(random(), query, field, searcher, results);
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
    Spans spans = MultiSpansWrapper.wrap(searcher.getIndexReader(), snq);

    assertEquals("first doc", 11, spans.nextDoc());
    assertEquals("first start", 0, spans.nextStartPosition());
    assertEquals("first end", 4, spans.endPosition());

    assertEquals("second start", 2, spans.nextStartPosition());
    assertEquals("second end", 6, spans.endPosition());

    tstEndSpans(spans);  
  }

  public void testSpanNearUnOrdered() throws Exception {
    //See http://www.gossamer-threads.com/lists/lucene/java-dev/52270 for discussion about this test
    SpanNearQuery senq;
    senq = new SpanNearQuery(
                              new SpanQuery[] {
                                makeSpanTermQuery("u1"),
                                makeSpanTermQuery("u2") },
                              0,
                              false);
    Spans spans = MultiSpansWrapper.wrap(reader, senq);
    tstNextSpans(spans, 4, 1, 3);
    tstNextSpans(spans, 5, 2, 4);
    tstNextSpans(spans, 8, 2, 4);
    tstNextSpans(spans, 9, 0, 2);
    tstNextSpans(spans, 10, 0, 2);
    tstEndSpans(spans);

    SpanNearQuery u1u2 = new SpanNearQuery(new SpanQuery[]{makeSpanTermQuery("u1"),
                                makeSpanTermQuery("u2")}, 0, false);
    senq = new SpanNearQuery(
                              new SpanQuery[] {
                                u1u2,
                                makeSpanTermQuery("u2")
                              },
                              1,
                              false);
    spans = MultiSpansWrapper.wrap(reader, senq);
    tstNextSpans(spans, 4, 0, 3);
    tstNextSpans(spans, 4, 1, 3); // unordered spans can be subsets
    tstNextSpans(spans, 5, 0, 4);
    tstNextSpans(spans, 5, 2, 4);
    tstNextSpans(spans, 8, 0, 4);
    tstNextSpans(spans, 8, 2, 4);
    tstNextSpans(spans, 9, 0, 2);
    tstNextSpans(spans, 9, 0, 4);
    tstNextSpans(spans, 10, 0, 2);
    tstEndSpans(spans);
  }



  private Spans orSpans(String[] terms) throws Exception {
    SpanQuery[] sqa = new SpanQuery[terms.length];
    for (int i = 0; i < terms.length; i++) {
      sqa[i] = makeSpanTermQuery(terms[i]);
    }
    return MultiSpansWrapper.wrap(searcher.getIndexReader(), new SpanOrQuery(sqa));
  }

  public static void tstNextSpans(Spans spans, int doc, int start, int end) throws IOException {
    if (spans.docID() >= doc) {
      assertEquals("docId", doc, spans.docID());
    } else { // nextDoc needed before testing start/end
      if (spans.docID() >= 0) {
        assertEquals("nextStartPosition of previous doc", Spans.NO_MORE_POSITIONS, spans.nextStartPosition());
        assertEquals("endPosition of previous doc", Spans.NO_MORE_POSITIONS, spans.endPosition());
      }
      assertEquals("nextDoc", doc, spans.nextDoc());
      if (doc != Spans.NO_MORE_DOCS) {
        assertEquals("first startPosition", -1, spans.startPosition());
        assertEquals("first endPosition", -1, spans.endPosition());
      }
    }
    if (doc != Spans.NO_MORE_DOCS) {
      assertEquals("nextStartPosition", start, spans.nextStartPosition());
      assertEquals("startPosition", start, spans.startPosition());
      assertEquals("endPosition", end, spans.endPosition());
    }
  }
  
  public static void tstEndSpans(Spans spans) throws Exception {
    if (spans != null) { // null Spans is empty
      tstNextSpans(spans, Spans.NO_MORE_DOCS, -2, -2); // start and end positions will be ignored
    }
  }

  public void testSpanOrEmpty() throws Exception {
    Spans spans = orSpans(new String[0]);
    tstEndSpans(spans);
    
    SpanOrQuery a = new SpanOrQuery();
    SpanOrQuery b = new SpanOrQuery();
    assertTrue("empty should equal", a.equals(b));
  }

  public void testSpanOrSingle() throws Exception {
    Spans spans = orSpans(new String[] {"w5"});
    tstNextSpans(spans, 0, 4, 5);
    tstEndSpans(spans);
  }
  
  public void testSpanOrDouble() throws Exception {
    Spans spans = orSpans(new String[] {"w5", "yy"});
    tstNextSpans(spans, 0, 4, 5);
    tstNextSpans(spans, 2, 3, 4);
    tstNextSpans(spans, 3, 4, 5);
    tstNextSpans(spans, 7, 3, 4);
    tstEndSpans(spans);
  }

  public void testSpanOrDoubleAdvance() throws Exception {
    Spans spans = orSpans(new String[] {"w5", "yy"});
    assertEquals("initial advance", 3, spans.advance(3));
    tstNextSpans(spans, 3, 4, 5);
    tstNextSpans(spans, 7, 3, 4);
    tstEndSpans(spans);
  }

  public void testSpanOrUnused() throws Exception {
    Spans spans = orSpans(new String[] {"w5", "unusedTerm", "yy"});
    tstNextSpans(spans, 0, 4, 5);
    tstNextSpans(spans, 2, 3, 4);
    tstNextSpans(spans, 3, 4, 5);
    tstNextSpans(spans, 7, 3, 4);
    tstEndSpans(spans);
  }

  public void testSpanOrTripleSameDoc() throws Exception {
    Spans spans = orSpans(new String[] {"t1", "t2", "t3"});
    tstNextSpans(spans, 11, 0, 1);
    tstNextSpans(spans, 11, 1, 2);
    tstNextSpans(spans, 11, 2, 3);
    tstNextSpans(spans, 11, 3, 4);
    tstNextSpans(spans, 11, 4, 5);
    tstNextSpans(spans, 11, 5, 6);
    tstEndSpans(spans);
  }

  public void testSpanScorerZeroSloppyFreq() throws Exception {
    boolean ordered = true;
    int slop = 1;
    IndexReaderContext topReaderContext = searcher.getTopReaderContext();
    List<LeafReaderContext> leaves = topReaderContext.leaves();
    int subIndex = ReaderUtil.subIndex(11, leaves);
    for (int i = 0, c = leaves.size(); i < c; i++) {
      final LeafReaderContext ctx = leaves.get(i);
     
      final Similarity sim = new DefaultSimilarity() {
        @Override
        public float sloppyFreq(int distance) {
          return 0.0f;
        }
      };
  
      final Similarity oldSim = searcher.getSimilarity();
      Scorer spanScorer;
      try {
        searcher.setSimilarity(sim);
        SpanNearQuery snq = new SpanNearQuery(
                                new SpanQuery[] {
                                  makeSpanTermQuery("t1"),
                                  makeSpanTermQuery("t2") },
                                slop,
                                ordered);
  
        spanScorer = searcher.createNormalizedWeight(snq, true).scorer(ctx, ctx.reader().getLiveDocs());
      } finally {
        searcher.setSimilarity(oldSim);
      }
      if (i == subIndex) {
        assertTrue("first doc", spanScorer.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
        assertEquals("first doc number", spanScorer.docID() + ctx.docBase, 11);
        float score = spanScorer.score();
        assertTrue("first doc score should be zero, " + score, score == 0.0f);
      }  else {
        assertTrue("no second doc", spanScorer.nextDoc() == DocIdSetIterator.NO_MORE_DOCS);
      }
    }
  }

  // LUCENE-1404
  private void addDoc(IndexWriter writer, String id, String text) throws IOException {
    final Document doc = new Document();
    doc.add( newStringField("id", id, Field.Store.YES) );
    doc.add( newTextField("text", text, Field.Store.YES) );
    writer.addDocument(doc);
  }

  // LUCENE-1404
  private int hitCount(IndexSearcher searcher, String word) throws Throwable {
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
    final Directory dir = newDirectory();
    final IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(new MockAnalyzer(random())));

    // Add documents
    addDoc(writer, "1", "the big dogs went running to the market");
    addDoc(writer, "2", "the cat chased the mouse, then the cat ate the mouse quickly");
    
    // Commit
    writer.close();

    // Get searcher
    final IndexReader reader = DirectoryReader.open(dir);
    final IndexSearcher searcher = newSearcher(reader);

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
  
  
  public void testSpanNots() throws Throwable{
     assertEquals("SpanNotIncludeExcludeSame1", 0, spanCount("s2", "s2", 0, 0), 0);
     assertEquals("SpanNotIncludeExcludeSame2", 0, spanCount("s2", "s2", 10, 10), 0);
     
     //focus on behind
     assertEquals("SpanNotS2NotS1_6_0", 1, spanCount("s2", "s1", 6, 0));
     assertEquals("SpanNotS2NotS1_5_0", 2, spanCount("s2", "s1", 5, 0));
     assertEquals("SpanNotS2NotS1_3_0", 3, spanCount("s2", "s1", 3, 0));
     assertEquals("SpanNotS2NotS1_2_0", 4, spanCount("s2", "s1", 2, 0));
     assertEquals("SpanNotS2NotS1_0_0", 4, spanCount("s2", "s1", 0, 0));
     
     //focus on both
     assertEquals("SpanNotS2NotS1_3_1", 2, spanCount("s2", "s1", 3, 1));
     assertEquals("SpanNotS2NotS1_2_1", 3, spanCount("s2", "s1", 2, 1));
     assertEquals("SpanNotS2NotS1_1_1", 3, spanCount("s2", "s1", 1, 1));
     assertEquals("SpanNotS2NotS1_10_10", 0, spanCount("s2", "s1", 10, 10));
     
     //focus on ahead
     assertEquals("SpanNotS1NotS2_10_10", 0, spanCount("s1", "s2", 10, 10));  
     assertEquals("SpanNotS1NotS2_0_1", 3, spanCount("s1", "s2", 0, 1));  
     assertEquals("SpanNotS1NotS2_0_2", 3, spanCount("s1", "s2", 0, 2));  
     assertEquals("SpanNotS1NotS2_0_3", 2, spanCount("s1", "s2", 0, 3));  
     assertEquals("SpanNotS1NotS2_0_4", 1, spanCount("s1", "s2", 0, 4));
     assertEquals("SpanNotS1NotS2_0_8", 0, spanCount("s1", "s2", 0, 8));
     
     //exclude doesn't exist
     assertEquals("SpanNotS1NotS3_8_8", 3, spanCount("s1", "s3", 8, 8));

     //include doesn't exist
     assertEquals("SpanNotS3NotS1_8_8", 0, spanCount("s3", "s1", 8, 8));

  }
  
  private int spanCount(String include, String exclude, int pre, int post) throws IOException{
     SpanTermQuery iq = new SpanTermQuery(new Term(field, include));
     SpanTermQuery eq = new SpanTermQuery(new Term(field, exclude));
     SpanNotQuery snq = new SpanNotQuery(iq, eq, pre, post);
     Spans spans = MultiSpansWrapper.wrap(searcher.getIndexReader(), snq);

     int i = 0;
     if (spans != null) {
       while (spans.nextDoc() != Spans.NO_MORE_DOCS){
         while (spans.nextStartPosition() != Spans.NO_MORE_POSITIONS) {
           i++;
         }
       }
     }
     return i;
  }
  
}
