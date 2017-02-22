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
package org.apache.lucene.search.spans;


import java.io.IOException;
import java.util.List;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.CheckHits;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

import static org.apache.lucene.search.spans.SpanTestUtil.assertFinished;
import static org.apache.lucene.search.spans.SpanTestUtil.assertNext;
import static org.apache.lucene.search.spans.SpanTestUtil.spanNearOrderedQuery;
import static org.apache.lucene.search.spans.SpanTestUtil.spanNearUnorderedQuery;
import static org.apache.lucene.search.spans.SpanTestUtil.spanNotQuery;
import static org.apache.lucene.search.spans.SpanTestUtil.spanOrQuery;
import static org.apache.lucene.search.spans.SpanTestUtil.spanTermQuery;

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
    writer.forceMerge(1);
    reader = writer.getReader();
    writer.close();
    searcher = newSearcher(getOnlyLeafReader(reader));
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
    "s2 s1 s1 xx xx s2 xx s2 xx s1 xx xx xx xx xx s2 xx",
    "r1 s11",
    "r1 s21"
  };
  
  private void checkHits(Query query, int[] results) throws IOException {
    CheckHits.checkHits(random(), query, field, searcher, results);
  }
  
  private void orderedSlopTest3SQ(
        SpanQuery q1,
        SpanQuery q2,
        SpanQuery q3,
        int slop,
        int[] expectedDocs) throws IOException {
    SpanQuery query = spanNearOrderedQuery(slop, q1, q2, q3);
    checkHits(query, expectedDocs);
  }
  
  public void orderedSlopTest3(int slop, int[] expectedDocs) throws IOException {
    orderedSlopTest3SQ(
       spanTermQuery(field, "w1"),
       spanTermQuery(field, "w2"),
       spanTermQuery(field, "w3"),
       slop,
       expectedDocs);
  }
  
  public void orderedSlopTest3Equal(int slop, int[] expectedDocs) throws IOException {
    orderedSlopTest3SQ(
       spanTermQuery(field, "w1"),
       spanTermQuery(field, "w3"),
       spanTermQuery(field, "w3"),
       slop,
       expectedDocs);
  }
  
  public void orderedSlopTest1Equal(int slop, int[] expectedDocs) throws IOException {
    orderedSlopTest3SQ(
       spanTermQuery(field, "u2"),
       spanTermQuery(field, "u2"),
       spanTermQuery(field, "u1"),
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
    final SpanQuery query = spanNearOrderedQuery(field, 1, "t1", "t2", "t3");
    
    Spans spans = query.createWeight(searcher, false, 1f).getSpans(searcher.getIndexReader().leaves().get(0), SpanWeight.Postings.POSITIONS);

    assertEquals("first doc", 11, spans.nextDoc());
    assertEquals("first start", 0, spans.nextStartPosition());
    assertEquals("first end", 4, spans.endPosition());

    assertEquals("second start", 2, spans.nextStartPosition());
    assertEquals("second end", 6, spans.endPosition());

    assertFinished(spans);  
  }

  public void testSpanNearUnOrdered() throws Exception {
    //See http://www.gossamer-threads.com/lists/lucene/java-dev/52270 for discussion about this test
    SpanQuery senq = spanNearUnorderedQuery(field, 0, "u1", "u2");
    Spans spans = senq.createWeight(searcher, false, 1f).getSpans(searcher.getIndexReader().leaves().get(0), SpanWeight.Postings.POSITIONS);
    assertNext(spans, 4, 1, 3);
    assertNext(spans, 5, 2, 4);
    assertNext(spans, 8, 2, 4);
    assertNext(spans, 9, 0, 2);
    assertNext(spans, 10, 0, 2);
    assertFinished(spans);

    senq = spanNearUnorderedQuery(1, senq, spanTermQuery(field, "u2")); 
    spans = senq.createWeight(searcher, false, 1f).getSpans(searcher.getIndexReader().leaves().get(0), SpanWeight.Postings.POSITIONS);
    assertNext(spans, 4, 0, 3);
    assertNext(spans, 4, 1, 3); // unordered spans can be subsets
    assertNext(spans, 5, 0, 4);
    assertNext(spans, 5, 2, 4);
    assertNext(spans, 8, 0, 4);
    assertNext(spans, 8, 2, 4);
    assertNext(spans, 9, 0, 2);
    assertNext(spans, 9, 0, 4);
    assertNext(spans, 10, 0, 2);
    assertFinished(spans);
  }

  private Spans orSpans(String[] terms) throws Exception {
    return spanOrQuery(field, terms).createWeight(searcher, false, 1f).getSpans(searcher.getIndexReader().leaves().get(0), SpanWeight.Postings.POSITIONS);
  }

  public void testSpanOrEmpty() throws Exception {
    Spans spans = orSpans(new String[0]);
    assertFinished(spans);
  }

  public void testSpanOrSingle() throws Exception {
    Spans spans = orSpans(new String[] {"w5"});
    assertNext(spans, 0, 4, 5);
    assertFinished(spans);
  }
  
  public void testSpanOrDouble() throws Exception {
    Spans spans = orSpans(new String[] {"w5", "yy"});
    assertNext(spans, 0, 4, 5);
    assertNext(spans, 2, 3, 4);
    assertNext(spans, 3, 4, 5);
    assertNext(spans, 7, 3, 4);
    assertFinished(spans);
  }

  public void testSpanOrDoubleAdvance() throws Exception {
    Spans spans = orSpans(new String[] {"w5", "yy"});
    assertEquals("initial advance", 3, spans.advance(3));
    assertNext(spans, 3, 4, 5);
    assertNext(spans, 7, 3, 4);
    assertFinished(spans);
  }

  public void testSpanOrUnused() throws Exception {
    Spans spans = orSpans(new String[] {"w5", "unusedTerm", "yy"});
    assertNext(spans, 0, 4, 5);
    assertNext(spans, 2, 3, 4);
    assertNext(spans, 3, 4, 5);
    assertNext(spans, 7, 3, 4);
    assertFinished(spans);
  }

  public void testSpanOrTripleSameDoc() throws Exception {
    Spans spans = orSpans(new String[] {"t1", "t2", "t3"});
    assertNext(spans, 11, 0, 1);
    assertNext(spans, 11, 1, 2);
    assertNext(spans, 11, 2, 3);
    assertNext(spans, 11, 3, 4);
    assertNext(spans, 11, 4, 5);
    assertNext(spans, 11, 5, 6);
    assertFinished(spans);
  }

  public void testSpanScorerZeroSloppyFreq() throws Exception {
    IndexReaderContext topReaderContext = searcher.getTopReaderContext();
    List<LeafReaderContext> leaves = topReaderContext.leaves();
    int subIndex = ReaderUtil.subIndex(11, leaves);
    for (int i = 0, c = leaves.size(); i < c; i++) {
      final LeafReaderContext ctx = leaves.get(i);
     
      final Similarity sim = new ClassicSimilarity() {
        @Override
        public float sloppyFreq(int distance) {
          return 0.0f;
        }
      };
  
      final Similarity oldSim = searcher.getSimilarity(true);
      Scorer spanScorer;
      try {
        searcher.setSimilarity(sim);
        SpanQuery snq = spanNearOrderedQuery(field, 1, "t1", "t2");
        spanScorer = searcher.createNormalizedWeight(snq, true).scorer(ctx);
      } finally {
        searcher.setSimilarity(oldSim);
      }
      if (i == subIndex) {
        assertTrue("first doc", spanScorer.iterator().nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
        assertEquals("first doc number", spanScorer.docID() + ctx.docBase, 11);
        float score = spanScorer.score();
        assertTrue("first doc score should be zero, " + score, score == 0.0f);
      } else {
        assertTrue("no second doc", spanScorer == null || spanScorer.iterator().nextDoc() == DocIdSetIterator.NO_MORE_DOCS);
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
    return spanTermQuery("text", value);
  }                     
  
  // LUCENE-1404
  private SpanQuery createSpan(int slop, boolean ordered, SpanQuery[] clauses) {
    if (ordered) {
      return spanNearOrderedQuery(slop, clauses);
    } else {
      return spanNearUnorderedQuery(slop, clauses);
    }
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

  public void testSpanNotWithMultiterm() throws Exception {
    SpanQuery q = spanNotQuery(
        spanTermQuery(field, "r1"),
        new SpanMultiTermQueryWrapper<>(new PrefixQuery(new Term(field, "s1"))),3,3);
    checkHits(q,  new int[] {14});

    q = spanNotQuery(
        spanTermQuery(field, "r1"),
        new SpanMultiTermQueryWrapper<>(new FuzzyQuery(new Term(field, "s12"), 1, 2)),3,3);
    checkHits(q,  new int[] {14});

    q = spanNotQuery(
        new SpanMultiTermQueryWrapper<>(new PrefixQuery(new Term(field, "r"))),
        spanTermQuery(field, "s21"),3,3);
    checkHits(q,  new int[] {13});


  }

  public void testSpanNots() throws Throwable {

    assertEquals("SpanNotIncludeExcludeSame1", 0, spanCount("s2", 0, "s2", 0, 0), 0);
    assertEquals("SpanNotIncludeExcludeSame2", 0, spanCount("s2", 0, "s2", 10, 10), 0);

    //focus on behind
    assertEquals("SpanNotS2NotS1_6_0", 1, spanCount("s2", 0, "s1", 6, 0));
    assertEquals("SpanNotS2NotS1_5_0", 2, spanCount("s2", 0, "s1", 5, 0));
    assertEquals("SpanNotS2NotS1_3_0", 3, spanCount("s2", 0, "s1", 3, 0));
    assertEquals("SpanNotS2NotS1_2_0", 4, spanCount("s2", 0, "s1", 2, 0));
    assertEquals("SpanNotS2NotS1_0_0", 4, spanCount("s2", 0, "s1", 0, 0));

    //focus on both
    assertEquals("SpanNotS2NotS1_3_1", 2, spanCount("s2", 0, "s1", 3, 1));
    assertEquals("SpanNotS2NotS1_2_1", 3, spanCount("s2", 0, "s1", 2, 1));
    assertEquals("SpanNotS2NotS1_1_1", 3, spanCount("s2", 0, "s1", 1, 1));
    assertEquals("SpanNotS2NotS1_10_10", 0, spanCount("s2", 0, "s1", 10, 10));

    //focus on ahead
    assertEquals("SpanNotS1NotS2_10_10", 0, spanCount("s1", 0, "s2", 10, 10));
    assertEquals("SpanNotS1NotS2_0_1", 3, spanCount("s1", 0, "s2", 0, 1));
    assertEquals("SpanNotS1NotS2_0_2", 3, spanCount("s1", 0, "s2", 0, 2));
    assertEquals("SpanNotS1NotS2_0_3", 2, spanCount("s1", 0, "s2", 0, 3));
    assertEquals("SpanNotS1NotS2_0_4", 1, spanCount("s1", 0, "s2", 0, 4));
    assertEquals("SpanNotS1NotS2_0_8", 0, spanCount("s1", 0, "s2", 0, 8));

    //exclude doesn't exist
    assertEquals("SpanNotS1NotS3_8_8", 3, spanCount("s1", 0, "s3", 8, 8));

    //include doesn't exist
    assertEquals("SpanNotS3NotS1_8_8", 0, spanCount("s3", 0, "s1", 8, 8));

    // Negative values
    assertEquals("SpanNotS2S1NotXXNeg_0_0", 1, spanCount("s2 s1", 10, "xx", 0, 0));
    assertEquals("SpanNotS2S1NotXXNeg_1_1", 1, spanCount("s2 s1", 10, "xx", -1, -1));
    assertEquals("SpanNotS2S1NotXXNeg_0_2", 2, spanCount("s2 s1", 10, "xx",  0, -2));
    assertEquals("SpanNotS2S1NotXXNeg_1_2", 2, spanCount("s2 s1", 10, "xx", -1, -2));
    assertEquals("SpanNotS2S1NotXXNeg_2_1", 2, spanCount("s2 s1", 10, "xx", -2, -1));
    assertEquals("SpanNotS2S1NotXXNeg_3_1", 2, spanCount("s2 s1", 10, "xx", -3, -1));
    assertEquals("SpanNotS2S1NotXXNeg_1_3", 2, spanCount("s2 s1", 10, "xx", -1, -3));
    assertEquals("SpanNotS2S1NotXXNeg_2_2", 3, spanCount("s2 s1", 10, "xx", -2, -2));
  }


  private int spanCount(String include, int slop, String exclude, int pre, int post) throws IOException{
     String[] includeTerms = include.split(" +");
     SpanQuery iq = includeTerms.length == 1 ? spanTermQuery(field, include) : spanNearOrderedQuery(field, slop, includeTerms);
     SpanQuery eq = spanTermQuery(field, exclude);
     SpanQuery snq = spanNotQuery(iq, eq, pre, post);
     Spans spans = snq.createWeight(searcher, false, 1f).getSpans(searcher.getIndexReader().leaves().get(0), SpanWeight.Postings.POSITIONS);

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
