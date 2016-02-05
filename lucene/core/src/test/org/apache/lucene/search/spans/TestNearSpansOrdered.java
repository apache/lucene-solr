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


import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.CheckHits;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

import static org.apache.lucene.search.spans.SpanTestUtil.assertFinished;
import static org.apache.lucene.search.spans.SpanTestUtil.assertNext;

public class TestNearSpansOrdered extends LuceneTestCase {
  protected IndexSearcher searcher;
  protected Directory directory;
  protected IndexReader reader;

  public static final String FIELD = "field";

  @Override
  public void tearDown() throws Exception {
    reader.close();
    directory.close();
    super.tearDown();
  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    directory = newDirectory();
    RandomIndexWriter writer= new RandomIndexWriter(random(), directory, newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));
    for (int i = 0; i < docFields.length; i++) {
      Document doc = new Document();
      doc.add(newTextField(FIELD, docFields[i], Field.Store.NO));
      writer.addDocument(doc);
    }
    reader = writer.getReader();
    writer.close();
    searcher = newSearcher(reader);
  }

  protected String[] docFields = {
    "w1 w2 w3 w4 w5",
    "w1 w3 w2 w3 zz",
    "w1 xx w2 yy w3",
    "w1 w3 xx w2 yy w3 zz",
    "t1 t2 t2 t1",
    "g x x g g x x x g g x x g"
  };

  protected SpanNearQuery makeQuery(String s1, String s2, String s3,
                                    int slop, boolean inOrder) {
    return new SpanNearQuery
      (new SpanQuery[] {
        new SpanTermQuery(new Term(FIELD, s1)),
        new SpanTermQuery(new Term(FIELD, s2)),
        new SpanTermQuery(new Term(FIELD, s3)) },
       slop,
       inOrder);
  }
  protected SpanNearQuery makeQuery() {
    return makeQuery("w1","w2","w3",1,true);
  }

  protected SpanNearQuery makeOverlappedQuery(
      String sqt1, String sqt2, boolean sqOrdered,
      String t3, boolean ordered) {
    return new SpanNearQuery(
      new SpanQuery[] {
        new SpanNearQuery(new SpanQuery[] {
          new SpanTermQuery(new Term(FIELD, sqt1)),
            new SpanTermQuery(new Term(FIELD, sqt2)) },
            1,
            sqOrdered
          ),
          new SpanTermQuery(new Term(FIELD, t3)) },
          0,
          ordered);
  }
  
  public void testSpanNearQuery() throws Exception {
    SpanNearQuery q = makeQuery();
    CheckHits.checkHits(random(), q, FIELD, searcher, new int[] {0,1});
  }

  public String s(Spans span) {
    return s(span.docID(), span.startPosition(), span.endPosition());
  }
  public String s(int doc, int start, int end) {
    return "s(" + doc + "," + start + "," + end +")";
  }
  
  public void testNearSpansNext() throws Exception {
    SpanNearQuery q = makeQuery();
    Spans span = MultiSpansWrapper.wrap(searcher.getIndexReader(), q);
    assertNext(span,0,0,3);
    assertNext(span,1,0,4);
    assertFinished(span);
  }

  /**
   * test does not imply that skipTo(doc+1) should work exactly the
   * same as next -- it's only applicable in this case since we know doc
   * does not contain more than one span
   */
  public void testNearSpansAdvanceLikeNext() throws Exception {
    SpanNearQuery q = makeQuery();
    Spans span = MultiSpansWrapper.wrap(searcher.getIndexReader(), q);
    assertEquals(0, span.advance(0));
    assertEquals(0, span.nextStartPosition());
    assertEquals(s(0,0,3), s(span));
    assertEquals(1, span.advance(1));
    assertEquals(0, span.nextStartPosition());
    assertEquals(s(1,0,4), s(span));
    assertEquals(Spans.NO_MORE_DOCS, span.advance(2));
  }
  
  public void testNearSpansNextThenAdvance() throws Exception {
    SpanNearQuery q = makeQuery();
    Spans span = MultiSpansWrapper.wrap(searcher.getIndexReader(), q);
    assertNotSame(Spans.NO_MORE_DOCS, span.nextDoc());
    assertEquals(0, span.nextStartPosition());
    assertEquals(s(0,0,3), s(span));
    assertNotSame(Spans.NO_MORE_DOCS, span.advance(1));
    assertEquals(0, span.nextStartPosition());
    assertEquals(s(1,0,4), s(span));
    assertEquals(Spans.NO_MORE_DOCS, span.nextDoc());
  }
  
  public void testNearSpansNextThenAdvancePast() throws Exception {
    SpanNearQuery q = makeQuery();
    Spans span = MultiSpansWrapper.wrap(searcher.getIndexReader(), q);
    assertNotSame(Spans.NO_MORE_DOCS, span.nextDoc());
    assertEquals(0, span.nextStartPosition());
    assertEquals(s(0,0,3), s(span));
    assertEquals(Spans.NO_MORE_DOCS, span.advance(2));
  }
  
  public void testNearSpansAdvancePast() throws Exception {
    SpanNearQuery q = makeQuery();
    Spans span = MultiSpansWrapper.wrap(searcher.getIndexReader(), q);
    assertEquals(Spans.NO_MORE_DOCS, span.advance(2));
  }
  
  public void testNearSpansAdvanceTo0() throws Exception {
    SpanNearQuery q = makeQuery();
    Spans span = MultiSpansWrapper.wrap(searcher.getIndexReader(), q);
    assertEquals(0, span.advance(0));
    assertEquals(0, span.nextStartPosition());
    assertEquals(s(0,0,3), s(span));
  }

  public void testNearSpansAdvanceTo1() throws Exception {
    SpanNearQuery q = makeQuery();
    Spans span = MultiSpansWrapper.wrap(searcher.getIndexReader(), q);
    assertEquals(1, span.advance(1));
    assertEquals(0, span.nextStartPosition());
    assertEquals(s(1,0,4), s(span));
  }

  /**
   * not a direct test of NearSpans, but a demonstration of how/when
   * this causes problems
   */
  public void testSpanNearScorerSkipTo1() throws Exception {
    SpanNearQuery q = makeQuery();
    Weight w = searcher.createNormalizedWeight(q, true);
    IndexReaderContext topReaderContext = searcher.getTopReaderContext();
    LeafReaderContext leave = topReaderContext.leaves().get(0);
    Scorer s = w.scorer(leave);
    assertEquals(1, s.iterator().advance(1));
  }

  public void testOverlappedOrderedSpan() throws Exception {
    SpanNearQuery q = makeOverlappedQuery("w5", "w3", false, "w4", true);
    CheckHits.checkHits(random(), q, FIELD, searcher, new int[] {});
  }
  
  public void testOverlappedNonOrderedSpan() throws Exception {
    SpanNearQuery q = makeOverlappedQuery("w3", "w5", true, "w4", false);
    CheckHits.checkHits(random(), q, FIELD, searcher, new int[] {0});
  }

  public void testNonOverlappedOrderedSpan() throws Exception {
    SpanNearQuery q = makeOverlappedQuery("w3", "w4", true, "w5", true);
    CheckHits.checkHits(random(), q, FIELD, searcher, new int[] {0});
  }

  public void testOrderedSpanIteration() throws Exception {
    SpanNearQuery q = new SpanNearQuery(new SpanQuery[]{
        new SpanOrQuery(new SpanTermQuery(new Term(FIELD, "w1")), new SpanTermQuery(new Term(FIELD, "w2"))),
        new SpanTermQuery(new Term(FIELD, "w4"))
    }, 10, true);
    Spans spans = MultiSpansWrapper.wrap(reader, q);
    assertNext(spans,0,0,4);
    assertNext(spans,0,1,4);
    assertFinished(spans);
  }

  public void testOrderedSpanIterationSameTerms1() throws Exception {
    SpanNearQuery q = new SpanNearQuery(new SpanQuery[]{
        new SpanTermQuery(new Term(FIELD, "t1")), new SpanTermQuery(new Term(FIELD, "t2"))
    }, 1, true);
    Spans spans = MultiSpansWrapper.wrap(reader, q);
    assertNext(spans,4,0,2);
    assertFinished(spans);
  }

  public void testOrderedSpanIterationSameTerms2() throws Exception {
    SpanNearQuery q = new SpanNearQuery(new SpanQuery[]{
        new SpanTermQuery(new Term(FIELD, "t2")), new SpanTermQuery(new Term(FIELD, "t1"))
    }, 1, true);
    Spans spans = MultiSpansWrapper.wrap(reader, q);
    assertNext(spans,4,1,4);
    assertNext(spans,4,2,4);
    assertFinished(spans);
  }

  /**
   * not a direct test of NearSpans, but a demonstration of how/when
   * this causes problems
   */
  public void testSpanNearScorerExplain() throws Exception {
    SpanNearQuery q = makeQuery();
    Explanation e = searcher.explain(q, 1);
    assertTrue("Scorer explanation value for doc#1 isn't positive: "
               + e.toString(),
               0.0f <= e.getValue());
  }

  public void testGaps() throws Exception {
    SpanNearQuery q = SpanNearQuery.newOrderedNearQuery(FIELD)
        .addClause(new SpanTermQuery(new Term(FIELD, "w1")))
        .addGap(1)
        .addClause(new SpanTermQuery(new Term(FIELD, "w2")))
        .build();
    Spans spans = MultiSpansWrapper.wrap(reader, q);
    assertNext(spans, 1, 0, 3);
    assertNext(spans, 2, 0, 3);
    assertFinished(spans);

    q = SpanNearQuery.newOrderedNearQuery(FIELD)
        .addClause(new SpanTermQuery(new Term(FIELD, "w1")))
        .addGap(1)
        .addClause(new SpanTermQuery(new Term(FIELD, "w2")))
        .addGap(1)
        .addClause(new SpanTermQuery(new Term(FIELD, "w3")))
        .setSlop(1)
        .build();
    spans = MultiSpansWrapper.wrap(reader, q);
    assertNext(spans, 2, 0, 5);
    assertNext(spans, 3, 0, 6);
    assertFinished(spans);
  }

  public void testMultipleGaps() throws Exception {
    SpanQuery q = SpanNearQuery.newOrderedNearQuery(FIELD)
        .addClause(new SpanTermQuery(new Term(FIELD, "g")))
        .addGap(2)
        .addClause(new SpanTermQuery(new Term(FIELD, "g")))
        .build();
    Spans spans = MultiSpansWrapper.wrap(reader, q);
    assertNext(spans, 5, 0, 4);
    assertNext(spans, 5, 9, 13);
    assertFinished(spans);
  }

  /*
    protected String[] docFields = {
    "w1 w2 w3 w4 w5",
    "w1 w3 w2 w3 zz",
    "w1 xx w2 yy w3",
    "w1 w3 xx w2 yy w3 zz",
    "t1 t2 t2 t1",
    "g x x g g x x x g g x x g"
  };
   */
}
