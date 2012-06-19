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
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.CheckHits;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

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
    RandomIndexWriter writer= new RandomIndexWriter(random(), directory, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));
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
    "w1 w3 xx w2 yy w3 zz"
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
  
  public void testSpanNearQuery() throws Exception {
    SpanNearQuery q = makeQuery();
    CheckHits.checkHits(random(), q, FIELD, searcher, new int[] {0,1});
  }

  public String s(Spans span) {
    return s(span.doc(), span.start(), span.end());
  }
  public String s(int doc, int start, int end) {
    return "s(" + doc + "," + start + "," + end +")";
  }
  
  public void testNearSpansNext() throws Exception {
    SpanNearQuery q = makeQuery();
    Spans span = MultiSpansWrapper.wrap(searcher.getTopReaderContext(), q);
    assertEquals(true, span.next());
    assertEquals(s(0,0,3), s(span));
    assertEquals(true, span.next());
    assertEquals(s(1,0,4), s(span));
    assertEquals(false, span.next());
  }

  /**
   * test does not imply that skipTo(doc+1) should work exactly the
   * same as next -- it's only applicable in this case since we know doc
   * does not contain more than one span
   */
  public void testNearSpansSkipToLikeNext() throws Exception {
    SpanNearQuery q = makeQuery();
    Spans span =  MultiSpansWrapper.wrap(searcher.getTopReaderContext(), q);
    assertEquals(true, span.skipTo(0));
    assertEquals(s(0,0,3), s(span));
    assertEquals(true, span.skipTo(1));
    assertEquals(s(1,0,4), s(span));
    assertEquals(false, span.skipTo(2));
  }
  
  public void testNearSpansNextThenSkipTo() throws Exception {
    SpanNearQuery q = makeQuery();
    Spans span =  MultiSpansWrapper.wrap(searcher.getTopReaderContext(), q);
    assertEquals(true, span.next());
    assertEquals(s(0,0,3), s(span));
    assertEquals(true, span.skipTo(1));
    assertEquals(s(1,0,4), s(span));
    assertEquals(false, span.next());
  }
  
  public void testNearSpansNextThenSkipPast() throws Exception {
    SpanNearQuery q = makeQuery();
    Spans span =  MultiSpansWrapper.wrap(searcher.getTopReaderContext(), q);
    assertEquals(true, span.next());
    assertEquals(s(0,0,3), s(span));
    assertEquals(false, span.skipTo(2));
  }
  
  public void testNearSpansSkipPast() throws Exception {
    SpanNearQuery q = makeQuery();
    Spans span =  MultiSpansWrapper.wrap(searcher.getTopReaderContext(), q);
    assertEquals(false, span.skipTo(2));
  }
  
  public void testNearSpansSkipTo0() throws Exception {
    SpanNearQuery q = makeQuery();
    Spans span = MultiSpansWrapper.wrap(searcher.getTopReaderContext(), q);
    assertEquals(true, span.skipTo(0));
    assertEquals(s(0,0,3), s(span));
  }

  public void testNearSpansSkipTo1() throws Exception {
    SpanNearQuery q = makeQuery();
    Spans span =  MultiSpansWrapper.wrap(searcher.getTopReaderContext(), q);
    assertEquals(true, span.skipTo(1));
    assertEquals(s(1,0,4), s(span));
  }

  /**
   * not a direct test of NearSpans, but a demonstration of how/when
   * this causes problems
   */
  public void testSpanNearScorerSkipTo1() throws Exception {
    SpanNearQuery q = makeQuery();
    Weight w = searcher.createNormalizedWeight(q);
    IndexReaderContext topReaderContext = searcher.getTopReaderContext();
    AtomicReaderContext leave = topReaderContext.leaves().get(0);
    Scorer s = w.scorer(leave, true, false, leave.reader().getLiveDocs());
    assertEquals(1, s.advance(1));
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
               0.0f < e.getValue());
  }
  
}
