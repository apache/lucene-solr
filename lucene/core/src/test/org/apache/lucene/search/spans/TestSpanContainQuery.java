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
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.CheckHits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

import static org.apache.lucene.search.spans.SpanTestUtil.*;

public class TestSpanContainQuery extends LuceneTestCase {
  IndexSearcher searcher;
  IndexReader reader;
  Directory directory;

  static final String field = "field";

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

  String[] docFields = {
    "w1 w2 w3 w4 w5",
    "w1 w3 w2 w3",
    "w1 xx w2 yy w3",
    "w1 w3 xx w2 yy w3",
  };

  void checkHits(Query query, int[] results) throws Exception {
    CheckHits.checkHits(random(), query, field, searcher, results);
  }

  Spans makeSpans(SpanQuery sq) throws Exception {
    return MultiSpansWrapper.wrap(searcher.getIndexReader(), sq);
  }

  void tstEqualSpans(String mes, SpanQuery expectedQ, SpanQuery actualQ) throws Exception {
    Spans expected = makeSpans(expectedQ);
    Spans actual = makeSpans(actualQ);
    tstEqualSpans(mes, expected, actual);
  }

  void tstEqualSpans(String mes, Spans expected, Spans actual) throws Exception {
    while (expected.nextDoc() != Spans.NO_MORE_DOCS) {
      assertEquals(expected.docID(), actual.nextDoc());
      assertEquals(expected.docID(), actual.docID());
      while (expected.nextStartPosition() != Spans.NO_MORE_POSITIONS) {
        assertEquals(expected.startPosition(), actual.nextStartPosition());
        assertEquals("start", expected.startPosition(), actual.startPosition());
        assertEquals("end", expected.endPosition(), actual.endPosition());
      }
    }
  }

  public void testSpanContainTerm() throws Exception {
    SpanQuery stq = spanTermQuery(field, "w3");
    SpanQuery containingQ = spanContainingQuery(stq, stq);
    SpanQuery containedQ = spanWithinQuery(stq, stq);
    tstEqualSpans("containing", stq, containingQ);
    tstEqualSpans("containing", stq, containedQ);
  }

  public void testSpanContainPhraseBothWords() throws Exception {
    String w2 = "w2";
    String w3 = "w3";
    SpanQuery phraseQ = spanNearOrderedQuery(field, 0, w2, w3);
    SpanQuery w23 = spanOrQuery(field, w2, w3);
    SpanQuery containingPhraseOr = spanContainingQuery(phraseQ, w23);
    SpanQuery containedPhraseOr = spanWithinQuery(phraseQ, w23);
    tstEqualSpans("containing phrase or", phraseQ, containingPhraseOr);
    Spans spans = makeSpans(containedPhraseOr);
    assertNext(spans,0,1,2);
    assertNext(spans,0,2,3);
    assertNext(spans,1,2,3);
    assertNext(spans,1,3,4);
    assertFinished(spans);
  }

  public void testSpanContainPhraseFirstWord() throws Exception {
    String w2 = "w2";
    String w3 = "w3";
    SpanQuery stqw2 = spanTermQuery(field, w2);
    SpanQuery phraseQ = spanNearOrderedQuery(field, 0, w2, w3);
    SpanQuery containingPhraseW2 = spanContainingQuery(phraseQ, stqw2);
    SpanQuery containedPhraseW2 = spanWithinQuery(phraseQ, stqw2);
    tstEqualSpans("containing phrase w2", phraseQ, containingPhraseW2);
    Spans spans = makeSpans(containedPhraseW2);
    assertNext(spans,0,1,2);
    assertNext(spans,1,2,3);
    assertFinished(spans);
  }

  public void testSpanContainPhraseSecondWord() throws Exception {
    String w2 = "w2";
    String w3 = "w3";
    SpanQuery stqw3 = spanTermQuery(field, w3);
    SpanQuery phraseQ = spanNearOrderedQuery(field, 0, w2, w3);
    SpanQuery containingPhraseW3 = spanContainingQuery(phraseQ, stqw3);
    SpanQuery containedPhraseW3 = spanWithinQuery(phraseQ, stqw3);
    tstEqualSpans("containing phrase w3", phraseQ, containingPhraseW3);
    Spans spans = makeSpans(containedPhraseW3);
    assertNext(spans,0,2,3);
    assertNext(spans,1,3,4);
    assertFinished(spans);
  }

}