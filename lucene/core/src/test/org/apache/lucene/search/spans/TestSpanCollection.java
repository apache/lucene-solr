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
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

public class TestSpanCollection extends LuceneTestCase {

  protected IndexSearcher searcher;
  protected Directory directory;
  protected IndexReader reader;

  public static final String FIELD = "field";

  public static FieldType OFFSETS = new FieldType(TextField.TYPE_STORED);
  static {
    OFFSETS.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
  }

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
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory,
        newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(NoMergePolicy.INSTANCE));
    for (int i = 0; i < docFields.length; i++) {
      Document doc = new Document();
      doc.add(newField(FIELD, docFields[i], OFFSETS));
      writer.addDocument(doc);
    }
    reader = writer.getReader();
    writer.close();
    searcher = newSearcher(reader);
  }

  private static class TermCollector implements SpanCollector {

    final Set<Term> terms = new HashSet<>();

    @Override
    public void collectLeaf(PostingsEnum postings, int position, Term term) throws IOException {
      terms.add(term);
    }

    @Override
    public void reset() {
      terms.clear();
    }

  }

  protected String[] docFields = {
      "w1 w2 w3 w4 w5",
      "w1 w3 w2 w3 zz",
      "w1 xx w2 yy w4",
      "w1 w2 w1 w4 w2 w3"
  };

  private void checkCollectedTerms(Spans spans, TermCollector collector, Term... expectedTerms) throws IOException {
    collector.reset();
    spans.collect(collector);
    for (Term t : expectedTerms) {
      assertTrue("Missing term " + t, collector.terms.contains(t));
    }
    assertEquals("Unexpected terms found", expectedTerms.length, collector.terms.size());
  }

  @Test
  public void testNestedNearQuery() throws IOException {

    // near(w1, near(w2, or(w3, w4)))

    SpanTermQuery q1 = new SpanTermQuery(new Term(FIELD, "w1"));
    SpanTermQuery q2 = new SpanTermQuery(new Term(FIELD, "w2"));
    SpanTermQuery q3 = new SpanTermQuery(new Term(FIELD, "w3"));
    SpanTermQuery q4 = new SpanTermQuery(new Term(FIELD, "w4"));

    SpanOrQuery q5 = new SpanOrQuery(q4, q3);
    SpanNearQuery q6 = new SpanNearQuery(new SpanQuery[]{q2, q5}, 1, true);
    SpanNearQuery q7 = new SpanNearQuery(new SpanQuery[]{q1, q6}, 1, true);

    TermCollector collector = new TermCollector();
    Spans spans = MultiSpansWrapper.wrap(reader, q7, SpanWeight.Postings.POSITIONS);
    assertEquals(0, spans.advance(0));
    spans.nextStartPosition();
    checkCollectedTerms(spans, collector, new Term(FIELD, "w1"), new Term(FIELD, "w2"), new Term(FIELD, "w3"));

    assertEquals(3, spans.advance(3));
    spans.nextStartPosition();
    checkCollectedTerms(spans, collector, new Term(FIELD, "w1"), new Term(FIELD, "w2"), new Term(FIELD, "w4"));
    spans.nextStartPosition();
    checkCollectedTerms(spans, collector, new Term(FIELD, "w1"), new Term(FIELD, "w2"), new Term(FIELD, "w3"));

  }

  @Test
  public void testOrQuery() throws IOException {
    SpanTermQuery q2 = new SpanTermQuery(new Term(FIELD, "w2"));
    SpanTermQuery q3 = new SpanTermQuery(new Term(FIELD, "w3"));
    SpanOrQuery orQuery = new SpanOrQuery(q2, q3);

    TermCollector collector = new TermCollector();
    Spans spans = MultiSpansWrapper.wrap(reader, orQuery, SpanWeight.Postings.POSITIONS);

    assertEquals(1, spans.advance(1));
    spans.nextStartPosition();
    checkCollectedTerms(spans, collector, new Term(FIELD, "w3"));
    spans.nextStartPosition();
    checkCollectedTerms(spans, collector, new Term(FIELD, "w2"));
    spans.nextStartPosition();
    checkCollectedTerms(spans, collector, new Term(FIELD, "w3"));

    assertEquals(3, spans.advance(3));
    spans.nextStartPosition();
    checkCollectedTerms(spans, collector, new Term(FIELD, "w2"));
    spans.nextStartPosition();
    checkCollectedTerms(spans, collector, new Term(FIELD, "w2"));
    spans.nextStartPosition();
    checkCollectedTerms(spans, collector, new Term(FIELD, "w3"));
  }

  @Test
  public void testSpanNotQuery() throws IOException {

    SpanTermQuery q1 = new SpanTermQuery(new Term(FIELD, "w1"));
    SpanTermQuery q2 = new SpanTermQuery(new Term(FIELD, "w2"));
    SpanTermQuery q3 = new SpanTermQuery(new Term(FIELD, "w3"));

    SpanNearQuery nq = new SpanNearQuery(new SpanQuery[]{q1, q2}, 2, true);
    SpanNotQuery notq = new SpanNotQuery(nq, q3);

    TermCollector collector = new TermCollector();
    Spans spans = MultiSpansWrapper.wrap(reader, notq, SpanWeight.Postings.POSITIONS);

    assertEquals(2, spans.advance(2));
    spans.nextStartPosition();
    checkCollectedTerms(spans, collector, new Term(FIELD, "w1"), new Term(FIELD, "w2"));

  }

}

