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

import org.apache.lucene.index.Term;
import org.apache.lucene.search.QueryUtils;

import static org.junit.Assert.*;

/** Some utility methods used for testing span queries */
public class SpanTestUtil {
  
  /** 
   * Adds additional asserts to a spanquery. Highly recommended 
   * if you want tests to actually be debuggable.
   */
  public static SpanQuery spanQuery(SpanQuery query) {
    QueryUtils.check(query);
    return new AssertingSpanQuery(query);
  }
  
  /**
   * Makes a new SpanTermQuery (with additional asserts).
   */
  public static SpanQuery spanTermQuery(String field, String term) {
    return spanQuery(new SpanTermQuery(new Term(field, term)));
  }
  
  /**
   * Makes a new SpanOrQuery (with additional asserts) from the provided {@code terms}.
   */
  public static SpanQuery spanOrQuery(String field, String... terms) {
    SpanQuery[] subqueries = new SpanQuery[terms.length];
    for (int i = 0; i < terms.length; i++) {
      subqueries[i] = spanTermQuery(field, terms[i]);
    }
    return spanOrQuery(subqueries);
  }
  
  /**
   * Makes a new SpanOrQuery (with additional asserts).
   */
  public static SpanQuery spanOrQuery(SpanQuery... subqueries) {
    return spanQuery(new SpanOrQuery(subqueries));
  }
  
  /**
   * Makes a new SpanNotQuery (with additional asserts).
   */
  public static SpanQuery spanNotQuery(SpanQuery include, SpanQuery exclude) {
    return spanQuery(new SpanNotQuery(include, exclude));
  }
  
  /**
   * Makes a new SpanNotQuery (with additional asserts).
   */
  public static SpanQuery spanNotQuery(SpanQuery include, SpanQuery exclude, int pre, int post) {
    return spanQuery(new SpanNotQuery(include, exclude, pre, post));
  }
  
  /**
   * Makes a new SpanFirstQuery (with additional asserts).
   */
  public static SpanQuery spanFirstQuery(SpanQuery query, int end) {
    return spanQuery(new SpanFirstQuery(query, end));
  }
  
  /**
   * Makes a new SpanPositionRangeQuery (with additional asserts).
   */
  public static SpanQuery spanPositionRangeQuery(SpanQuery query, int start, int end) {
    return spanQuery(new SpanPositionRangeQuery(query, start, end));
  }
  
  /**
   * Makes a new SpanContainingQuery (with additional asserts).
   */
  public static SpanQuery spanContainingQuery(SpanQuery big, SpanQuery little) {
    return spanQuery(new SpanContainingQuery(big, little));
  }
  
  /**
   * Makes a new SpanWithinQuery (with additional asserts).
   */
  public static SpanQuery spanWithinQuery(SpanQuery big, SpanQuery little) {
    return spanQuery(new SpanWithinQuery(big, little));
  }
  
  /**
   * Makes a new ordered SpanNearQuery (with additional asserts) from the provided {@code terms}
   */
  public static SpanQuery spanNearOrderedQuery(String field, int slop, String... terms) {
    SpanQuery[] subqueries = new SpanQuery[terms.length];
    for (int i = 0; i < terms.length; i++) {
      subqueries[i] = spanTermQuery(field, terms[i]);
    }
    return spanNearOrderedQuery(slop, subqueries);
  }
  
  /**
   * Makes a new ordered SpanNearQuery (with additional asserts)
   */
  public static SpanQuery spanNearOrderedQuery(int slop, SpanQuery... subqueries) {
    return spanQuery(new SpanNearQuery(subqueries, slop, true));
  }
  
  /**
   * Makes a new unordered SpanNearQuery (with additional asserts) from the provided {@code terms}
   */
  public static SpanQuery spanNearUnorderedQuery(String field, int slop, String... terms) {
    SpanNearQuery.Builder builder = SpanNearQuery.newUnorderedNearQuery(field);
    builder.setSlop(slop);
    for (String term : terms) {
      builder.addClause(new SpanTermQuery(new Term(field, term)));
    }
    return spanQuery(builder.build());
  }
  
  /**
   * Makes a new unordered SpanNearQuery (with additional asserts)
   */
  public static SpanQuery spanNearUnorderedQuery(int slop, SpanQuery... subqueries) {
    return spanQuery(new SpanNearQuery(subqueries, slop, false));
  }
  
  /** 
   * Assert the next iteration from {@code spans} is a match
   * from {@code start} to {@code end} in {@code doc}.
   */
  public static void assertNext(Spans spans, int doc, int start, int end) throws IOException {
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
  
  /** 
   * Assert that {@code spans} is exhausted.
   */
  public static void assertFinished(Spans spans) throws Exception {
    if (spans != null) { // null Spans is empty
      assertNext(spans, Spans.NO_MORE_DOCS, -2, -2); // start and end positions will be ignored
    }
  }
}
