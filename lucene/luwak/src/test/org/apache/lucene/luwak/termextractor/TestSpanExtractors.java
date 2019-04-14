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

package org.apache.lucene.luwak.termextractor;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.index.Term;
import org.apache.lucene.luwak.termextractor.weights.TermWeightor;
import org.apache.lucene.luwak.termextractor.weights.TokenLengthNorm;
import org.apache.lucene.queries.payloads.MaxPayloadFunction;
import org.apache.lucene.queries.payloads.PayloadDecoder;
import org.apache.lucene.queries.payloads.PayloadScoreQuery;
import org.apache.lucene.queries.payloads.SpanPayloadCheckQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.spans.FieldMaskingSpanQuery;
import org.apache.lucene.search.spans.SpanBoostQuery;
import org.apache.lucene.search.spans.SpanContainingQuery;
import org.apache.lucene.search.spans.SpanFirstQuery;
import org.apache.lucene.search.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.search.spans.SpanWithinQuery;
import org.apache.lucene.util.LuceneTestCase;

public class TestSpanExtractors extends LuceneTestCase  {

  private static final QueryAnalyzer treeBuilder = new QueryAnalyzer();

  private static final TermWeightor WEIGHTOR = new TermWeightor(new TokenLengthNorm());

  public void testOrderedNearExtractor() {
    SpanNearQuery q = new SpanNearQuery(new SpanQuery[]{
        new SpanTermQuery(new Term("field1", "term1")),
        new SpanTermQuery(new Term("field1", "term"))
    }, 0, true);

    Set<QueryTerm> expected = Collections.singleton(new QueryTerm("field1", "term1", QueryTerm.Type.EXACT));
    assertEquals(expected, treeBuilder.collectTerms(q, WEIGHTOR));
  }

  public void testOrderedNearWithWildcardExtractor() {
    SpanNearQuery q = new SpanNearQuery(new SpanQuery[]{
        new SpanMultiTermQueryWrapper<>(new RegexpQuery(new Term("field", "super.*cali.*"))),
        new SpanTermQuery(new Term("field", "is"))
    }, 0, true);

    Set<QueryTerm> expected = Collections.singleton(new QueryTerm("field", "is", QueryTerm.Type.EXACT));
    assertEquals(expected, treeBuilder.collectTerms(q, WEIGHTOR));
  }

  public void testSpanOrExtractor() {
    SpanOrQuery or = new SpanOrQuery(new SpanTermQuery(new Term("field", "term1")),
        new SpanTermQuery(new Term("field", "term2")));
    Set<QueryTerm> expected = new HashSet<>(Arrays.asList(
        new QueryTerm("field", "term1", QueryTerm.Type.EXACT),
        new QueryTerm("field", "term2", QueryTerm.Type.EXACT)
    ));
    assertEquals(expected, treeBuilder.collectTerms(or, WEIGHTOR));
  }

  public void testSpanMultiTerms() {
    SpanQuery q = new SpanMultiTermQueryWrapper<>(new RegexpQuery(new Term("field", "term.*")));
    Set<QueryTerm> terms = treeBuilder.collectTerms(q, WEIGHTOR);
    assertEquals(1, terms.size());
    assertEquals(QueryTerm.Type.ANY, terms.iterator().next().type);
  }

  public void testSpanWithin() {
    Term t1 = new Term("field", "term1");
    Term t2 = new Term("field", "term22");
    Term t3 = new Term("field", "term333");
    SpanWithinQuery swq = new SpanWithinQuery(
        SpanNearQuery.newOrderedNearQuery("field")
            .addClause(new SpanTermQuery(t1))
            .addClause(new SpanTermQuery(t2))
            .build(),
        new SpanTermQuery(t3));

    assertEquals(Collections.singleton(new QueryTerm(t3)), treeBuilder.collectTerms(swq, WEIGHTOR));
  }

  public void testSpanContains() {
    Term t1 = new Term("field", "term1");
    Term t2 = new Term("field", "term22");
    Term t3 = new Term("field", "term333");
    SpanContainingQuery swq = new SpanContainingQuery(
        SpanNearQuery.newOrderedNearQuery("field")
            .addClause(new SpanTermQuery(t1))
            .addClause(new SpanTermQuery(t2))
            .build(),
        new SpanTermQuery(t3));

    assertEquals(Collections.singleton(new QueryTerm(t3)), treeBuilder.collectTerms(swq, WEIGHTOR));
  }

  public void testSpanBoost() {
    Term t1 = new Term("field", "term1");
    SpanBoostQuery q = new SpanBoostQuery(new SpanTermQuery(t1), 0.1f);
    assertEquals(Collections.singleton(new QueryTerm(t1)), treeBuilder.collectTerms(q, WEIGHTOR));
  }

  public void testFieldMaskingSpanQuery() {
    Term t1 = new Term("field", "term1");
    FieldMaskingSpanQuery q = new FieldMaskingSpanQuery(new SpanTermQuery(t1), "field2");
    assertEquals(Collections.singleton(new QueryTerm(t1)), treeBuilder.collectTerms(q, WEIGHTOR));
  }

  public void testSpanPositionQuery() {
    Term t1 = new Term("field", "term");
    Query q = new SpanFirstQuery(new SpanTermQuery(t1), 10);
    assertEquals(Collections.singleton(new QueryTerm(t1)), treeBuilder.collectTerms(q, WEIGHTOR));
  }

  public void testPayloadScoreQuery() {
    Term t1 = new Term("field", "term");
    Query q = new PayloadScoreQuery(new SpanTermQuery(t1), new MaxPayloadFunction(), PayloadDecoder.FLOAT_DECODER);
    assertEquals(Collections.singleton(new QueryTerm(t1)), treeBuilder.collectTerms(q, WEIGHTOR));
  }

  public void testSpanPayloadCheckQuery() {
    Term t1 = new Term("field", "term");
    Query q = new SpanPayloadCheckQuery(new SpanTermQuery(t1), Collections.emptyList());
    assertEquals(Collections.singleton(new QueryTerm(t1)), treeBuilder.collectTerms(q, WEIGHTOR));
  }
}
