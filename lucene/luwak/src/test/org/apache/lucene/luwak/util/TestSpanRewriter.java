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

package org.apache.lucene.luwak.util;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.util.LuceneTestCase;

public class TestSpanRewriter extends LuceneTestCase {

  public void testBoostQuery() throws Exception {
    Query q = new SpanRewriter().rewrite(new BoostQuery(new TermQuery(new Term("f", "t")), 2.0f), null);
    assertTrue(q instanceof SpanOffsetReportingQuery);
  }

  public void testMultiTermQueryEquals() throws Exception {

    WildcardQuery wq = new WildcardQuery(new Term("field", "term"));
    Query q1 = new SpanRewriter().rewrite(wq, null);
    Query q2 = new SpanRewriter().rewrite(wq, null);

    assertEquals(q1, q2);
  }

  public void testPhraseQuery() throws Exception {

    PhraseQuery pq = new PhraseQuery(1, "field1", "term1", "term2");

    Query q = new SpanRewriter().rewrite(pq, null);
    assertTrue(q instanceof SpanOffsetReportingQuery);

    SpanOffsetReportingQuery or = (SpanOffsetReportingQuery) q;
    assertTrue(or.getWrappedQuery() instanceof SpanNearQuery);
    SpanNearQuery sq = (SpanNearQuery) or.getWrappedQuery();
    SpanNearQuery expected = SpanNearQuery.newOrderedNearQuery("field1")
        .addClause(new SpanTermQuery(new Term("field1", "term1")))
        .addClause(new SpanTermQuery(new Term("field1", "term2")))
        .setSlop(1)
        .build();
    assertEquals(expected, sq);
  }

  public void testConstantScoreQuery() throws Exception {

    Query q = new ConstantScoreQuery(new TermQuery(new Term("field", "term")));

    Query rewritten = new SpanRewriter().rewrite(q, null);
    assertTrue(rewritten instanceof SpanOffsetReportingQuery);

  }
}
