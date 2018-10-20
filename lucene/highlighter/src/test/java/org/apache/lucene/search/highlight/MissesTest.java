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
package org.apache.lucene.search.highlight;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.util.LuceneTestCase;

public class MissesTest extends LuceneTestCase {
  public void testTermQuery() throws IOException, InvalidTokenOffsetsException {
    try (Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false)) {
      final Query query = new TermQuery(new Term("test", "foo"));
      final Highlighter highlighter = new Highlighter(new SimpleHTMLFormatter(), new QueryScorer(query));
      assertEquals("this is a <B>foo</B> bar example",
          highlighter.getBestFragment(analyzer, "test", "this is a foo bar example"));
      assertNull(highlighter.getBestFragment(analyzer, "test", "this does not match"));
    }
  }

  public void testBooleanQuery() throws IOException, InvalidTokenOffsetsException {
    try (Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false)) {
      final BooleanQuery.Builder query = new BooleanQuery.Builder();
      query.add(new TermQuery(new Term("test", "foo")), Occur.MUST);
      query.add(new TermQuery(new Term("test", "bar")), Occur.MUST);
      final Highlighter highlighter = new Highlighter(new SimpleHTMLFormatter(), new QueryScorer(query.build()));
      assertEquals("this is a <B>foo</B> <B>bar</B> example",
          highlighter.getBestFragment(analyzer, "test", "this is a foo bar example"));
      assertNull(highlighter.getBestFragment(analyzer, "test", "this does not match"));
    }
  }

  public void testPhraseQuery() throws IOException, InvalidTokenOffsetsException {
    try (Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false)) {
      final PhraseQuery query = new PhraseQuery("test", "foo", "bar");
      final Highlighter highlighter = new Highlighter(new SimpleHTMLFormatter(), new QueryScorer(query));
      assertEquals("this is a <B>foo</B> <B>bar</B> example",
          highlighter.getBestFragment(analyzer, "test", "this is a foo bar example"));
      assertNull(highlighter.getBestFragment(analyzer, "test", "this does not match"));
    }
  }

  public void testSpanNearQuery() throws IOException, InvalidTokenOffsetsException {
    try (Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false)) {
      final Query query = new SpanNearQuery(new SpanQuery[] {
          new SpanTermQuery(new Term("test", "foo")),
          new SpanTermQuery(new Term("test", "bar"))}, 0, true);
      final Highlighter highlighter = new Highlighter(new SimpleHTMLFormatter(), new QueryScorer(query));
      assertEquals("this is a <B>foo</B> <B>bar</B> example",
          highlighter.getBestFragment(analyzer, "test", "this is a foo bar example"));
      assertNull(highlighter.getBestFragment(analyzer, "test", "this does not match"));
    }
  }
}
