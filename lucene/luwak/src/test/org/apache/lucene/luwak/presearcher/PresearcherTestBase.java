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

package org.apache.lucene.luwak.presearcher;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.luwak.DocumentBatch;
import org.apache.lucene.luwak.InputDocument;
import org.apache.lucene.luwak.Matches;
import org.apache.lucene.luwak.Monitor;
import org.apache.lucene.luwak.MonitorQuery;
import org.apache.lucene.luwak.MonitorQueryParser;
import org.apache.lucene.luwak.Presearcher;
import org.apache.lucene.luwak.QueryMatch;
import org.apache.lucene.luwak.UpdateException;
import org.apache.lucene.luwak.matchers.SimpleMatcher;
import org.apache.lucene.luwak.queryparsers.LuceneQueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.AttributeReflector;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

public abstract class PresearcherTestBase extends LuceneTestCase {

  Monitor newMonitor() throws IOException {
    return new Monitor(new LuceneQueryParser(TEXTFIELD, WHITESPACE), createPresearcher());
  }

  protected abstract Presearcher createPresearcher();

  static final String TEXTFIELD = "text";
  static final Analyzer WHITESPACE = new WhitespaceAnalyzer();

  public static InputDocument buildDoc(String id, String field, String text) {
    return InputDocument.builder(id).addField(field, text, WHITESPACE).build();
  }

  public void testNullFieldHandling() throws IOException, UpdateException {
    try (Monitor monitor = newMonitor()) {
      monitor.update(new MonitorQuery("1", "field_1:test"));

      assertEquals(0,
          monitor.match(buildDoc("doc1", "field_2", "test"), SimpleMatcher.FACTORY).getMatchCount("doc1"));
    }

  }

  public void testEmptyMonitorHandling() throws IOException {
    try (Monitor monitor = newMonitor()) {
      Matches<QueryMatch> matches = monitor.match(buildDoc("doc1", "field_2", "test"), SimpleMatcher.FACTORY);
      assertEquals(0, matches.getMatchCount("doc1"));
      assertEquals(0, matches.getQueriesRun());
    }
  }

  public void testMatchAllQueryHandling() throws IOException, UpdateException {
    try (Monitor monitor = newMonitor()) {
      monitor.update(new MonitorQuery("1", "*:*"));
      assertEquals(1,
          monitor.match(buildDoc("doc1", "f", "wibble"), SimpleMatcher.FACTORY).getMatchCount("doc1"));
    }
  }

  public void testNegativeQueryHandling() throws IOException, UpdateException {
    try (Monitor monitor = newMonitor()) {
      monitor.update(new MonitorQuery("1", "*:* -f:foo"));

      DocumentBatch batch = DocumentBatch.of(
          InputDocument.builder("doc1").addField("f", "bar", WHITESPACE).build(),
          InputDocument.builder("doc2").addField("f", "foo", WHITESPACE).build());

      Matches<QueryMatch> matches = monitor.match(batch, SimpleMatcher.FACTORY);
      assertEquals(1, matches.getMatchCount("doc1"));
      assertEquals(0, matches.getMatchCount("doc2"));
    }
  }

  static class TestQuery extends Query {

    @Override
    public String toString(String field) {
      return "TestQuery";
    }

    @Override
    public Query rewrite(IndexReader reader) {
      return new MatchAllDocsQuery();
    }

    @Override
    public void visit(QueryVisitor visitor) {

    }

    @Override
    public boolean equals(Object o) {
      return false;
    }

    @Override
    public int hashCode() {
      return 0;
    }
  }

  static class TestQueryParser implements MonitorQueryParser {

    @Override
    public Query parse(String queryString, Map<String, String> metadata) {
      return new TestQuery();
    }
  }

  public void testAnyTokenHandling() throws IOException, UpdateException {

    try (Monitor monitor = new Monitor(new TestQueryParser(), createPresearcher())) {
      monitor.update(new MonitorQuery("1", "testquery"));
      Matches<QueryMatch> matches = monitor.match(buildDoc("1", "f", "wibble"), SimpleMatcher.FACTORY);
      assertEquals(1, matches.getMatchCount("1"));
      assertEquals(1, matches.getQueriesRun());
    }
  }

  private static final BytesRef NON_STRING_TERM = new BytesRef(new byte[]{60, 8, 0, 0, 0, 9});

  static class NonStringTermQueryParser implements MonitorQueryParser {

    @Override
    public Query parse(String queryString, Map<String, String> metadata) {
      return new TermQuery(new Term("f", NON_STRING_TERM));
    }
  }

  static class BytesRefAttribute extends AttributeImpl implements TermToBytesRefAttribute {

    @Override
    public BytesRef getBytesRef() {
      return NON_STRING_TERM;
    }

    @Override
    public void clear() {

    }

    @Override
    public void reflectWith(AttributeReflector attributeReflector) {

    }

    @Override
    public void copyTo(AttributeImpl attribute) {

    }
  }

  static final class NonStringTokenStream extends TokenStream {

    final TermToBytesRefAttribute att;
    boolean done = false;

    NonStringTokenStream() {
      addAttributeImpl(new BytesRefAttribute());
      this.att = addAttribute(TermToBytesRefAttribute.class);
    }

    @Override
    public boolean incrementToken() {
      if (done)
        return false;
      return done = true;
    }
  }

  public void testNonStringTermHandling() throws IOException, UpdateException {

    try (Monitor monitor = new Monitor(new NonStringTermQueryParser(), createPresearcher())) {
      monitor.update(new MonitorQuery("1", "testquery"));

      InputDocument doc = InputDocument.builder("1").addField(new TextField("f", new NonStringTokenStream())).build();
      Matches<QueryMatch> m = monitor.match(doc, SimpleMatcher.FACTORY);
      assertEquals(1, m.getMatchCount("1"));
      assertEquals(1, m.getQueriesRun());
    }

  }

  public static BooleanClause must(Query q) {
    return new BooleanClause(q, BooleanClause.Occur.MUST);
  }

  public static BooleanClause should(Query q) {
    return new BooleanClause(q, BooleanClause.Occur.SHOULD);
  }

}
