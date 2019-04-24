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

package org.apache.lucene.monitor;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.AttributeReflector;
import org.apache.lucene.util.BytesRef;

public abstract class PresearcherTestBase extends MonitorTestBase {

  public Monitor newMonitor() throws IOException {
    return new Monitor(WHITESPACE, createPresearcher());
  }

  protected abstract Presearcher createPresearcher();

  static final String TEXTFIELD = FIELD;
  static final Analyzer WHITESPACE = new WhitespaceAnalyzer();

  public static Document buildDoc(String field, String text) {
    Document doc = new Document();
    doc.add(newTextField(field, text, Field.Store.NO));
    return doc;
  }

  public void testNullFieldHandling() throws IOException {
    try (Monitor monitor = newMonitor()) {
      monitor.register(new MonitorQuery("1", parse("field_1:test")));

      assertEquals(0,
          monitor.match(buildDoc("field_2", "test"), QueryMatch.SIMPLE_MATCHER).getMatchCount());
    }

  }

  public void testEmptyMonitorHandling() throws IOException {
    try (Monitor monitor = newMonitor()) {
      MatchingQueries<QueryMatch> matches = monitor.match(buildDoc("field_2", "test"), QueryMatch.SIMPLE_MATCHER);
      assertEquals(0, matches.getMatchCount());
      assertEquals(0, matches.getQueriesRun());
    }
  }

  public void testMatchAllQueryHandling() throws IOException {
    try (Monitor monitor = newMonitor()) {
      monitor.register(new MonitorQuery("1", new MatchAllDocsQuery()));
      assertEquals(1,
          monitor.match(buildDoc("f", "wibble"), QueryMatch.SIMPLE_MATCHER).getMatchCount());
    }
  }

  public void testNegativeQueryHandling() throws IOException {
    Query q = new BooleanQuery.Builder()
        .add(new MatchAllDocsQuery(), BooleanClause.Occur.SHOULD)
        .add(new TermQuery(new Term("f", "foo")), BooleanClause.Occur.MUST_NOT)
        .build();
    try (Monitor monitor = newMonitor()) {
      monitor.register(new MonitorQuery("1", q));

      MultiMatchingQueries<QueryMatch> matches = monitor.match(new Document[]{
          buildDoc("f", "bar"), buildDoc("f", "foo")
      }, QueryMatch.SIMPLE_MATCHER);
      assertEquals(1, matches.getMatchCount(0));
      assertEquals(0, matches.getMatchCount(1));
    }
  }

  public void testAnyTokenHandling() throws IOException {

    try (Monitor monitor = newMonitor()) {
      monitor.register(new MonitorQuery("1", new MatchAllDocsQuery()));
      MatchingQueries<QueryMatch> matches = monitor.match(buildDoc("f", "wibble"), QueryMatch.SIMPLE_MATCHER);
      assertEquals(1, matches.getMatchCount());
      assertEquals(1, matches.getQueriesRun());
    }
  }

  private static final BytesRef NON_STRING_TERM = new BytesRef(new byte[]{60, 8, 0, 0, 0, 9});

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

  public void testNonStringTermHandling() throws IOException {

    FieldType ft = new FieldType();
    ft.setTokenized(true);
    ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS);

    try (Monitor monitor = newMonitor()) {
      monitor.register(new MonitorQuery("1", new TermQuery(new Term("f", NON_STRING_TERM))));

      Document doc = new Document();
      doc.add(new Field("f", new NonStringTokenStream(), ft));
      MatchingQueries<QueryMatch> m = monitor.match(doc, QueryMatch.SIMPLE_MATCHER);
      assertEquals(1, m.getMatchCount());
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
