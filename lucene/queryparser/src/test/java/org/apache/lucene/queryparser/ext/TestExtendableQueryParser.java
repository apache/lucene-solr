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
package org.apache.lucene.queryparser.ext;

import java.util.Locale;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.classic.QueryParserBase;
import org.apache.lucene.queryparser.classic.TestQueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

/**
 * Testcase for the class {@link ExtendableQueryParser}
 */
public class TestExtendableQueryParser extends TestQueryParser {
  private static char[] DELIMITERS = new char[] {
      Extensions.DEFAULT_EXTENSION_FIELD_DELIMITER, '-', '|' };

  @Override
  public QueryParser getParser(Analyzer a) throws Exception {
    return getParser(a, null);
  }

  public QueryParser getParser(Analyzer a, Extensions extensions)
      throws Exception {
    if (a == null)
      a = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true);
    QueryParser qp = extensions == null ? new ExtendableQueryParser(
        getDefaultField(), a) : new ExtendableQueryParser(
        getDefaultField(), a, extensions);
    qp.setDefaultOperator(QueryParserBase.OR_OPERATOR);
    qp.setSplitOnWhitespace(splitOnWhitespace);
    return qp;
  }

  public void testUnescapedExtDelimiter() throws Exception {
    Extensions ext = newExtensions(':');
    ext.add("testExt", new ExtensionStub());
    ExtendableQueryParser parser = (ExtendableQueryParser) getParser(null, ext);
    expectThrows(ParseException.class, () -> {
      parser.parse("aField:testExt:\"foo \\& bar\"");
    });
  }

  public void testExtFieldUnqoted() throws Exception {
    for (int i = 0; i < DELIMITERS.length; i++) {
      Extensions ext = newExtensions(DELIMITERS[i]);
      ext.add("testExt", new ExtensionStub());
      ExtendableQueryParser parser = (ExtendableQueryParser) getParser(null,
          ext);
      String field = ext.buildExtensionField("testExt", "aField");
      Query query = parser.parse(String.format(Locale.ROOT, "%s:foo bar", field));
      assertTrue("expected instance of BooleanQuery but was "
          + query.getClass(), query instanceof BooleanQuery);
      BooleanQuery bquery = (BooleanQuery) query;
      BooleanClause[] clauses = bquery.clauses().toArray(new BooleanClause[0]);
      assertEquals(2, clauses.length);
      BooleanClause booleanClause = clauses[0];
      query = booleanClause.getQuery();
      assertTrue("expected instance of TermQuery but was " + query.getClass(),
          query instanceof TermQuery);
      TermQuery tquery = (TermQuery) query;
      assertEquals("aField", tquery.getTerm()
          .field());
      assertEquals("foo", tquery.getTerm().text());

      booleanClause = clauses[1];
      query = booleanClause.getQuery();
      assertTrue("expected instance of TermQuery but was " + query.getClass(),
          query instanceof TermQuery);
      tquery = (TermQuery) query;
      assertEquals(getDefaultField(), tquery.getTerm().field());
      assertEquals("bar", tquery.getTerm().text());
    }
  }

  public void testExtDefaultField() throws Exception {
    for (int i = 0; i < DELIMITERS.length; i++) {
      Extensions ext = newExtensions(DELIMITERS[i]);
      ext.add("testExt", new ExtensionStub());
      ExtendableQueryParser parser = (ExtendableQueryParser) getParser(null,
          ext);
      String field = ext.buildExtensionField("testExt");
      Query parse = parser.parse(String.format(Locale.ROOT, "%s:\"foo \\& bar\"", field));
      assertTrue("expected instance of TermQuery but was " + parse.getClass(),
          parse instanceof TermQuery);
      TermQuery tquery = (TermQuery) parse;
      assertEquals(getDefaultField(), tquery.getTerm().field());
      assertEquals("foo & bar", tquery.getTerm().text());
    }
  }

  public Extensions newExtensions(char delimiter) {
    return new Extensions(delimiter);
  }

  public void testExtField() throws Exception {
    for (int i = 0; i < DELIMITERS.length; i++) {
      Extensions ext = newExtensions(DELIMITERS[i]);
      ext.add("testExt", new ExtensionStub());
      ExtendableQueryParser parser = (ExtendableQueryParser) getParser(null,
          ext);
      String field = ext.buildExtensionField("testExt", "afield");
      Query parse = parser.parse(String.format(Locale.ROOT, "%s:\"foo \\& bar\"", field));
      assertTrue("expected instance of TermQuery but was " + parse.getClass(),
          parse instanceof TermQuery);
      TermQuery tquery = (TermQuery) parse;
      assertEquals("afield", tquery.getTerm().field());
      assertEquals("foo & bar", tquery.getTerm().text());
    }
  }

}
