package org.apache.lucene.queryParser.standard;

/**
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

import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.standard.MultiFieldQueryParserWrapper;
import org.apache.lucene.queryParser.standard.QueryParserWrapper;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

/**
 * Tests multi field query parsing using the
 * {@link MultiFieldQueryParserWrapper}.
 * 
 * @deprecated this tests test the deprecated MultiFieldQueryParserWrapper, so
 *             when the latter is gone, so should this test.
 */
@Deprecated
public class TestMultiFieldQueryParserWrapper extends LuceneTestCase {

  /**
   * test stop words parsing for both the non static form, and for the
   * corresponding static form (qtxt, fields[]).
   */
  public void testStopwordsParsing() throws Exception {
    assertStopQueryEquals("one", "b:one t:one");
    assertStopQueryEquals("one stop", "b:one t:one");
    assertStopQueryEquals("one (stop)", "b:one t:one");
    assertStopQueryEquals("one ((stop))", "b:one t:one");
    assertStopQueryEquals("stop", "");
    assertStopQueryEquals("(stop)", "");
    assertStopQueryEquals("((stop))", "");
  }

  // verify parsing of query using a stopping analyzer
  private void assertStopQueryEquals(String qtxt, String expectedRes)
      throws Exception {
    String[] fields = { "b", "t" };
    Occur occur[] = { Occur.SHOULD, Occur.SHOULD };
    TestQueryParserWrapper.QPTestAnalyzer a = new TestQueryParserWrapper.QPTestAnalyzer();
    MultiFieldQueryParserWrapper mfqp = new MultiFieldQueryParserWrapper(
        fields, a);

    Query q = mfqp.parse(qtxt);
    assertEquals(expectedRes, q.toString());

    q = MultiFieldQueryParserWrapper.parse(qtxt, fields, occur, a);
    assertEquals(expectedRes, q.toString());
  }

  public void testSimple() throws Exception {
    String[] fields = { "b", "t" };
    MultiFieldQueryParserWrapper mfqp = new MultiFieldQueryParserWrapper(
        fields, new StandardAnalyzer(TEST_VERSION_CURRENT));

    Query q = mfqp.parse("one");
    assertEquals("b:one t:one", q.toString());

    q = mfqp.parse("one two");
    assertEquals("(b:one t:one) (b:two t:two)", q.toString());

    q = mfqp.parse("+one +two");
    assertEquals("+(b:one t:one) +(b:two t:two)", q.toString());

    q = mfqp.parse("+one -two -three");
    assertEquals("+(b:one t:one) -(b:two t:two) -(b:three t:three)", q
        .toString());

    q = mfqp.parse("one^2 two");
    assertEquals("((b:one t:one)^2.0) (b:two t:two)", q.toString());

    q = mfqp.parse("one~ two");
    assertEquals("(b:one~0.5 t:one~0.5) (b:two t:two)", q.toString());

    q = mfqp.parse("one~0.8 two^2");
    assertEquals("(b:one~0.8 t:one~0.8) ((b:two t:two)^2.0)", q.toString());

    q = mfqp.parse("one* two*");
    assertEquals("(b:one* t:one*) (b:two* t:two*)", q.toString());

    q = mfqp.parse("[a TO c] two");
    assertEquals("(b:[a TO c] t:[a TO c]) (b:two t:two)", q.toString());

    q = mfqp.parse("w?ldcard");
    assertEquals("b:w?ldcard t:w?ldcard", q.toString());

    q = mfqp.parse("\"foo bar\"");
    assertEquals("b:\"foo bar\" t:\"foo bar\"", q.toString());

    q = mfqp.parse("\"aa bb cc\" \"dd ee\"");
    assertEquals("(b:\"aa bb cc\" t:\"aa bb cc\") (b:\"dd ee\" t:\"dd ee\")", q
        .toString());

    q = mfqp.parse("\"foo bar\"~4");
    assertEquals("b:\"foo bar\"~4 t:\"foo bar\"~4", q.toString());

    // LUCENE-1213: MultiFieldQueryParserWrapper was ignoring slop when phrase
    // had a field.
    q = mfqp.parse("b:\"foo bar\"~4");
    assertEquals("b:\"foo bar\"~4", q.toString());

    // make sure that terms which have a field are not touched:
    q = mfqp.parse("one f:two");
    assertEquals("(b:one t:one) f:two", q.toString());

    // AND mode:
    mfqp.setDefaultOperator(QueryParserWrapper.AND_OPERATOR);
    q = mfqp.parse("one two");
    assertEquals("+(b:one t:one) +(b:two t:two)", q.toString());
    q = mfqp.parse("\"aa bb cc\" \"dd ee\"");
    assertEquals("+(b:\"aa bb cc\" t:\"aa bb cc\") +(b:\"dd ee\" t:\"dd ee\")",
        q.toString());

  }

  public void testBoostsSimple() throws Exception {
    Map<String,Float> boosts = new HashMap<String,Float>();
    boosts.put("b", Float.valueOf(5));
    boosts.put("t", Float.valueOf(10));
    String[] fields = { "b", "t" };
    MultiFieldQueryParserWrapper mfqp = new MultiFieldQueryParserWrapper(
        fields, new StandardAnalyzer(TEST_VERSION_CURRENT), boosts);

    // Check for simple
    Query q = mfqp.parse("one");
    assertEquals("b:one^5.0 t:one^10.0", q.toString());

    // Check for AND
    q = mfqp.parse("one AND two");
    assertEquals("+(b:one^5.0 t:one^10.0) +(b:two^5.0 t:two^10.0)", q
        .toString());

    // Check for OR
    q = mfqp.parse("one OR two");
    assertEquals("(b:one^5.0 t:one^10.0) (b:two^5.0 t:two^10.0)", q.toString());

    // Check for AND and a field
    q = mfqp.parse("one AND two AND foo:test");
    assertEquals("+(b:one^5.0 t:one^10.0) +(b:two^5.0 t:two^10.0) +foo:test", q
        .toString());

    q = mfqp.parse("one^3 AND two^4");
    assertEquals("+((b:one^5.0 t:one^10.0)^3.0) +((b:two^5.0 t:two^10.0)^4.0)",
        q.toString());
  }

  public void testStaticMethod1() throws ParseException {
    String[] fields = { "b", "t" };
    String[] queries = { "one", "two" };
    Query q = MultiFieldQueryParserWrapper.parse(queries, fields,
        new StandardAnalyzer(TEST_VERSION_CURRENT));
    assertEquals("b:one t:two", q.toString());

    String[] queries2 = { "+one", "+two" };
    q = MultiFieldQueryParserWrapper.parse(queries2, fields,
        new StandardAnalyzer(TEST_VERSION_CURRENT));
    assertEquals("(+b:one) (+t:two)", q.toString());

    String[] queries3 = { "one", "+two" };
    q = MultiFieldQueryParserWrapper.parse(queries3, fields,
        new StandardAnalyzer(TEST_VERSION_CURRENT));
    assertEquals("b:one (+t:two)", q.toString());

    String[] queries4 = { "one +more", "+two" };
    q = MultiFieldQueryParserWrapper.parse(queries4, fields,
        new StandardAnalyzer(TEST_VERSION_CURRENT));
    assertEquals("(b:one +b:more) (+t:two)", q.toString());

    String[] queries5 = { "blah" };
    try {
      q = MultiFieldQueryParserWrapper.parse(queries5, fields,
          new StandardAnalyzer(TEST_VERSION_CURRENT));
      fail();
    } catch (IllegalArgumentException e) {
      // expected exception, array length differs
    }

    // check also with stop words for this static form (qtxts[], fields[]).
    TestQueryParserWrapper.QPTestAnalyzer stopA = new TestQueryParserWrapper.QPTestAnalyzer();

    String[] queries6 = { "((+stop))", "+((stop))" };
    q = MultiFieldQueryParserWrapper.parse(queries6, fields, stopA);
    assertEquals("", q.toString());

    String[] queries7 = { "one ((+stop)) +more", "+((stop)) +two" };
    q = MultiFieldQueryParserWrapper.parse(queries7, fields, stopA);
    assertEquals("(b:one +b:more) (+t:two)", q.toString());

  }

  public void testStaticMethod2() throws ParseException {
    String[] fields = { "b", "t" };
    BooleanClause.Occur[] flags = { BooleanClause.Occur.MUST,
        BooleanClause.Occur.MUST_NOT };
    Query q = MultiFieldQueryParserWrapper.parse("one", fields, flags,
        new StandardAnalyzer(TEST_VERSION_CURRENT));
    assertEquals("+b:one -t:one", q.toString());

    q = MultiFieldQueryParserWrapper.parse("one two", fields, flags,
        new StandardAnalyzer(TEST_VERSION_CURRENT));
    assertEquals("+(b:one b:two) -(t:one t:two)", q.toString());

    try {
      BooleanClause.Occur[] flags2 = { BooleanClause.Occur.MUST };
      q = MultiFieldQueryParserWrapper.parse("blah", fields, flags2,
          new StandardAnalyzer(TEST_VERSION_CURRENT));
      fail();
    } catch (IllegalArgumentException e) {
      // expected exception, array length differs
    }
  }

  public void testStaticMethod2Old() throws ParseException {
    String[] fields = { "b", "t" };
    // int[] flags = {MultiFieldQueryParserWrapper.REQUIRED_FIELD,
    // MultiFieldQueryParserWrapper.PROHIBITED_FIELD};
    BooleanClause.Occur[] flags = { BooleanClause.Occur.MUST,
        BooleanClause.Occur.MUST_NOT };

    Query q = MultiFieldQueryParserWrapper.parse("one", fields, flags,
        new StandardAnalyzer(TEST_VERSION_CURRENT));// , fields, flags, new StandardAnalyzer());
    assertEquals("+b:one -t:one", q.toString());

    q = MultiFieldQueryParserWrapper.parse("one two", fields, flags,
        new StandardAnalyzer(TEST_VERSION_CURRENT));
    assertEquals("+(b:one b:two) -(t:one t:two)", q.toString());

    try {
      BooleanClause.Occur[] flags2 = { BooleanClause.Occur.MUST };
      q = MultiFieldQueryParserWrapper.parse("blah", fields, flags2,
          new StandardAnalyzer(TEST_VERSION_CURRENT));
      fail();
    } catch (IllegalArgumentException e) {
      // expected exception, array length differs
    }
  }

  public void testStaticMethod3() throws ParseException {
    String[] queries = { "one", "two", "three" };
    String[] fields = { "f1", "f2", "f3" };
    BooleanClause.Occur[] flags = { BooleanClause.Occur.MUST,
        BooleanClause.Occur.MUST_NOT, BooleanClause.Occur.SHOULD };
    Query q = MultiFieldQueryParserWrapper.parse(queries, fields, flags,
        new StandardAnalyzer(TEST_VERSION_CURRENT));
    assertEquals("+f1:one -f2:two f3:three", q.toString());

    try {
      BooleanClause.Occur[] flags2 = { BooleanClause.Occur.MUST };
      q = MultiFieldQueryParserWrapper.parse(queries, fields, flags2,
          new StandardAnalyzer(TEST_VERSION_CURRENT));
      fail();
    } catch (IllegalArgumentException e) {
      // expected exception, array length differs
    }
  }

  public void testStaticMethod3Old() throws ParseException {
    String[] queries = { "one", "two" };
    String[] fields = { "b", "t" };
    BooleanClause.Occur[] flags = { BooleanClause.Occur.MUST,
        BooleanClause.Occur.MUST_NOT };
    Query q = MultiFieldQueryParserWrapper.parse(queries, fields, flags,
        new StandardAnalyzer(TEST_VERSION_CURRENT));
    assertEquals("+b:one -t:two", q.toString());

    try {
      BooleanClause.Occur[] flags2 = { BooleanClause.Occur.MUST };
      q = MultiFieldQueryParserWrapper.parse(queries, fields, flags2,
          new StandardAnalyzer(TEST_VERSION_CURRENT));
      fail();
    } catch (IllegalArgumentException e) {
      // expected exception, array length differs
    }
  }

  public void testAnalyzerReturningNull() throws ParseException {
    String[] fields = new String[] { "f1", "f2", "f3" };
    MultiFieldQueryParserWrapper parser = new MultiFieldQueryParserWrapper(
        fields, new AnalyzerReturningNull());
    Query q = parser.parse("bla AND blo");
    assertEquals("+(f2:bla f3:bla) +(f2:blo f3:blo)", q.toString());
    // the following queries are not affected as their terms are not analyzed
    // anyway:
    q = parser.parse("bla*");
    assertEquals("f1:bla* f2:bla* f3:bla*", q.toString());
    q = parser.parse("bla~");
    assertEquals("f1:bla~0.5 f2:bla~0.5 f3:bla~0.5", q.toString());
    q = parser.parse("[a TO c]");
    assertEquals("f1:[a TO c] f2:[a TO c] f3:[a TO c]", q.toString());
  }

  public void testStopWordSearching() throws Exception {
    Analyzer analyzer = new StandardAnalyzer(TEST_VERSION_CURRENT);
    Directory ramDir = newDirectory();
    IndexWriter iw = new IndexWriter(ramDir, analyzer, true,
        IndexWriter.MaxFieldLength.LIMITED);
    Document doc = new Document();
    doc.add(newField("body", "blah the footest blah", Field.Store.NO,
        Field.Index.ANALYZED));
    iw.addDocument(doc);
    iw.close();

    MultiFieldQueryParserWrapper mfqp = new MultiFieldQueryParserWrapper(
        new String[] { "body" }, analyzer);
    mfqp.setDefaultOperator(QueryParserWrapper.Operator.AND);
    Query q = mfqp.parse("the footest");
    IndexSearcher is = new IndexSearcher(ramDir, true);
    ScoreDoc[] hits = is.search(q, null, 1000).scoreDocs;
    assertEquals(1, hits.length);
    is.close();
    ramDir.close();
  }

  /**
   * Return empty tokens for field "f1".
   */
  private static class AnalyzerReturningNull extends Analyzer {
    StandardAnalyzer stdAnalyzer = new StandardAnalyzer(TEST_VERSION_CURRENT);

    public AnalyzerReturningNull() {
    }

    @Override
    public TokenStream tokenStream(String fieldName, Reader reader) {
      if ("f1".equals(fieldName)) {
        return new EmptyTokenStream();
      } else {
        return stdAnalyzer.tokenStream(fieldName, reader);
      }
    }

    private static class EmptyTokenStream extends TokenStream {
      @Override
      public boolean incrementToken() {
        return false;
      }
    }
  }

}
