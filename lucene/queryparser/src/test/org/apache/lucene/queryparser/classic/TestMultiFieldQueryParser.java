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
package org.apache.lucene.queryparser.classic;

import java.io.Reader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockSynonymFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;

/**
 * Tests QueryParser.
 */
public class TestMultiFieldQueryParser extends LuceneTestCase {

  /** test stop words parsing for both the non static form, and for the
   * corresponding static form (qtxt, fields[]). */
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
  private void assertStopQueryEquals (String qtxt, String expectedRes) throws Exception {
    String[] fields = {"b", "t"};
    Occur occur[] = {Occur.SHOULD, Occur.SHOULD};
    TestQueryParser.QPTestAnalyzer a = new TestQueryParser.QPTestAnalyzer();
    MultiFieldQueryParser mfqp = new MultiFieldQueryParser(fields, a);

    Query q = mfqp.parse(qtxt);
    assertEquals(expectedRes, q.toString());

    q = MultiFieldQueryParser.parse(qtxt, fields, occur, a);
    assertEquals(expectedRes, q.toString());
  }

  public void testSimple() throws Exception {
    String[] fields = {"b", "t"};
    MultiFieldQueryParser mfqp = new MultiFieldQueryParser(fields, new MockAnalyzer(random()));

    Query q = mfqp.parse("one");
    assertEquals("b:one t:one", q.toString());

    q = mfqp.parse("one two");
    assertEquals("(b:one t:one) (b:two t:two)", q.toString());

    q = mfqp.parse("+one +two");
    assertEquals("+(b:one t:one) +(b:two t:two)", q.toString());

    q = mfqp.parse("+one -two -three");
    assertEquals("+(b:one t:one) -(b:two t:two) -(b:three t:three)", q.toString());

    q = mfqp.parse("one^2 two");
    assertEquals("(b:one t:one)^2.0 (b:two t:two)", q.toString());

    q = mfqp.parse("one~ two");
    assertEquals("(b:one~2 t:one~2) (b:two t:two)", q.toString());

    q = mfqp.parse("one~0.8 two^2");
    assertEquals("(b:one~0 t:one~0) (b:two t:two)^2.0", q.toString());

    q = mfqp.parse("one* two*");
    assertEquals("(b:one* t:one*) (b:two* t:two*)", q.toString());

    q = mfqp.parse("[a TO c] two");
    assertEquals("(b:[a TO c] t:[a TO c]) (b:two t:two)", q.toString());

    q = mfqp.parse("w?ldcard");
    assertEquals("b:w?ldcard t:w?ldcard", q.toString());

    q = mfqp.parse("\"foo bar\"");
    assertEquals("b:\"foo bar\" t:\"foo bar\"", q.toString());

    q = mfqp.parse("\"aa bb cc\" \"dd ee\"");
    assertEquals("(b:\"aa bb cc\" t:\"aa bb cc\") (b:\"dd ee\" t:\"dd ee\")", q.toString());

    q = mfqp.parse("\"foo bar\"~4");
    assertEquals("b:\"foo bar\"~4 t:\"foo bar\"~4", q.toString());

    // LUCENE-1213: MultiFieldQueryParser was ignoring slop when phrase had a field.
    q = mfqp.parse("b:\"foo bar\"~4");
    assertEquals("b:\"foo bar\"~4", q.toString());

    // make sure that terms which have a field are not touched:
    q = mfqp.parse("one f:two");
    assertEquals("(b:one t:one) f:two", q.toString());

    // AND mode:
    mfqp.setDefaultOperator(QueryParserBase.AND_OPERATOR);
    q = mfqp.parse("one two");
    assertEquals("+(b:one t:one) +(b:two t:two)", q.toString());
    q = mfqp.parse("\"aa bb cc\" \"dd ee\"");
    assertEquals("+(b:\"aa bb cc\" t:\"aa bb cc\") +(b:\"dd ee\" t:\"dd ee\")", q.toString());

  }

  public void testBoostsSimple() throws Exception {
      Map<String,Float> boosts = new HashMap<>();
      boosts.put("b", Float.valueOf(5));
      boosts.put("t", Float.valueOf(10));
      String[] fields = {"b", "t"};
      MultiFieldQueryParser mfqp = new MultiFieldQueryParser(fields, new MockAnalyzer(random()), boosts);


      //Check for simple
      Query q = mfqp.parse("one");
      assertEquals("(b:one)^5.0 (t:one)^10.0", q.toString());

      //Check for AND
      q = mfqp.parse("one AND two");
      assertEquals("+((b:one)^5.0 (t:one)^10.0) +((b:two)^5.0 (t:two)^10.0)", q.toString());

      //Check for OR
      q = mfqp.parse("one OR two");
      assertEquals("((b:one)^5.0 (t:one)^10.0) ((b:two)^5.0 (t:two)^10.0)", q.toString());

      //Check for AND and a field
      q = mfqp.parse("one AND two AND foo:test");
      assertEquals("+((b:one)^5.0 (t:one)^10.0) +((b:two)^5.0 (t:two)^10.0) +foo:test", q.toString());

      q = mfqp.parse("one^3 AND two^4");
      assertEquals("+((b:one)^5.0 (t:one)^10.0)^3.0 +((b:two)^5.0 (t:two)^10.0)^4.0", q.toString());
  }

  public void testStaticMethod1() throws ParseException {
    String[] fields = {"b", "t"};
    String[] queries = {"one", "two"};
    Query q = MultiFieldQueryParser.parse(queries, fields, new MockAnalyzer(random()));
    assertEquals("b:one t:two", q.toString());

    String[] queries2 = {"+one", "+two"};
    q = MultiFieldQueryParser.parse(queries2, fields, new MockAnalyzer(random()));
    assertEquals("(+b:one) (+t:two)", q.toString());

    String[] queries3 = {"one", "+two"};
    q = MultiFieldQueryParser.parse(queries3, fields, new MockAnalyzer(random()));
    assertEquals("b:one (+t:two)", q.toString());

    String[] queries4 = {"one +more", "+two"};
    q = MultiFieldQueryParser.parse(queries4, fields, new MockAnalyzer(random()));
    assertEquals("(b:one +b:more) (+t:two)", q.toString());

    String[] queries5 = {"blah"};
    // expected exception, array length differs
    expectThrows(IllegalArgumentException.class, () -> {
      MultiFieldQueryParser.parse(queries5, fields, new MockAnalyzer(random()));
    });

    // check also with stop words for this static form (qtxts[], fields[]).
    TestQueryParser.QPTestAnalyzer stopA = new TestQueryParser.QPTestAnalyzer();

    String[] queries6 = {"((+stop))", "+((stop))"};
    q = MultiFieldQueryParser.parse(queries6, fields, stopA);
    assertEquals("", q.toString());

    String[] queries7 = {"one ((+stop)) +more", "+((stop)) +two"};
    q = MultiFieldQueryParser.parse(queries7, fields, stopA);
    assertEquals("(b:one +b:more) (+t:two)", q.toString());

  }

  public void testStaticMethod2() throws ParseException {
    String[] fields = {"b", "t"};
    BooleanClause.Occur[] flags = {BooleanClause.Occur.MUST, BooleanClause.Occur.MUST_NOT};
    Query q = MultiFieldQueryParser.parse("one", fields, flags, new MockAnalyzer(random()));
    assertEquals("+b:one -t:one", q.toString());

    q = MultiFieldQueryParser.parse("one two", fields, flags, new MockAnalyzer(random()));
    assertEquals("+(b:one b:two) -(t:one t:two)", q.toString());

    // expected exception, array length differs
    expectThrows(IllegalArgumentException.class, () -> {
      BooleanClause.Occur[] flags2 = {BooleanClause.Occur.MUST};
      MultiFieldQueryParser.parse("blah", fields, flags2, new MockAnalyzer(random()));
    });
  }

  public void testStaticMethod2Old() throws ParseException {
    String[] fields = {"b", "t"};
    //int[] flags = {MultiFieldQueryParser.REQUIRED_FIELD, MultiFieldQueryParser.PROHIBITED_FIELD};
      BooleanClause.Occur[] flags = {BooleanClause.Occur.MUST, BooleanClause.Occur.MUST_NOT};

    Query q = MultiFieldQueryParser.parse("one", fields, flags, new MockAnalyzer(random()));//, fields, flags, new MockAnalyzer(random));
    assertEquals("+b:one -t:one", q.toString());

    q = MultiFieldQueryParser.parse("one two", fields, flags, new MockAnalyzer(random()));
    assertEquals("+(b:one b:two) -(t:one t:two)", q.toString());

    // expected exception, array length differs
    expectThrows(IllegalArgumentException.class, () -> {
      BooleanClause.Occur[] flags2 = {BooleanClause.Occur.MUST};
      MultiFieldQueryParser.parse("blah", fields, flags2, new MockAnalyzer(random()));
    });
  }

  public void testStaticMethod3() throws ParseException {
    String[] queries = {"one", "two", "three"};
    String[] fields = {"f1", "f2", "f3"};
    BooleanClause.Occur[] flags = {BooleanClause.Occur.MUST,
        BooleanClause.Occur.MUST_NOT, BooleanClause.Occur.SHOULD};
    Query q = MultiFieldQueryParser.parse(queries, fields, flags, new MockAnalyzer(random()));
    assertEquals("+f1:one -f2:two f3:three", q.toString());

    // expected exception, array length differs
    expectThrows(IllegalArgumentException.class, () -> {
      BooleanClause.Occur[] flags2 = {BooleanClause.Occur.MUST};
      MultiFieldQueryParser.parse(queries, fields, flags2, new MockAnalyzer(random()));
    });
  }

  public void testStaticMethod3Old() throws ParseException {
    String[] queries = {"one", "two"};
    String[] fields = {"b", "t"};
      BooleanClause.Occur[] flags = {BooleanClause.Occur.MUST, BooleanClause.Occur.MUST_NOT};
    Query q = MultiFieldQueryParser.parse(queries, fields, flags, new MockAnalyzer(random()));
    assertEquals("+b:one -t:two", q.toString());

    // expected exception, array length differs
    expectThrows(IllegalArgumentException.class, () -> {
      BooleanClause.Occur[] flags2 = {BooleanClause.Occur.MUST};
      MultiFieldQueryParser.parse(queries, fields, flags2, new MockAnalyzer(random()));
    });
  }

  public void testAnalyzerReturningNull() throws ParseException {
    String[] fields = new String[] { "f1", "f2", "f3" };
    MultiFieldQueryParser parser = new MultiFieldQueryParser(fields, new AnalyzerReturningNull());
    Query q = parser.parse("bla AND blo");
    assertEquals("+(f2:bla f3:bla) +(f2:blo f3:blo)", q.toString());
    // the following queries are not affected as their terms are not analyzed anyway:
    q = parser.parse("bla*");
    assertEquals("f1:bla* f2:bla* f3:bla*", q.toString());
    q = parser.parse("bla~");
    assertEquals("f1:bla~2 f2:bla~2 f3:bla~2", q.toString());
    q = parser.parse("[a TO c]");
    assertEquals("f1:[a TO c] f2:[a TO c] f3:[a TO c]", q.toString());
  }

  public void testStopWordSearching() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    Directory ramDir = newDirectory();
    IndexWriter iw =  new IndexWriter(ramDir, newIndexWriterConfig(analyzer));
    Document doc = new Document();
    doc.add(newTextField("body", "blah the footest blah", Field.Store.NO));
    iw.addDocument(doc);
    iw.close();

    MultiFieldQueryParser mfqp =
      new MultiFieldQueryParser(new String[] {"body"}, analyzer);
    mfqp.setDefaultOperator(QueryParser.Operator.AND);
    Query q = mfqp.parse("the footest");
    IndexReader ir = DirectoryReader.open(ramDir);
    IndexSearcher is = newSearcher(ir);
    ScoreDoc[] hits = is.search(q, 1000).scoreDocs;
    assertEquals(1, hits.length);
    ir.close();
    ramDir.close();
  }

  /**
   * Return no tokens for field "f1".
   */
  private static class AnalyzerReturningNull extends Analyzer {
    MockAnalyzer stdAnalyzer = new MockAnalyzer(random());

    public AnalyzerReturningNull() {
      super(PER_FIELD_REUSE_STRATEGY);
    }

    @Override
    protected Reader initReader(String fieldName, Reader reader) {
      if ("f1".equals(fieldName)) {
        // we don't use the reader, so close it:
        IOUtils.closeWhileHandlingException(reader);
        // return empty reader, so MockTokenizer returns no tokens:
        return new StringReader("");
      } else {
        return super.initReader(fieldName, reader);
      }
    }

    @Override
    public TokenStreamComponents createComponents(String fieldName) {
      return stdAnalyzer.createComponents(fieldName);
    }
  }

  public void testSimpleRegex() throws ParseException {
    String[] fields = new String[] {"a", "b"};
    MultiFieldQueryParser mfqp = new MultiFieldQueryParser(fields, new MockAnalyzer(random()));

    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.setDisableCoord(true);
    bq.add(new RegexpQuery(new Term("a", "[a-z][123]")), Occur.SHOULD);
    bq.add(new RegexpQuery(new Term("b", "[a-z][123]")), Occur.SHOULD);
    assertEquals(bq.build(), mfqp.parse("/[a-z][123]/"));
  }

  /** whitespace+lowercase analyzer with synonyms (dogs,dog) and (guinea pig,cavy) */
  private static class MockSynonymAnalyzer extends Analyzer {
    @Override
    public TokenStreamComponents createComponents(String fieldName) {
      Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, true);
      return new TokenStreamComponents(tokenizer, new MockSynonymFilter(tokenizer));
    }
  }

  public void testSynonyms() throws ParseException {
    String[] fields = {"b", "t"};
    MultiFieldQueryParser parser = new MultiFieldQueryParser(fields, new MockSynonymAnalyzer());
    Query q = parser.parse("dogs");
    assertEquals("Synonym(b:dog b:dogs) Synonym(t:dog t:dogs)", q.toString());
    q = parser.parse("guinea pig");
    assertTrue(parser.getSplitOnWhitespace());
    assertEquals("(b:guinea t:guinea) (b:pig t:pig)", q.toString());
    parser.setSplitOnWhitespace(false);
    q = parser.parse("guinea pig");
    assertFalse(parser.getSplitOnWhitespace());
    assertEquals("((+b:guinea +b:pig) (+t:guinea +t:pig)) (b:cavy t:cavy)", q.toString());
    parser.setSplitOnWhitespace(true);
    q = parser.parse("guinea pig");
    assertEquals("(b:guinea t:guinea) (b:pig t:pig)", q.toString());
  }
}
