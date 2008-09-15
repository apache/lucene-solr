package org.apache.lucene.queryParser;

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
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.LuceneTestCase;

/**
 * Tests QueryParser.
 */
public class TestMultiFieldQueryParser extends LuceneTestCase {

  /** test stop words arsing for both the non static form, and for the 
   * corresponding static form (qtxt, fields[]). */
  public void tesStopwordsParsing() throws Exception {
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
    MultiFieldQueryParser mfqp = new MultiFieldQueryParser(fields, new StandardAnalyzer());
    
    Query q = mfqp.parse("one");
    assertEquals("b:one t:one", q.toString());
    
    q = mfqp.parse("one two");
    assertEquals("(b:one t:one) (b:two t:two)", q.toString());
    
    q = mfqp.parse("+one +two");
    assertEquals("+(b:one t:one) +(b:two t:two)", q.toString());

    q = mfqp.parse("+one -two -three");
    assertEquals("+(b:one t:one) -(b:two t:two) -(b:three t:three)", q.toString());
    
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
    mfqp.setDefaultOperator(QueryParser.AND_OPERATOR);
    q = mfqp.parse("one two");
    assertEquals("+(b:one t:one) +(b:two t:two)", q.toString());
    q = mfqp.parse("\"aa bb cc\" \"dd ee\"");
    assertEquals("+(b:\"aa bb cc\" t:\"aa bb cc\") +(b:\"dd ee\" t:\"dd ee\")", q.toString());

  }
  
  public void testBoostsSimple() throws Exception {
      Map boosts = new HashMap();
      boosts.put("b", new Float(5));
      boosts.put("t", new Float(10));
      String[] fields = {"b", "t"};
      MultiFieldQueryParser mfqp = new MultiFieldQueryParser(fields, new StandardAnalyzer(), boosts);
      
      
      //Check for simple
      Query q = mfqp.parse("one");
      assertEquals("b:one^5.0 t:one^10.0", q.toString());
      
      //Check for AND
      q = mfqp.parse("one AND two");
      assertEquals("+(b:one^5.0 t:one^10.0) +(b:two^5.0 t:two^10.0)", q.toString());
      
      //Check for OR
      q = mfqp.parse("one OR two");
      assertEquals("(b:one^5.0 t:one^10.0) (b:two^5.0 t:two^10.0)", q.toString());
      
      //Check for AND and a field
      q = mfqp.parse("one AND two AND foo:test");
      assertEquals("+(b:one^5.0 t:one^10.0) +(b:two^5.0 t:two^10.0) +foo:test", q.toString());
      
      q = mfqp.parse("one^3 AND two^4");
      assertEquals("+((b:one^5.0 t:one^10.0)^3.0) +((b:two^5.0 t:two^10.0)^4.0)", q.toString());
  }

  public void testStaticMethod1() throws ParseException {
    String[] fields = {"b", "t"};
    String[] queries = {"one", "two"};
    Query q = MultiFieldQueryParser.parse(queries, fields, new StandardAnalyzer());
    assertEquals("b:one t:two", q.toString());

    String[] queries2 = {"+one", "+two"};
    q = MultiFieldQueryParser.parse(queries2, fields, new StandardAnalyzer());
    assertEquals("(+b:one) (+t:two)", q.toString());

    String[] queries3 = {"one", "+two"};
    q = MultiFieldQueryParser.parse(queries3, fields, new StandardAnalyzer());
    assertEquals("b:one (+t:two)", q.toString());

    String[] queries4 = {"one +more", "+two"};
    q = MultiFieldQueryParser.parse(queries4, fields, new StandardAnalyzer());
    assertEquals("(b:one +b:more) (+t:two)", q.toString());

    String[] queries5 = {"blah"};
    try {
      q = MultiFieldQueryParser.parse(queries5, fields, new StandardAnalyzer());
      fail();
    } catch(IllegalArgumentException e) {
      // expected exception, array length differs
    }
    
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
    Query q = MultiFieldQueryParser.parse("one", fields, flags, new StandardAnalyzer());
    assertEquals("+b:one -t:one", q.toString());

    q = MultiFieldQueryParser.parse("one two", fields, flags, new StandardAnalyzer());
    assertEquals("+(b:one b:two) -(t:one t:two)", q.toString());

    try {
      BooleanClause.Occur[] flags2 = {BooleanClause.Occur.MUST};
      q = MultiFieldQueryParser.parse("blah", fields, flags2, new StandardAnalyzer());
      fail();
    } catch(IllegalArgumentException e) {
      // expected exception, array length differs
    }
  }

  public void testStaticMethod2Old() throws ParseException {
    String[] fields = {"b", "t"};
    //int[] flags = {MultiFieldQueryParser.REQUIRED_FIELD, MultiFieldQueryParser.PROHIBITED_FIELD};
      BooleanClause.Occur[] flags = {BooleanClause.Occur.MUST, BooleanClause.Occur.MUST_NOT};
      MultiFieldQueryParser parser = new MultiFieldQueryParser(fields, new StandardAnalyzer());

    Query q = MultiFieldQueryParser.parse("one", fields, flags, new StandardAnalyzer());//, fields, flags, new StandardAnalyzer());
    assertEquals("+b:one -t:one", q.toString());

    q = MultiFieldQueryParser.parse("one two", fields, flags, new StandardAnalyzer());
    assertEquals("+(b:one b:two) -(t:one t:two)", q.toString());

    try {
      BooleanClause.Occur[] flags2 = {BooleanClause.Occur.MUST};
      q = MultiFieldQueryParser.parse("blah", fields, flags2, new StandardAnalyzer());
      fail();
    } catch(IllegalArgumentException e) {
      // expected exception, array length differs
    }
  }

  public void testStaticMethod3() throws ParseException {
    String[] queries = {"one", "two", "three"};
    String[] fields = {"f1", "f2", "f3"};
    BooleanClause.Occur[] flags = {BooleanClause.Occur.MUST,
        BooleanClause.Occur.MUST_NOT, BooleanClause.Occur.SHOULD};
    Query q = MultiFieldQueryParser.parse(queries, fields, flags, new StandardAnalyzer());
    assertEquals("+f1:one -f2:two f3:three", q.toString());

    try {
      BooleanClause.Occur[] flags2 = {BooleanClause.Occur.MUST};
      q = MultiFieldQueryParser.parse(queries, fields, flags2, new StandardAnalyzer());
      fail();
    } catch(IllegalArgumentException e) {
      // expected exception, array length differs
    }
  }

  public void testStaticMethod3Old() throws ParseException {
    String[] queries = {"one", "two"};
    String[] fields = {"b", "t"};
      BooleanClause.Occur[] flags = {BooleanClause.Occur.MUST, BooleanClause.Occur.MUST_NOT};
    Query q = MultiFieldQueryParser.parse(queries, fields, flags, new StandardAnalyzer());
    assertEquals("+b:one -t:two", q.toString());

    try {
      BooleanClause.Occur[] flags2 = {BooleanClause.Occur.MUST};
      q = MultiFieldQueryParser.parse(queries, fields, flags2, new StandardAnalyzer());
      fail();
    } catch(IllegalArgumentException e) {
      // expected exception, array length differs
    }
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
    assertEquals("f1:bla~0.5 f2:bla~0.5 f3:bla~0.5", q.toString());
    q = parser.parse("[a TO c]");
    assertEquals("f1:[a TO c] f2:[a TO c] f3:[a TO c]", q.toString());
  }

  public void testStopWordSearching() throws Exception {
    Analyzer analyzer = new StandardAnalyzer();
    Directory ramDir = new RAMDirectory();
    IndexWriter iw =  new IndexWriter(ramDir, analyzer, true, IndexWriter.MaxFieldLength.LIMITED);
    Document doc = new Document();
    doc.add(new Field("body", "blah the footest blah", Field.Store.NO, Field.Index.ANALYZED));
    iw.addDocument(doc);
    iw.close();
    
    MultiFieldQueryParser mfqp = 
      new MultiFieldQueryParser(new String[] {"body"}, analyzer);
    mfqp.setDefaultOperator(QueryParser.Operator.AND);
    Query q = mfqp.parse("the footest");
    IndexSearcher is = new IndexSearcher(ramDir);
    ScoreDoc[] hits = is.search(q, null, 1000).scoreDocs;
    assertEquals(1, hits.length);
    is.close();
  }
  
  /**
   * Return empty tokens for field "f1".
   */
  private static class AnalyzerReturningNull extends Analyzer {
    StandardAnalyzer stdAnalyzer = new StandardAnalyzer();

    public AnalyzerReturningNull() {
    }

    public TokenStream tokenStream(String fieldName, Reader reader) {
      if ("f1".equals(fieldName)) {
        return new EmptyTokenStream();
      } else {
        return stdAnalyzer.tokenStream(fieldName, reader);
      }
    }

    private static class EmptyTokenStream extends TokenStream {
      public Token next(final Token reusableToken) {
        return null;
      }
    }
  }

}
