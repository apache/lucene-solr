package org.apache.lucene.queryParser;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import junit.framework.TestCase;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.search.Query;

/**
 * Tests QueryParser.
 * @author Daniel Naber
 */
public class TestMultiFieldQueryParser extends TestCase {

  public void testSimple() throws Exception {
    String[] fields = {"b", "t"};
    MultiFieldQueryParser mfqp = new MultiFieldQueryParser(fields, new StandardAnalyzer());
    
    Query q = mfqp.parse("one");
    assertEquals("b:one t:one", q.toString());
    
    q = mfqp.parse("one two");
    assertEquals("(b:one t:one) (b:two t:two)", q.toString());
    
    q = mfqp.parse("+one +two");
    assertEquals("+(b:one t:one) +(b:two t:two)", q.toString());

    q = mfqp.parse("+one -two -three)");
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

    q = mfqp.parse("\"foo bar\"");
    assertEquals("b:\"foo bar\" t:\"foo bar\"", q.toString());

    q = mfqp.parse("\"aa bb cc\" \"dd ee\"");
    assertEquals("(b:\"aa bb cc\" t:\"aa bb cc\") (b:\"dd ee\" t:\"dd ee\")", q.toString());

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
  
  // TODO: remove this for Lucene 2.0
  public void testOldMethods() throws ParseException {
    // testing the old static calls that are now deprecated:
    assertQueryEquals("b:one t:one", "one");
    assertQueryEquals("(b:one b:two) (t:one t:two)", "one two");
    assertQueryEquals("(b:one -b:two) (t:one -t:two)", "one -two");
    assertQueryEquals("(b:one -(b:two b:three)) (t:one -(t:two t:three))", "one -(two three)");
    assertQueryEquals("(+b:one +b:two) (+t:one +t:two)", "+one +two");
  }
  
  // TODO: remove this for Lucene 2.0
  private void assertQueryEquals(String expected, String query) throws ParseException {
    String[] fields = {"b", "t"};
    Query q = MultiFieldQueryParser.parse(query, fields, new StandardAnalyzer());
    String s = q.toString();
    assertEquals(expected, s);
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
  }
  
  public void testStaticMethod2() throws ParseException {
    String[] fields = {"b", "t"};
    int[] flags = {MultiFieldQueryParser.REQUIRED_FIELD, MultiFieldQueryParser.PROHIBITED_FIELD};
    Query q = MultiFieldQueryParser.parse("one", fields, flags, new StandardAnalyzer());
    assertEquals("+b:one -t:one", q.toString());

    q = MultiFieldQueryParser.parse("one two", fields, flags, new StandardAnalyzer());
    assertEquals("+(b:one b:two) -(t:one t:two)", q.toString());

    try {
      int[] flags2 = {MultiFieldQueryParser.REQUIRED_FIELD};
      q = MultiFieldQueryParser.parse("blah", fields, flags2, new StandardAnalyzer());
      fail();
    } catch(IllegalArgumentException e) {
      // expected exception, array length differs
    }
  }

  public void testStaticMethod3() throws ParseException {
    String[] queries = {"one", "two"};
    String[] fields = {"b", "t"};
    int[] flags = {MultiFieldQueryParser.REQUIRED_FIELD, MultiFieldQueryParser.PROHIBITED_FIELD};
    Query q = MultiFieldQueryParser.parse(queries, fields, flags, new StandardAnalyzer());
    assertEquals("+b:one -t:two", q.toString());

    try {
      int[] flags2 = {MultiFieldQueryParser.REQUIRED_FIELD};
      q = MultiFieldQueryParser.parse(queries, fields, flags2, new StandardAnalyzer());
      fail();
    } catch(IllegalArgumentException e) {
      // expected exception, array length differs
    }
  }

}
