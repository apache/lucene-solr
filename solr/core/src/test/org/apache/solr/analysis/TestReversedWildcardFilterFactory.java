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
package org.apache.solr.analysis;
import static org.apache.lucene.analysis.BaseTokenStreamTestCase.assertTokenStreamContents;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.search.AutomatonQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.parser.CharStream;
import org.apache.solr.parser.ParseException;
import org.apache.solr.parser.SolrQueryParserBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.IndexSchemaFactory;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SolrQueryParser;
import org.apache.solr.search.SyntaxError;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


public class TestReversedWildcardFilterFactory extends SolrTestCaseJ4 {

  Map<String,String> args = new HashMap<>();
  IndexSchema schema;

  @BeforeClass
  public static void beforeClass() throws Exception {
    assumeWorkingMockito();
    initCore("solrconfig.xml","schema-reversed.xml");
  }
  
  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    schema = IndexSchemaFactory.buildIndexSchema(getSchemaFile(), solrConfig);
    clearIndex();
    assertU(commit());
  }

  @Test
  public void testReversedTokens() throws IOException {
    String text = "simple text";
    args.put("withOriginal", "true");
    ReversedWildcardFilterFactory factory = new ReversedWildcardFilterFactory(args);
    TokenStream input = factory.create(whitespaceMockTokenizer(text));
    assertTokenStreamContents(input, 
        new String[] { "\u0001elpmis", "simple", "\u0001txet", "text" },
        new int[] { 1, 0, 1, 0 });

    // now without original tokens
    args.put("withOriginal", "false");
    factory = new ReversedWildcardFilterFactory(args);
    input = factory.create(whitespaceMockTokenizer(text));
    assertTokenStreamContents(input,
        new String[] { "\u0001elpmis", "\u0001txet" },
        new int[] { 1, 1 });
  }
  
  @Test
  public void testIndexingAnalysis() throws Exception {
    Analyzer a = schema.getIndexAnalyzer();
    String text = "one two three si\uD834\uDD1Ex";

    // field one
    TokenStream input = a.tokenStream("one", text);
    assertTokenStreamContents(input,
        new String[] { "\u0001eno", "one", "\u0001owt", "two", 
          "\u0001eerht", "three", "\u0001x\uD834\uDD1Eis", "si\uD834\uDD1Ex" },
        new int[] { 0, 0, 4, 4, 8, 8, 14, 14 },
        new int[] { 3, 3, 7, 7, 13, 13, 19, 19 },
        new int[] { 1, 0, 1, 0, 1, 0, 1, 0 }
    );
    // field two
    input = a.tokenStream("two", text);
    assertTokenStreamContents(input,
        new String[] { "\u0001eno", "\u0001owt", 
          "\u0001eerht", "\u0001x\uD834\uDD1Eis" },
        new int[] { 0, 4, 8, 14 },
        new int[] { 3, 7, 13, 19 },
        new int[] { 1, 1, 1, 1 }
    );
    // field three
    input = a.tokenStream("three", text);
    assertTokenStreamContents(input,
        new String[] { "one", "two", "three", "si\uD834\uDD1Ex" },
        new int[] { 0, 4, 8, 14 },
        new int[] { 3, 7, 13, 19 }
    );
  }
  
  @Test
  public void testQueryParsing() throws Exception {

    // add some docs
    assertU(adoc("id", "1", "one", "one"));
    assertU(adoc("id", "2", "two", "two"));
    assertU(adoc("id", "3", "three", "three"));
    assertU(adoc("id", "4", "one", "four"));
    assertU(adoc("id", "5", "two", "five"));
    assertU(adoc("id", "6", "three", "si\uD834\uDD1Ex"));
    assertU(commit());
    
    assertQ("should have matched",
        req("+id:1 +one:one"),
        "//result[@numFound=1]");
    
    assertQ("should have matched",
        req("+id:4 +one:f*ur"),
        "//result[@numFound=1]");
        
    assertQ("should have matched",
        req("+id:6 +three:*si\uD834\uDD1Ex"),
        "//result[@numFound=1]");

    SolrQueryRequest req = req();
    QParser qparser = QParser.getParser("id:1", req);

    SolrQueryParser parserTwo = new SolrQueryParser(qparser, "two");
    assertTrue(parserTwo.getAllowLeadingWildcard());

    // test conditional reversal
    assertTrue(wasReversed(parserTwo, "*hree"));
    assertTrue(wasReversed(parserTwo, "t*ree"));
    assertTrue(wasReversed(parserTwo, "th*ee"));
    assertFalse(wasReversed(parserTwo, "thr*e"));
    assertTrue(wasReversed(parserTwo, "?hree"));
    assertTrue(wasReversed(parserTwo, "t?ree"));
    assertFalse(wasReversed(parserTwo, "th?ee"));
    assertFalse(wasReversed(parserTwo, "th?*ee"));
    assertFalse(wasReversed(parserTwo, "short*token"));
    assertTrue(wasReversed(parserTwo, "ver*longtoken"));

    req.close();
  }
  
  /** fragile assert: depends on our implementation, but cleanest way to check for now */ 
  private boolean wasReversed(SolrQueryParser qp, String query) throws Exception {
    Query q = qp.parse(query);
    if (!(q instanceof AutomatonQuery)) {
      return false;
    }
    Automaton automaton = ((AutomatonQuery) q).getAutomaton();
    String prefix = Operations.getCommonPrefix(Operations.determinize(automaton,
      Operations.DEFAULT_DETERMINIZE_WORK_LIMIT));
    return prefix.length() > 0 && prefix.charAt(0) == '\u0001';
  }

  @Test
  public void testFalsePositives() throws Exception {
    // add a doc
    assertU(adoc("id", "1", "one", "gomez", "two", "gomez", "three", "gomez"));
    assertU(commit());
    
    assertQ("false positive",
        req("+id:1 +one:*zemog*"),
        "//result[@numFound=0]");
    
    assertQ("no reverse, no false positive",
        req("q", "+id:1 +three:[* TO a]", 
            "debugQuery", "true"),
        "//result[@numFound=0]");
    {
      String reverseField = random().nextBoolean() ? "one":"two";
      assertQ("false positive",
          req("q", "+id:1 +"+reverseField+":[* TO a]", 
              "debugQuery", "true"),
          "//result[@numFound=0]");
    }
    assertQ("false positive",
        req("+id:1 +two:*zemog*"),
        "//result[@numFound=0]");
    assertQ("false positive",
        req("+id:1 +three:*zemog*"),
        "//result[@numFound=0]");
    
    assertQ("should have matched",
        req("+id:1 +one:*omez*"),
        "//result[@numFound=1]");
  }
  
  private static final class SolrQParser extends SolrQueryParserBase {
    @Override
    public Query TopLevelQuery(String field) throws ParseException, SyntaxError {
      return null;
    }

    @Override
    public void ReInit(CharStream stream) {}

    @Override
    protected ReversedWildcardFilterFactory getReversedWildcardFilterFactory(FieldType fieldType) {
      return super.getReversedWildcardFilterFactory(fieldType);
    }
  }
  
  @Test 
  public void testCachingInQueryParser() {
    SolrQParser parser = new SolrQParser();
    
    SolrQueryRequest req = req();
    String[] fields = new String[]{"one", "two", "three"};
    String aField = fields[random().nextInt(fields.length)];
    FieldType type = req.getSchema().getField(aField).getType();
    
    FieldType typeSpy = spy(type);
    // calling twice 
    parser.getReversedWildcardFilterFactory(typeSpy);
    parser.getReversedWildcardFilterFactory(typeSpy);
    // but it should reach only once 
    verify(typeSpy, times(1)).getIndexAnalyzer();
  }
}
