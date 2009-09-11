package org.apache.solr.analysis;
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


import java.io.IOException;
import java.io.StringReader;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WhitespaceTokenizer;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.Query;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.SolrQueryParser;

public class TestReversedWildcardFilterFactory extends BaseTokenTestCase {
  Map<String,String> args = new HashMap<String, String>();
  ReversedWildcardFilterFactory factory = new ReversedWildcardFilterFactory();
  IndexSchema schema;

  public String getSchemaFile() {
    return "schema-reversed.xml";
  }

  public String getSolrConfigFile() {
    return "solrconfig.xml";
  }
  
  public void setUp() throws Exception {
    super.setUp();
    schema = new IndexSchema(solrConfig, getSchemaFile(), null);
  }

  public void testReversedTokens() throws IOException {
    String text = "simple text";
    String expected1 = "simple \u0001elpmis text \u0001txet";
    String expected2 = "\u0001elpmis \u0001txet";
    args.put("withOriginal", "true");
    factory.init(args);
    TokenStream input = factory.create(new WhitespaceTokenizer(new StringReader(text)));
    List<Token> realTokens = getTokens(input);
    List<Token> expectedTokens = tokens(expected1);
    // set positionIncrements in expected tokens
    for (int i = 1; i < expectedTokens.size(); i += 2) {
      expectedTokens.get(i).setPositionIncrement(0);
    }
    assertTokEqual(realTokens, expectedTokens);
    
    // now without original tokens
    args.put("withOriginal", "false");
    factory.init(args);
    input = factory.create(new WhitespaceTokenizer(new StringReader(text)));
    realTokens = getTokens(input);
    expectedTokens = tokens(expected2);
    assertTokEqual(realTokens, expectedTokens);
  }
  
  public void testIndexingAnalysis() throws Exception {
    Analyzer a = schema.getAnalyzer();
    String text = "one two three";
    String expected1 = "one \u0001eno two \u0001owt three \u0001eerht";
    List<Token> expectedTokens1 = getTokens(
            new WhitespaceTokenizer(new StringReader(expected1)));
    // set positionIncrements and offsets in expected tokens
    for (int i = 1; i < expectedTokens1.size(); i += 2) {
      Token t = expectedTokens1.get(i);
      t.setPositionIncrement(0);
    }
    String expected2 = "\u0001eno \u0001owt \u0001eerht";
    List<Token> expectedTokens2 = getTokens(
            new WhitespaceTokenizer(new StringReader(expected2)));
    String expected3 = "one two three";
    List<Token> expectedTokens3 = getTokens(
            new WhitespaceTokenizer(new StringReader(expected3)));
    // field one
    TokenStream input = a.tokenStream("one", new StringReader(text));
    List<Token> realTokens = getTokens(input);
    assertTokEqual(realTokens, expectedTokens1);
    // field two
    input = a.tokenStream("two", new StringReader(text));
    realTokens = getTokens(input);
    assertTokEqual(realTokens, expectedTokens2);
    // field three
    input = a.tokenStream("three", new StringReader(text));
    realTokens = getTokens(input);
    assertTokEqual(realTokens, expectedTokens3);
  }
  
  public void testQueryParsing() throws IOException, ParseException {

    SolrQueryParser parserOne = new SolrQueryParser(schema, "one");
    assertTrue(parserOne.getAllowLeadingWildcard());
    SolrQueryParser parserTwo = new SolrQueryParser(schema, "two");
    assertTrue(parserTwo.getAllowLeadingWildcard());
    SolrQueryParser parserThree = new SolrQueryParser(schema, "three");
    // XXX note: this should be false, but for now we return true for any field,
    // XXX if at least one field uses the reversing
    assertTrue(parserThree.getAllowLeadingWildcard());
    String text = "one +two *hree f*ur fiv*";
    String expectedOne = "one:one +one:two one:\u0001eerh* one:\u0001ru*f one:fiv*";
    String expectedTwo = "two:one +two:two two:\u0001eerh* two:\u0001ru*f two:fiv*";
    String expectedThree = "three:one +three:two three:*hree three:f*ur three:fiv*";
    Query q = parserOne.parse(text);
    assertEquals(expectedOne, q.toString());
    q = parserTwo.parse(text);
    assertEquals(expectedTwo, q.toString());
    q = parserThree.parse(text);
    assertEquals(expectedThree, q.toString());
    // test conditional reversal
    String condText = "*hree t*ree th*ee thr*e ?hree t?ree th?ee th?*ee " + 
        "short*token ver*longtoken";
    String expected = "two:\u0001eerh* two:\u0001eer*t two:\u0001ee*ht " +
        "two:thr*e " +
        "two:\u0001eerh? two:\u0001eer?t " +
        "two:th?ee " +
        "two:th?*ee " +
        "two:short*token " +
        "two:\u0001nekotgnol*rev";
    q = parserTwo.parse(condText);
    assertEquals(expected, q.toString());
  }

}
