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
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.Query;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.SolrQueryParser;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.solr.analysis.BaseTokenTestCase.*;

public class TestReversedWildcardFilterFactory extends SolrTestCaseJ4 {
  Map<String,String> args = new HashMap<String, String>();
  ReversedWildcardFilterFactory factory = new ReversedWildcardFilterFactory();
  IndexSchema schema;

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml","schema-reversed.xml");
  }
  
  @Before
  public void setUp() throws Exception {
    super.setUp();
    schema = new IndexSchema(solrConfig, getSchemaFile(), null);
  }

  @Test
  public void testReversedTokens() throws IOException {
    String text = "simple text";
    args.put("withOriginal", "true");
    factory.init(args);
    TokenStream input = factory.create(new WhitespaceTokenizer(DEFAULT_VERSION, new StringReader(text)));
    assertTokenStreamContents(input, 
        new String[] { "\u0001elpmis", "simple", "\u0001txet", "text" },
        new int[] { 1, 0, 1, 0 });

    // now without original tokens
    args.put("withOriginal", "false");
    factory.init(args);
    input = factory.create(new WhitespaceTokenizer(DEFAULT_VERSION, new StringReader(text)));
    assertTokenStreamContents(input,
        new String[] { "\u0001elpmis", "\u0001txet" },
        new int[] { 1, 1 });
  }
  
  @Test
  public void testIndexingAnalysis() throws Exception {
    Analyzer a = schema.getAnalyzer();
    String text = "one two three si\uD834\uDD1Ex";

    // field one
    TokenStream input = a.tokenStream("one", new StringReader(text));
    assertTokenStreamContents(input,
        new String[] { "\u0001eno", "one", "\u0001owt", "two", 
          "\u0001eerht", "three", "\u0001x\uD834\uDD1Eis", "si\uD834\uDD1Ex" },
        new int[] { 0, 0, 4, 4, 8, 8, 14, 14 },
        new int[] { 3, 3, 7, 7, 13, 13, 19, 19 },
        new int[] { 1, 0, 1, 0, 1, 0, 1, 0 }
    );
    // field two
    input = a.tokenStream("two", new StringReader(text));
    assertTokenStreamContents(input,
        new String[] { "\u0001eno", "\u0001owt", 
          "\u0001eerht", "\u0001x\uD834\uDD1Eis" },
        new int[] { 0, 4, 8, 14 },
        new int[] { 3, 7, 13, 19 },
        new int[] { 1, 1, 1, 1 }
    );
    // field three
    input = a.tokenStream("three", new StringReader(text));
    assertTokenStreamContents(input,
        new String[] { "one", "two", "three", "si\uD834\uDD1Ex" },
        new int[] { 0, 4, 8, 14 },
        new int[] { 3, 7, 13, 19 }
    );
  }
  
  @Test
  public void testQueryParsing() throws IOException, ParseException {

    SolrQueryParser parserOne = new SolrQueryParser(schema, "one");
    assertTrue(parserOne.getAllowLeadingWildcard());
    SolrQueryParser parserTwo = new SolrQueryParser(schema, "two");
    assertTrue(parserTwo.getAllowLeadingWildcard());
    SolrQueryParser parserThree = new SolrQueryParser(schema, "three");
    // XXX note: this should be false, but for now we return true for any field,
    // XXX if at least one field uses the reversing
    assertTrue(parserThree.getAllowLeadingWildcard());
    String text = "one +two *hree f*ur fiv* *si\uD834\uDD1Ex";
    String expectedOne = "one:one +one:two one:\u0001eerh* one:\u0001ru*f one:fiv* one:\u0001x\uD834\uDD1Eis*";
    String expectedTwo = "two:one +two:two two:\u0001eerh* two:\u0001ru*f two:fiv* two:\u0001x\uD834\uDD1Eis*";
    String expectedThree = "three:one +three:two three:*hree three:f*ur three:fiv* three:*si\uD834\uDD1Ex";
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

  @Test
  public void testFalsePositives() throws Exception {
    // add a doc
    assertU(adoc("id", "1", "one", "gomez", "two", "gomez", "three", "gomez"));
    assertU(commit());
    
    assertQ("false positive",
        req("+id:1 +one:*zemog*"),
        "//result[@numFound=0]");
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
}
