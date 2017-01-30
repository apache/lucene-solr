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
package org.apache.lucene.queryparser.xml;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.AfterClass;

import java.io.IOException;
import java.io.InputStream;

public class TestCoreParser extends LuceneTestCase {

  final private static String defaultField = "contents";

  private static Analyzer analyzer;
  private static CoreParser coreParser;

  private static CoreParserTestIndexData indexData;

  protected Analyzer newAnalyzer() {
    // TODO: rewrite test (this needs to set QueryParser.enablePositionIncrements, too, for work with CURRENT):
    return new MockAnalyzer(random(), MockTokenizer.WHITESPACE, true, MockTokenFilter.ENGLISH_STOPSET);
  }

  protected CoreParser newCoreParser(String defaultField, Analyzer analyzer) {
    return new CoreParser(defaultField, analyzer);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (indexData != null) {
      indexData.close();
      indexData = null;
    }
    coreParser = null;
    analyzer = null;
  }

  public void testTermQueryXML() throws ParserException, IOException {
    Query q = parse("TermQuery.xml");
    dumpResults("TermQuery", q, 5);
  }

  public void testTermQueryEmptyXML() throws ParserException, IOException {
    parseShouldFail("TermQueryEmpty.xml",
        "TermQuery has no text");
  }

  public void testTermsQueryXML() throws ParserException, IOException {
    Query q = parse("TermsQuery.xml");
    dumpResults("TermsQuery", q, 5);
  }

  public void testBooleanQueryXML() throws ParserException, IOException {
    Query q = parse("BooleanQuery.xml");
    dumpResults("BooleanQuery", q, 5);
  }
  
  public void testDisjunctionMaxQueryXML() throws ParserException, IOException {
    Query q = parse("DisjunctionMaxQuery.xml");
    assertTrue(q instanceof DisjunctionMaxQuery);
    DisjunctionMaxQuery d = (DisjunctionMaxQuery)q;
    assertEquals(0.0f, d.getTieBreakerMultiplier(), 0.0001f);
    assertEquals(2, d.getDisjuncts().size());
    DisjunctionMaxQuery ndq = (DisjunctionMaxQuery) d.getDisjuncts().get(1);
    assertEquals(1.2f, ndq.getTieBreakerMultiplier(), 0.0001f);
    assertEquals(1, ndq.getDisjuncts().size());
  }

  public void testRangeQueryXML() throws ParserException, IOException {
    Query q = parse("RangeQuery.xml");
    dumpResults("RangeQuery", q, 5);
  }

  public void testUserQueryXML() throws ParserException, IOException {
    Query q = parse("UserInputQuery.xml");
    dumpResults("UserInput with Filter", q, 5);
  }

  public void testCustomFieldUserQueryXML() throws ParserException, IOException {
    Query q = parse("UserInputQueryCustomField.xml");
    int h = searcher().search(q, 1000).totalHits;
    assertEquals("UserInputQueryCustomField should produce 0 result ", 0, h);
  }

  public void testBoostingTermQueryXML() throws Exception {
    Query q = parse("BoostingTermQuery.xml");
    dumpResults("BoostingTermQuery", q, 5);
  }

  public void testSpanTermXML() throws Exception {
    Query q = parse("SpanQuery.xml");
    dumpResults("Span Query", q, 5);
    SpanQuery sq = parseAsSpan("SpanQuery.xml");
    dumpResults("Span Query", sq, 5);
    assertEquals(q, sq);
  }

  public void testConstantScoreQueryXML() throws Exception {
    Query q = parse("ConstantScoreQuery.xml");
    dumpResults("ConstantScoreQuery", q, 5);
  }

  public void testMatchAllDocsPlusFilterXML() throws ParserException, IOException {
    Query q = parse("MatchAllDocsQuery.xml");
    dumpResults("MatchAllDocsQuery with range filter", q, 5);
  }

  public void testNestedBooleanQuery() throws ParserException, IOException {
    Query q = parse("NestedBooleanQuery.xml");
    dumpResults("Nested Boolean query", q, 5);
  }

  public void testNumericRangeQueryXML() throws ParserException, IOException {
    Query q = parse("LegacyNumericRangeQuery.xml");
    dumpResults("LegacyNumericRangeQuery", q, 5);
  }

  public void testNumericRangeQueryXMLWithoutLowerTerm() throws ParserException, IOException {
    Query q = parse("LegacyNumericRangeQueryWithoutLowerTerm.xml");
    dumpResults("LegacyNumericRangeQueryWithoutLowerTerm", q, 5);
  }

  public void testNumericRangeQueryXMLWithoutUpperTerm() throws ParserException, IOException {
    Query q = parse("LegacyNumericRangeQueryWithoutUpperTerm.xml");
    dumpResults("LegacyNumericRangeQueryWithoutUpperTerm", q, 5);
  }

  public void testNumericRangeQueryXMLWithoutRange() throws ParserException, IOException {
    Query q = parse("LegacyNumericRangeQueryWithoutRange.xml");
    dumpResults("LegacyNumericRangeQueryWithoutRange", q, 5);
  }
  
  public void testPointRangeQuery() throws ParserException, IOException {
    Query q = parse("PointRangeQuery.xml");
    dumpResults("PointRangeQuery", q, 5);
  }

  public void testPointRangeQueryWithoutLowerTerm() throws ParserException, IOException {
    Query q = parse("PointRangeQueryWithoutLowerTerm.xml");
    dumpResults("PointRangeQueryWithoutLowerTerm", q, 5);
  }

  public void testPointRangeQueryWithoutUpperTerm() throws ParserException, IOException {
    Query q = parse("PointRangeQueryWithoutUpperTerm.xml");
    dumpResults("PointRangeQueryWithoutUpperTerm", q, 5);
  }

  public void testPointRangeQueryWithoutRange() throws ParserException, IOException {
    Query q = parse("PointRangeQueryWithoutRange.xml");
    dumpResults("PointRangeQueryWithoutRange", q, 5);
  }

  //================= Helper methods ===================================

  protected String defaultField() {
    return defaultField;
  }

  protected Analyzer analyzer() {
    if (analyzer == null) {
      analyzer = newAnalyzer();
    }
    return analyzer;
  }

  protected CoreParser coreParser() {
    if (coreParser == null) {
      coreParser = newCoreParser(defaultField, analyzer());
    }
    return coreParser;
  }

  private CoreParserTestIndexData indexData() {
    if (indexData == null) {
      try {
        indexData = new CoreParserTestIndexData(analyzer());
      } catch (Exception e) {
        fail("caught Exception "+e);
      }
    }
    return indexData;
  }

  protected IndexReader reader() {
    return indexData().reader;
  }

  protected IndexSearcher searcher() {
    return indexData().searcher;
  }

  protected void parseShouldFail(String xmlFileName, String expectedParserExceptionMessage) throws IOException {
    Query q = null;
    ParserException pe = null;
    try {
      q = parse(xmlFileName);
    } catch (ParserException e) {
      pe = e;
    }
    assertNull("for "+xmlFileName+" unexpectedly got "+q, q);
    assertNotNull("expected a ParserException for "+xmlFileName, pe);
    assertEquals("expected different ParserException for "+xmlFileName,
        expectedParserExceptionMessage, pe.getMessage());
  }

  protected Query parse(String xmlFileName) throws ParserException, IOException {
    return implParse(xmlFileName, false);
  }

  protected SpanQuery parseAsSpan(String xmlFileName) throws ParserException, IOException {
    return (SpanQuery)implParse(xmlFileName, true);
  }

  private Query implParse(String xmlFileName, boolean span) throws ParserException, IOException {
    try (InputStream xmlStream = TestCoreParser.class.getResourceAsStream(xmlFileName)) {
      assertNotNull("Test XML file " + xmlFileName + " cannot be found", xmlStream);
      if (span) {
        return coreParser().parseAsSpanQuery(xmlStream);
      } else {
        return coreParser().parse(xmlStream);
      }
    }
  }

  protected Query rewrite(Query q) throws IOException {
    return q.rewrite(reader());
  }

  protected void dumpResults(String qType, Query q, int numDocs) throws IOException {
    if (VERBOSE) {
      System.out.println("TEST: qType=" + qType + " numDocs=" + numDocs + " " + q.getClass().getCanonicalName() + " query=" + q);
    }
    final IndexSearcher searcher = searcher();
    TopDocs hits = searcher.search(q, numDocs);
    final boolean producedResults = (hits.totalHits > 0);
    if (!producedResults) {
      System.out.println("TEST: qType=" + qType + " numDocs=" + numDocs + " " + q.getClass().getCanonicalName() + " query=" + q);
    }
    if (VERBOSE) {
      ScoreDoc[] scoreDocs = hits.scoreDocs;
      for (int i = 0; i < Math.min(numDocs, hits.totalHits); i++) {
        Document ldoc = searcher.doc(scoreDocs[i].doc);
        System.out.println("[" + ldoc.get("date") + "]" + ldoc.get("contents"));
      }
      System.out.println();
    }
    assertTrue(qType + " produced no results", producedResults);
  }
}
