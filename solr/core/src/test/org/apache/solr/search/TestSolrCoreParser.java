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
package org.apache.solr.search;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.xml.CoreParser;
import org.apache.lucene.queryparser.xml.ParserException;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.spans.SpanBoostQuery;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.NamedList;
import org.junit.BeforeClass;

public class TestSolrCoreParser extends SolrTestCaseJ4 {

  @BeforeClass
  public static void init() throws Exception {
    initCore("solrconfig.xml","schema.xml");
  }
  
  private SolrCoreParser solrCoreParser;

  private CoreParser solrCoreParser() {
    if (solrCoreParser == null) {
      final String defaultField = "contents";
      final Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, true, MockTokenFilter.ENGLISH_STOPSET);
      solrCoreParser = new SolrCoreParser(defaultField, analyzer, req());
      {
        final NamedList<String> args = new NamedList<>();
        args.add("HelloQuery", HelloQueryBuilder.class.getCanonicalName());
        args.add("GoodbyeQuery", GoodbyeQueryBuilder.class.getCanonicalName());
        args.add("HandyQuery", HandyQueryBuilder.class.getCanonicalName());
        args.add("ApacheLuceneSolr", ApacheLuceneSolrNearQueryBuilder.class.getCanonicalName());
        args.add("ChooseOneWord", ChooseOneWordQueryBuilder.class.getCanonicalName());
        solrCoreParser.init(args);
      }
    }
    return solrCoreParser;
  }

  private Query parseXmlString(String xml) throws IOException, ParserException {
    final byte[] xmlBytes = xml.getBytes(StandardCharsets.UTF_8);
    final InputStream xmlStream = new ByteArrayInputStream(xmlBytes);
    return solrCoreParser().parse(xmlStream);
  }

  private Query parseHandyQuery(String lhsXml, String rhsXml) throws IOException, ParserException {
    final String xml = "<HandyQuery>"
        + "<Left>" + lhsXml + "</Left>"
        + "<Right>" + rhsXml + "</Right>"
        + "</HandyQuery>";
    return parseXmlString(xml);
  }

  public void testHello() throws IOException, ParserException {
    final Query query = parseXmlString("<HelloQuery/>");
    assertTrue(query instanceof MatchAllDocsQuery);
  }

  public void testGoodbye() throws IOException, ParserException {
    final Query query = parseXmlString("<GoodbyeQuery/>");
    assertTrue(query instanceof MatchNoDocsQuery);
  }

  public void testApacheLuceneSolr() throws IOException, ParserException {
    final String fieldName = "contents";
    final Query query = parseXmlString("<ApacheLuceneSolr fieldName='"+fieldName+"'/>");
    checkApacheLuceneSolr(query, fieldName);
  }

  private static void checkApacheLuceneSolr(Query query, String fieldName) {
    assertTrue(query instanceof SpanNearQuery);
    final SpanNearQuery snq = (SpanNearQuery)query;
    assertEquals(fieldName, snq.getField());
    assertEquals(42, snq.getSlop());
    assertFalse(snq.isInOrder());
    assertEquals(3, snq.getClauses().length);
    assertTrue(snq.getClauses()[0] instanceof SpanTermQuery);
    assertTrue(snq.getClauses()[1] instanceof SpanTermQuery);
    assertTrue(snq.getClauses()[2] instanceof SpanTermQuery);
  }

  // test custom query (HandyQueryBuilder) wrapping a Query
  public void testHandyQuery() throws IOException, ParserException {
    final String lhsXml = "<HelloQuery/>";
    final String rhsXml = "<GoodbyeQuery/>";
    final Query query = parseHandyQuery(lhsXml, rhsXml);
    assertTrue(query instanceof BooleanQuery);
    final BooleanQuery bq = (BooleanQuery)query;
    assertEquals(2, bq.clauses().size());
    assertTrue(bq.clauses().get(0).getQuery() instanceof MatchAllDocsQuery);
    assertTrue(bq.clauses().get(1).getQuery() instanceof MatchNoDocsQuery);
  }

  private static SpanQuery unwrapSpanBoostQuery(Query query) {
    assertTrue(query instanceof SpanBoostQuery);
    final SpanBoostQuery spanBoostQuery = (SpanBoostQuery)query;
    return spanBoostQuery.getQuery();
  }

  // test custom query (HandyQueryBuilder) wrapping a SpanQuery
  public void testHandySpanQuery() throws IOException, ParserException {
    final String lhsXml = "<SpanOr fieldName='contents'>"
        + "<SpanTerm>rain</SpanTerm>"
        + "<SpanTerm>spain</SpanTerm>"
        + "<SpanTerm>plain</SpanTerm>"
        + "</SpanOr>";
    final String rhsXml = "<SpanNear fieldName='contents' slop='2' inOrder='true'>"
        + "<SpanTerm>sunny</SpanTerm>"
        + "<SpanTerm>sky</SpanTerm>"
        + "</SpanNear>";
    final Query query = parseHandyQuery(lhsXml, rhsXml);
    final BooleanQuery bq = (BooleanQuery)query;
    assertEquals(2, bq.clauses().size());
    for (int ii=0; ii<bq.clauses().size(); ++ii) {
      final Query clauseQuery = bq.clauses().get(ii).getQuery();
      switch (ii) {
        case 0:
          assertTrue(unwrapSpanBoostQuery(clauseQuery) instanceof SpanOrQuery);
          break;
        case 1:
          assertTrue(unwrapSpanBoostQuery(clauseQuery) instanceof SpanNearQuery);
          break;
        default:
          fail("unexpected clause index "+ii);
      }
    }
  }

  private static String composeChooseOneWordQueryXml(String fieldName, String... termTexts) {
    final StringBuilder sb = new StringBuilder("<ChooseOneWord fieldName='"+fieldName+"'>");
    for (String termText : termTexts) {
      sb.append("<Word>").append(termText).append("</Word>");
    }
    sb.append("</ChooseOneWord>");
    return sb.toString();
  }

  // test custom queries being wrapped in a Query or SpanQuery
  public void testCustomQueryWrapping() throws IOException, ParserException {
    final boolean span = random().nextBoolean();
    // the custom queries
    final String fieldName = "contents";
    final String[] randomTerms = new String[] {"bumble", "honey", "solitary"};
    final String randomQuery = composeChooseOneWordQueryXml(fieldName, randomTerms);
    final String apacheLuceneSolr = "<ApacheLuceneSolr fieldName='"+fieldName+"'/>";
    // the wrapping query
    final String parentQuery = (span ? "SpanOr" : "BooleanQuery");
    final String subQueryPrefix = (span ? "" : "<Clause occurs='must'>");
    final String subQuerySuffix = (span ? "" : "</Clause>");
    final String xml = "<"+parentQuery+">"
        + subQueryPrefix+randomQuery+subQuerySuffix
        + subQueryPrefix+apacheLuceneSolr+subQuerySuffix
        + "</"+parentQuery+">";
    // the test
    final Query query = parseXmlString(xml);
    if (span) {
      assertTrue(unwrapSpanBoostQuery(query) instanceof SpanOrQuery);
      final SpanOrQuery soq = (SpanOrQuery)unwrapSpanBoostQuery(query);
      assertEquals(2, soq.getClauses().length);
      checkChooseOneWordQuery(span, soq.getClauses()[0], fieldName, randomTerms);
      checkApacheLuceneSolr(soq.getClauses()[1], fieldName);
    } else {
      assertTrue(query instanceof BooleanQuery);
      final BooleanQuery bq = (BooleanQuery)query;
      assertEquals(2, bq.clauses().size());
      checkChooseOneWordQuery(span, bq.clauses().get(0).getQuery(), fieldName, randomTerms);
      checkApacheLuceneSolr(bq.clauses().get(1).getQuery(), fieldName);
    }
  }

  private static void checkChooseOneWordQuery(boolean span, Query query, String fieldName, String ... expectedTermTexts) {
    final Term term;
    if (span) {
      assertTrue(query instanceof SpanTermQuery);
      final SpanTermQuery stq = (SpanTermQuery)query;
      term = stq.getTerm();
    } else {
      assertTrue(query instanceof TermQuery);
      final TermQuery tq = (TermQuery)query;
      term = tq.getTerm();
    }
    final String text = term.text();
    boolean foundExpected = false;
    for (String expected : expectedTermTexts) {
      foundExpected |= expected.equals(text);
    }
    assertEquals(fieldName, term.field());
    assertTrue("expected term text ("+text+") not found in ("+expectedTermTexts+")", foundExpected);
  }

}
