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
import org.apache.lucene.queryparser.xml.CoreParser;
import org.apache.lucene.queryparser.xml.ParserException;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;

public class TestSolrCoreParser extends LuceneTestCase {

  private SolrCoreParser solrCoreParser;

  private CoreParser solrCoreParser() {
    if (solrCoreParser == null) {
      final String defaultField = "contents";
      final Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, true, MockTokenFilter.ENGLISH_STOPSET);
      final SolrQueryRequest req = null;
      solrCoreParser = new SolrCoreParser(defaultField, analyzer, req);
      {
        final NamedList<String> args = new NamedList<>();
        args.add("HelloQuery", HelloQueryBuilder.class.getCanonicalName());
        args.add("GoodbyeQuery", GoodbyeQueryBuilder.class.getCanonicalName());
        args.add("HandyQuery", HandyQueryBuilder.class.getCanonicalName());
        args.add("ApacheLuceneSolr", ApacheLuceneSolrNearQueryBuilder.class.getCanonicalName());
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

}
