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
import org.apache.lucene.search.Query;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.BeforeClass;

/**
 *
 *
 **/
public class QueryParsingTest extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml","schema.xml");
  }

  /**
   * Test that the main QParserPlugins people are likely to use
   * as defaults fail with a consistent exception when the query string 
   * is either empty or null.
   * @see <a href="https://issues.apache.org/jira/browse/SOLR-435">SOLR-435</a>
   * @see <a href="https://issues.apache.org/jira/browse/SOLR-2001">SOLR-2001</a>
   */
  public void testQParserEmptyInput() throws Exception {
    
    SolrQueryRequest req = req("df", "text");

    final String[] parsersTested = new String[] {
      LuceneQParserPlugin.NAME,
      DisMaxQParserPlugin.NAME,
      ExtendedDismaxQParserPlugin.NAME
    };

    for (String defType : parsersTested) {
      for (String qstr : new String[] {null, ""}) {
        QParser parser = null;
        try {
          parser = QParser.getParser(qstr, defType, req);
        } catch (Exception e) {
          throw new RuntimeException("getParser excep using defType=" +
                                     defType + " with qstr="+qstr, e);
        }

        Query q = parser.parse();
        assertNull("expected no query",q);
      }
    }
  }
  
  public void testLocalParamsWithModifiableSolrParams() throws Exception {
    ModifiableSolrParams target = new ModifiableSolrParams();
    QueryParsing.parseLocalParams("{!handler foo1=bar1 foo2=bar2 multi=loser multi=winner}", 0, target, new ModifiableSolrParams(), "{!", '}');
    assertEquals("bar1", target.get("foo1"));
    assertEquals("bar2", target.get("foo2"));
    assertArrayEquals(new String[]{"loser", "winner"}, target.getParams("multi"));
  }

  public void testLiteralFunction() throws Exception {
    
    final String NAME = FunctionQParserPlugin.NAME;

    SolrQueryRequest req = req("variable", "foobar");
    
    assertNotNull(QParser.getParser
                  ("literal('a value')",
                   NAME, req).getQuery());
    assertNotNull(QParser.getParser
                  ("literal('a value')",
                   NAME, req).getQuery());
    assertNotNull(QParser.getParser
                  ("literal(\"a value\")",
                   NAME, req).getQuery());
    assertNotNull(QParser.getParser
                  ("literal($variable)",
                   NAME, req).getQuery());
    assertNotNull(QParser.getParser
                  ("strdist(\"a value\",literal('a value'),edit)",
                   NAME, req).getQuery());
  }

  public void testGetQParser() throws Exception {
    // invalid defType
    SolrException exception = expectThrows(SolrException.class, () -> h.query(req("q", "ad", "defType", "bleh")));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, exception.code());
    assertEquals("invalid query parser 'bleh' for query 'ad'", exception.getMessage());

    // invalid qparser override in the local params
    exception = expectThrows(SolrException.class, () -> h.query(req("q", "{!bleh}")));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, exception.code());
    assertEquals("invalid query parser 'bleh' for query '{!bleh}'", exception.getMessage());

    // invalid qParser with fq params
    exception = expectThrows(SolrException.class, () -> h.query(req("q", "*:*", "fq", "{!some}")));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, exception.code());
    assertEquals("invalid query parser 'some' for query '{!some}'", exception.getMessage());

    // invalid qparser with function queries
    exception = expectThrows(SolrException.class, () -> h.query(req("q", "*:*", "defType", "edismax", "boost", "{!hmm}")));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, exception.code());
    assertEquals("invalid query parser 'hmm' for query '{!hmm}'", exception.getMessage());

    exception = expectThrows(SolrException.class, () -> h.query(req("q", "*:*", "defType", "edismax", "boost", "query({!bleh v=ak})")));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, exception.code());
    assertEquals("invalid query parser 'bleh' for query '{!bleh v=ak}'", exception.getMessage());

    exception = expectThrows(SolrException.class, () ->
        h.query(req("q", "*:*", "defType", "edismax", "boost", "query($qq)", "qq", "{!bleh v=a}")));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, exception.code());
    assertEquals("invalid query parser 'bleh' for query '{!bleh v=a}'", exception.getMessage());

    // ranking doesn't use defType
    exception = expectThrows(SolrException.class, () -> h.query(req("q", "*:*", "rq", "{!bleh}")));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, exception.code());
    assertEquals("invalid query parser 'bleh' for query '{!bleh}'", exception.getMessage());

    // with stats.field
    exception = expectThrows(SolrException.class, () -> h.query(req("q", "*:*", "stats", "true", "stats.field", "{!bleh}")));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, exception.code());
    assertEquals("invalid query parser 'bleh' for query '{!bleh}'", exception.getMessage());
  }
}
