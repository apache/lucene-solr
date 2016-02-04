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
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.SolrException;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.SchemaField;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.List;

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
    
    SolrQueryRequest req = req();
    
    final String[] parsersTested = new String[] {
      OldLuceneQParserPlugin.NAME,
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
}
