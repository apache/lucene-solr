package org.apache.solr.core;
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

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.FooQParserPlugin;
import org.apache.solr.search.ValueSourceParser;
import org.junit.BeforeClass;


/**
 * Test for https://issues.apache.org/jira/browse/SOLR-749
 *
 **/
public class SOLR749Test extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-SOLR-749.xml","schema.xml");
  }

  public void testConstruction() throws Exception {
    SolrCore core = h.getCore();
    assertTrue("core is null and it shouldn't be", core != null);
    QParserPlugin parserPlugin = core.getQueryPlugin(QParserPlugin.DEFAULT_QTYPE);
    assertTrue("parserPlugin is null and it shouldn't be", parserPlugin != null);
    assertTrue("parserPlugin is not an instanceof " + FooQParserPlugin.class, parserPlugin instanceof FooQParserPlugin);

    ValueSourceParser vsp = core.getValueSourceParser("boost");
    assertTrue("vsp is null and it shouldn't be", vsp != null);
    assertTrue("vsp is not an instanceof " + DummyValueSourceParser.class, vsp instanceof DummyValueSourceParser);
  }

  public void testHowManyDocsHaveBoostFunctionComputed() throws Exception {
    for (int i = 0; i < 100; i++) {
      assertU(adoc("id",""+i));
    }
    assertU(commit());

    // NOTE: we can't rely on the default lucene syntax because "FooQParser" is registered as "lucene"
    assertQ(req("q","{!notfoo}*:*"), "//result[@numFound=100]");
    assertQ(req("q","{!notfoo}id:[* TO 49]"), "//result[@numFound=50]");
    try {
      assertQ("query wrapped in boost func should only eval func for query matches",
              req("q","{!boost b=$boostFunc defType=notfoo}id:[* TO 49]",
                  "boostFunc", "countUsage('boost_func',3.4)"),
              "//result[@numFound=50]");
      assertEquals(50, CountUsageValueSourceParser.getAndClearCount("boost_func"));

      assertQ("func query that is filtered should be evaled only for filtered docs",
              req("q","{!func}product(id,countUsage('func_q',4.5))",
                  "fq", "{!notfoo}id:[30 TO 59]"),
              "//result[@numFound=30]");
      assertEquals(30, CountUsageValueSourceParser.getAndClearCount("func_q"));

      assertQ("func query that wraps a query which is also used as a should be evaled only for filtered docs",
              req("q","{!func}product(query($qq),countUsage('func_q_wrapping_fq',4.5))",
                  "qq", "{!notfoo}id:[20 TO 39]",
                  "fq", "{!query v=$qq}"),
              "//result[@numFound=20]");
      assertEquals(20, CountUsageValueSourceParser.getAndClearCount("func_q_wrapping_fq"));

    } finally {
      CountUsageValueSourceParser.clearCounters();
    }
  }
  
}
