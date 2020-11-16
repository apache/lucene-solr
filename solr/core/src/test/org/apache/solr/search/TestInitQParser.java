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

import org.apache.solr.SolrTestCaseJ4;
import org.junit.Before;
import org.junit.Test;

/**
 * Checking QParser plugin initialization, failing with NPE during Solr startup.
 * Ensures that query is working by registered in solrconfig.xml "fail" query parser.
 */
public class TestInitQParser extends SolrTestCaseJ4 {
  private static void createIndex() {
    String v;
    v = "how now brown cow";
    assertU(adoc("id", "1", "text", v, "text_np", v));
    v = "now cow";
    assertU(adoc("id", "2", "text", v, "text_np", v));
    assertU(adoc("id", "3", "foo_s", "a ' \" \\ {! ) } ( { z"));  // A value filled with special chars

    assertU(adoc("id", "10", "qqq_s", "X"));
    assertU(adoc("id", "11", "www_s", "X"));
    assertU(adoc("id", "12", "eee_s", "X"));
    assertU(adoc("id", "13", "eee_s", "'balance'"));

    assertU(commit());

  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    System.setProperty("enable.update.log", "false"); // schema12 doesn't support _version_
    initCore("solrconfig-query-parser-init.xml", "schema12.xml");
    createIndex();
  }

  @Test
  public void testQueryParserInit() throws Exception {
    // should query using registered fail (defType=fail) QParser and match only one doc
    assertQ(req("q", "id:1", "indent", "true", "defType", "fail")
        , "//*[@numFound='1']"
    );
  }

}
