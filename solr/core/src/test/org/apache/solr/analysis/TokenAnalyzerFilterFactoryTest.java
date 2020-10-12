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

import java.util.HashMap;
import java.util.Map;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.IndexSchemaFactory;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


public class TokenAnalyzerFilterFactoryTest extends SolrTestCaseJ4 {

  Map<String,String> args = new HashMap<>();
  IndexSchema schema;

  @BeforeClass
  public static void beforeClass() throws Exception {
    assumeWorkingMockito();
    initCore("solrconfig.xml","schema-tok-analyze-filter.xml");
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
  public void test() throws Exception {

    // add some docs
    assertU(adoc("id", "1", "text", "One", "taf1", "One"));
    assertU(adoc("id", "2", "text", "One TWO", "taf1", "One TWO"));
    assertU(adoc("id", "3", "text", "two THREE", "taf1", "two THREE"));
    assertU(commit());

    assertQ("should have matched",
        req("+text:one"),
        "//result[@numFound=2]");
    assertQ("should have matched",
        req("+taf1:one"),
        "//result[@numFound=2]");
    assertQ("should have matched",
        req("+taf1:oNe"),
        "//result[@numFound=2]");

    assertQ("should have matched",
        req("+text:TWO"),
        "//result[@numFound=2]");
    assertQ("should have matched",
        req("+taf1:TWO"),
        "//result[@numFound=2]");

    assertQ("should have matched",
        req("+text:three"),
        "//result[@numFound=1]");
    assertQ("should have matched",
        req("+taf1:three"),
        "//result[@numFound=1]");
  }

}
