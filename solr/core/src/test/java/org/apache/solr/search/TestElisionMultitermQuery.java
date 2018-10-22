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
import org.junit.BeforeClass;
import org.junit.Test;

public class TestElisionMultitermQuery extends SolrTestCaseJ4 {

  public String getCoreName() {
    return "basic";
  }

  @BeforeClass
  public static void beforeTests() throws Exception {
    initCore("solrconfig-basic.xml", "schema-folding.xml");
    
    assertU(adoc("id", "1", "text_fr", "l'Auberge"));
    assertU(adoc("id", "2", "text_fr", "Auberge"));
    assertU(adoc("id", "3", "text_fr", "other"));
    assertU(commit());
  }
  
  @Test
  public void testElisionMultitermQuery() {
    assertQ(req("q", "text_fr:auberge"), "//result[@numFound='2']");
    assertQ(req("q", "text_fr:Auberge"), "//result[@numFound='2']");
    assertQ(req("q", "text_fr:l'auberge"), "//result[@numFound='2']");
    assertQ(req("q", "text_fr:l'Auberge"), "//result[@numFound='2']");
    assertQ(req("q", "text_fr:aub*"), "//result[@numFound='2']");
    assertQ(req("q", "text_fr:Aub*"), "//result[@numFound='2']");
    assertQ(req("q", "text_fr:l'aub*"), "//result[@numFound='2']");
    assertQ(req("q", "text_fr:l'Aub*"), "//result[@numFound='2']");
  }

}
