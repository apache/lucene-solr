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

public class TestPayloadScoreQParserPlugin extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema11.xml");
    createIndex();
  }

  public static void createIndex() {
    assertU(adoc("id","1", "vals_dpf","A|1.0 B|2.0 C|3.0 mult|50 mult|100 x|22 x|37 x|19"));
    assertU(commit());
  }

  @Test
  public void test() {

    assertQ(req("fl","*,score", "q", "{!payload_score f=vals_dpf v=B func=min}"), "//float[@name='score']='2.0'");
    assertQ(req("fl","*,score", "q", "{!payload_score f=vals_dpf v=mult func=min}"), "//float[@name='score']='50.0'");
    assertQ(req("fl","*,score", "q", "{!payload_score f=vals_dpf v=mult func=max}"), "//float[@name='score']='100.0'");
    assertQ(req("fl","*,score", "q", "{!payload_score f=vals_dpf v=mult func=average}"), "//float[@name='score']='75.0'");
    assertQ(req("fl","*,score", "q", "{!payload_score f=vals_dpf func=min}A B"), "//float[@name='score']='1.0'");
    assertQ(req("fl","*,score", "q", "{!payload_score f=vals_dpf func=min}B C"), "//float[@name='score']='2.0'");
    assertQ(req("fl","*,score", "q", "{!payload_score f=vals_dpf func=max}B C"), "//float[@name='score']='3.0'");
    assertQ(req("fl","*,score", "q", "{!payload_score f=vals_dpf func=average}B C"), "//float[@name='score']='2.5'");
    assertQ(req("fl","*,score", "q", "{!payload_score f=vals_dpf func=max}A B C"), "//float[@name='score']='3.0'");

    assertQ(req("fl","*,score", "q", "{!payload_score f=vals_dpf func=sum}A B C"), "//float[@name='score']='6.0'");
    assertQ(req("fl","*,score", "q", "{!payload_score f=vals_dpf func=sum operator=or}A C"), "//float[@name='score']='4.0'");
    assertQ(req("fl","*,score", "q", "{!payload_score f=vals_dpf func=sum operator=or}A"), "//float[@name='score']='1.0'");
    assertQ(req("fl","*,score", "q", "{!payload_score f=vals_dpf func=sum operator=or}foo"), "//result[@numFound='0']");

    assertQ(req("fl","*,score", "q", "{!payload_score f=vals_dpf func=max operator=or}A C"), "//float[@name='score']='3.0'");
    assertQ(req("fl","*,score", "q", "{!payload_score f=vals_dpf func=min operator=or}A x"), "//float[@name='score']='1.0'");
    assertQ(req("fl","*,score", "q", "{!payload_score f=vals_dpf func=average operator=or}A C"), "//float[@name='score']='2.0'");

    // TODO: fix this includeSpanScore test to be less brittle - score result is score of "A" (via BM25) multipled by 1.0 (payload value)
    assertQ(req("fl","*,score", "q", "{!payload_score f=vals_dpf v=A func=min}"), "//float[@name='score']='1.0'");
    assertQ(req("fl","*,score", "q", "{!payload_score f=vals_dpf v=A func=min includeSpanScore=true}"), "//float[@name='score']='0.13076457'");
  }
}
