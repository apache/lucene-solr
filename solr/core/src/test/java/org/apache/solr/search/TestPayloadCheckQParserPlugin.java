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

public class TestPayloadCheckQParserPlugin extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema11.xml");
    createIndex();
  }

  public static void createIndex() {
    assertU(adoc("id","1", "vals_dpi","A|1 B|2 C|3"));
    assertU(adoc("id","2", "vals_dpf","one|1.0 two|2.0 three|3.0"));
    assertU(adoc("id","3", "vals_dps","the|ARTICLE cat|NOUN jumped|VERB"));
    assertU(commit());
  }

  @Test
  public void test() {
    clearIndex();

    String[] should_matches = new String[] {
        "{!payload_check f=vals_dpi v=A payloads=1}",
        "{!payload_check f=vals_dpi v=B payloads=2}",
        "{!payload_check f=vals_dpi v=C payloads=3}",
        "{!payload_check f=vals_dpi payloads='1 2'}A B",
        // "{!payload_check f=vals_dpi payloads='1 2.0'}A B",  // ideally this should pass, but IntegerEncoder can't handle "2.0"
        "{!payload_check f=vals_dpi payloads='1 2 3'}A B C",

        "{!payload_check f=vals_dpf payloads='1 2'}one two",
        "{!payload_check f=vals_dpf payloads='1 2.0'}one two", // shows that FloatEncoder can handle "1"

        "{!payload_check f=vals_dps payloads='NOUN VERB'}cat jumped"
    };

    String[] should_not_matches = new String[] {
        "{!payload_check f=vals_dpi v=A payloads=2}",
        "{!payload_check f=vals_dpi payloads='1 2'}B C",
        "{!payload_check f=vals_dpi payloads='1 2 3'}A B",
        "{!payload_check f=vals_dpi payloads='1 2'}A B C",
        "{!payload_check f=vals_dpf payloads='1 2.0'}two three",
        "{!payload_check f=vals_dps payloads='VERB NOUN'}cat jumped"
    };

    for(String should_match : should_matches) {
      assertQ(should_match, req("fl","*,score", "q", should_match), "//result[@numFound='1']");
    }

    for(String should_not_match : should_not_matches) {
      assertQ(should_not_match, req("fl","*,score", "q", should_not_match), "//result[@numFound='0']");
    }
  }
}
