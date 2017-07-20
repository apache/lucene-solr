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


/**
 * Test QueryComponent.doFieldSortValues
 */
public class TestFieldSortValues extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty("solr.tests.payload.fieldtype",
                       Boolean.getBoolean(NUMERIC_POINTS_SYSPROP) ?
                       "wrapped_point_int" : "wrapped_trie_int");
    initCore("solrconfig-minimal.xml", "schema-field-sort-values.xml");
  }
  
  public void testCustomComparator() throws Exception {
    clearIndex();
    assertU(adoc(sdoc("id", "1", "payload", "2")));
    assertU(adoc(sdoc("id", "2", "payload", "3")));
    assertU(adoc(sdoc("id", "3", "payload", "1")));
    assertU(adoc(sdoc("id", "4", "payload", "5")));
    assertU(adoc(sdoc("id", "5", "payload", "4")));
    assertU(commit());

    // payload is backed by a custom sort field which returns the payload value mod 3
    assertQ(req("q", "*:*", "fl", "id", "sort", "payload asc, id asc", "fsv", "true")
        , "//result/doc[str='2'  and position()=1]"
        , "//result/doc[str='3'  and position()=2]"
        , "//result/doc[str='5'  and position()=3]"
        , "//result/doc[str='1'  and position()=4]"
        , "//result/doc[str='4'  and position()=5]");
  }
}
