package org.apache.solr.schema;

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
import org.junit.BeforeClass;

public class DateRangeFieldTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  public void test() {
    assertU(adoc("id", "0", "dateRange", "[* TO *]"));
    assertU(adoc("id", "1", "dateRange", "2014-05-21T12:00:00.000Z"));
    assertU(adoc("id", "2", "dateRange", "[2000 TO 2014-05-21]"));
    assertU(commit());

    //ensure stored value is the same (not toString of Shape)
    assertQ(req("q", "id:1", "fl", "dateRange"), "//result/doc/str[.='2014-05-21T12:00:00.000Z']");

    String[] commonParams = {"q", "{!field f=dateRange op=$op v=$qq}", "sort", "id asc"};
    assertQ(req(commonParams, "qq", "[* TO *]"), xpathMatches(0, 1, 2));
    assertQ(req(commonParams, "qq", "2012"), xpathMatches(0, 2));
    assertQ(req(commonParams, "qq", "2013", "op", "Contains"), xpathMatches(0, 2));
    assertQ(req(commonParams, "qq", "2014", "op", "Contains"), xpathMatches(0));
    assertQ(req(commonParams, "qq", "[1999 TO 2001]", "op", "IsWithin"), xpathMatches());
    assertQ(req(commonParams, "qq", "2014-05", "op", "IsWithin"), xpathMatches(1));

    //show without local-params
    assertQ(req("q", "dateRange:\"2014-05-21T12:00:00.000Z\""), xpathMatches(0, 1, 2));
    assertQ(req("q", "dateRange:[1999 TO 2001]"), xpathMatches(0, 2));
  }

  private String[] xpathMatches(int... docIds) {
    String[] tests = new String[docIds != null ? docIds.length + 1 : 1];
    tests[0] = "*[count(//doc)=" + (tests.length-1) + "]";
    if (docIds != null && docIds.length > 0) {
      int i = 1;
      for (int docId : docIds) {
        tests[i++] = "//result/doc/int[@name='id'][.='" + docId + "']";
      }
    }
    return tests;
  }

}
