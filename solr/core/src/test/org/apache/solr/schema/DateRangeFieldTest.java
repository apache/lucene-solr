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
package org.apache.solr.schema;

import org.apache.solr.SolrTestCaseJ4;
import org.junit.BeforeClass;
import org.junit.Test;

public class DateRangeFieldTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  public void test() {
    assertU(delQ("*:*"));
    assertU(adoc("id", "0", "dateRange", "[* TO *]"));
    assertU(adoc("id", "1", "dateRange", "2014-05-21T12:00:00.000Z"));
    assertU(adoc("id", "2", "dateRange", "[2000 TO 2014-05-21]"));
    assertU(adoc("id", "3", "dateRange", "2020-05-21T12:00:00.000Z/DAY"));//DateMath syntax
    assertU(commit());

    //ensure stored value resolves datemath
    assertQ(req("q", "id:1", "fl", "dateRange"), "//result/doc/arr[@name='dateRange']/str[.='2014-05-21T12:00:00Z']");//no 000 ms
    assertQ(req("q", "id:2", "fl", "dateRange"), "//result/doc/arr[@name='dateRange']/str[.='[2000 TO 2014-05-21]']");//a range; same
    assertQ(req("q", "id:3", "fl", "dateRange"), "//result/doc/arr[@name='dateRange']/str[.='2020-05-21T00:00:00Z']");//resolve datemath

    String[] commonParams = {"q", "{!field f=dateRange op=$op v=$qq}", "sort", "id asc"};
    assertQ(req(commonParams, "qq", "[* TO *]"), xpathMatches(0, 1, 2, 3));
    assertQ(req(commonParams, "qq", "2012"), xpathMatches(0, 2));
    assertQ(req(commonParams, "qq", "2013", "op", "Contains"), xpathMatches(0, 2));
    assertQ(req(commonParams, "qq", "2014", "op", "Contains"), xpathMatches(0));
    assertQ(req(commonParams, "qq", "[1999 TO 2001]", "op", "IsWithin"), xpathMatches());
    assertQ(req(commonParams, "qq", "2014-05", "op", "IsWithin"), xpathMatches(1));

    assertQ(req("q", "dateRange:[1998 TO 2000}"), xpathMatches(0));//exclusive end, so we barely miss one doc

    //show without local-params
    assertQ(req("q", "dateRange:\"2014-05-21T12:00:00.000Z\""), xpathMatches(0, 1, 2));
    assertQ(req("q", "dateRange:[1999 TO 2001]"), xpathMatches(0, 2));
  }

  @Test
  public void testMultiValuedDateRanges() {
    assertU(delQ("*:*"));
    assertU(adoc("id", "0", "dateRange", "[2000 TO 2010]", "dateRange", "[2011 TO 2014]"));
    assertU(adoc("id", "1", "dateRange", "[2000-01 TO 2010-10]", "dateRange", "[2010-11 TO 2014-12]"));
    assertU(adoc("id", "2", "dateRange", "[2000-01-01 TO 2010-08-01]", "dateRange", "[2010-08-01 TO 2014-12-01]"));
    assertU(adoc("id", "3", "dateRange", "[1990 TO 1995]", "dateRange", "[1997 TO 1999]"));
    assertU(commit());

    String[] commonParams = {"q", "{!field f=dateRange op=$op v=$qq}", "sort", "id asc"};

    assertQ(req(commonParams, "qq", "[2000 TO 2014]", "op", "IsWithin"), xpathMatches(0, 1, 2));
    assertQ(req(commonParams, "qq", "[2000 TO 2013]", "op", "IsWithin"), xpathMatches());
    assertQ(req(commonParams, "qq", "[2000 TO 2014]", "op", "Contains"), xpathMatches(0, 1));
    assertQ(req(commonParams, "qq", "[2000 TO 2015]", "op", "Contains"), xpathMatches());

    assertQ(req(commonParams, "qq", "[2000-01 TO 2014-12]", "op", "IsWithin"), xpathMatches(0, 1, 2));
    assertQ(req(commonParams, "qq", "[2000 TO 2014-11]", "op", "IsWithin"), xpathMatches());
    assertQ(req(commonParams, "qq", "[2000-01 TO 2014-12]", "op", "Contains"), xpathMatches(0, 1));

    assertQ(req(commonParams, "qq", "[2000-01-01 TO 2014-12-31]", "op", "IsWithin"), xpathMatches(0, 1, 2));
    assertQ(req(commonParams, "qq", "[2000-01-01 TO 2014-12-01]", "op", "Contains"), xpathMatches(0, 1, 2));
    assertQ(req(commonParams, "qq", "[2000 TO 2000]", "op", "Contains"), xpathMatches(0, 1, 2));

    assertQ(req(commonParams, "qq", "[2000 TO 2000]", "op", "Contains"), xpathMatches(0, 1, 2));

    assertQ(req(commonParams, "qq", "[1996-01-01 TO 1996-12-31]", "op", "Contains"), xpathMatches());
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
