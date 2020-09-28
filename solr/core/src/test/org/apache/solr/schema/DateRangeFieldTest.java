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
    assertQ(req("q", "dateRange:[* TO *]"), xpathMatches(0, 1, 2, 3));
    assertQ(req("q", "dateRange:*"), xpathMatches(0, 1, 2, 3));
    assertQ(req("q", "dateRange:\"2014-05-21T12:00:00.000Z\""), xpathMatches(0, 1, 2));
    assertQ(req("q", "dateRange:[1999 TO 2001]"), xpathMatches(0, 2));
  }

  public void testBeforeGregorianChangeDate() { // GCD is the year 1582
    assertU(delQ("*:*"));
    assertU(adoc("id", "0", "dateRange", "1500-01-01T00:00:00Z"));
    assertU(adoc("id", "1", "dateRange", "-1500-01-01T00:00:00Z")); // BC
    assertU(adoc("id", "2", "dateRange", "1400-01-01T00:00:00Z/YEAR")); // date math of month or year can cause issues
    assertU(adoc("id", "3", "dateRange", "1300")); // the whole year of 1300
    assertU(commit());

    //ensure round-trip toString
    assertQ(req("q", "id:0", "fl", "dateRange"), "//result/doc/arr[@name='dateRange']/str[.='1500-01-01T00:00:00Z']");
    assertQ(req("q", "id:1", "fl", "dateRange"), "//result/doc/arr[@name='dateRange']/str[.='-1500-01-01T00:00:00Z']");
    //    note: fixed by SOLR-9080, would instead find "1399-01-09T00:00:00Z"
    assertQ(req("q", "id:2", "fl", "dateRange"), "//result/doc/arr[@name='dateRange']/str[.='1400-01-01T00:00:00Z']");
    assertQ(req("q", "id:3", "fl", "dateRange"), "//result/doc/arr[@name='dateRange']/str[.='1300']");

    //ensure range syntax works
    assertQ(req("q", "dateRange:[1450-01-01T00:00:00Z TO 1499-12-31T23:59:59Z]"), xpathMatches());// before
    assertQ(req("q", "dateRange:[1500-01-01T00:00:00Z TO 1500-01-01T00:00:00Z]"), xpathMatches(0));// spot on
    assertQ(req("q", "dateRange:[1500-01-01T00:00:01Z TO 1550-01-01T00:00:00Z]"), xpathMatches());// after

    assertQ(req("q", "dateRange:[-1500-01-01T00:00:00Z TO -1500-01-01T00:00:00Z]"), xpathMatches(1));

    // do range queries in the vicinity of docId=3 val:"1300"
    assertQ(req("q", "dateRange:[1299 TO 1299-12-31T23:59:59Z]"), xpathMatches());//adjacent
    assertQ(req("q", "dateRange:[1299 TO 1300-01-01T00:00:00Z]"), xpathMatches(3));// expand + 1 sec
    assertQ(req("q", "dateRange:1301"), xpathMatches()); // adjacent
    assertQ(req("q", "dateRange:[1300-12-31T23:59:59Z TO 1301]"), xpathMatches(3)); // expand + 1 sec
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
        tests[i++] = "//result/doc/str[@name='id'][.='" + docId + "']";
      }
    }
    return tests;
  }

}
