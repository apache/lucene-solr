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
import org.apache.solr.request.SolrQueryRequest;
import org.junit.BeforeClass;
import org.junit.Test;

public class EnumFieldTest extends SolrTestCaseJ4 {

  private final static String FIELD_NAME = "severity";

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-minimal.xml", "schema-enums.xml");
  }

  @Test
  public void testEnumSchema() throws Exception {
    IndexSchema schema = h.getCore().getLatestSchema();

    SchemaField enumField = schema.getField(FIELD_NAME);
    assertNotNull(enumField);
  }

  @Test
  public void testEnumRangeSearch() throws Exception {
    clearIndex();

    assertU(adoc("id", "0", FIELD_NAME, "Not Available"));
    assertU(adoc("id", "1", FIELD_NAME, "Not Available"));
    assertU(adoc("id", "2", FIELD_NAME, "Not Available"));
    assertU(adoc("id", "3", FIELD_NAME, "Not Available"));
    assertU(adoc("id", "4", FIELD_NAME, "Not Available"));
    assertU(adoc("id", "5", FIELD_NAME, "Low"));
    assertU(adoc("id", "6", FIELD_NAME, "Low"));
    assertU(adoc("id", "7", FIELD_NAME, "Low"));
    assertU(adoc("id", "8", FIELD_NAME, "Low"));
    assertU(adoc("id", "9", FIELD_NAME, "Medium"));
    assertU(adoc("id", "10", FIELD_NAME, "Medium"));
    assertU(adoc("id", "11", FIELD_NAME, "Medium"));
    assertU(adoc("id", "12", FIELD_NAME, "High"));
    assertU(adoc("id", "13", FIELD_NAME, "High"));
    assertU(adoc("id", "14", FIELD_NAME, "Critical"));

    // two docs w/o values
    for (int i = 20; i <= 21; i++) {
      assertU(adoc("id", "" + i));
    }

    assertU(commit());

    //range with the same value
    assertQ(req("fl", "" + FIELD_NAME, "q",
            FIELD_NAME + ":[\"Not Available\" TO \"Not Available\"]"),
            "//*[@numFound='5']");

    assertQ(req("fl", "" + FIELD_NAME, "q",
            FIELD_NAME + ":[\"Not Available\" TO Critical]"),
            "//*[@numFound='15']");

    assertQ(req("fl", "" + FIELD_NAME, "q",
            FIELD_NAME + ":[Low TO High]"),
            "//*[@numFound='9']");

    assertQ(req("fl", "" + FIELD_NAME, "q",
            FIELD_NAME + ":[High TO Low]"),
            "//*[@numFound='0']");

    //with int values
    assertQ(req("fl", "" + FIELD_NAME, "q",
            FIELD_NAME + ":[High TO 4]"),
            "//*[@numFound='3']");
    assertQ(req("fl", "" + FIELD_NAME, "q",
            FIELD_NAME + ":[3 TO Critical]"),
            "//*[@numFound='3']");
    assertQ(req("fl", "" + FIELD_NAME, "q",
            FIELD_NAME + ":[3 TO 4]"),
            "//*[@numFound='3']");

    //exclusive
    assertQ(req("fl", "" + FIELD_NAME, "q",
            FIELD_NAME + ":{Low TO High]"),
            "//*[@numFound='5']");
    assertQ(req("fl", "" + FIELD_NAME, "q",
            FIELD_NAME + ":[Low TO High}"),
            "//*[@numFound='7']");
    assertQ(req("fl", "" + FIELD_NAME, "q",
            FIELD_NAME + ":{Low TO High}"),
            "//*[@numFound='3']");

    //all docs
    assertQ(req("fl", "" + FIELD_NAME, "q",
            "*:*"),
            "//*[@numFound='17']");

    //all docs with values
    assertQ(req("fl", "" + FIELD_NAME, "q",
            FIELD_NAME + ":[* TO *]"),
            "//*[@numFound='15']");

    //empty docs
    assertQ(req("fl", "" + FIELD_NAME, "q",
            "-" + FIELD_NAME + ":[* TO *]"),
            "//*[@numFound='2']");
  }

  @Test
  public void testBogusEnumSearch() throws Exception {
    clearIndex();

    assertU(adoc("id", "0", FIELD_NAME, "Not Available"));
    assertU(adoc("id", "1", FIELD_NAME, "Low"));
    assertU(adoc("id", "2", FIELD_NAME, "Medium"));
    assertU(adoc("id", "3", FIELD_NAME, "High"));
    assertU(adoc("id", "4", FIELD_NAME, "Critical"));

    // two docs w/o values
    for (int i = 8; i <= 9; i++) {
      assertU(adoc("id", "" + i));
    }

    assertU(commit());

    SolrQueryRequest eoe = req("fl", "" + FIELD_NAME, "q",
        FIELD_NAME + ":bla");
    String eoe1 = eoe.toString();

    assertQ(req("fl", "" + FIELD_NAME, "q",
            FIELD_NAME + ":bla"),
            "//*[@numFound='0']");

    assertQ(req("fl", "" + FIELD_NAME, "q",
            FIELD_NAME + ":7"),
            "//*[@numFound='0']");

    assertQ(req("fl", "" + FIELD_NAME, "q",
            FIELD_NAME + ":\"-3\""),
            "//*[@numFound='0']");
  }

  @Test
  public void testBogusEnumIndexing() throws Exception {

    ignoreException("Unknown value for enum field: blabla");
    ignoreException("Unknown value for enum field: 10");
    ignoreException("Unknown value for enum field: -4");

    clearIndex();

    assertFailedU(adoc("id", "0", FIELD_NAME, "blabla"));
    assertFailedU(adoc("id", "0", FIELD_NAME, "10"));
    assertFailedU(adoc("id", "0", FIELD_NAME, "-4"));

  }

  @Test
  public void testKnownIntegerEnumIndexing() throws Exception {
    clearIndex();

    assertU(adoc("id", "0", FIELD_NAME, "1"));

    assertU(commit());

    assertQ(req("fl", "" + FIELD_NAME, "q", "*:*"), "//doc[1]/str[@name='severity']/text()='Low'");
  }

  @Test
  public void testEnumSort() throws Exception {
    clearIndex();

    assertU(adoc("id", "0", FIELD_NAME, "Not Available"));
    assertU(adoc("id", "1", FIELD_NAME, "Low"));
    assertU(adoc("id", "2", FIELD_NAME, "Medium"));
    assertU(adoc("id", "3", FIELD_NAME, "High"));
    assertU(adoc("id", "4", FIELD_NAME, "Critical"));

    // two docs w/o values
    for (int i = 8; i <= 9; i++) {
      assertU(adoc("id", "" + i));
    }

    assertU(commit());

    assertQ(req("fl", "" + FIELD_NAME, "q", "*:*", "sort", FIELD_NAME + " desc"), "//doc[1]/str[@name='severity']/text()='Critical'",
            "//doc[2]/str[@name='severity']/text()='High'", "//doc[3]/str[@name='severity']/text()='Medium'", "//doc[4]/str[@name='severity']/text()='Low'",
            "//doc[5]/str[@name='severity']/text()='Not Available'");

    //sort ascending - empty values will be first
    assertQ(req("fl", "" + FIELD_NAME, "q", "*:*", "sort", FIELD_NAME + " asc"), "//doc[3]/str[@name='severity']/text()='Not Available'");

    //q for not empty docs
    assertQ(req("fl", "" + FIELD_NAME, "q", FIELD_NAME + ":[* TO *]" , "sort", FIELD_NAME + " asc"), "//doc[1]/str[@name='severity']/text()='Not Available'",
            "//doc[2]/str[@name='severity']/text()='Low'", "//doc[3]/str[@name='severity']/text()='Medium'", "//doc[4]/str[@name='severity']/text()='High'",
            "//doc[5]/str[@name='severity']/text()='Critical'"
    );
  }

}

