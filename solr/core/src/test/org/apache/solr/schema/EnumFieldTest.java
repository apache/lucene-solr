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

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.EnumFieldValue;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.SolrQueryParser;
import org.junit.BeforeClass;
import org.junit.Test;

public class EnumFieldTest extends SolrTestCaseJ4 {

  private final String FIELD_NAME = "severity";
  private final String MV_FIELD_NAME = "severity_mv";

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty("solr.tests.EnumFieldTest.indexed", Boolean.toString(random().nextBoolean()));
    doInitCore();

//    System.out.println("solr.tests.numeric.dv: " + System.getProperty("solr.tests.numeric.dv"));
//    System.out.println("solr.tests.EnumFieldTest.indexed: " + System.getProperty("solr.tests.EnumFieldTest.indexed"));
//    System.out.println("solr.tests.EnumFieldType: " + System.getProperty("solr.tests.EnumFieldType"));
  }
  
  private static void doInitCore() throws Exception {
    initCore("solrconfig-minimal.xml", "schema-enums.xml");
  }

  @Test
  public void testEnumSchema() throws Exception {
    assumeFalse("Skipping testing of EnumFieldType without docValues, which is unsupported.",
        System.getProperty("solr.tests.EnumFieldType").equals("solr.EnumFieldType")
            && System.getProperty("solr.tests.numeric.dv").equals("false"));

    IndexSchema schema = h.getCore().getLatestSchema();

    SchemaField enumField = schema.getField(FIELD_NAME);
    assertNotNull(enumField);
    SchemaField mvEnumField = schema.getField(MV_FIELD_NAME);
    assertNotNull(mvEnumField);
  }
  
  @Test
  public void testEnumRangeSearch() throws Exception {
    assumeFalse("Skipping testing of EnumFieldType without docValues, which is unsupported.",
        System.getProperty("solr.tests.EnumFieldType").equals("solr.EnumFieldType")
            && System.getProperty("solr.tests.numeric.dv").equals("false"));
    assumeFalse("Skipping testing of unindexed EnumField without docValues, which is unsupported.",
        System.getProperty("solr.tests.EnumFieldType").equals("solr.EnumField")
            && System.getProperty("solr.tests.EnumFieldTest.indexed").equals("false")
            && System.getProperty("solr.tests.numeric.dv").equals("false"));

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
    assertQ(req("fl", "" + FIELD_NAME, "q", FIELD_NAME + ":[\"Not Available\" TO \"Not Available\"]"),
            "//*[@numFound='5']");

    assertQ(req("fl", "" + FIELD_NAME, "q", FIELD_NAME + ":[\"Not Available\" TO Critical]"),
            "//*[@numFound='15']");

    assertQ(req("fl", "" + FIELD_NAME, "q", FIELD_NAME + ":[Low TO High]"),
            "//*[@numFound='9']");

    assertQ(req("fl", "" + FIELD_NAME, "q", FIELD_NAME + ":[High TO Low]"),
            "//*[@numFound='0']");

    //with int values
    assertQ(req("fl", "" + FIELD_NAME, "q", FIELD_NAME + ":[High TO 11]"),
            "//*[@numFound='3']");
    assertQ(req("fl", "" + FIELD_NAME, "q", FIELD_NAME + ":[3 TO Critical]"),
            "//*[@numFound='3']");
    assertQ(req("fl", "" + FIELD_NAME, "q", FIELD_NAME + ":[3 TO 11]"),
            "//*[@numFound='3']");

    //exclusive
    assertQ(req("fl", "" + FIELD_NAME, "q", FIELD_NAME + ":{Low TO High]"),
            "//*[@numFound='5']");
    assertQ(req("fl", "" + FIELD_NAME, "q", FIELD_NAME + ":[Low TO High}"),
            "//*[@numFound='7']");
    assertQ(req("fl", "" + FIELD_NAME, "q", FIELD_NAME + ":{Low TO High}"),
            "//*[@numFound='3']");

    //all docs
    assertQ(req("fl", "" + FIELD_NAME, "q", "*:*"),
        "//*[@numFound='17']");

    //all docs with values
    assertQ(req("fl", "" + FIELD_NAME, "q", FIELD_NAME + ":[* TO *]"),
            "//*[@numFound='15']");

    //empty docs
    assertQ(req("fl", "" + FIELD_NAME, "q", "-" + FIELD_NAME + ":[* TO *]"),
            "//*[@numFound='2']");
  }

  @Test
  public void testMultivaluedEnumRangeSearch() {
    assumeFalse("Skipping testing of EnumFieldType without docValues, which is unsupported.",
        System.getProperty("solr.tests.EnumFieldType").equals("solr.EnumFieldType")
            && System.getProperty("solr.tests.numeric.dv").equals("false"));
    assumeFalse("Skipping testing of range searching over multivalued EnumField - see SOLR-11193",
        System.getProperty("solr.tests.EnumFieldType").equals("solr.EnumField"));

    clearIndex();

    assertU(adoc("id", "0", MV_FIELD_NAME, "Not Available"));                // Single value
    assertU(adoc("id", "1", MV_FIELD_NAME, "Not Available", MV_FIELD_NAME, "Critical"));
    assertU(adoc("id", "2", MV_FIELD_NAME, "Not Available", MV_FIELD_NAME, "Critical"));
    assertU(adoc("id", "3"));                                                // No values
    assertU(adoc("id", "4"));                                                // No values
    assertU(adoc("id", "5", MV_FIELD_NAME, "Low", MV_FIELD_NAME, "Medium"));
    assertU(adoc("id", "6", MV_FIELD_NAME, "Low", MV_FIELD_NAME, "Medium"));
    assertU(adoc("id", "7", MV_FIELD_NAME, "Low", MV_FIELD_NAME, "Medium"));
    assertU(adoc("id", "8", MV_FIELD_NAME, "Low", MV_FIELD_NAME, "Critical"));
    assertU(adoc("id", "9", MV_FIELD_NAME, "Medium"));                       // Single value
    assertU(adoc("id", "10", MV_FIELD_NAME, "Medium", MV_FIELD_NAME, "High"));
    assertU(adoc("id", "11", MV_FIELD_NAME, "Medium", MV_FIELD_NAME, "High"));
    assertU(adoc("id", "12", MV_FIELD_NAME, "High", MV_FIELD_NAME, "Critical"));
    assertU(adoc("id", "13", MV_FIELD_NAME, "High", MV_FIELD_NAME, "High")); // Two of same value
    assertU(adoc("id", "14", MV_FIELD_NAME, "Critical", MV_FIELD_NAME, "Medium", MV_FIELD_NAME, "Not Available"));

    assertU(commit());
    
    //range with the same value
    assertQ(req("fl", "" + MV_FIELD_NAME, "q", MV_FIELD_NAME + ":[\"Not Available\" TO \"Not Available\"]"),
        "//*[@numFound='4']");

    assertQ(req("fl", "" + MV_FIELD_NAME, "q", MV_FIELD_NAME + ":[\"Not Available\" TO Critical]"),
        "//*[@numFound='13']");

    assertQ(req("fl", "" + MV_FIELD_NAME, "q", MV_FIELD_NAME + ":[Low TO High]"),
        "//*[@numFound='10']");

    assertQ(req("fl", "" + MV_FIELD_NAME, "q", MV_FIELD_NAME + ":[High TO Low]"),
        "//*[@numFound='0']");

    //with int values
    assertQ(req("fl", "" + MV_FIELD_NAME, "q", MV_FIELD_NAME + ":[High TO 11]"),
        "//*[@numFound='8']");
    assertQ(req("fl", "" + MV_FIELD_NAME, "q", MV_FIELD_NAME + ":[3 TO Critical]"),
        "//*[@numFound='8']");
    assertQ(req("fl", "" + MV_FIELD_NAME, "q", MV_FIELD_NAME + ":[3 TO 11]"),
        "//*[@numFound='8']");

    //exclusive
    assertQ(req("fl", "" + MV_FIELD_NAME, "q", MV_FIELD_NAME + ":{Low TO High]"),
        "//*[@numFound='9']");
    assertQ(req("fl", "" + MV_FIELD_NAME, "q", MV_FIELD_NAME + ":[Low TO High}"),
        "//*[@numFound='8']");
    assertQ(req("fl", "" + MV_FIELD_NAME, "q", MV_FIELD_NAME + ":{Low TO High}"),
        "//*[@numFound='7']");

    //all docs
    assertQ(req("fl", "" + MV_FIELD_NAME, "q", "*:*"),
        "//*[@numFound='15']");

    //all docs with values
    assertQ(req("fl", "" + MV_FIELD_NAME, "q", MV_FIELD_NAME + ":[* TO *]"),
        "//*[@numFound='13']");

    //empty docs
    assertQ(req("fl", "" + MV_FIELD_NAME, "q", "-" + MV_FIELD_NAME + ":[* TO *]"),
        "//*[@numFound='2']");
  }

  @Test
  public void testBogusEnumSearch() throws Exception {
    assumeFalse("Skipping testing of EnumFieldType without docValues, which is unsupported.",
        System.getProperty("solr.tests.EnumFieldType").equals("solr.EnumFieldType")
            && System.getProperty("solr.tests.numeric.dv").equals("false"));

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

    assertQ(req("fl", "" + FIELD_NAME, "q", FIELD_NAME + ":bla"),
            "//*[@numFound='0']");

    assertQ(req("fl", "" + FIELD_NAME, "q", FIELD_NAME + ":7"),
            "//*[@numFound='0']");

    assertQ(req("fl", "" + FIELD_NAME, "q", FIELD_NAME + ":\"-3\""),
            "//*[@numFound='0']");
  }

  @Test
  public void testMultivaluedBogusEnumSearch() throws Exception {
    assumeFalse("Skipping testing of EnumFieldType without docValues, which is unsupported.",
        System.getProperty("solr.tests.EnumFieldType").equals("solr.EnumFieldType")
            && System.getProperty("solr.tests.numeric.dv").equals("false"));

    clearIndex();

    assertU(adoc("id", "0", MV_FIELD_NAME, "Not Available", MV_FIELD_NAME, "Critical"));
    assertU(adoc("id", "1", MV_FIELD_NAME, "Low", MV_FIELD_NAME, "Not Available"));
    assertU(adoc("id", "2", MV_FIELD_NAME, "Medium", MV_FIELD_NAME, "Low"));
    assertU(adoc("id", "3", MV_FIELD_NAME, "High", MV_FIELD_NAME, "Medium"));
    assertU(adoc("id", "4", MV_FIELD_NAME, "Critical", MV_FIELD_NAME, "High"));

    // two docs w/o values
    for (int i = 8; i <= 9; i++) {
      assertU(adoc("id", "" + i));
    }

    assertU(commit());

    assertQ(req("fl", "" + MV_FIELD_NAME, "q", MV_FIELD_NAME + ":bla"),
        "//*[@numFound='0']");

    assertQ(req("fl", "" + MV_FIELD_NAME, "q", MV_FIELD_NAME + ":7"),
        "//*[@numFound='0']");

    assertQ(req("fl", "" + MV_FIELD_NAME, "q", MV_FIELD_NAME + ":\"-3\""),
        "//*[@numFound='0']");
  }

  @Test
  public void testBogusEnumIndexing() throws Exception {
    assumeFalse("Skipping testing of EnumFieldType without docValues, which is unsupported.",
        System.getProperty("solr.tests.EnumFieldType").equals("solr.EnumFieldType")
            && System.getProperty("solr.tests.numeric.dv").equals("false"));

    ignoreException("Unknown value for enum field: " + FIELD_NAME + ", value: blabla");
    ignoreException("Unknown value for enum field: " + FIELD_NAME + ", value: 145");
    ignoreException("Unknown value for enum field: " + FIELD_NAME + ", value: -4");

    clearIndex();

    assertFailedU(adoc("id", "0", FIELD_NAME, "blabla"));
    assertFailedU(adoc("id", "0", FIELD_NAME, "145"));
    assertFailedU(adoc("id", "0", FIELD_NAME, "-4"));
  }

  @Test
  public void testMultivaluedBogusEnumIndexing() throws Exception {
    assumeFalse("Skipping testing of EnumFieldType without docValues, which is unsupported.",
        System.getProperty("solr.tests.EnumFieldType").equals("solr.EnumFieldType")
            && System.getProperty("solr.tests.numeric.dv").equals("false"));

    ignoreException("Unknown value for enum field: " + MV_FIELD_NAME + ", value: blabla");
    ignoreException("Unknown value for enum field: " + MV_FIELD_NAME + ", value: 145");
    ignoreException("Unknown value for enum field: " + MV_FIELD_NAME + ", value: -4");

    clearIndex();

    assertFailedU(adoc("id", "0", MV_FIELD_NAME, "blabla", MV_FIELD_NAME, "High"));
    assertFailedU(adoc("id", "0", MV_FIELD_NAME, "145", MV_FIELD_NAME, "Low"));
    assertFailedU(adoc("id", "0", MV_FIELD_NAME, "-4", MV_FIELD_NAME, "Critical"));
  }

  @Test
  public void testKnownIntegerEnumIndexing() throws Exception {
    assumeFalse("Skipping testing of EnumFieldType without docValues, which is unsupported.",
        System.getProperty("solr.tests.EnumFieldType").equals("solr.EnumFieldType")
            && System.getProperty("solr.tests.numeric.dv").equals("false"));

    clearIndex();

    assertU(adoc("id", "0", FIELD_NAME, "1"));
    assertU(adoc("id", "1", FIELD_NAME, "11"));

    assertU(commit());

    assertQ(req("fl", "id," + FIELD_NAME, "q", "*:*"),
            "//doc[str[@name='id']/text()='0']/str[@name='" + FIELD_NAME + "']/text()='Low'",
            "//doc[str[@name='id']/text()='1']/str[@name='" + FIELD_NAME + "']/text()='Critical'");
  }

  @Test
  public void testMultivaluedKnownIntegerEnumIndexing() throws Exception {
    assumeFalse("Skipping testing of EnumFieldType without docValues, which is unsupported.",
        System.getProperty("solr.tests.EnumFieldType").equals("solr.EnumFieldType")
            && System.getProperty("solr.tests.numeric.dv").equals("false"));

    clearIndex();

    assertU(adoc("id", "0", MV_FIELD_NAME, "1", MV_FIELD_NAME, "2"));
    assertU(adoc("id", "1", MV_FIELD_NAME, "11", MV_FIELD_NAME, "3"));

    assertU(commit());

    assertQ(req("fl", "id," + MV_FIELD_NAME, "q", "*:*"),
        "//doc[str[@name='id']/text()='0']/arr[@name='" + MV_FIELD_NAME + "']/str/text()='Low'",
        "//doc[str[@name='id']/text()='0']/arr[@name='" + MV_FIELD_NAME + "']/str/text()='Medium'",
        "//doc[str[@name='id']/text()='1']/arr[@name='" + MV_FIELD_NAME + "']/str/text()='Critical'",
        "//doc[str[@name='id']/text()='1']/arr[@name='" + MV_FIELD_NAME + "']/str/text()='High'");
  }

  @Test
  public void testEnumSort() throws Exception {
    assumeFalse("Skipping testing of EnumFieldType without docValues, which is unsupported.",
        System.getProperty("solr.tests.EnumFieldType").equals("solr.EnumFieldType")
            && System.getProperty("solr.tests.numeric.dv").equals("false"));
    assumeFalse("Skipping testing of unindexed EnumField without docValues, which is unsupported.",
        System.getProperty("solr.tests.EnumFieldType").equals("solr.EnumField")
            && System.getProperty("solr.tests.EnumFieldTest.indexed").equals("false")
            && System.getProperty("solr.tests.numeric.dv").equals("false"));

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

    assertQ(req("fl", "id," + FIELD_NAME, "q", "*:*", "sort", FIELD_NAME + " desc", "indent","on"),
            "//doc[1]/str[@name='" + FIELD_NAME + "']/text()='Critical'",
            "//doc[2]/str[@name='" + FIELD_NAME + "']/text()='High'",
            "//doc[3]/str[@name='" + FIELD_NAME + "']/text()='Medium'",
            "//doc[4]/str[@name='" + FIELD_NAME + "']/text()='Low'",
            "//doc[5]/str[@name='" + FIELD_NAME + "']/text()='Not Available'");

    //sort ascending - empty values will be first
    assertQ(req("fl", "id," + FIELD_NAME, "q", "*:*", "sort", FIELD_NAME + " asc", "indent", "on"),
            "//doc[3]/str[@name='" + FIELD_NAME + "']/text()='Not Available'");

    //q for not empty docs
    assertQ(req("fl", "id," + FIELD_NAME, "q", FIELD_NAME + ":[* TO *]" , "sort", FIELD_NAME + " asc", "indent", "on"),
            "//doc[1]/str[@name='" + FIELD_NAME + "']/text()='Not Available'",
            "//doc[2]/str[@name='" + FIELD_NAME + "']/text()='Low'",
            "//doc[3]/str[@name='" + FIELD_NAME + "']/text()='Medium'",
            "//doc[4]/str[@name='" + FIELD_NAME + "']/text()='High'",
            "//doc[5]/str[@name='" + FIELD_NAME + "']/text()='Critical'"
    );

    // missing first....
    for (String dir : Arrays.asList("asc", "desc")) {
      assertQ(req("fl", "id", "q", "*:*", "sort", FIELD_NAME + "_missingFirst " + dir + ", id desc")
              , "//doc[1]/str[@name='id']/text()='9'"
              , "//doc[2]/str[@name='id']/text()='8'"
              );
    }
    // missing last...
    for (String dir : Arrays.asList("asc", "desc")) {
      assertQ(req("fl", "id", "q", "*:*", "sort", FIELD_NAME + "_missingLast " + dir + ", id desc")
              , "//doc[6]/str[@name='id']/text()='9'"
              , "//doc[7]/str[@name='id']/text()='8'"
              );
    }
  }

  @Test
  public void testMultivaluedEnumSort() throws Exception {
    assumeFalse("Skipping testing of EnumFieldType without docValues, which is unsupported.",
        System.getProperty("solr.tests.EnumFieldType").equals("solr.EnumFieldType")
            && System.getProperty("solr.tests.numeric.dv").equals("false"));
    assumeFalse("Skipping testing of sorting over multivalued EnumField - see SOLR-11193",
        System.getProperty("solr.tests.EnumFieldType").equals("solr.EnumField"));

    clearIndex();
    assertU(adoc("id", "0", MV_FIELD_NAME, "Not Available"));                // Single value
    assertU(adoc("id", "1", MV_FIELD_NAME, "Not Available", MV_FIELD_NAME, "Critical"));
    assertU(adoc("id", "2", MV_FIELD_NAME, "Not Available", MV_FIELD_NAME, "Critical"));
    assertU(adoc("id", "3"));                                                // No values
    assertU(adoc("id", "4"));                                                // No values
    assertU(adoc("id", "5", MV_FIELD_NAME, "Low", MV_FIELD_NAME, "Medium"));
    assertU(adoc("id", "6", MV_FIELD_NAME, "Low", MV_FIELD_NAME, "Medium"));
    assertU(adoc("id", "7", MV_FIELD_NAME, "Low", MV_FIELD_NAME, "Medium"));
    assertU(adoc("id", "8", MV_FIELD_NAME, "Low", MV_FIELD_NAME, "Critical"));
    assertU(adoc("id", "9", MV_FIELD_NAME, "Medium"));                       // Single value
    assertU(adoc("id", "10", MV_FIELD_NAME, "Medium", MV_FIELD_NAME, "High"));
    assertU(adoc("id", "11", MV_FIELD_NAME, "Medium", MV_FIELD_NAME, "High"));
    assertU(adoc("id", "12", MV_FIELD_NAME, "High", MV_FIELD_NAME, "Critical"));
    assertU(adoc("id", "13", MV_FIELD_NAME, "High", MV_FIELD_NAME, "High")); // Two of same value
    assertU(adoc("id", "14", MV_FIELD_NAME, "Critical", MV_FIELD_NAME, "Medium", MV_FIELD_NAME, "Not Available"));

    assertU(commit());

    assertQ(req("fl", "id," + MV_FIELD_NAME, "q", "*:*", "sort", "field(" + MV_FIELD_NAME + ",min) desc", "indent","on"),
        "//doc[1]/arr[@name='" + MV_FIELD_NAME + "']/str/text()='Critical'",
        "//doc[2]/arr[@name='" + MV_FIELD_NAME + "']/str/text()='High'",
        "//doc[3]/arr[@name='" + MV_FIELD_NAME + "']/str/text()='Medium'",
        "//doc[6]/arr[@name='" + MV_FIELD_NAME + "']/str/text()='Low'",
        "//doc[10]/arr[@name='" + MV_FIELD_NAME + "']/str/text()='Not Available'");

    //sort ascending - empty values will be first
    assertQ(req("fl", "id," + MV_FIELD_NAME, "q", "*:*", "sort", "field(" + MV_FIELD_NAME + ",min) asc", "indent", "on"),
        "//doc[3]/arr[@name='" + MV_FIELD_NAME + "']/str/text()='Not Available'");

    //q for not empty docs
    assertQ(req("fl", "id," + MV_FIELD_NAME, 
        "q", MV_FIELD_NAME + ":[* TO *]" , "sort", "field(" + MV_FIELD_NAME + ",'min') asc", 
        "rows","15", "indent","on"),
        "//doc[1]/arr[@name='" + MV_FIELD_NAME + "']/str/text()='Not Available'",
        "//doc[5]/arr[@name='" + MV_FIELD_NAME + "']/str/text()='Low'",
        "//doc[9]/arr[@name='" + MV_FIELD_NAME + "']/str/text()='Medium'",
        "//doc[10]/arr[@name='" + MV_FIELD_NAME + "']/str/text()='High'",
        "//doc[12]/arr[@name='" + MV_FIELD_NAME + "']/str/text()='Critical'");
  }
  
  @Test
  public void testSetQuery() throws Exception {
    assumeFalse("Skipping testing of EnumFieldType without docValues, which is unsupported.",
        System.getProperty("solr.tests.EnumFieldType").equals("solr.EnumFieldType")
            && System.getProperty("solr.tests.numeric.dv").equals("false"));
    assumeFalse("Skipping testing of unindexed EnumField without docValues, which is unsupported.",
        System.getProperty("solr.tests.EnumFieldType").equals("solr.EnumField")
            && System.getProperty("solr.tests.EnumFieldTest.indexed").equals("false")
            && System.getProperty("solr.tests.numeric.dv").equals("false"));

    clearIndex();

    SchemaField sf = h.getCore().getLatestSchema().getField(FIELD_NAME);
    Set<String> enumStrs = ((AbstractEnumField)sf.getType()).getEnumMapping().enumStringToIntMap.keySet();
    assertTrue(enumStrs.size() > SolrQueryParser.TERMS_QUERY_THRESHOLD);

    Iterator<String> enumStrIter = enumStrs.iterator();
    for (int i = 0 ; enumStrIter.hasNext() ; ++i) {
      assertU(adoc("id", "" + i, FIELD_NAME, enumStrIter.next()));
    }
    assertU(commit());
    
    StringBuilder builder = new StringBuilder(FIELD_NAME + ":(");
    enumStrs.forEach(v -> builder.append(v.replace(" ", "\\ ")).append(' '));
    builder.append(')');
      
    if (sf.indexed()) { // SolrQueryParser should also be generating a TermInSetQuery if indexed
      String setQuery = sf.getType().getSetQuery(null, sf, enumStrs).toString();
      if (sf.getType() instanceof EnumField) { // Trie field TermInSetQuery non-XML chars serialize with "#XX;" syntax
        Pattern nonXMLCharPattern = Pattern.compile("[\u0000-\u0008\u000B\u000C\u000E-\u0019]");
        StringBuffer munged = new StringBuffer();
        Matcher matcher = nonXMLCharPattern.matcher(setQuery);
        while (matcher.find()) {
          matcher.appendReplacement(munged, "#" + (int)matcher.group(0).charAt(0) + ";");
        }
        matcher.appendTail(munged);
        setQuery = munged.toString();
      }
      assertQ(req(CommonParams.DEBUG, CommonParams.QUERY, "q", "*:*", "fq", builder.toString(),
          "fl", "id," + FIELD_NAME, "rows", "" + enumStrs.size(), "indent", "on"),
          "//*[@numFound='" + enumStrs.size() + "']",
          "//*[@name='parsed_filter_queries']/str[normalize-space(.)=normalize-space('TermInSetQuery(" + setQuery + ")')]");  // fix [\r\n] problems
    } else {
      // Won't use TermInSetQuery if the field is not indexed, but should match the same docs
      assertQ(req(CommonParams.DEBUG, CommonParams.QUERY, "q", "*:*", "fq", builder.toString(), "fl", "id," + FIELD_NAME, "indent", "on"),
          "//*[@numFound='" + enumStrs.size() + "']");
    }
  }

  @Test
  public void testMultivaluedSetQuery() throws Exception {
    assumeFalse("Skipping testing of EnumFieldType without docValues, which is unsupported.",
        System.getProperty("solr.tests.EnumFieldType").equals("solr.EnumFieldType")
            && System.getProperty("solr.tests.numeric.dv").equals("false"));
    assumeFalse("Skipping testing of unindexed EnumField without docValues, which is unsupported.",
        System.getProperty("solr.tests.EnumFieldType").equals("solr.EnumField")
            && System.getProperty("solr.tests.EnumFieldTest.indexed").equals("false")
            && System.getProperty("solr.tests.numeric.dv").equals("false"));

    clearIndex();

    SchemaField sf = h.getCore().getLatestSchema().getField(MV_FIELD_NAME);
    Set<String> enumStrs = ((AbstractEnumField)sf.getType()).getEnumMapping().enumStringToIntMap.keySet();
    assertTrue(enumStrs.size() > SolrQueryParser.TERMS_QUERY_THRESHOLD);

    Iterator<String> enumStrIter = enumStrs.iterator();
    String prevEnumStr = "x18"; // wrap around
    for (int i = 0 ; enumStrIter.hasNext() ; ++i) {
      String thisEnumStr = enumStrIter.next();
      assertU(adoc("id", "" + i, MV_FIELD_NAME, thisEnumStr, MV_FIELD_NAME, prevEnumStr));
      prevEnumStr = thisEnumStr; 
    }
    assertU(commit());

    StringBuilder builder = new StringBuilder(MV_FIELD_NAME + ":(");
    enumStrs.forEach(v -> builder.append(v.replace(" ", "\\ ")).append(' '));
    builder.append(')');

    if (sf.indexed()) { // SolrQueryParser should also be generating a TermInSetQuery if indexed
      String setQuery = sf.getType().getSetQuery(null, sf, enumStrs).toString();
      if (sf.getType() instanceof EnumField) { // Trie field TermInSetQuery non-XML chars serialize with "#XX;" syntax
        Pattern nonXMLCharPattern = Pattern.compile("[\u0000-\u0008\u000B\u000C\u000E-\u0019]");
        StringBuffer munged = new StringBuffer();
        Matcher matcher = nonXMLCharPattern.matcher(setQuery);
        while (matcher.find()) {
          matcher.appendReplacement(munged, "#" + (int)matcher.group(0).charAt(0) + ";");
        }
        matcher.appendTail(munged);
        setQuery = munged.toString();
      }
      assertQ(req(CommonParams.DEBUG, CommonParams.QUERY, "q", "*:*", "fq", builder.toString(),
          "fl", "id," + MV_FIELD_NAME, "rows", "" + enumStrs.size(), "indent", "on"),
          "//*[@numFound='" + enumStrs.size() + "']",
          "//*[@name='parsed_filter_queries']/str[normalize-space(.)=normalize-space('TermInSetQuery(" + setQuery + ")')]");  // fix [\r\n] problems
    } else {
      // Won't use TermInSetQuery if the field is not indexed, but should match the same docs
      assertQ(req(CommonParams.DEBUG, CommonParams.QUERY, "q", "*:*", "fq", builder.toString(), "fl", "id," + MV_FIELD_NAME, "indent", "on"),
          "//*[@numFound='" + enumStrs.size() + "']");
    }
  }

  @Test
  public void testEnumFieldTypeWithoutDocValues() throws Exception {
    assumeTrue("Only testing EnumFieldType without docValues.",
        System.getProperty("solr.tests.EnumFieldType").equals("solr.EnumFieldType")
            && System.getProperty("solr.tests.numeric.dv").equals("false"));

    try {
      deleteCore();
      initCore("solrconfig-minimal.xml", "bad-schema-enums.xml");
      SolrException e = expectThrows(SolrException.class, 
          () -> assertU(adoc("id", "0", FIELD_NAME, "Not Available")));
      assertTrue(e.getMessage().contains("EnumFieldType requires docValues=\"true\""));
    } finally { // put back the core expected by other tests
      deleteCore();
      doInitCore();
    }
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void testFacetEnumSearch() throws Exception {
    assumeFalse("This requires docValues",
            System.getProperty("solr.tests.numeric.dv").equals("false"));

    clearIndex();

    assertU(adoc("id", "0", FIELD_NAME, "Not Available"));
    assertU(adoc("id", "1", FIELD_NAME, "Low"));
    assertU(adoc("id", "2", FIELD_NAME, "Medium"));
    assertU(adoc("id", "3", FIELD_NAME, "High"));
    assertU(adoc("id", "4", FIELD_NAME, "Critical"));
    assertU(adoc("id", "5", FIELD_NAME, "Critical"));

    assertU(commit());
    
    final String jsonFacetParam = "{ " + FIELD_NAME + " : { type : terms, field : " + FIELD_NAME + ", "+
        "missing : true, exists : true, allBuckets : true, method : enum }}";

    assertQ(req("fl", "" + FIELD_NAME, "q", FIELD_NAME + ":*", "json.facet", jsonFacetParam),
        "//*[@name='facets']/int/text()=6",
        "//*[@name='allBuckets']/long/text()=6",
        "//*[@name='buckets']/lst[int[@name='count'][.='2']][str[@name='val'][.='Critical']]",
        "//*[@name='buckets']/lst[int[@name='count'][.='1']][str[@name='val'][.='Not Available']]",
        "//*[@name='buckets']/lst[int[@name='count'][.='1']][str[@name='val'][.='Low']]",
        "//*[@name='buckets']/lst[int[@name='count'][.='1']][str[@name='val'][.='Medium']]",
        "//*[@name='buckets']/lst[int[@name='count'][.='1']][str[@name='val'][.='High']]");
    
    try (SolrQueryRequest req = req("fl", "" + FIELD_NAME, "q", FIELD_NAME + ":*", "json.facet", jsonFacetParam, "wt", "json")) {
      SolrQueryResponse rsp = h.queryAndResponse(req.getParams().get(CommonParams.QT),req);
      List<NamedList<?>> buckets = (List<NamedList<?>>) ((NamedList<?>)((NamedList<?>)rsp.getValues().get("facets")).get("severity")).get("buckets");
      for (NamedList<?> bucket : buckets) {
        assertTrue("Bucket value must be instance of EnumFieldVale!", bucket.get("val") instanceof EnumFieldValue);
      }
    }
  }
}
