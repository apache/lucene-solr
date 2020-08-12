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
package org.apache.solr.handler.export;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.StreamParams;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.common.util.Utils;
import org.apache.solr.index.LogDocMergePolicyFactory;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.SchemaField;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestExportWriter extends SolrTestCaseJ4 {

  private ObjectMapper mapper = new ObjectMapper();
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    // force LogDocMergePolicy so that we get a predictable doc order
    // when testing index order results
    systemSetPropertySolrTestsMergePolicyFactory(LogDocMergePolicyFactory.class.getName());
    initCore("solrconfig-sortingresponse.xml","schema-sortingresponse.xml");
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    assertU(delQ("*:*"));
    assertU(commit());

  }

  @Test
  public void testEmptyValues() throws Exception {
    //Index 2 document with one document that doesn't have field2_i_p
    //Sort and return field2_i_p
    //Test SOLR-12572 for potential NPEs
    assertU(delQ("*:*"));
    assertU(commit());


    assertU(adoc("id","1", "field2_i_p","1"));
    assertU(adoc("id","2"));
    assertU(commit());

    String resp = h.query(req("q", "*:*", "qt", "/export", "fl", "id,field2_i_p", "sort", "field2_i_p asc"));
    assertJsonEquals(resp, "{\n" +
        "  \"responseHeader\":{\"status\":0},\n" +
        "  \"response\":{\n" +
        "    \"numFound\":2,\n" +
        "    \"docs\":[{\n" +
        "        \"id\":\"2\"}\n" +
        "      ,{\n" +
        "        \"id\":\"1\",\n" +
        "        \"field2_i_p\":1}]}}");

  }

  @Test
  public void testSortingOnFieldWithNoValues() throws Exception {
    assertU(delQ("*:*"));
    assertU(commit());

    assertU(adoc("id","1"));
    assertU(commit());

    // 10 fields
    List<String> fieldNames = new ArrayList<>(Arrays.asList("floatdv", "intdv", "stringdv", "longdv", "doubledv",
        "datedv", "booleandv", "field1_s_dv", "field2_i_p", "field3_l_p"));
    for (String sortField : fieldNames) {
      String resp = h.query(req("q", "*:*", "qt", "/export", "fl", "id," + sortField, "sort", sortField + " desc"));
      assertJsonEquals(resp, "{\n" +
          "  \"responseHeader\":{\"status\":0},\n" +
          "  \"response\":{\n" +
          "    \"numFound\":1,\n" +
          "    \"docs\":[{\n" +
          "        \"id\":\"1\"}]}}");
    }

  }

  public static void createIndex() {
    assertU(adoc("id","1",
                 "floatdv","2.1",
                 "intdv", "1",
                 "stringdv", "hello world",
                 "longdv", "323223232323",
                 "doubledv","2344.345",
                 "intdv_m","100",
                 "intdv_m","250",
                 "floatdv_m", "123.321",
                 "floatdv_m", "345.123",
                 "doubledv_m", "3444.222",
                 "doubledv_m", "23232.2",
                 "longdv_m", "43434343434",
                 "longdv_m", "343332",
                 "stringdv_m", "manchester \"city\"",
                 "stringdv_m", "liverpool",
                 "stringdv_m", "Everton",
                 "datedv", "2017-06-16T07:00:00Z",
                 "datedv_m", "2017-06-16T01:00:00Z",
                 "datedv_m", "2017-06-16T02:00:00Z",
                 "datedv_m", "2017-06-16T03:00:00Z",
                 "datedv_m", "2017-06-16T04:00:00Z",
                 "sortabledv_m", "this is some text one_1",
                 "sortabledv_m", "this is some text two_1",
                 "sortabledv_m", "this is some text three_1",
                 "sortabledv_m_udvas", "this is some text one_1",
                 "sortabledv_m_udvas", "this is some text two_1",
                 "sortabledv_m_udvas", "this is some text three_1"));

    assertU(adoc("id","7",
        "floatdv","2.1",
        "intdv", "7",
        "longdv", "323223232323",
        "doubledv","2344.345",
        "floatdv_m", "123.321",
        "floatdv_m", "345.123",
        "doubledv_m", "3444.222",
        "doubledv_m", "23232.2",
        "longdv_m", "43434343434",
        "longdv_m", "343332"));

    assertU(commit());
    assertU(adoc("id","2", "floatdv","2.1", "intdv", "2", "stringdv", "hello world", "longdv", "323223232323","doubledv","2344.344"));
    assertU(commit());
    assertU(adoc("id","3",
        "floatdv","2.1",
        "intdv", "3",
        "stringdv", "chello world",
        "longdv", "323223232323",
        "doubledv","2344.346",
        "intdv_m","100",
        "intdv_m","250",
        "floatdv_m", "123.321",
        "floatdv_m", "345.123",
        "doubledv_m", "3444.222",
        "doubledv_m", "23232.2",
        "longdv_m", "43434343434",
        "longdv_m", "343332",
        "stringdv_m", "manchester \"city\"",
        "stringdv_m", "liverpool",
        "stringdv_m", "everton",
        "int_is_t", "1",
        "int_is_t", "1",
        "int_is_t", "1",
        "int_is_t", "1",
        "sortabledv", "this is some text_1",
        "sortabledv_udvas", "this is some text_1"));
    assertU(commit());
    assertU(adoc("id","8",
        "floatdv","2.1",
        "intdv", "10000000",
        "stringdv", "chello \"world\"",
        "longdv", "323223232323",
        "doubledv","2344.346",
        "intdv_m","100",
        "intdv_m","250",
        "floatdv_m", "123.321",
        "floatdv_m", "345.123",
        "doubledv_m", "3444.222",
        "doubledv_m", "23232.2",
        "longdv_m", "43434343434",
        "longdv_m", "343332",
        "stringdv_m", "manchester \"city\"",
        "stringdv_m", "liverpool",
        "stringdv_m", "everton",
        "datedv", "2017-01-01T00:00:00Z",
        "datedv_m", "2017-01-01T01:00:00Z",
        "datedv_m", "2017-01-01T02:00:00Z",
        "int_is_p", "1",
        "int_is_p", "1",
        "int_is_p", "1",
        "int_is_p", "1",
        "sortabledv", "this is some text_2",
        "sortabledv_udvas", "this is some text_2",
        "sortabledv_m", "this is some text one_2",
        "sortabledv_m", "this is some text two_2",
        "sortabledv_m", "this is some text three_2",
        "sortabledv_m_udvas", "this is some text one_2",
        "sortabledv_m_udvas", "this is some text two_2",
        "sortabledv_m_udvas", "this is some text three_2"
    ));
    assertU(commit());


  }

  @Test
  public void test() throws Exception {
    clearIndex();
    createIndex();

    testSortingOutput();
    testExportRequiredParams();
    testDates();
    testDuplicates();

    clearIndex();
  }

  @Test
  public void testSmallChains() throws Exception {
    clearIndex();

    assertU(adoc("id","1",
        "field1_i_p",Integer.toString(Integer.MIN_VALUE),
        "field2_i_p","1"));
    assertU(commit());

    assertU(adoc("id","2",
        "field1_i_p",Integer.toString(Integer.MIN_VALUE),
        "field2_i_p",Integer.toString(Integer.MIN_VALUE + 1)));
    assertU(commit());

    assertU(adoc("id","3",
        "field1_i_p",Integer.toString(Integer.MIN_VALUE),
        "field2_i_p",Integer.toString(Integer.MIN_VALUE)));
    assertU(commit());

    //Test single value DocValue output
    //Expected for asc sort doc3 -> doc2 -> doc1
    String s =  h.query(req("q", "*:*", "qt", "/export", "fl", "id", "sort", "field1_i_p asc,field2_i_p asc"));
    assertJsonEquals(s, "{\n" +
        "  \"responseHeader\":{\"status\":0},\n" +
        "  \"response\":{\n" +
        "    \"numFound\":3,\n" +
        "    \"docs\":[{\n" +
        "        \"id\":\"3\"}\n" +
        "      ,{\n" +
        "        \"id\":\"2\"}\n" +
        "      ,{\n" +
        "        \"id\":\"1\"}]}}");

    clearIndex();
    //Adding 3 docs of integers with the following values
    //  doc1: Integer.MIN_VALUE,1,2,Integer.MAX_VALUE,3,4,5,6
    //  doc2: Integer.MIN_VALUE,Integer.MIN_VALUE,2,Integer.MAX_VALUE,4,4,5,6
    //  doc3: Integer.MIN_VALUE,Integer.MIN_VALUE,2,Integer.MAX_VALUE,3,4,5,6

    assertU(adoc("id","1",
        "field1_i_p",Integer.toString(Integer.MIN_VALUE),
        "field2_i_p","1",
        "field3_i_p","2",
        "field4_i_p",Integer.toString(Integer.MAX_VALUE),
        "field5_i_p","3",
        "field6_i_p","4",
        "field7_i_p","5",
        "field8_i_p","6"));
    assertU(commit());

    assertU(adoc("id","2",
        "field1_i_p",Integer.toString(Integer.MIN_VALUE),
        "field2_i_p",Integer.toString(Integer.MIN_VALUE),
        "field3_i_p","2",
        "field4_i_p",Integer.toString(Integer.MAX_VALUE),
        "field5_i_p","4",
        "field6_i_p","4",
        "field7_i_p","5",
        "field8_i_p","6"));
    assertU(commit());

    assertU(adoc("id","3",
        "field1_i_p",Integer.toString(Integer.MIN_VALUE),
        "field2_i_p",Integer.toString(Integer.MIN_VALUE),
        "field3_i_p","2",
        "field4_i_p",Integer.toString(Integer.MAX_VALUE),
        "field5_i_p","3",
        "field6_i_p","4",
        "field7_i_p","5",
        "field8_i_p","6"));
    assertU(commit());

    s =  h.query(req("q", "*:*", "qt", "/export", "fl", "id", "sort", "field1_i_p asc,field2_i_p asc,field3_i_p asc,field4_i_p asc,field5_i_p desc,field6_i_p desc,field7_i_p desc,field8_i_p asc"));
    assertJsonEquals(s, "{\n" +
        "  \"responseHeader\":{\"status\":0},\n" +
        "  \"response\":{\n" +
        "    \"numFound\":3,\n" +
        "    \"docs\":[{\n" +
        "        \"id\":\"2\"}\n" +
        "      ,{\n" +
        "        \"id\":\"3\"}\n" +
        "      ,{\n" +
        "        \"id\":\"1\"}]}}");

  }

  @Test
  public void testIndexOrder() throws Exception {
    clearIndex();

    assertU(adoc("id","1", "stringdv","a"));
    assertU(adoc("id","2", "stringdv","a"));

    assertU(commit());

    assertU(adoc("id","3", "stringdv","a"));
    assertU(adoc("id","4", "stringdv","a"));

    assertU(commit());

    String expectedResult = "{\n" +
        "  \"responseHeader\":{\"status\":0},\n" +
        "  \"response\":{\n" +
        "    \"numFound\":4,\n" +
        "    \"docs\":[{\n" +
        "        \"id\":\"1\"}\n" +
        "      ,{\n" +
        "        \"id\":\"2\"}\n" +
        "      ,{\n" +
        "        \"id\":\"3\"}\n" +
        "      ,{\n" +
        "        \"id\":\"4\"}]}}";

    String s =  h.query(req("q", "*:*", "qt", "/export", "fl", "id", "sort", "stringdv asc"));
    assertJsonEquals(s, expectedResult);

    s =  h.query(req("q", "*:*", "qt", "/export", "fl", "id", "sort", "stringdv desc"));
    assertJsonEquals(s, expectedResult);

  }

  @Test
  public void testStringWithCase() throws Exception {
    clearIndex();

    assertU(adoc("id","1", "stringdv","a"));
    assertU(adoc("id","2", "stringdv","ABC"));

    assertU(commit());

    assertU(adoc("id","3", "stringdv","xyz"));
    assertU(adoc("id","4", "stringdv","a"));

    assertU(commit());

    String s =  h.query(req("q", "*:*", "qt", "/export", "fl", "id", "sort", "stringdv desc"));
    assertJsonEquals(s, "{\n" +
        "  \"responseHeader\":{\"status\":0},\n" +
        "  \"response\":{\n" +
        "    \"numFound\":4,\n" +
        "    \"docs\":[{\n" +
        "        \"id\":\"3\"}\n" +
        "      ,{\n" +
        "        \"id\":\"1\"}\n" +
        "      ,{\n" +
        "        \"id\":\"4\"}\n" +
        "      ,{\n" +
        "        \"id\":\"2\"}]}}");
  }

  @Test
  public void testBooleanField() throws Exception {
    clearIndex();

    assertU(adoc("id","1",
        "booleandv","true"));
    assertU(commit());

    assertU(adoc("id","2",
        "booleandv","false"));
    assertU(commit());

    String s =  h.query(req("q", "*:*", "qt", "/export", "fl", "id", "sort", "booleandv asc"));
    assertJsonEquals(s, "{\n" +
        "  \"responseHeader\":{\"status\":0},\n" +
        "  \"response\":{\n" +
        "    \"numFound\":2,\n" +
        "    \"docs\":[{\n" +
        "        \"id\":\"2\"}\n" +
        "      ,{\n" +
        "        \"id\":\"1\"}]}}");

    s =  h.query(req("q", "*:*", "qt", "/export", "fl", "id", "sort", "booleandv desc"));
    assertJsonEquals(s, "{\n" +
        "  \"responseHeader\":{\"status\":0},\n" +
        "  \"response\":{\n" +
        "    \"numFound\":2,\n" +
        "    \"docs\":[{\n" +
        "        \"id\":\"1\"}\n" +
        "      ,{\n" +
        "        \"id\":\"2\"}]}}");
  }

  private void testSortingOutput() throws Exception {

    //Test single value DocValue output
    String s =  h.query(req("q", "id:1", "qt", "/export", "fl", "floatdv,intdv,stringdv,longdv,doubledv", "sort", "intdv asc"));

    assertJsonEquals(s, "{\"responseHeader\": {\"status\": 0}, \"response\":{\"numFound\":1, \"docs\":[{\"floatdv\":2.1,\"intdv\":1,\"stringdv\":\"hello world\",\"longdv\":323223232323,\"doubledv\":2344.345}]}}");

    //Test null value string:
    s = h.query(req("q", "id:7", "qt", "/export", "fl", "floatdv,intdv,stringdv,longdv,doubledv", "sort", "intdv asc"));

    assertJsonEquals(s, "{\"responseHeader\": {\"status\": 0}, \"response\":{\"numFound\":1, \"docs\":[{\"floatdv\":2.1,\"intdv\":7,\"longdv\":323223232323,\"doubledv\":2344.345}]}}");

    //Test multiValue docValues output
    s = h.query(req("q", "id:1", "qt", "/export", "fl", "intdv_m,floatdv_m,doubledv_m,longdv_m,stringdv_m", "sort", "intdv asc"));
    assertJsonEquals(s, "{\"responseHeader\": {\"status\": 0}, \"response\":{\"numFound\":1, \"docs\":[{\"intdv_m\":[100,250],\"floatdv_m\":[123.321,345.123],\"doubledv_m\":[3444.222,23232.2],\"longdv_m\":[343332,43434343434],\"stringdv_m\":[\"Everton\",\"liverpool\",\"manchester \\\"city\\\"\"]}]}}");

    //Test multiValues docValues output with nulls
    s =  h.query(req("q", "id:7", "qt", "/export", "fl", "intdv_m,floatdv_m,doubledv_m,longdv_m,stringdv_m", "sort", "intdv asc"));
    assertJsonEquals(s, "{\"responseHeader\": {\"status\": 0}, \"response\":{\"numFound\":1, \"docs\":[{\"floatdv_m\":[123.321,345.123],\"doubledv_m\":[3444.222,23232.2],\"longdv_m\":[343332,43434343434]}]}}");

    //Test single sort param is working
    s =  h.query(req("q", "id:(1 2)", "qt", "/export", "fl", "intdv", "sort", "intdv desc"));
    assertJsonEquals(s, "{\"responseHeader\": {\"status\": 0}, \"response\":{\"numFound\":2, \"docs\":[{\"intdv\":2},{\"intdv\":1}]}}");

    s =  h.query(req("q", "id:(1 2)", "qt", "/export", "fl", "intdv", "sort", "intdv asc"));
    assertJsonEquals(s, "{\"responseHeader\": {\"status\": 0}, \"response\":{\"numFound\":2, \"docs\":[{\"intdv\":1},{\"intdv\":2}]}}");

    // Test sort on String will null value. Null value should sort last on desc and first on asc.

    s =  h.query(req("q", "id:(1 7)", "qt", "/export", "fl", "intdv", "sort", "stringdv desc"));
    assertJsonEquals(s, "{\"responseHeader\": {\"status\": 0}, \"response\":{\"numFound\":2, \"docs\":[{\"intdv\":1},{\"intdv\":7}]}}");

    s =  h.query(req("q", "id:(1 7)", "qt", "/export", "fl", "intdv", "sort", "stringdv asc"));
    assertJsonEquals(s, "{\"responseHeader\": {\"status\": 0}, \"response\":{\"numFound\":2, \"docs\":[{\"intdv\":7},{\"intdv\":1}]}}");


    //Test multi-sort params
    s =  h.query(req("q", "id:(1 2)", "qt", "/export", "fl", "intdv", "sort", "floatdv asc,intdv desc"));
    assertJsonEquals(s, "{\"responseHeader\": {\"status\": 0}, \"response\":{\"numFound\":2, \"docs\":[{\"intdv\":2},{\"intdv\":1}]}}");

    s =  h.query(req("q", "id:(1 2)", "qt", "/export", "fl", "intdv", "sort", "floatdv desc,intdv asc"));
    assertJsonEquals(s, "{\"responseHeader\": {\"status\": 0}, \"response\":{\"numFound\":2, \"docs\":[{\"intdv\":1},{\"intdv\":2}]}}");

    //Test three sort fields
    s =  h.query(req("q", "id:(1 2 3)", "qt", "/export", "fl", "intdv", "sort", "floatdv asc,stringdv asc,intdv desc"));
    assertJsonEquals(s, "{\"responseHeader\": {\"status\": 0}, \"response\":{\"numFound\":3, \"docs\":[{\"intdv\":3},{\"intdv\":2},{\"intdv\":1}]}}");

    //Test three sort fields
    s =  h.query(req("q", "id:(1 2 3)", "qt", "/export", "fl", "intdv", "sort", "floatdv asc,stringdv desc,intdv asc"));
    assertJsonEquals(s, "{\"responseHeader\": {\"status\": 0}, \"response\":{\"numFound\":3, \"docs\":[{\"intdv\":1},{\"intdv\":2},{\"intdv\":3}]}}");

    //Test four sort fields
    s =  h.query(req("q", "id:(1 2 3)", "qt", "/export", "fl", "intdv", "sort", "floatdv asc,floatdv desc,floatdv asc,intdv desc"));
    assertJsonEquals(s, "{\"responseHeader\": {\"status\": 0}, \"response\":{\"numFound\":3, \"docs\":[{\"intdv\":3},{\"intdv\":2},{\"intdv\":1}]}}");

    //Test five sort fields
    s =  h.query(req("q", "id:(1 2 3)", "qt", "/export", "fl", "intdv", "sort", "intdv desc,floatdv asc,floatdv desc,floatdv asc,intdv desc"));
    assertJsonEquals(s, "{\"responseHeader\": {\"status\": 0}, \"response\":{\"numFound\":3, \"docs\":[{\"intdv\":3},{\"intdv\":2},{\"intdv\":1}]}}");
    s =  h.query(req("q", "id:(1 2 3)", "qt", "/export", "fl", "intdv", "sort", "floatdv desc,intdv asc,floatdv desc,floatdv desc,intdv desc"));
    assertJsonEquals(s, "{\"responseHeader\": {\"status\": 0}, \"response\":{\"numFound\":3, \"docs\":[{\"intdv\":1},{\"intdv\":2},{\"intdv\":3}]}}");

    //Test six sort fields
    s =  h.query(req("q", "id:(1 2 3)", "qt", "/export", "fl", "intdv", "sort", "floatdv asc,intdv desc,floatdv asc,floatdv desc,floatdv asc,intdv asc"));
    assertJsonEquals(s, "{\"responseHeader\": {\"status\": 0}, \"response\":{\"numFound\":3, \"docs\":[{\"intdv\":3},{\"intdv\":2},{\"intdv\":1}]}}");

    //Test seven sort fields
    s =  h.query(req("q", "id:(1 2 3)", "qt", "/export", "fl", "intdv", "sort", "floatdv desc,intdv asc,floatdv desc,floatdv asc,floatdv desc,floatdv asc,intdv desc"));
    assertJsonEquals(s, "{\"responseHeader\": {\"status\": 0}, \"response\":{\"numFound\":3, \"docs\":[{\"intdv\":1},{\"intdv\":2},{\"intdv\":3}]}}");

    //Test eight sort fields
    s =  h.query(req("q", "id:(1 2 3)", "qt", "/export", "fl", "intdv", "sort", "floatdv asc,intdv desc,floatdv asc,floatdv desc,floatdv asc,floatdv desc,floatdv asc,intdv asc"));
    assertJsonEquals(s, "{\"responseHeader\": {\"status\": 0}, \"response\":{\"numFound\":3, \"docs\":[{\"intdv\":3},{\"intdv\":2},{\"intdv\":1}]}}");
    s =  h.query(req("q", "id:(1 2 3)", "qt", "/export", "fl", "intdv", "sort", "intdv asc,floatdv desc,floatdv asc,floatdv desc,floatdv asc,floatdv desc,floatdv asc,intdv desc"));
    assertJsonEquals(s, "{\"responseHeader\": {\"status\": 0}, \"response\":{\"numFound\":3, \"docs\":[{\"intdv\":1},{\"intdv\":2},{\"intdv\":3}]}}");

    //Test nine sort fields
    s =  h.query(req("q", "id:(1 2 3)", "qt", "/export", "fl", "intdv", "sort", "intdv asc,floatdv desc,floatdv asc,floatdv desc,floatdv asc,floatdv desc,intdv asc,intdv desc,floatdv asc"));
    assertJsonEquals(s, "{\"responseHeader\": {\"status\": 0}, \"response\":{\"numFound\":3, \"docs\":[{\"intdv\":1},{\"intdv\":2},{\"intdv\":3}]}}");
    s =  h.query(req("q", "id:(1 2 3)", "qt", "/export", "fl", "intdv", "sort", "floatdv asc,intdv desc,floatdv asc,floatdv desc,floatdv asc,floatdv desc,intdv desc,intdv asc,floatdv asc"));
    assertJsonEquals(s, "{\"responseHeader\": {\"status\": 0}, \"response\":{\"numFound\":3, \"docs\":[{\"intdv\":3},{\"intdv\":2},{\"intdv\":1}]}}");

    //Test ten sort fields
    s =  h.query(req("q", "id:(1 2 3)", "qt", "/export", "fl", "intdv", "sort", "intdv asc,floatdv desc,floatdv asc,floatdv desc,floatdv asc,floatdv desc,intdv asc,intdv desc,floatdv desc,floatdv asc"));
    assertJsonEquals(s, "{\"responseHeader\": {\"status\": 0}, \"response\":{\"numFound\":3, \"docs\":[{\"intdv\":1},{\"intdv\":2},{\"intdv\":3}]}}");
    s =  h.query(req("q", "id:(1 2 3)", "qt", "/export", "fl", "intdv", "sort", "floatdv asc,intdv desc,floatdv asc,floatdv desc,floatdv asc,floatdv desc,intdv desc,intdv asc,floatdv desc,floatdv asc"));
    assertJsonEquals(s, "{\"responseHeader\": {\"status\": 0}, \"response\":{\"numFound\":3, \"docs\":[{\"intdv\":3},{\"intdv\":2},{\"intdv\":1}]}}");

    s =  h.query(req("q", "id:(1 2 3)", "qt", "/export", "fl", "intdv", "sort", "doubledv desc"));
    assertJsonEquals(s, "{\"responseHeader\": {\"status\": 0}, \"response\":{\"numFound\":3, \"docs\":[{\"intdv\":3},{\"intdv\":1},{\"intdv\":2}]}}");

    s =  h.query(req("q", "intdv:[2 TO 1000]", "qt", "/export", "fl", "intdv", "sort", "doubledv desc"));
    assertJsonEquals(s, "{\"responseHeader\": {\"status\": 0}, \"response\":{\"numFound\":3, \"docs\":[{\"intdv\":3},{\"intdv\":7},{\"intdv\":2}]}}");

    s =  h.query(req("q", "stringdv:blah", "qt", "/export", "fl", "intdv", "sort", "doubledv desc"));
    assertJsonEquals(s, "{\"responseHeader\": {\"status\": 0}, \"response\":{\"numFound\":0, \"docs\":[]}}");

    s =  h.query(req("q", "id:8", "qt", "/export", "fl", "stringdv", "sort", "intdv asc"));
    assertJsonEquals(s, "{\"responseHeader\": {\"status\": 0}, \"response\":{\"numFound\":1, \"docs\":[{\"stringdv\":\"chello \\\"world\\\"\"}]}}");

    // Test sortable text fields:
    s =  h.query(req("q", "id:(1 OR 3 OR 8)", "qt", "/export", "fl", "sortabledv_m_udvas,sortabledv_udvas", "sort", "sortabledv_udvas asc"));
    assertJsonEquals(s, "{\n" +
        "  \"responseHeader\":{\"status\":0},\n" +
        "  \"response\":{\n" +
        "    \"numFound\":3,\n" +
        "    \"docs\":[{\n" +
        "        \"sortabledv_m_udvas\":[\"this is some text one_1\"\n" +
        "          ,\"this is some text three_1\"\n" +
        "          ,\"this is some text two_1\"]}\n" +
        "      ,{\n" +
        "        \"sortabledv_udvas\":\"this is some text_1\"}\n" +
        "      ,{\n" +
        "        \"sortabledv_m_udvas\":[\"this is some text one_2\"\n" +
        "          ,\"this is some text three_2\"\n" +
        "          ,\"this is some text two_2\"],\n" +
        "        \"sortabledv_udvas\":\"this is some text_2\"}]}}");

    s =  h.query(req("q", "id:(1 OR 3 OR 8)", "qt", "/export", "fl", "sortabledv_m", "sort", "sortabledv_udvas asc"));
    assertTrue("Should have 400 status when exporting sortabledv_m, it does not have useDocValuesAsStored='true'", s.contains("\"status\":400}"));
    assertTrue("Should have a cause when exporting sortabledv_m, it does not have useDocValuesAsStored='true'", s.contains("Must have useDocValuesAsStored='true' to be used with export writer"));

    s =  h.query(req("q", "id:(1 OR 3 OR 8)", "qt", "/export", "fl", "sortabledv", "sort", "sortabledv_udvas asc"));
    assertTrue("Should have 400 status when exporting sortabledv, it does not have useDocValuesAsStored='true'", s.contains("\"status\":400}"));
    assertTrue("Should have a cause when exporting sortabledv, it does not have useDocValuesAsStored='true'", s.contains("Must have useDocValuesAsStored='true' to be used with export writer"));

  }

  private void assertJsonEquals(String actual, String expected) {
    assertEquals(Utils.toJSONString(Utils.fromJSONString(expected)), Utils.toJSONString(Utils.fromJSONString(actual)));
  }

  private void testExportRequiredParams() throws Exception {

    //Test whether missing required parameters returns expected errors.

    //String s =  h.query(req("q", "id:1", "qt", "/export", "fl", "floatdv,intdv,stringdv,longdv,doubledv", "sort", "intdv asc"));
    String s;
    s = h.query(req("qt", "/export"));
    assertTrue("Should have had a sort error", s.contains("No sort criteria"));
    s = h.query(req("sort", "intdv asc", "qt", "/export"));
    assertTrue("Should have had fl error", s.contains("export field list (fl) must be specified"));
    s = h.query(req("sort", "intdv asc", "qt", "/export", "fl", "stringdv"));
    // Interesting you don't even need to specify a "q" parameter.
    
  }

  private void testDates() throws Exception {
    String s =  h.query(req("q", "id:1", "qt", "/export", "fl", "datedv", "sort", "datedv asc"));
    assertJsonEquals(s, "{\"responseHeader\": {\"status\": 0}, \"response\":{\"numFound\":1, \"docs\":[{\"datedv\":\"2017-06-16T07:00:00Z\"}]}}");
    s =  h.query(req("q", "id:1", "qt", "/export", "fl", "datedv_m", "sort", "datedv asc"));
    assertJsonEquals(s, "{\"responseHeader\": {\"status\": 0}, \"response\":{\"numFound\":1, \"docs\":[{\"datedv_m\":[\"2017-06-16T01:00:00Z\",\"2017-06-16T02:00:00Z\",\"2017-06-16T03:00:00Z\",\"2017-06-16T04:00:00Z\"]}]}}");
  }

  private void testDuplicates() throws Exception {
    // see SOLR-10924
    String expected = h.getCore().getLatestSchema().getField("int_is_t").getType().isPointField()
      ? "1,1,1,1" : "1";
    String s =  h.query(req("q", "id:3", "qt", "/export", "fl", "int_is_t", "sort", "intdv asc"));
    assertJsonEquals(s, "{\"responseHeader\": {\"status\": 0}, \"response\":{\"numFound\":1, \"docs\":[{\"int_is_t\":["+expected+"]}]}}");
    expected = h.getCore().getLatestSchema().getField("int_is_p").getType().isPointField()
      ? "1,1,1,1" : "1";
    s =  h.query(req("q", "id:8", "qt", "/export", "fl", "int_is_p", "sort", "intdv asc"));
    assertJsonEquals(s, "{\"responseHeader\": {\"status\": 0}, \"response\":{\"numFound\":1, \"docs\":[{\"int_is_p\":[1,1,1,1]}]}}");
  }
  
  /**
   * This test doesn't validate the correctness of results, it just compares the response of the same request
   * when asking for Trie fields vs Point fields. Can be removed once Trie fields are no longer supported
   */
  @Test
  @SuppressForbidden(reason="using new Date(time) to create random dates")
  public void testRandomNumerics() throws Exception {
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ROOT);
    assertU(delQ("*:*"));
    assertU(commit());
    List<String> trieFields = new ArrayList<String>();
    List<String> pointFields = new ArrayList<String>();
    for (String mv:new String[]{"s", ""}) {
      for (String indexed:new String[]{"_ni", ""}) {
        for (String type:new String[]{"i", "l", "f", "d", "dt"}) {
          String field = "number_" + type + mv + indexed;
          SchemaField sf = h.getCore().getLatestSchema().getField(field + "_t");
          assertTrue(sf.hasDocValues());
          assertTrue(sf.getType().getNumberType() != null);
          
          sf = h.getCore().getLatestSchema().getField(field + "_p");
          assertTrue(sf.hasDocValues());
          assertTrue(sf.getType().getNumberType() != null);
          assertTrue(sf.getType().isPointField());
          
          trieFields.add(field + "_t");
          pointFields.add(field + "_p");
        }
      }
    }
    for (int i = 0; i < atLeast(100); i++) {
      if (random().nextInt(20) == 0) {
        //have some empty docs
        assertU(adoc("id", String.valueOf(i)));
        continue;
      }

      if (random().nextInt(20) == 0 && i > 0) {
        //delete some docs
        assertU(delI(String.valueOf(i - 1)));
      }
      
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", String.valueOf(i));
      addInt(doc, random().nextInt(), false);
      addLong(doc, random().nextLong(), false);
      addFloat(doc, random().nextFloat() * 3000 * (random().nextBoolean()?1:-1), false);
      addDouble(doc, random().nextDouble() * 3000 * (random().nextBoolean()?1:-1), false);
      addDate(doc, dateFormat.format(new Date()), false);

      // MV need to be unique in order to be the same in Trie vs Points
      Set<Integer> ints = new HashSet<>();
      Set<Long> longs = new HashSet<>();
      Set<Float> floats = new HashSet<>();
      Set<Double> doubles = new HashSet<>();
      Set<String> dates = new HashSet<>();
      for (int j=0; j < random().nextInt(20); j++) {
        ints.add(random().nextInt());
        longs.add(random().nextLong());
        floats.add(random().nextFloat() * 3000 * (random().nextBoolean()?1:-1));
        doubles.add(random().nextDouble() * 3000 * (random().nextBoolean()?1:-1));
        dates.add(dateFormat.format(new Date(System.currentTimeMillis() + random().nextInt())));
      }
      ints.stream().forEach((val)->addInt(doc, val, true));
      longs.stream().forEach((val)->addLong(doc, val, true));
      floats.stream().forEach((val)->addFloat(doc, val, true));
      doubles.stream().forEach((val)->addDouble(doc, val, true));
      dates.stream().forEach((val)->addDate(doc, val, true));
      
      assertU(adoc(doc));
      if (random().nextInt(20) == 0) {
        assertU(commit());
      }
    }
    assertU(commit());
    doTestQuery("id:1", trieFields, pointFields);
    doTestQuery("*:*", trieFields, pointFields);
    doTestQuery("id:[0 TO 2]", trieFields, pointFields);// "id" field is really a string, this is not a numeric range query
    doTestQuery("id:[0 TO 9]", trieFields, pointFields);
    doTestQuery("id:DOES_NOT_EXIST", trieFields, pointFields);
  }

  @Test
  public void testMultipleSorts() throws Exception {
    assertU(delQ("*:*"));
    assertU(commit());

    int numDocs = 1000;

    //10 unique values
    String[] str_vals = new String[10];
    for (int i=0; i<str_vals.length; i++) {
      str_vals[i] = TestUtil.randomSimpleString(random(), 10);
    }

    float[] float_vals = new float[10];
    float_vals[0] = 0.0f;
    float_vals[1] = +0.0f;
    float_vals[2] = -0.0f;
    float_vals[3] = +0.00001f;
    float_vals[4] = +0.000011f;
    float_vals[5] = Float.MAX_VALUE;
    float_vals[6] = Float.MIN_VALUE;
    float_vals[7] = 1/3f; //0.33333334
    float_vals[8] = 0.33333333f;
    float_vals[9] = random().nextFloat();

    for (int i = 0; i < numDocs; i++) {
      int number = TestUtil.nextInt(random(), 0, 9);
      assertU(adoc("id", String.valueOf(i),
          "floatdv", String.valueOf(number),
          "intdv", String.valueOf(number),
          "stringdv", String.valueOf(str_vals[number]),
          "longdv", String.valueOf(number),
          "doubledv", String.valueOf(number),
          "datedv", randomSkewedDate(),
          "booleandv", String.valueOf(random().nextBoolean()),
          "field1_s_dv", String.valueOf(str_vals[number]),
          "field2_i_p", String.valueOf(number),
          "field3_l_p", String.valueOf(number)));
      if (numDocs % 300 ==0) {
        assertU(commit());
      }
    }
    assertU(commit());

    validateSort(numDocs);
  }

  private void createLargeIndex() throws Exception {
    int BATCH_SIZE = 5000;
    int NUM_BATCHES = 20;
    SolrInputDocument[] docs = new SolrInputDocument[BATCH_SIZE];
    for (int i = 0; i < NUM_BATCHES; i++) {
      for (int j = 0; j < BATCH_SIZE; j++) {
        docs[j] = new SolrInputDocument(
            "id", String.valueOf(i * BATCH_SIZE + j),
            "sortabledv", TestUtil.randomSimpleString(random(), 2, 3),
            "sortabledv_udvas", String.valueOf((i + j) % 101),
            "small_i_p", String.valueOf((i + j) % 37)
            );
      }
      updateJ(jsonAdd(docs), null);
    }
    assertU(commit());
  }

  @Test
  public void testExpr() throws Exception {
    assertU(delQ("*:*"));
    assertU(commit());
    createLargeIndex();
    SolrQueryRequest req = req("q", "*:*", "qt", "/export", "fl", "id", "sort", "id asc", "expr", "top(n=2,input(),sort=\"id desc\")");
    assertJQ(req,
        "response/numFound==100000",
        "response/docs/[0]/id=='99999'",
        "response/docs/[1]/id=='99998'"
        );
    req = req("q", "*:*", "qt", "/export", "fl", "id,sortabledv_udvas", "sort", "sortabledv_udvas asc", "expr", "unique(input(),over=\"sortabledv_udvas\")");
    String rsp = h.query(req);
    @SuppressWarnings({"unchecked"})
    Map<String, Object> rspMap = mapper.readValue(rsp, HashMap.class);
    @SuppressWarnings({"unchecked"})
    List<Map<String, Object>> docs = (List<Map<String, Object>>) Utils.getObjectByPath(rspMap, false, "/response/docs");
    assertNotNull("missing document results: " + rspMap, docs);
    assertEquals("wrong number of unique docs", 101, docs.size());
    for (int i = 0; i < 100; i++) {
      boolean found = false;
      String si = String.valueOf(i);
      for (int j = 0; j < docs.size(); j++) {
        if (docs.get(j).get("sortabledv_udvas").equals(si)) {
          found = true;
          break;
        }
      }
      assertTrue("missing value " + i + " in results", found);
    }
    req = req("q", "*:*", "qt", "/export", "fl", "id,sortabledv_udvas,small_i_p", "sort", "sortabledv_udvas asc", "expr", "rollup(input(),over=\"sortabledv_udvas\", sum(small_i_p),avg(small_i_p),min(small_i_p),count(*))");
    rsp = h.query(req);
    rspMap = mapper.readValue(rsp, HashMap.class);
    docs = (List<Map<String, Object>>) Utils.getObjectByPath(rspMap, false, "/response/docs");
    assertNotNull("missing document results: " + rspMap, docs);
    assertEquals("wrong number of unique docs", 101, docs.size());
    for (Map<String, Object> doc : docs) {
      assertNotNull("missing sum: " + doc, doc.get("sum(small_i_p)"));
      assertEquals(18000.0, ((Number)doc.get("sum(small_i_p)")).doubleValue(), 1000.0);
      assertNotNull("missing avg: " + doc, doc.get("avg(small_i_p)"));
      assertEquals(18.0, ((Number)doc.get("avg(small_i_p)")).doubleValue(), 1.0);
      assertNotNull("missing count: " + doc, doc.get("count(*)"));
      assertEquals(1000.0, ((Number)doc.get("count(*)")).doubleValue(), 100.0);
    }
    // try invalid field types
    req = req("q", "*:*", "qt", "/export", "fl", "id,sortabledv,small_i_p", "sort", "sortabledv asc", "expr", "unique(input(),over=\"sortabledv\")");
    rsp = h.query(req);
    rspMap = mapper.readValue(rsp, HashMap.class);
    assertEquals("wrong response status", 400, ((Number)Utils.getObjectByPath(rspMap, false, "/responseHeader/status")).intValue());
    docs = (List<Map<String, Object>>) Utils.getObjectByPath(rspMap, false, "/response/docs");
    assertEquals("wrong number of docs", 1, docs.size());
    Map<String, Object> doc = docs.get(0);
    assertTrue("doc doesn't have exception", doc.containsKey(StreamParams.EXCEPTION));
    assertTrue("wrong exception message", doc.get(StreamParams.EXCEPTION).toString().contains("Must have useDocValuesAsStored='true'"));
  }

  private void validateSort(int numDocs) throws Exception {
    // 10 fields
    List<String> fieldNames = new ArrayList<>(Arrays.asList("floatdv", "intdv", "stringdv", "longdv", "doubledv",
        "datedv", "booleandv", "field1_s_dv", "field2_i_p", "field3_l_p"));

    SortFields[] fieldSorts = new SortFields[TestUtil.nextInt(random(), 1, fieldNames.size())];
    for (int i = 0; i < fieldSorts.length; i++) {
      fieldSorts[i] = new SortFields(fieldNames.get(TestUtil.nextInt(random(), 0, fieldNames.size() - 1)));
      fieldNames.remove(fieldSorts[i].getField());
    }
    String[] fieldWithOrderStrs = new String[fieldSorts.length];
    String[] fieldStrs = new String[fieldSorts.length];
    for (int i = 0; i < fieldSorts.length; i++) {
      fieldWithOrderStrs[i] = fieldSorts[i].getFieldWithOrder();
      fieldStrs[i] = fieldSorts[i].getField();
    }

    String sortStr = String.join(",", fieldWithOrderStrs); // sort : field1 asc, field2 desc
    String fieldsStr = String.join(",", fieldStrs); // fl :  field1, field2

    String resp = h.query(req("q", "*:*", "qt", "/export", "fl", "id," + fieldsStr, "sort", sortStr));
    @SuppressWarnings({"rawtypes"})
    HashMap respMap = mapper.readValue(resp, HashMap.class);
    @SuppressWarnings({"rawtypes"})
    List docs = (ArrayList) ((HashMap) respMap.get("response")).get("docs");

    SolrQueryRequest selectReq = req("q", "*:*", "qt", "/select", "fl", "id," + fieldsStr, "sort", sortStr, "rows", Integer.toString(numDocs), "wt", "json");
    String response = h.query(selectReq);
    @SuppressWarnings({"rawtypes"})
    Map rsp = (Map)Utils.fromJSONString(response);
    @SuppressWarnings({"rawtypes"})
    List doclist = (List)(((Map)rsp.get("response")).get("docs"));

    assert docs.size() == numDocs;

    for (int i = 0; i < docs.size() - 1; i++) { // docs..
      assertEquals("Position:" + i + " has different id value" , ((LinkedHashMap)doclist.get(i)).get("id"), String.valueOf(((HashMap) docs.get(i)).get("id")));

      for (int j = 0; j < fieldSorts.length; j++) { // fields ..
        String field = fieldSorts[j].getField();
        String sort = fieldSorts[j].getSort();
        String fieldVal1 = String.valueOf(((HashMap) docs.get(i)).get(field)); // 1st doc
        String fieldVal2 = String.valueOf(((HashMap) docs.get(i + 1)).get(field)); // 2nd obj
        if (fieldVal1.equals(fieldVal2)) {
          continue;
        } else {
          if (sort.equals("asc")) {
            if (field.equals("stringdv") || field.equals("field1_s_dv")|| field.equals("datedv") || field.equals("booleandv")) { // use string comparator
              assertTrue(fieldVal1.compareTo(fieldVal2) < 0);
            } else if (field.equals("doubledv")){
              assertTrue(Double.compare(Double.valueOf(fieldVal1), Double.valueOf(fieldVal2)) <= 0);
            } else if(field.equals("floatdv")) {
              assertTrue(Float.compare(Float.valueOf(fieldVal1), Float.valueOf(fieldVal2)) <= 0);
            } else if(field.equals("intdv") || "field2_i_p".equals(field)) {
              assertTrue(Integer.compare(Integer.valueOf(fieldVal1), Integer.valueOf(fieldVal2)) <= 0);
            } else if(field.equals("longdv") || field.equals("field3_l_p")) {
              assertTrue(Long.compare(Integer.valueOf(fieldVal1), Long.valueOf(fieldVal2)) <= 0);
            }
          } else {
            if (field.equals("stringdv") || field.equals("field1_s_dv")|| field.equals("datedv") || field.equals("booleandv")) { // use string comparator
              assertTrue(fieldVal1.compareTo(fieldVal2) > 0);
            } else if (field.equals("doubledv")){
              assertTrue(Double.compare(Double.valueOf(fieldVal1), Double.valueOf(fieldVal2)) >= 0);
            } else if(field.equals("floatdv")) {
              assertTrue(Float.compare(Float.valueOf(fieldVal1), Float.valueOf(fieldVal2)) >= 0);
            } else if(field.equals("intdv") || "field2_i_p".equals(field)) {
              assertTrue(Integer.compare(Integer.valueOf(fieldVal1), Integer.valueOf(fieldVal2)) >= 0);
            } else if(field.equals("longdv") || field.equals("field3_l_p")) {
              assertTrue(Long.compare(Integer.valueOf(fieldVal1), Long.valueOf(fieldVal2)) >= 0);
            }
          }
          break;
        }
      }
    }
  }

  private class SortFields {
    String fieldName;
    String sortOrder;
    String[] orders = {"asc", "desc"};

    SortFields(String fn) {
      this.fieldName = fn;
      this.sortOrder = orders[random().nextInt(2)];
    }

    public String getFieldWithOrder() {
      return this.fieldName + " " + this.sortOrder;
    }

    public String getField() {
      return this.fieldName;
    }

    public String getSort() {
      return this.sortOrder;
    }
  }

  private void doTestQuery(String query, List<String> trieFields, List<String> pointFields) throws Exception {
    String trieFieldsFl = String.join(",", trieFields);
    String pointFieldsFl = String.join(",", pointFields);
    String sort = pickRandom((String)pickRandom(trieFields.toArray()), (String)pickRandom(pointFields.toArray())).replace("s_", "_") + pickRandom(" asc", " desc");
    String resultPoints =  h.query(req("q", query, "qt", "/export", "fl", pointFieldsFl, "sort", sort));
    String resultTries =  h.query(req("q", query, "qt", "/export", "fl", trieFieldsFl, "sort", sort));
    assertJsonEquals(resultPoints.replaceAll("_p", ""), resultTries.replaceAll("_t", ""));
  }

  private void addFloat(SolrInputDocument doc, float value, boolean mv) {
    addField(doc, "f", String.valueOf(value), mv);
  }

  private void addDouble(SolrInputDocument doc, double value, boolean mv) {
    addField(doc, "d", String.valueOf(value), mv);
  }

  private void addLong(SolrInputDocument doc, long value, boolean mv) {
    addField(doc, "l", String.valueOf(value), mv);
  }

  private void addInt(SolrInputDocument doc, int value, boolean mv) {
    addField(doc, "i", String.valueOf(value), mv);
  }
  
  private void addDate(SolrInputDocument doc, String value, boolean mv) {
    addField(doc, "dt", value, mv);
  }
  
  private void addField(SolrInputDocument doc, String type, String value, boolean mv) {
    doc.addField("number_" + type + (mv?"s":"") + "_t", value);
    doc.addField("number_" + type + (mv?"s":"") + "_p", value);
    doc.addField("number_" + type + (mv?"s":"") + "_ni_t", value);
    doc.addField("number_" + type + (mv?"s":"") + "_ni_p", value);
  }

}
