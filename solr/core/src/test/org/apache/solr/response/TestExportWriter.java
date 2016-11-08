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
package org.apache.solr.response;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.Utils;
import org.junit.*;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;

@SuppressCodecs({"Lucene3x", "Lucene40","Lucene41","Lucene42","Lucene45"})
public class TestExportWriter extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty("export.test", "true");
    initCore("solrconfig-sortingresponse.xml","schema-sortingresponse.xml");
    createIndex();
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
                 "stringdv_m", "Everton"));

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
        "stringdv_m", "everton"));
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
        "stringdv_m", "everton"));
    assertU(commit());


  }

  @Test
  public void testSortingOutput() throws Exception {

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

    s =  h.query(req("q", "id:(1 2 3)", "qt", "/export", "fl", "intdv", "sort", "doubledv desc"));
    assertJsonEquals(s, "{\"responseHeader\": {\"status\": 0}, \"response\":{\"numFound\":3, \"docs\":[{\"intdv\":3},{\"intdv\":1},{\"intdv\":2}]}}");

    s =  h.query(req("q", "intdv:[2 TO 1000]", "qt", "/export", "fl", "intdv", "sort", "doubledv desc"));
    assertJsonEquals(s, "{\"responseHeader\": {\"status\": 0}, \"response\":{\"numFound\":3, \"docs\":[{\"intdv\":3},{\"intdv\":7},{\"intdv\":2}]}}");

    s =  h.query(req("q", "stringdv:blah", "qt", "/export", "fl", "intdv", "sort", "doubledv desc"));
    assertJsonEquals(s, "{\"responseHeader\": {\"status\": 0}, \"response\":{\"numFound\":0, \"docs\":[]}}");

    s =  h.query(req("q", "id:8", "qt", "/export", "fl", "stringdv", "sort", "intdv asc"));
    assertJsonEquals(s, "{\"responseHeader\": {\"status\": 0}, \"response\":{\"numFound\":1, \"docs\":[{\"stringdv\":\"chello \\\"world\\\"\"}]}}");
  }

  private void assertJsonEquals(String actual, String expected) {
    assertEquals(Utils.toJSONString(Utils.fromJSONString(expected)), Utils.toJSONString(Utils.fromJSONString(actual)));
  }

  @Test
  public void testExportRequiredParams() throws Exception {

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
}
