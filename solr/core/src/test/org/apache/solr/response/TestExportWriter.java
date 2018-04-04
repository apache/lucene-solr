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

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.common.util.Utils;
import org.apache.solr.schema.SchemaField;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestExportWriter extends SolrTestCaseJ4 {
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty("export.test", "true");
    initCore("solrconfig-sortingresponse.xml","schema-sortingresponse.xml");
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    assertU(delQ("*:*"));
    assertU(commit());
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
                 "stringdv_m", "Everton",
                 "datedv", "2017-06-16T07:00:00Z",
                 "datedv_m", "2017-06-16T01:00:00Z",
                 "datedv_m", "2017-06-16T02:00:00Z",
                 "datedv_m", "2017-06-16T03:00:00Z",
                 "datedv_m", "2017-06-16T04:00:00Z"));

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
        "int_is_t", "1"));
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
        "int_is_p", "1"));
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
  
  @Test
  public void testDates() throws Exception {
    String s =  h.query(req("q", "id:1", "qt", "/export", "fl", "datedv", "sort", "datedv asc"));
    assertJsonEquals(s, "{\"responseHeader\": {\"status\": 0}, \"response\":{\"numFound\":1, \"docs\":[{\"datedv\":\"2017-06-16T07:00:00Z\"}]}}");
    s =  h.query(req("q", "id:1", "qt", "/export", "fl", "datedv_m", "sort", "datedv asc"));
    assertJsonEquals(s, "{\"responseHeader\": {\"status\": 0}, \"response\":{\"numFound\":1, \"docs\":[{\"datedv_m\":[\"2017-06-16T01:00:00Z\",\"2017-06-16T02:00:00Z\",\"2017-06-16T03:00:00Z\",\"2017-06-16T04:00:00Z\"]}]}}");
  }
  
  @Test
  public void testDuplicates() throws Exception {
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
