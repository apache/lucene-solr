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
package org.apache.solr.handler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.InputStreamResponseParser;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.ExceptionStream;
import org.apache.solr.client.solrj.io.stream.SolrStream;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

@LuceneTestCase.Slow
@SolrTestCaseJ4.SuppressSSL
@LuceneTestCase.SuppressCodecs({"Lucene3x", "Lucene40","Lucene41","Lucene42","Lucene45"})
public class TestSQLHandler extends AbstractFullDistribZkTestBase {

  static {
    schemaString = "schema-sql.xml";
  }

  public TestSQLHandler() {
    sliceCount = 2;
  }

  @Override
  protected String getCloudSolrConfig() {
    return "solrconfig-sql.xml";
  }

  @Override
  protected String getCloudSchemaFile() {
    return schemaString;
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();

    System.setProperty("numShards", Integer.toString(sliceCount));
  }

  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
    resetExceptionIgnores();
  }

  @Test
  public void doTest() throws Exception {
    cloudClient.waitForState(DEFAULT_COLLECTION, 30, TimeUnit.SECONDS,
        SolrCloudTestCase.activeClusterShape(sliceCount, 4));

    testBasicSelect();
    testWhere();
    testMixedCaseFields();
    testBasicGrouping();
    testBasicGroupingTint();
    testBasicGroupingIntLongPoints();
    testBasicGroupingFloatDoublePoints();
    testBasicGroupingFacets();
    testSelectDistinct();
    testSelectDistinctFacets();
    testAggregatesWithoutGrouping();
    testSQLException();
    testTimeSeriesGrouping();
    testTimeSeriesGroupingFacet();
    testParallelBasicGrouping();
    testParallelSelectDistinct();
    testParallelTimeSeriesGrouping();
  }

  private void testBasicSelect() throws Exception {

    CloudJettyRunner jetty = this.cloudJettys.get(0);

    del("*:*");

    commit();

    indexDoc(sdoc("id", "1", "text", "XXXX XXXX", "str_s", "a", "field_i", "7", "field_i_p", "7",
        "field_f_p", "7.5", "field_d_p", "7.5", "field_l_p", "7"));
    indexDoc(sdoc("id", "2", "text", "XXXX XXXX", "str_s", "b", "field_i", "8", "field_i_p", "8",
        "field_f_p", "8.5", "field_d_p", "8.5", "field_l_p", "8"));
    indexDoc(sdoc("id", "3", "text", "XXXX XXXX", "str_s", "a", "field_i", "20", "field_i_p", "20",
        "field_f_p", "20.5", "field_d_p", "20.5", "field_l_p", "20"));
    indexDoc(sdoc("id", "4", "text", "XXXX XXXX", "str_s", "b", "field_i", "11", "field_i_p", "11",
        "field_f_p", "11.5", "field_d_p", "11.5", "field_l_p", "11"));
    indexDoc(sdoc("id", "5", "text", "XXXX XXXX", "str_s", "c", "field_i", "30", "field_i_p", "30", "" +
        "field_f_p", "30.5", "field_d_p", "30.5", "field_l_p", "30"));
    indexDoc(sdoc("id", "6", "text", "XXXX XXXX", "str_s", "c", "field_i", "40", "field_i_p", "40",
        "field_f_p", "40.5", "field_d_p", "40.5", "field_l_p", "40"));
    indexDoc(sdoc("id", "7", "text", "XXXX XXXX", "str_s", "c", "field_i", "50", "field_i_p", "50",
        "field_f_p", "50.5", "field_d_p", "50.5", "field_l_p", "50"));
    indexDoc(sdoc("id", "8", "text", "XXXX XXXX", "str_s", "c", "field_i", "60", "field_i_p", "60",
        "field_f_p", "60.5", "field_d_p", "60.5", "field_l_p", "60"));
    commit();

    System.out.println("############# testBasicSelect() ############");

    SolrParams sParams = mapParams(CommonParams.QT, "/sql",
        "stmt",
        "select id, field_i, str_s, field_i_p, field_f_p, field_d_p, field_l_p from collection1 where (text='(XXXX)' OR text='XXXX') AND text='XXXX' order by field_i desc");

    SolrStream solrStream = new SolrStream(jetty.url, sParams);
    List<Tuple> tuples = getTuples(solrStream);

    assert (tuples.size() == 8);
    Tuple tuple;

    tuple = tuples.get(0);
    assertEquals(tuple.getLong("id").longValue(), 8);
    assertEquals(tuple.getLong("field_i").longValue(), 60);
    assert (tuple.get("str_s").equals("c"));
    assertEquals(tuple.getLong("field_i_p").longValue(), 60L);
    assertEquals(tuple.getDouble("field_f_p"), 60.5, 0.0);
    assertEquals(tuple.getDouble("field_d_p"), 60.5, 0.0);
    assertEquals(tuple.getLong("field_l_p").longValue(), 60);

    tuple = tuples.get(1);
    assertEquals(tuple.getLong("id").longValue(), 7);
    assertEquals(tuple.getLong("field_i").longValue(), 50);
    assert (tuple.get("str_s").equals("c"));
    assertEquals(tuple.getLong("field_i_p").longValue(), 50);
    assertEquals(tuple.getDouble("field_f_p"), 50.5, 0.0);
    assertEquals(tuple.getDouble("field_d_p"), 50.5, 0.0);
    assertEquals(tuple.getLong("field_l_p").longValue(), 50);

    tuple = tuples.get(2);
    assertEquals(tuple.getLong("id").longValue(), 6);
    assertEquals(tuple.getLong("field_i").longValue(), 40);
    assert (tuple.get("str_s").equals("c"));
    assertEquals(tuple.getLong("field_i_p").longValue(), 40);
    assertEquals(tuple.getDouble("field_f_p"), 40.5, 0.0);
    assertEquals(tuple.getDouble("field_d_p"), 40.5, 0.0);
    assertEquals(tuple.getLong("field_l_p").longValue(), 40);

    tuple = tuples.get(3);
    assertEquals(tuple.getLong("id").longValue(), 5);
    assertEquals(tuple.getLong("field_i").longValue(), 30);
    assert (tuple.get("str_s").equals("c"));
    assertEquals(tuple.getLong("field_i_p").longValue(), 30);
    assertEquals(tuple.getDouble("field_f_p"), 30.5, 0.0);
    assertEquals(tuple.getDouble("field_d_p"), 30.5, 0.0);
    assertEquals(tuple.getLong("field_l_p").longValue(), 30);

    tuple = tuples.get(4);
    assertEquals(tuple.getLong("id").longValue(), 3);
    assertEquals(tuple.getLong("field_i").longValue(), 20);
    assert (tuple.get("str_s").equals("a"));
    assertEquals(tuple.getLong("field_i_p").longValue(), 20);
    assertEquals(tuple.getDouble("field_f_p"), 20.5, 0.0);
    assertEquals(tuple.getDouble("field_d_p"), 20.5, 0.0);
    assertEquals(tuple.getLong("field_l_p").longValue(), 20);

    tuple = tuples.get(5);
    assertEquals(tuple.getLong("id").longValue(), 4);
    assertEquals(tuple.getLong("field_i").longValue(), 11);
    assert (tuple.get("str_s").equals("b"));
    assertEquals(tuple.getLong("field_i_p").longValue(), 11);
    assertEquals(tuple.getDouble("field_f_p"), 11.5, 0.0);
    assertEquals(tuple.getDouble("field_d_p"), 11.5, 0.0);
    assertEquals(tuple.getLong("field_l_p").longValue(), 11);

    tuple = tuples.get(6);
    assertEquals(tuple.getLong("id").longValue(), 2);
    assertEquals(tuple.getLong("field_i").longValue(), 8);
    assert (tuple.get("str_s").equals("b"));
    assertEquals(tuple.getLong("field_i_p").longValue(), 8);
    assertEquals(tuple.getDouble("field_f_p"), 8.5, 0.0);
    assertEquals(tuple.getDouble("field_d_p"), 8.5, 0.0);
    assertEquals(tuple.getLong("field_l_p").longValue(), 8);

    tuple = tuples.get(7);
    assertEquals(tuple.getLong("id").longValue(), 1);
    assertEquals(tuple.getLong("field_i").longValue(), 7);
    assert (tuple.get("str_s").equals("a"));
    assertEquals(tuple.getLong("field_i_p").longValue(), 7);
    assertEquals(tuple.getDouble("field_f_p"), 7.5, 0.0);
    assertEquals(tuple.getDouble("field_d_p"), 7.5, 0.0);
    assertEquals(tuple.getLong("field_l_p").longValue(), 7);

    // Assert field order
    assertResponseContains(clients.get(0), sParams,
        "{\"docs\":[{\"id\":\"8\",\"field_i\":60,\"str_s\":\"c\",\"field_i_p\":60,\"field_f_p\":60.5,\"field_d_p\":60.5,\"field_l_p\":60}");

    // Test unlimited unsorted result. Should sort on _version_ desc
    sParams = mapParams(CommonParams.QT, "/sql", "stmt",
        "select id, field_i, str_s from collection1 where text='XXXX'");

    solrStream = new SolrStream(jetty.url, sParams);
    tuples = getTuples(solrStream);

    assert (tuples.size() == 8);

    tuple = tuples.get(0);
    assert (tuple.getLong("id") == 8);
    assert (tuple.getLong("field_i") == 60);
    assert (tuple.get("str_s").equals("c"));

    tuple = tuples.get(1);
    assert (tuple.getLong("id") == 7);
    assert (tuple.getLong("field_i") == 50);
    assert (tuple.get("str_s").equals("c"));

    tuple = tuples.get(2);
    assert (tuple.getLong("id") == 6);
    assert (tuple.getLong("field_i") == 40);
    assert (tuple.get("str_s").equals("c"));

    tuple = tuples.get(3);
    assert (tuple.getLong("id") == 5);
    assert (tuple.getLong("field_i") == 30);
    assert (tuple.get("str_s").equals("c"));

    tuple = tuples.get(4);
    assert (tuple.getLong("id") == 4);
    assert (tuple.getLong("field_i") == 11);
    assert (tuple.get("str_s").equals("b"));

    tuple = tuples.get(5);
    assert (tuple.getLong("id") == 3);
    assert (tuple.getLong("field_i") == 20);
    assert (tuple.get("str_s").equals("a"));

    tuple = tuples.get(6);
    assert (tuple.getLong("id") == 2);
    assert (tuple.getLong("field_i") == 8);
    assert (tuple.get("str_s").equals("b"));

    tuple = tuples.get(7);
    assert (tuple.getLong("id") == 1);
    assert (tuple.getLong("field_i") == 7);
    assert (tuple.get("str_s").equals("a"));

    sParams = mapParams(CommonParams.QT, "/sql",
        "stmt", "select id, field_i, str_s from collection1 where text='XXXX' order by field_i desc limit 1");

    solrStream = new SolrStream(jetty.url, sParams);
    tuples = getTuples(solrStream);

    assert (tuples.size() == 1);

    tuple = tuples.get(0);
    assert (tuple.getLong("id") == 8);
    assert (tuple.getLong("field_i") == 60);
    assert (tuple.get("str_s").equals("c"));

    sParams = mapParams(CommonParams.QT, "/sql", "stmt",
        "select id, field_i, str_s from collection1 where text='XXXX' AND id='(1 2 3)' order by field_i desc");

    solrStream = new SolrStream(jetty.url, sParams);
    tuples = getTuples(solrStream);

    assert (tuples.size() == 3);

    tuple = tuples.get(0);
    assert (tuple.getLong("id") == 3);
    assert (tuple.getLong("field_i") == 20);
    assert (tuple.get("str_s").equals("a"));

    tuple = tuples.get(1);
    assert (tuple.getLong("id") == 2);
    assert (tuple.getLong("field_i") == 8);
    assert (tuple.get("str_s").equals("b"));

    tuple = tuples.get(2);
    assert (tuple.getLong("id") == 1);
    assert (tuple.getLong("field_i") == 7);
    assert (tuple.get("str_s").equals("a"));

    sParams = mapParams(CommonParams.QT, "/sql",
        "stmt",
        "select id as myId, field_i as myInt, str_s as myString from collection1 where text='XXXX' AND id='(1 2 3)' order by myInt desc");

    solrStream = new SolrStream(jetty.url, sParams);
    tuples = getTuples(solrStream);

    assert (tuples.size() == 3);

    tuple = tuples.get(0);
    assert (tuple.getLong("myId") == 3);
    assert (tuple.getLong("myInt") == 20);
    assert (tuple.get("myString").equals("a"));

    tuple = tuples.get(1);
    assert (tuple.getLong("myId") == 2);
    assert (tuple.getLong("myInt") == 8);
    assert (tuple.get("myString").equals("b"));

    tuple = tuples.get(2);
    assert (tuple.getLong("myId") == 1);
    assert (tuple.getLong("myInt") == 7);
    assert (tuple.get("myString").equals("a"));

    sParams = mapParams(CommonParams.QT, "/sql",
        "stmt",
        "select id as myId, field_i as myInt, str_s as myString from collection1 where text='XXXX' AND id='(1 2 3)' order by field_i desc");

    solrStream = new SolrStream(jetty.url, sParams);
    tuples = getTuples(solrStream);

    assert (tuples.size() == 3);

    tuple = tuples.get(0);
    assert (tuple.getLong("myId") == 3);
    assert (tuple.getLong("myInt") == 20);
    assert (tuple.get("myString").equals("a"));

    tuple = tuples.get(1);
    assert (tuple.getLong("myId") == 2);
    assert (tuple.getLong("myInt") == 8);
    assert (tuple.get("myString").equals("b"));

    tuple = tuples.get(2);
    assert (tuple.getLong("myId") == 1);
    assert (tuple.getLong("myInt") == 7);
    assert (tuple.get("myString").equals("a"));

    // Test after reload SOLR-9059//
    Replica leader = getShardLeader("collection1", "shard1", 30 /* timeout secs */);

    // reload collection and wait to see the core report it has been reloaded
    boolean wasReloaded = reloadCollection(leader, "collection1");
    assertTrue(wasReloaded);

    sParams = mapParams(CommonParams.QT, "/sql",
        "stmt",
        "select id as myId, field_i as myInt, str_s as myString from collection1 where text='XXXX' AND id='(1 2 3)' order by field_i desc");

    solrStream = new SolrStream(jetty.url, sParams);
    tuples = getTuples(solrStream);

    assert (tuples.size() == 3);

    tuple = tuples.get(0);
    assert (tuple.getLong("myId") == 3);
    assert (tuple.getLong("myInt") == 20);
    assert (tuple.get("myString").equals("a"));

    tuple = tuples.get(1);
    assert (tuple.getLong("myId") == 2);
    assert (tuple.getLong("myInt") == 8);
    assert (tuple.get("myString").equals("b"));

    tuple = tuples.get(2);
    assert (tuple.getLong("myId") == 1);
    assert (tuple.getLong("myInt") == 7);
    assert (tuple.get("myString").equals("a"));

    // SOLR-8845 - Test to make sure that 1 = 0 works for things like Spark SQL
    sParams = mapParams(CommonParams.QT, "/sql",
        "stmt", "select id, field_i, str_s from collection1 where 1 = 0");

    solrStream = new SolrStream(jetty.url, sParams);
    tuples = getTuples(solrStream);

    assertEquals(0, tuples.size());

  }

  private void testWhere() throws Exception {

    CloudJettyRunner jetty = this.cloudJettys.get(0);

    del("*:*");

    commit();

    indexDoc(sdoc("id", "1", "text", "XXXX XXXX", "str_s", "a", "field_i", "7"));
    indexDoc(sdoc("id", "2", "text", "XXXX XXXX", "str_s", "b", "field_i", "8"));
    indexDoc(sdoc("id", "3", "text", "XXXX XXXX", "str_s", "a", "field_i", "20"));
    indexDoc(sdoc("id", "4", "text", "XXXX XXXX", "str_s", "b", "field_i", "11"));
    indexDoc(sdoc("id", "5", "text", "XXXX XXXX", "str_s", "c", "field_i", "30"));
    indexDoc(sdoc("id", "6", "text", "XXXX XXXX", "str_s", "c", "field_i", "40"));
    indexDoc(sdoc("id", "7", "text", "XXXX XXXX", "str_s", "c", "field_i", "50"));
    indexDoc(sdoc("id", "8", "text", "XXXX XXXX", "str_s", "c", "field_i", "60"));
    commit();

    // Equals
    SolrParams sParams = mapParams(CommonParams.QT, "/sql",
        "stmt", "select id from collection1 where id = 1 order by id asc");

    SolrStream solrStream = new SolrStream(jetty.url, sParams);
    List<Tuple> tuples = getTuples(solrStream);

    assertEquals(1, tuples.size());

    Tuple tuple = tuples.get(0);
    assertEquals("1", tuple.get("id"));

    // Not Equals <>
    sParams = mapParams(CommonParams.QT, "/sql",
        "stmt", "select id from collection1 where id <> 1 order by id asc limit 10");

    solrStream = new SolrStream(jetty.url, sParams);
    tuples = getTuples(solrStream);

    assertEquals(7, tuples.size());

    tuple = tuples.get(0);
    assertEquals("2", tuple.get("id"));
    tuple = tuples.get(1);
    assertEquals("3", tuple.get("id"));
    tuple = tuples.get(2);
    assertEquals("4", tuple.get("id"));
    tuple = tuples.get(3);
    assertEquals("5", tuple.get("id"));
    tuple = tuples.get(4);
    assertEquals("6", tuple.get("id"));
    tuple = tuples.get(5);
    assertEquals("7", tuple.get("id"));
    tuple = tuples.get(6);
    assertEquals("8", tuple.get("id"));

    // TODO requires different Calcite SQL conformance level
    // Not Equals !=
    // sParams = mapParams(CommonParams.QT, "/sql",
    // "stmt", "select id from collection1 where id != 1 order by id asc limit 10");
    //
    // solrStream = new SolrStream(jetty.url, sParams);
    // tuples = getTuples(solrStream);
    //
    // assertEquals(7, tuples.size());
    //
    // tuple = tuples.get(0);
    // assertEquals(2L, tuple.get("id"));
    // tuple = tuples.get(1);
    // assertEquals(3L, tuple.get("id"));
    // tuple = tuples.get(2);
    // assertEquals(4L, tuple.get("id"));
    // tuple = tuples.get(3);
    // assertEquals(5L, tuple.get("id"));
    // tuple = tuples.get(4);
    // assertEquals(6L, tuple.get("id"));
    // tuple = tuples.get(5);
    // assertEquals(7L, tuple.get("id"));
    // tuple = tuples.get(6);
    // assertEquals(8L, tuple.get("id"));

    // Less than
    sParams = mapParams(CommonParams.QT, "/sql",
        "stmt", "select id from collection1 where id < 2 order by id asc");

    solrStream = new SolrStream(jetty.url, sParams);
    tuples = getTuples(solrStream);

    assertEquals(1, tuples.size());

    tuple = tuples.get(0);
    assertEquals("1", tuple.get("id"));

    // Less than equal
    sParams = mapParams(CommonParams.QT, "/sql",
        "stmt", "select id from collection1 where id <= 2 order by id asc");

    solrStream = new SolrStream(jetty.url, sParams);
    tuples = getTuples(solrStream);

    assertEquals(2, tuples.size());

    tuple = tuples.get(0);
    assertEquals("1", tuple.get("id"));
    tuple = tuples.get(1);
    assertEquals("2", tuple.get("id"));

    // Greater than
    sParams = mapParams(CommonParams.QT, "/sql",
        "stmt", "select id from collection1 where id > 7 order by id asc");

    solrStream = new SolrStream(jetty.url, sParams);
    tuples = getTuples(solrStream);

    assertEquals(1, tuples.size());

    tuple = tuples.get(0);
    assertEquals("8", tuple.get("id"));

    // Greater than equal
    sParams = mapParams(CommonParams.QT, "/sql",
        "stmt", "select id from collection1 where id >= 7 order by id asc");

    solrStream = new SolrStream(jetty.url, sParams);
    tuples = getTuples(solrStream);

    assertEquals(2, tuples.size());

    tuple = tuples.get(0);
    assertEquals("7", tuple.get("id"));
    tuple = tuples.get(1);
    assertEquals("8", tuple.get("id"));

  }

  private void testMixedCaseFields() throws Exception {

    CloudJettyRunner jetty = this.cloudJettys.get(0);

    del("*:*");

    commit();

    indexDoc(sdoc("id", "1", "Text_t", "XXXX XXXX", "Str_s", "a", "Field_i", "7"));
    indexDoc(sdoc("id", "2", "Text_t", "XXXX XXXX", "Str_s", "b", "Field_i", "8"));
    indexDoc(sdoc("id", "3", "Text_t", "XXXX XXXX", "Str_s", "a", "Field_i", "20"));
    indexDoc(sdoc("id", "4", "Text_t", "XXXX XXXX", "Str_s", "b", "Field_i", "11"));
    indexDoc(sdoc("id", "5", "Text_t", "XXXX XXXX", "Str_s", "c", "Field_i", "30"));
    indexDoc(sdoc("id", "6", "Text_t", "XXXX XXXX", "Str_s", "c", "Field_i", "40"));
    indexDoc(sdoc("id", "7", "Text_t", "XXXX XXXX", "Str_s", "c", "Field_i", "50"));
    indexDoc(sdoc("id", "8", "Text_t", "XXXX XXXX", "Str_s", "c", "Field_i", "60"));
    commit();

    SolrParams sParams = mapParams(CommonParams.QT, "/sql", "aggregationMode", "map_reduce",
        "stmt", "select id, Field_i, Str_s from collection1 where Text_t='XXXX' order by Field_i desc");

    SolrStream solrStream = new SolrStream(jetty.url, sParams);
    List<Tuple> tuples = getTuples(solrStream);

    assert (tuples.size() == 8);

    Tuple tuple;

    tuple = tuples.get(0);
    assert (tuple.getLong("id") == 8);
    assert (tuple.getLong("Field_i") == 60);
    assert (tuple.get("Str_s").equals("c"));

    tuple = tuples.get(1);
    assert (tuple.getLong("id") == 7);
    assert (tuple.getLong("Field_i") == 50);
    assert (tuple.get("Str_s").equals("c"));

    tuple = tuples.get(2);
    assert (tuple.getLong("id") == 6);
    assert (tuple.getLong("Field_i") == 40);
    assert (tuple.get("Str_s").equals("c"));

    tuple = tuples.get(3);
    assert (tuple.getLong("id") == 5);
    assert (tuple.getLong("Field_i") == 30);
    assert (tuple.get("Str_s").equals("c"));

    tuple = tuples.get(4);
    assert (tuple.getLong("id") == 3);
    assert (tuple.getLong("Field_i") == 20);
    assert (tuple.get("Str_s").equals("a"));

    tuple = tuples.get(5);
    assert (tuple.getLong("id") == 4);
    assert (tuple.getLong("Field_i") == 11);
    assert (tuple.get("Str_s").equals("b"));

    tuple = tuples.get(6);
    assert (tuple.getLong("id") == 2);
    assert (tuple.getLong("Field_i") == 8);
    assert (tuple.get("Str_s").equals("b"));

    tuple = tuples.get(7);
    assert (tuple.getLong("id") == 1);
    assert (tuple.getLong("Field_i") == 7);
    assert (tuple.get("Str_s").equals("a"));

    // TODO get sum(Field_i) as named one
    sParams = mapParams(CommonParams.QT, "/sql",
        "stmt",
        "select Str_s, sum(Field_i) from collection1 where id='(1 8)' group by Str_s having (sum(Field_i) = 7 OR sum(Field_i) = 60) order by sum(Field_i) desc");

    solrStream = new SolrStream(jetty.url, sParams);
    tuples = getTuples(solrStream);

    assert (tuples.size() == 2);

    tuple = tuples.get(0);
    assert (tuple.get("Str_s").equals("c"));
    assert (tuple.getDouble("EXPR$1") == 60);

    tuple = tuples.get(1);
    assert (tuple.get("Str_s").equals("a"));
    assert (tuple.getDouble("EXPR$1") == 7);

    sParams = mapParams(CommonParams.QT, "/sql", "aggregationMode", "map_reduce",
        "stmt",
        "select Str_s, sum(Field_i) from collection1 where id='(1 8)' group by Str_s having (sum(Field_i) = 7 OR sum(Field_i) = 60) order by sum(Field_i) desc");

    solrStream = new SolrStream(jetty.url, sParams);
    tuples = getTuples(solrStream);

    assert (tuples.size() == 2);

    tuple = tuples.get(0);
    assert (tuple.get("Str_s").equals("c"));
    assert (tuple.getDouble("EXPR$1") == 60);

    tuple = tuples.get(1);
    assert (tuple.get("Str_s").equals("a"));
    assert (tuple.getDouble("EXPR$1") == 7);

  }

  private void testSQLException() throws Exception {

    CloudJettyRunner jetty = this.cloudJettys.get(0);

    del("*:*");

    commit();

    indexDoc(sdoc("id", "1", "text", "XXXX XXXX", "str_s", "a", "field_i", "7"));
    indexDoc(sdoc("id", "2", "text", "XXXX XXXX", "str_s", "b", "field_i", "8"));
    indexDoc(sdoc("id", "3", "text", "XXXX XXXX", "str_s", "a", "field_i", "20"));
    indexDoc(sdoc("id", "4", "text", "XXXX XXXX", "str_s", "b", "field_i", "11"));
    indexDoc(sdoc("id", "5", "text", "XXXX XXXX", "str_s", "c", "field_i", "30"));
    indexDoc(sdoc("id", "6", "text", "XXXX XXXX", "str_s", "c", "field_i", "40"));
    indexDoc(sdoc("id", "7", "text", "XXXX XXXX", "str_s", "c", "field_i", "50"));
    indexDoc(sdoc("id", "8", "text", "XXXX XXXX", "str_s", "c", "field_i", "60"));
    commit();

    SolrParams sParams = mapParams(CommonParams.QT, "/sql", "aggregationMode", "map_reduce",
        "stmt", "select id, str_s from collection1 where text='XXXX' order by field_iff desc");

    SolrStream solrStream = new SolrStream(jetty.url, sParams);
    Tuple tuple = getTuple(new ExceptionStream(solrStream));
    assert (tuple.EOF);
    assert (tuple.EXCEPTION);
    assert (tuple.getException().contains("Column 'field_iff' not found in any table"));

    sParams = mapParams(CommonParams.QT, "/sql",
        "stmt", "select id, field_iff, str_s from collection1 where text='XXXX' order by field_iff desc");

    solrStream = new SolrStream(jetty.url, sParams);
    tuple = getTuple(new ExceptionStream(solrStream));
    assert (tuple.EOF);
    assert (tuple.EXCEPTION);

    assert (tuple.getException().contains("Column 'field_iff' not found in any table"));

    sParams = mapParams(CommonParams.QT, "/sql", "aggregationMode", "map_reduce",
        "stmt",
        "select str_s, count(*), sum(field_iff), min(field_i), max(field_i), cast(avg(1.0 * field_i) as float) from collection1 where text='XXXX' group by str_s having ((sum(field_iff) = 19) AND (min(field_i) = 8))");

    solrStream = new SolrStream(jetty.url, sParams);
    tuple = getTuple(new ExceptionStream(solrStream));
    assert (tuple.EOF);
    assert (tuple.EXCEPTION);
    assert (tuple.getException().contains("Column 'field_iff' not found in any table"));

    sParams = mapParams(CommonParams.QT, "/sql", "aggregationMode", "map_reduce",
        "stmt",
        "select str_s, count(*), blah(field_i), min(field_i), max(field_i), cast(avg(1.0 * field_i) as float) from collection1 where text='XXXX' group by str_s having ((sum(field_i) = 19) AND (min(field_i) = 8))");

    solrStream = new SolrStream(jetty.url, sParams);
    tuple = getTuple(new ExceptionStream(solrStream));
    assert (tuple.EOF);
    assert (tuple.EXCEPTION);
    assert (tuple.getException().contains("No match found for function signature blah"));

  }

  private void testBasicGrouping() throws Exception {

    CloudJettyRunner jetty = this.cloudJettys.get(0);

    del("*:*");

    commit();

    indexr("id", "1", "text", "XXXX XXXX", "str_s", "a", "field_i", "7");
    indexr("id", "2", "text", "XXXX XXXX", "str_s", "b", "field_i", "8");
    indexr("id", "3", "text", "XXXX XXXX", "str_s", "a", "field_i", "20");
    indexr("id", "4", "text", "XXXX XXXX", "str_s", "b", "field_i", "11");
    indexr("id", "5", "text", "XXXX XXXX", "str_s", "c", "field_i", "30");
    indexr("id", "6", "text", "XXXX XXXX", "str_s", "c", "field_i", "40");
    indexr("id", "7", "text", "XXXX XXXX", "str_s", "c", "field_i", "50");
    indexr("id", "8", "text", "XXXX XXXX", "str_s", "c", "field_i", "60");
    indexr("id", "9", "text", "XXXX XXXY", "str_s", "d", "field_i", "70");
    commit();

    SolrParams sParams = mapParams(CommonParams.QT, "/sql", "aggregationMode", "map_reduce",
        "stmt",
        "select str_s, count(*), sum(field_i), min(field_i), max(field_i), avg(field_i) from collection1 where text='XXXX' group by str_s order by sum(field_i) asc limit 2");

    SolrStream solrStream = new SolrStream(jetty.url, sParams);
    List<Tuple> tuples = getTuples(solrStream);

    // Only two results because of the limit.
    assert (tuples.size() == 2);
    Tuple tuple;

    tuple = tuples.get(0);
    assert (tuple.get("str_s").equals("b"));
    assert (tuple.getDouble("EXPR$1") == 2); // count(*)
    assert (tuple.getDouble("EXPR$2") == 19); // sum(field_i)
    assert (tuple.getDouble("EXPR$3") == 8); // min(field_i)
    assert (tuple.getDouble("EXPR$4") == 11); // max(field_i)
    assert (tuple.getDouble("EXPR$5") == 10); // avg(field_i)

    tuple = tuples.get(1);
    assert (tuple.get("str_s").equals("a"));
    assert (tuple.getDouble("EXPR$1") == 2); // count(*)
    assert (tuple.getDouble("EXPR$2") == 27); // sum(field_i)
    assert (tuple.getDouble("EXPR$3") == 7); // min(field_i)
    assert (tuple.getDouble("EXPR$4") == 20); // max(field_i)
    assert (tuple.getDouble("EXPR$5") == 14); // avg(field_i)

    sParams = mapParams(CommonParams.QT, "/sql", "aggregationMode", "map_reduce",
        "stmt",
        "select str_s, count(*), sum(field_i), min(field_i), max(field_i), cast(avg(1.0 * field_i) as float) as blah from collection1 where text='XXXX' group by str_s order by sum(field_i) asc limit 2");

    solrStream = new SolrStream(jetty.url, sParams);
    tuples = getTuples(solrStream);

    // Only two results because of the limit.
    assert (tuples.size() == 2);

    tuple = tuples.get(0);
    assert (tuple.get("str_s").equals("b"));
    assert (tuple.getDouble("EXPR$1") == 2); // count(*)
    assert (tuple.getDouble("EXPR$2") == 19); // sum(field_i)
    assert (tuple.getDouble("EXPR$3") == 8); // min(field_i)
    assert (tuple.getDouble("EXPR$4") == 11); // max(field_i)
    assert (tuple.getDouble("blah") == 9.5); // avg(field_i)

    tuple = tuples.get(1);
    assert (tuple.get("str_s").equals("a"));
    assert (tuple.getDouble("EXPR$1") == 2); // count(*)
    assert (tuple.getDouble("EXPR$2") == 27); // sum(field_i)
    assert (tuple.getDouble("EXPR$3") == 7); // min(field_i)
    assert (tuple.getDouble("EXPR$4") == 20); // max(field_i)
    assert (tuple.getDouble("blah") == 13.5); // avg(field_i)

    sParams = mapParams(CommonParams.QT, "/sql", "aggregationMode", "map_reduce",
        "stmt",
        "select str_s as myString, count(*), sum(field_i) as mySum, min(field_i), max(field_i), avg(field_i)  from collection1 where text='XXXX' group by str_s order by mySum asc limit 2");

    solrStream = new SolrStream(jetty.url, sParams);
    tuples = getTuples(solrStream);

    // Only two results because of the limit.
    assert (tuples.size() == 2);

    tuple = tuples.get(0);
    assert (tuple.get("myString").equals("b"));
    assert (tuple.getDouble("EXPR$1") == 2); // count(*)
    assert (tuple.getDouble("mySum") == 19);
    assert (tuple.getDouble("EXPR$3") == 8); // min(field_i)
    assert (tuple.getDouble("EXPR$4") == 11); // max(field_i)
    assert (tuple.getDouble("EXPR$5") == 10); // avg(field_i)

    tuple = tuples.get(1);
    assert (tuple.get("myString").equals("a"));
    assert (tuple.getDouble("EXPR$1") == 2); // count(*)
    assert (tuple.getDouble("mySum") == 27);
    assert (tuple.getDouble("EXPR$3") == 7); // min(field_i)
    assert (tuple.getDouble("EXPR$4") == 20); // max(field_i)
    assert (tuple.getDouble("EXPR$5") == 14); // avg(field_i)

    sParams = mapParams(CommonParams.QT, "/sql", "aggregationMode", "map_reduce",
        "stmt", "select str_s, count(*), sum(field_i), min(field_i), max(field_i), "
            + "avg(field_i) from collection1 where (text='XXXX' AND NOT ((text='XXXY') AND (text='XXXY' OR text='XXXY'))) "
            + "group by str_s order by str_s desc");

    solrStream = new SolrStream(jetty.url, sParams);
    tuples = getTuples(solrStream);

    // The sort by and order by match and no limit is applied. All the Tuples should be returned in
    // this scenario.

    assert (tuples.size() == 3);

    tuple = tuples.get(0);
    assert (tuple.get("str_s").equals("c"));
    assert (tuple.getDouble("EXPR$1") == 4); // count(*)
    assert (tuple.getDouble("EXPR$2") == 180); // sum(field_i)
    assert (tuple.getDouble("EXPR$3") == 30); // min(field_i)
    assert (tuple.getDouble("EXPR$4") == 60); // max(field_i)
    assert (tuple.getDouble("EXPR$5") == 45); // avg(field_i)

    tuple = tuples.get(1);
    assert (tuple.get("str_s").equals("b"));
    assert (tuple.getDouble("EXPR$1") == 2); // count(*)
    assert (tuple.getDouble("EXPR$2") == 19); // sum(field_i)
    assert (tuple.getDouble("EXPR$3") == 8); // min(field_i)
    assert (tuple.getDouble("EXPR$4") == 11); // max(field_i)
    assert (tuple.getDouble("EXPR$5") == 10D); // avg(field_i)

    tuple = tuples.get(2);
    assert (tuple.get("str_s").equals("a"));
    assert (tuple.getDouble("EXPR$1") == 2); // count(*)
    assert (tuple.getDouble("EXPR$2") == 27); // sum(field_i)
    assert (tuple.getDouble("EXPR$3") == 7); // min(field_i)
    assert (tuple.getDouble("EXPR$4") == 20); // max(field_i)

    assert (tuple.getDouble("EXPR$5") == 14); // avg(field_i)

    sParams = mapParams(CommonParams.QT, "/sql", "aggregationMode", "map_reduce",
        "stmt", "select str_s as myString, count(*) as myCount, sum(field_i) as mySum, min(field_i) as myMin, "
            + "max(field_i) as myMax, avg(field_i) as myAvg from collection1 "
            + "where (text='XXXX' AND NOT (text='XXXY')) group by str_s order by str_s desc");

    solrStream = new SolrStream(jetty.url, sParams);
    tuples = getTuples(solrStream);

    // The sort by and order by match and no limit is applied. All the Tuples should be returned in
    // this scenario.

    assert (tuples.size() == 3);

    tuple = tuples.get(0);
    assert (tuple.get("myString").equals("c"));
    assert (tuple.getDouble("myCount") == 4);
    assert (tuple.getDouble("mySum") == 180);
    assert (tuple.getDouble("myMin") == 30);
    assert (tuple.getDouble("myMax") == 60);
    assert (tuple.getDouble("myAvg") == 45);

    tuple = tuples.get(1);
    assert (tuple.get("myString").equals("b"));
    assert (tuple.getDouble("myCount") == 2);
    assert (tuple.getDouble("mySum") == 19);
    assert (tuple.getDouble("myMin") == 8);
    assert (tuple.getDouble("myMax") == 11);
    assert (tuple.getDouble("myAvg") == 10);

    tuple = tuples.get(2);
    assert (tuple.get("myString").equals("a"));
    assert (tuple.getDouble("myCount") == 2);
    assert (tuple.getDouble("mySum") == 27);
    assert (tuple.getDouble("myMin") == 7);
    assert (tuple.getDouble("myMax") == 20);
    assert (tuple.getDouble("myAvg") == 14);

    sParams = mapParams(CommonParams.QT, "/sql", "aggregationMode", "map_reduce",
        "stmt", "select str_s, count(*), sum(field_i), min(field_i), max(field_i), avg(field_i) " +
            "from collection1 where text='XXXX' group by str_s having sum(field_i) = 19");

    solrStream = new SolrStream(jetty.url, sParams);
    tuples = getTuples(solrStream);

    assert (tuples.size() == 1);

    tuple = tuples.get(0);
    assert (tuple.get("str_s").equals("b"));
    assert (tuple.getDouble("EXPR$1") == 2); // count(*)
    assert (tuple.getDouble("EXPR$2") == 19); // sum(field_i)
    assert (tuple.getDouble("EXPR$3") == 8); // min(field_i)
    assert (tuple.getDouble("EXPR$4") == 11); // max(field_i)
    assert (tuple.getDouble("EXPR$5") == 10); // avg(field_i)

    sParams = mapParams(CommonParams.QT, "/sql", "aggregationMode", "map_reduce",
        "stmt", "select str_s, count(*), sum(field_i), min(field_i), max(field_i), avg(field_i) " +
            "from collection1 where text='XXXX' group by str_s having ((sum(field_i) = 19) AND (min(field_i) = 8))");

    solrStream = new SolrStream(jetty.url, sParams);
    tuples = getTuples(solrStream);

    assert (tuples.size() == 1);

    tuple = tuples.get(0);
    assert (tuple.get("str_s").equals("b"));
    assert (tuple.getDouble("EXPR$1") == 2); // count(*)
    assert (tuple.getDouble("EXPR$2") == 19); // sum(field_i)
    assert (tuple.getDouble("EXPR$3") == 8); // min(field_i)
    assert (tuple.getDouble("EXPR$4") == 11); // max(field_i)
    assert (tuple.getDouble("EXPR$5") == 10); // avg(field_i)

    sParams = mapParams(CommonParams.QT, "/sql", "aggregationMode", "map_reduce",
        "stmt", "select str_s, count(*), sum(field_i) as mySum, min(field_i), max(field_i), " +
            "avg(field_i) from collection1 where text='XXXX' group by str_s " +
            "having ((sum(field_i) = 19) AND (min(field_i) = 8))");

    solrStream = new SolrStream(jetty.url, sParams);
    tuples = getTuples(solrStream);

    assert (tuples.size() == 1);

    tuple = tuples.get(0);
    assert (tuple.get("str_s").equals("b"));
    assert (tuple.getDouble("EXPR$1") == 2); // count(*)
    assert (tuple.getDouble("mySum") == 19);
    assert (tuple.getDouble("EXPR$3") == 8); // min(field_i)
    assert (tuple.getDouble("EXPR$4") == 11); // max(field_i)
    assert (tuple.getDouble("EXPR$5") == 10); // avg(field_i)

    sParams = mapParams(CommonParams.QT, "/sql", "aggregationMode", "map_reduce",
        "stmt", "select str_s, count(*), sum(field_i), min(field_i), max(field_i), " +
            "avg(field_i) from collection1 where text='XXXX' group by str_s " +
            "having ((sum(field_i) = 19) AND (min(field_i) = 100))");

    solrStream = new SolrStream(jetty.url, sParams);
    tuples = getTuples(solrStream);

    assert (tuples.size() == 0);

  }

  private void testBasicGroupingTint() throws Exception {

    CloudJettyRunner jetty = this.cloudJettys.get(0);

    del("*:*");

    commit();

    indexr("id", "1", "text", "XXXX XXXX", "str_s", "a", "field_ti", "7");
    indexr("id", "2", "text", "XXXX XXXX", "str_s", "b", "field_ti", "8");
    indexr("id", "3", "text", "XXXX XXXX", "str_s", "a", "field_ti", "20");
    indexr("id", "4", "text", "XXXX XXXX", "str_s", "b", "field_ti", "11");
    indexr("id", "5", "text", "XXXX XXXX", "str_s", "c", "field_ti", "30");
    indexr("id", "6", "text", "XXXX XXXX", "str_s", "c", "field_ti", "40");
    indexr("id", "7", "text", "XXXX XXXX", "str_s", "c", "field_ti", "50");
    indexr("id", "8", "text", "XXXX XXXX", "str_s", "c", "field_ti", "60");
    indexr("id", "9", "text", "XXXX XXXY", "str_s", "d", "field_ti", "70");
    commit();

    SolrParams sParams = mapParams(CommonParams.QT, "/sql",
        "stmt",
        "select str_s, count(*), sum(field_ti), min(field_ti), max(field_ti), avg(field_ti) from collection1 where text='XXXX' group by str_s order by sum(field_ti) asc limit 2");

    SolrStream solrStream = new SolrStream(jetty.url, sParams);
    List<Tuple> tuples = getTuples(solrStream);

    // Only two results because of the limit.
    assert (tuples.size() == 2);
    Tuple tuple;

    tuple = tuples.get(0);
    assert (tuple.get("str_s").equals("b"));
    assert (tuple.getDouble("EXPR$1") == 2); // count(*)
    assert (tuple.getDouble("EXPR$2") == 19); // sum(field_i)
    assert (tuple.getDouble("EXPR$3") == 8); // min(field_i)
    assert (tuple.getDouble("EXPR$4") == 11); // max(field_i)
    assert (tuple.getDouble("EXPR$5") == 10); // avg(field_i)

    tuple = tuples.get(1);
    assert (tuple.get("str_s").equals("a"));
    assert (tuple.getDouble("EXPR$1") == 2); // count(*)
    assert (tuple.getDouble("EXPR$2") == 27); // sum(field_i)
    assert (tuple.getDouble("EXPR$3") == 7); // min(field_i)
    assert (tuple.getDouble("EXPR$4") == 20); // max(field_i)
    assert (tuple.getDouble("EXPR$5") == 14); // avg(field_i)

  }

  private void testBasicGroupingIntLongPoints() throws Exception {

    Random random = random();
    int r = random.nextInt(2);
    String[] intOrLong = {"field_i_p", "field_l_p"};
    String[] facetOrMap = {"facet", "map_reduce"};
    String field = intOrLong[r];
    r = random.nextInt(2);
    String mode = facetOrMap[r];
    CloudJettyRunner jetty = this.cloudJettys.get(0);

    del("*:*");

    commit();

    indexr("id", "1", "text", "XXXX XXXX", "str_s", "a", field, "7");
    indexr("id", "2", "text", "XXXX XXXX", "str_s", "b", field, "8");
    indexr("id", "3", "text", "XXXX XXXX", "str_s", "a", field, "20");
    indexr("id", "4", "text", "XXXX XXXX", "str_s", "b", field, "11");
    indexr("id", "5", "text", "XXXX XXXX", "str_s", "c", field, "30");
    indexr("id", "6", "text", "XXXX XXXX", "str_s", "c", field, "40");
    indexr("id", "7", "text", "XXXX XXXX", "str_s", "c", field, "50");
    indexr("id", "8", "text", "XXXX XXXX", "str_s", "c", field, "60");
    indexr("id", "9", "text", "XXXX XXXY", "str_s", "d", field, "70");
    commit();

    SolrParams sParams = mapParams(CommonParams.QT, "/sql", "aggregationMode", mode,
        "stmt", "select str_s, count(*), sum(" + field + "), min(" + field + "), max(" + field + "), avg(" + field
            + ") from collection1 where text='XXXX' group by str_s order by sum(" + field + ") asc limit 2");

    SolrStream solrStream = new SolrStream(jetty.url, sParams);
    List<Tuple> tuples = getTuples(solrStream);

    // Only two results because of the limit.
    assert (tuples.size() == 2);
    Tuple tuple;

    tuple = tuples.get(0);
    assert (tuple.get("str_s").equals("b"));
    assert (tuple.getDouble("EXPR$1") == 2); // count(*)
    assert (tuple.getDouble("EXPR$2") == 19); // sum(field_i)
    assert (tuple.getDouble("EXPR$3") == 8); // min(field_i)
    assert (tuple.getDouble("EXPR$4") == 11); // max(field_i)
    assert (tuple.getDouble("EXPR$5") == 10); // avg(field_i)

    tuple = tuples.get(1);
    assert (tuple.get("str_s").equals("a"));
    assert (tuple.getDouble("EXPR$1") == 2); // count(*)
    assert (tuple.getDouble("EXPR$2") == 27); // sum(field_i)
    assert (tuple.getDouble("EXPR$3") == 7); // min(field_i)
    assert (tuple.getDouble("EXPR$4") == 20); // max(field_i)
    assert (tuple.getDouble("EXPR$5") == 14); // avg(field_i)

  }

  private void testBasicGroupingFloatDoublePoints() throws Exception {

    Random random = random();
    int r = random.nextInt(2);
    String[] intOrLong = {"field_f_p", "field_d_p"};
    String[] facetOrMap = {"facet", "map_reduce"};
    String field = intOrLong[r];
    r = random.nextInt(2);
    String mode = facetOrMap[r];

    CloudJettyRunner jetty = this.cloudJettys.get(0);

    del("*:*");

    commit();

    indexr("id", "1", "text", "XXXX XXXX", "str_s", "a", field, "7.0");
    indexr("id", "2", "text", "XXXX XXXX", "str_s", "b", field, "8.0");
    indexr("id", "3", "text", "XXXX XXXX", "str_s", "a", field, "20.0");
    indexr("id", "4", "text", "XXXX XXXX", "str_s", "b", field, "11.0");
    indexr("id", "5", "text", "XXXX XXXX", "str_s", "c", field, "30.0");
    indexr("id", "6", "text", "XXXX XXXX", "str_s", "c", field, "40.0");
    indexr("id", "7", "text", "XXXX XXXX", "str_s", "c", field, "50.0");
    indexr("id", "8", "text", "XXXX XXXX", "str_s", "c", field, "60.0");
    indexr("id", "9", "text", "XXXX XXXY", "str_s", "d", field, "70.0");
    commit();

    SolrParams sParams = mapParams(CommonParams.QT, "/sql", "aggregationMode", mode,
        "stmt", "select str_s, count(*), sum(" + field + "), min(" + field + "), max(" + field + "), avg(" + field
            + ") from collection1 where text='XXXX' group by str_s order by sum(" + field + ") asc limit 2");

    SolrStream solrStream = new SolrStream(jetty.url, sParams);
    List<Tuple> tuples = getTuples(solrStream);

    // Only two results because of the limit.
    assert (tuples.size() == 2);
    Tuple tuple;

    tuple = tuples.get(0);
    assert (tuple.get("str_s").equals("b"));
    assertEquals(tuple.getDouble("EXPR$1"), 2, 0.0); // count(*)
    assertEquals(tuple.getDouble("EXPR$2"), 19, 0.0); // sum(field_i)
    assertEquals(tuple.getDouble("EXPR$3"), 8, 0.0); // min(field_i)
    assertEquals(tuple.getDouble("EXPR$4"), 11, 0.0); // max(field_i)
    assertEquals(tuple.getDouble("EXPR$5"), 9.5, 0.0); // avg(field_i)

    tuple = tuples.get(1);
    assert (tuple.get("str_s").equals("a"));
    assertEquals(tuple.getDouble("EXPR$1"), 2, 0.0); // count(*)
    assertEquals(tuple.getDouble("EXPR$2"), 27, 0.0); // sum(field_i)
    assertEquals(tuple.getDouble("EXPR$3"), 7, 0.0); // min(field_i)
    assertEquals(tuple.getDouble("EXPR$4"), 20, 0.0); // max(field_i)
    assertEquals(tuple.getDouble("EXPR$5"), 13.5, 0.0); // avg(field_i)

  }

  private void testSelectDistinctFacets() throws Exception {

    CloudJettyRunner jetty = this.cloudJettys.get(0);

    del("*:*");

    commit();

    indexr("id", "1", "text", "XXXX XXXX", "str_s", "a", "field_i", "1");
    indexr("id", "2", "text", "XXXX XXXX", "str_s", "b", "field_i", "2");
    indexr("id", "3", "text", "XXXX XXXX", "str_s", "a", "field_i", "20");
    indexr("id", "4", "text", "XXXX XXXX", "str_s", "b", "field_i", "2");
    indexr("id", "5", "text", "XXXX XXXX", "str_s", "c", "field_i", "30");
    indexr("id", "6", "text", "XXXX XXXX", "str_s", "c", "field_i", "30");
    indexr("id", "7", "text", "XXXX XXXX", "str_s", "c", "field_i", "50");
    indexr("id", "8", "text", "XXXX XXXX", "str_s", "c", "field_i", "60");
    commit();

    SolrParams sParams = mapParams(CommonParams.QT, "/sql", "aggregationMode", "facet",
        "stmt", "select distinct str_s, field_i from collection1 order by str_s asc, field_i asc");

    System.out.println("######## selectDistinctFacets #######");

    SolrStream solrStream = new SolrStream(jetty.url, sParams);
    List<Tuple> tuples = getTuples(solrStream);

    // assert(false);
    assert (tuples.size() == 6);

    Tuple tuple;

    tuple = tuples.get(0);
    assert (tuple.get("str_s").equals("a"));
    assert (tuple.getLong("field_i") == 1);

    tuple = tuples.get(1);
    assert (tuple.get("str_s").equals("a"));
    assert (tuple.getLong("field_i") == 20);

    tuple = tuples.get(2);
    assert (tuple.get("str_s").equals("b"));
    assert (tuple.getLong("field_i") == 2);

    tuple = tuples.get(3);
    assert (tuple.get("str_s").equals("c"));
    assert (tuple.getLong("field_i") == 30);

    tuple = tuples.get(4);
    assert (tuple.get("str_s").equals("c"));
    assert (tuple.getLong("field_i") == 50);

    tuple = tuples.get(5);
    assert (tuple.get("str_s").equals("c"));
    assert (tuple.getLong("field_i") == 60);

    // reverse the sort
    sParams = mapParams(CommonParams.QT, "/sql", "aggregationMode", "facet",
        "stmt", "select distinct str_s, field_i from collection1 order by str_s desc, field_i desc");

    solrStream = new SolrStream(jetty.url, sParams);
    tuples = getTuples(solrStream);

    assert (tuples.size() == 6);

    tuple = tuples.get(0);
    assert (tuple.get("str_s").equals("c"));
    assert (tuple.getLong("field_i") == 60);

    tuple = tuples.get(1);
    assert (tuple.get("str_s").equals("c"));
    assert (tuple.getLong("field_i") == 50);

    tuple = tuples.get(2);
    assert (tuple.get("str_s").equals("c"));
    assert (tuple.getLong("field_i") == 30);

    tuple = tuples.get(3);
    assert (tuple.get("str_s").equals("b"));
    assert (tuple.getLong("field_i") == 2);

    tuple = tuples.get(4);
    assert (tuple.get("str_s").equals("a"));
    assert (tuple.getLong("field_i") == 20);

    tuple = tuples.get(5);
    assert (tuple.get("str_s").equals("a"));
    assert (tuple.getLong("field_i") == 1);

    // reverse the sort
    sParams = mapParams(CommonParams.QT, "/sql", "aggregationMode", "facet",
        "stmt", "select distinct str_s as myString, field_i as myInt from collection1 order by str_s desc, myInt desc");

    solrStream = new SolrStream(jetty.url, sParams);
    tuples = getTuples(solrStream);

    assert (tuples.size() == 6);

    tuple = tuples.get(0);
    assert (tuple.get("myString").equals("c"));
    assert (tuple.getLong("myInt") == 60);

    tuple = tuples.get(1);
    assert (tuple.get("myString").equals("c"));
    assert (tuple.getLong("myInt") == 50);

    tuple = tuples.get(2);
    assert (tuple.get("myString").equals("c"));
    assert (tuple.getLong("myInt") == 30);

    tuple = tuples.get(3);
    assert (tuple.get("myString").equals("b"));
    assert (tuple.getLong("myInt") == 2);

    tuple = tuples.get(4);
    assert (tuple.get("myString").equals("a"));
    assert (tuple.getLong("myInt") == 20);

    tuple = tuples.get(5);
    assert (tuple.get("myString").equals("a"));
    assert (tuple.getLong("myInt") == 1);

    // test with limit
    sParams = mapParams(CommonParams.QT, "/sql", "aggregationMode", "facet",
        "stmt", "select distinct str_s, field_i from collection1 order by str_s desc, field_i desc limit 2");

    solrStream = new SolrStream(jetty.url, sParams);
    tuples = getTuples(solrStream);

    assert (tuples.size() == 2);

    tuple = tuples.get(0);
    assert (tuple.get("str_s").equals("c"));
    assert (tuple.getLong("field_i") == 60);

    tuple = tuples.get(1);
    assert (tuple.get("str_s").equals("c"));
    assert (tuple.getLong("field_i") == 50);

    // Test without a sort. Sort should be asc by default.
    sParams = mapParams(CommonParams.QT, "/sql", "aggregationMode", "facet",
        "stmt", "select distinct str_s, field_i from collection1");

    solrStream = new SolrStream(jetty.url, sParams);
    tuples = getTuples(solrStream);

    assert (tuples.size() == 6);

    tuple = tuples.get(0);
    assert (tuple.get("str_s").equals("a"));
    assert (tuple.getLong("field_i") == 1);

    tuple = tuples.get(1);

    assert (tuple.get("str_s").equals("a"));
    assert (tuple.getLong("field_i") == 20);

    tuple = tuples.get(2);

    assert (tuple.get("str_s").equals("b"));
    assert (tuple.getLong("field_i") == 2);

    tuple = tuples.get(3);

    assert (tuple.get("str_s").equals("c"));
    assert (tuple.getLong("field_i") == 30);

    tuple = tuples.get(4);

    assert (tuple.get("str_s").equals("c"));
    assert (tuple.getLong("field_i") == 50);

    tuple = tuples.get(5);

    assert (tuple.get("str_s").equals("c"));
    assert (tuple.getLong("field_i") == 60);

    // Test with a predicate.
    sParams = mapParams(CommonParams.QT, "/sql", "aggregationMode", "facet",
        "stmt", "select distinct str_s, field_i from collection1 where str_s = 'a'");

    solrStream = new SolrStream(jetty.url, sParams);
    tuples = getTuples(solrStream);

    assert (tuples.size() == 2);

    tuple = tuples.get(0);
    assert (tuple.get("str_s").equals("a"));
    assert (tuple.getLong("field_i") == 1);

    tuple = tuples.get(1);
    assert (tuple.get("str_s").equals("a"));
    assert (tuple.getLong("field_i") == 20);

  }

  private void testSelectDistinct() throws Exception {

    CloudJettyRunner jetty = this.cloudJettys.get(0);

    del("*:*");

    commit();

    indexr("id", "1", "text", "XXXX XXXX", "str_s", "a", "field_i", "1");
    indexr("id", "2", "text", "XXXX XXXX", "str_s", "b", "field_i", "2");
    indexr("id", "3", "text", "XXXX XXXX", "str_s", "a", "field_i", "20");
    indexr("id", "4", "text", "XXXX XXXX", "str_s", "b", "field_i", "2");
    indexr("id", "5", "text", "XXXX XXXX", "str_s", "c", "field_i", "30");
    indexr("id", "6", "text", "XXXX XXXX", "str_s", "c", "field_i", "30");
    indexr("id", "7", "text", "XXXX XXXX", "str_s", "c", "field_i", "50");
    indexr("id", "8", "text", "XXXX XXXX", "str_s", "c", "field_i", "60");
    commit();

    SolrParams sParams = mapParams(CommonParams.QT, "/sql", "aggregationMode", "map_reduce",
        "stmt", "select distinct str_s, field_i from collection1 order by str_s asc, field_i asc");

    System.out.println("##################### testSelectDistinct()");

    TupleStream solrStream = new SolrStream(jetty.url, sParams);
    List<Tuple> tuples = getTuples(solrStream);

    assert (tuples.size() == 6);
    Tuple tuple = tuples.get(0);
    assert (tuple.get("str_s").equals("a"));
    assert (tuple.getLong("field_i") == 1);

    tuple = tuples.get(1);
    assert (tuple.get("str_s").equals("a"));
    assert (tuple.getLong("field_i") == 20);

    tuple = tuples.get(2);
    assert (tuple.get("str_s").equals("b"));
    assert (tuple.getLong("field_i") == 2);

    tuple = tuples.get(3);
    assert (tuple.get("str_s").equals("c"));
    assert (tuple.getLong("field_i") == 30);

    tuple = tuples.get(4);
    assert (tuple.get("str_s").equals("c"));
    assert (tuple.getLong("field_i") == 50);

    tuple = tuples.get(5);
    assert (tuple.get("str_s").equals("c"));
    assert (tuple.getLong("field_i") == 60);

    // reverse the sort
    sParams = mapParams(CommonParams.QT, "/sql", "aggregationMode", "map_reduce",
        "stmt", "select distinct str_s, field_i from collection1 order by str_s desc, field_i desc");

    solrStream = new SolrStream(jetty.url, sParams);
    tuples = getTuples(solrStream);

    assert (tuples.size() == 6);

    tuple = tuples.get(0);
    assert (tuple.get("str_s").equals("c"));
    assert (tuple.getLong("field_i") == 60);

    tuple = tuples.get(1);
    assert (tuple.get("str_s").equals("c"));
    assert (tuple.getLong("field_i") == 50);

    tuple = tuples.get(2);
    assert (tuple.get("str_s").equals("c"));
    assert (tuple.getLong("field_i") == 30);

    tuple = tuples.get(3);
    assert (tuple.get("str_s").equals("b"));
    assert (tuple.getLong("field_i") == 2);

    tuple = tuples.get(4);
    assert (tuple.get("str_s").equals("a"));
    assert (tuple.getLong("field_i") == 20);

    tuple = tuples.get(5);
    assert (tuple.get("str_s").equals("a"));
    assert (tuple.getLong("field_i") == 1);

    sParams = mapParams(CommonParams.QT, "/sql", "aggregationMode", "map_reduce",
        "stmt", "select distinct str_s as myString, field_i from collection1 order by myString desc, field_i desc");

    solrStream = new SolrStream(jetty.url, sParams);
    tuples = getTuples(solrStream);

    assert (tuples.size() == 6);

    tuple = tuples.get(0);
    assert (tuple.get("myString").equals("c"));
    assert (tuple.getLong("field_i") == 60);

    tuple = tuples.get(1);
    assert (tuple.get("myString").equals("c"));
    assert (tuple.getLong("field_i") == 50);

    tuple = tuples.get(2);
    assert (tuple.get("myString").equals("c"));
    assert (tuple.getLong("field_i") == 30);

    tuple = tuples.get(3);
    assert (tuple.get("myString").equals("b"));
    assert (tuple.getLong("field_i") == 2);

    tuple = tuples.get(4);
    assert (tuple.get("myString").equals("a"));
    assert (tuple.getLong("field_i") == 20);

    tuple = tuples.get(5);
    assert (tuple.get("myString").equals("a"));
    assert (tuple.getLong("field_i") == 1);

    // test with limit
    sParams = mapParams(CommonParams.QT, "/sql", "aggregationMode", "map_reduce",
        "stmt", "select distinct str_s, field_i from collection1 order by str_s desc, field_i desc limit 2");

    solrStream = new SolrStream(jetty.url, sParams);
    tuples = getTuples(solrStream);

    assert (tuples.size() == 2);

    tuple = tuples.get(0);
    assert (tuple.get("str_s").equals("c"));
    assert (tuple.getLong("field_i") == 60);

    tuple = tuples.get(1);
    assert (tuple.get("str_s").equals("c"));
    assert (tuple.getLong("field_i") == 50);

    // Test without a sort. Sort should be asc by default.
    sParams = mapParams(CommonParams.QT, "/sql", "aggregationMode", "map_reduce",
        "stmt", "select distinct str_s, field_i from collection1");

    solrStream = new SolrStream(jetty.url, sParams);
    tuples = getTuples(solrStream);

    assert (tuples.size() == 6);

    tuple = tuples.get(0);
    assert (tuple.get("str_s").equals("a"));
    assert (tuple.getLong("field_i") == 1);

    tuple = tuples.get(1);
    assert (tuple.get("str_s").equals("a"));
    assert (tuple.getLong("field_i") == 20);

    tuple = tuples.get(2);
    assert (tuple.get("str_s").equals("b"));
    assert (tuple.getLong("field_i") == 2);

    tuple = tuples.get(3);
    assert (tuple.get("str_s").equals("c"));
    assert (tuple.getLong("field_i") == 30);

    tuple = tuples.get(4);
    assert (tuple.get("str_s").equals("c"));
    assert (tuple.getLong("field_i") == 50);

    tuple = tuples.get(5);
    assert (tuple.get("str_s").equals("c"));
    assert (tuple.getLong("field_i") == 60);

    // Test with a predicate.
    sParams = mapParams(CommonParams.QT, "/sql", "aggregationMode", "map_reduce",
        "stmt", "select distinct str_s, field_i from collection1 where str_s = 'a'");

    solrStream = new SolrStream(jetty.url, sParams);
    tuples = getTuples(solrStream);

    assert (tuples.size() == 2);

    tuple = tuples.get(0);
    assert (tuple.get("str_s").equals("a"));
    assert (tuple.getLong("field_i") == 1);

    tuple = tuples.get(1);
    assert (tuple.get("str_s").equals("a"));
    assert (tuple.getLong("field_i") == 20);

  }

  private void testParallelSelectDistinct() throws Exception {

    CloudJettyRunner jetty = this.cloudJettys.get(0);

    del("*:*");

    commit();

    indexr("id", "1", "text", "XXXX XXXX", "str_s", "a", "field_i", "1");
    indexr("id", "2", "text", "XXXX XXXX", "str_s", "b", "field_i", "2");
    indexr("id", "3", "text", "XXXX XXXX", "str_s", "a", "field_i", "20");
    indexr("id", "4", "text", "XXXX XXXX", "str_s", "b", "field_i", "2");
    indexr("id", "5", "text", "XXXX XXXX", "str_s", "c", "field_i", "30");
    indexr("id", "6", "text", "XXXX XXXX", "str_s", "c", "field_i", "30");
    indexr("id", "7", "text", "XXXX XXXX", "str_s", "c", "field_i", "50");
    indexr("id", "8", "text", "XXXX XXXX", "str_s", "c", "field_i", "60");
    commit();
    SolrParams sParams = mapParams(CommonParams.QT, "/sql", "numWorkers", "2", "aggregationMode", "map_reduce",
        "stmt", "select distinct str_s, field_i from collection1 order by str_s asc, field_i asc");

    SolrStream solrStream = new SolrStream(jetty.url, sParams);
    List<Tuple> tuples = getTuples(solrStream);

    assert (tuples.size() == 6);

    Tuple tuple;

    tuple = tuples.get(0);
    assert (tuple.get("str_s").equals("a"));
    assert (tuple.getLong("field_i") == 1);

    tuple = tuples.get(1);
    assert (tuple.get("str_s").equals("a"));
    assert (tuple.getLong("field_i") == 20);

    tuple = tuples.get(2);
    assert (tuple.get("str_s").equals("b"));
    assert (tuple.getLong("field_i") == 2);

    tuple = tuples.get(3);
    assert (tuple.get("str_s").equals("c"));
    assert (tuple.getLong("field_i") == 30);

    tuple = tuples.get(4);
    assert (tuple.get("str_s").equals("c"));
    assert (tuple.getLong("field_i") == 50);

    tuple = tuples.get(5);
    assert (tuple.get("str_s").equals("c"));
    assert (tuple.getLong("field_i") == 60);

    // reverse the sort
    sParams = mapParams(CommonParams.QT, "/sql", "numWorkers", "2", "aggregationMode", "map_reduce",
        "stmt", "select distinct str_s, field_i from collection1 order by str_s desc, field_i desc");

    solrStream = new SolrStream(jetty.url, sParams);
    tuples = getTuples(solrStream);

    assert (tuples.size() == 6);

    tuple = tuples.get(0);
    assert (tuple.get("str_s").equals("c"));
    assert (tuple.getLong("field_i") == 60);

    tuple = tuples.get(1);
    assert (tuple.get("str_s").equals("c"));
    assert (tuple.getLong("field_i") == 50);

    tuple = tuples.get(2);
    assert (tuple.get("str_s").equals("c"));
    assert (tuple.getLong("field_i") == 30);

    tuple = tuples.get(3);
    assert (tuple.get("str_s").equals("b"));
    assert (tuple.getLong("field_i") == 2);

    tuple = tuples.get(4);
    assert (tuple.get("str_s").equals("a"));
    assert (tuple.getLong("field_i") == 20);

    tuple = tuples.get(5);
    assert (tuple.get("str_s").equals("a"));
    assert (tuple.getLong("field_i") == 1);

    // reverse the sort
    sParams = mapParams(CommonParams.QT, "/sql", "numWorkers", "2", "aggregationMode", "map_reduce",
        "stmt", "select distinct str_s as myString, field_i from collection1 order by myString desc, field_i desc");

    solrStream = new SolrStream(jetty.url, sParams);
    tuples = getTuples(solrStream);

    assert (tuples.size() == 6);

    tuple = tuples.get(0);
    assert (tuple.get("myString").equals("c"));
    assert (tuple.getLong("field_i") == 60);

    tuple = tuples.get(1);
    assert (tuple.get("myString").equals("c"));
    assert (tuple.getLong("field_i") == 50);

    tuple = tuples.get(2);
    assert (tuple.get("myString").equals("c"));
    assert (tuple.getLong("field_i") == 30);

    tuple = tuples.get(3);
    assert (tuple.get("myString").equals("b"));
    assert (tuple.getLong("field_i") == 2);

    tuple = tuples.get(4);
    assert (tuple.get("myString").equals("a"));
    assert (tuple.getLong("field_i") == 20);

    tuple = tuples.get(5);
    assert (tuple.get("myString").equals("a"));
    assert (tuple.getLong("field_i") == 1);

    // test with limit
    sParams = mapParams(CommonParams.QT, "/sql", "numWorkers", "2", "aggregationMode", "map_reduce",
        "stmt", "select distinct str_s, field_i from collection1 order by str_s desc, field_i desc limit 2");

    solrStream = new SolrStream(jetty.url, sParams);
    tuples = getTuples(solrStream);

    assert (tuples.size() == 2);

    tuple = tuples.get(0);
    assert (tuple.get("str_s").equals("c"));
    assert (tuple.getLong("field_i") == 60);

    tuple = tuples.get(1);
    assert (tuple.get("str_s").equals("c"));
    assert (tuple.getLong("field_i") == 50);

    // Test without a sort. Sort should be asc by default.
    sParams = mapParams(CommonParams.QT, "/sql", "numWorkers", "2", "aggregationMode", "map_reduce",
        "stmt", "select distinct str_s, field_i from collection1");

    solrStream = new SolrStream(jetty.url, sParams);
    tuples = getTuples(solrStream);

    assert (tuples.size() == 6);

    tuple = tuples.get(0);
    assert (tuple.get("str_s").equals("a"));
    assert (tuple.getLong("field_i") == 1);

    tuple = tuples.get(1);
    assert (tuple.get("str_s").equals("a"));
    assert (tuple.getLong("field_i") == 20);

    tuple = tuples.get(2);
    assert (tuple.get("str_s").equals("b"));
    assert (tuple.getLong("field_i") == 2);

    tuple = tuples.get(3);
    assert (tuple.get("str_s").equals("c"));
    assert (tuple.getLong("field_i") == 30);

    tuple = tuples.get(4);
    assert (tuple.get("str_s").equals("c"));
    assert (tuple.getLong("field_i") == 50);

    tuple = tuples.get(5);
    assert (tuple.get("str_s").equals("c"));
    assert (tuple.getLong("field_i") == 60);

    // Test with a predicate.
    sParams = mapParams(CommonParams.QT, "/sql", "numWorkers", "2", "aggregationMode", "map_reduce",
        "stmt", "select distinct str_s, field_i from collection1 where str_s = 'a'");

    solrStream = new SolrStream(jetty.url, sParams);
    tuples = getTuples(solrStream);

    assert (tuples.size() == 2);

    tuple = tuples.get(0);
    assert (tuple.get("str_s").equals("a"));
    assert (tuple.getLong("field_i") == 1);

    tuple = tuples.get(1);
    assert (tuple.get("str_s").equals("a"));
    assert (tuple.getLong("field_i") == 20);

  }

  private void testBasicGroupingFacets() throws Exception {

    CloudJettyRunner jetty = this.cloudJettys.get(0);

    del("*:*");

    commit();

    indexr("id", "1", "text", "XXXX XXXX", "str_s", "a", "field_i", "7");
    indexr("id", "2", "text", "XXXX XXXX", "str_s", "b", "field_i", "8");
    indexr("id", "3", "text", "XXXX XXXX", "str_s", "a", "field_i", "20");
    indexr("id", "4", "text", "XXXX XXXX", "str_s", "b", "field_i", "11");
    indexr("id", "5", "text", "XXXX XXXX", "str_s", "c", "field_i", "30");
    indexr("id", "6", "text", "XXXX XXXX", "str_s", "c", "field_i", "40");
    indexr("id", "7", "text", "XXXX XXXX", "str_s", "c", "field_i", "50");
    indexr("id", "8", "text", "XXXX XXXX", "str_s", "c", "field_i", "60");
    indexr("id", "9", "text", "XXXX XXXY", "str_s", "d", "field_i", "70");
    commit();

    SolrParams sParams = mapParams(CommonParams.QT, "/sql", "aggregationMode", "facet",
        "stmt", "select str_s, count(*), sum(field_i), min(field_i), max(field_i), " +
            "cast(avg(1.0 * field_i) as float) from collection1 where text='XXXX' group by str_s " +
            "order by sum(field_i) asc limit 2");

    SolrStream solrStream = new SolrStream(jetty.url, sParams);
    List<Tuple> tuples = getTuples(solrStream);

    // Only two results because of the limit.
    assert (tuples.size() == 2);

    Tuple tuple;

    tuple = tuples.get(0);
    assert (tuple.get("str_s").equals("b"));
    assert (tuple.getDouble("EXPR$1") == 2); // count(*)
    assert (tuple.getDouble("EXPR$2") == 19); // sum(field_i)
    assert (tuple.getDouble("EXPR$3") == 8); // min(field_i)
    assert (tuple.getDouble("EXPR$4") == 11); // max(field_i)
    assert (tuple.getDouble("EXPR$5") == 9.5D); // avg(field_i)

    tuple = tuples.get(1);
    assert (tuple.get("str_s").equals("a"));
    assert (tuple.getDouble("EXPR$1") == 2); // count(*)
    assert (tuple.getDouble("EXPR$2") == 27); // sum(field_i)
    assert (tuple.getDouble("EXPR$3") == 7); // min(field_i)
    assert (tuple.getDouble("EXPR$4") == 20); // max(field_i)
    assert (tuple.getDouble("EXPR$5") == 13.5D); // avg(field_i)

    sParams = mapParams(CommonParams.QT, "/sql", "aggregationMode", "facet",
        "stmt", "select str_s, count(*), sum(field_i), min(field_i), max(field_i), " +
            "avg(field_i) from collection1 where text='XXXX' group by str_s " +
            "order by sum(field_i) asc limit 2");

    solrStream = new SolrStream(jetty.url, sParams);
    tuples = getTuples(solrStream);

    // Only two results because of the limit.
    assert (tuples.size() == 2);

    tuple = tuples.get(0);
    assert (tuple.get("str_s").equals("b"));
    assert (tuple.getDouble("EXPR$1") == 2); // count(*)
    assert (tuple.getDouble("EXPR$2") == 19); // sum(field_i)
    assert (tuple.getDouble("EXPR$3") == 8); // min(field_i)
    assert (tuple.getDouble("EXPR$4") == 11); // max(field_i)
    assert (tuple.getDouble("EXPR$5") == 10); // avg(field_i)

    tuple = tuples.get(1);
    assert (tuple.get("str_s").equals("a"));
    assert (tuple.getDouble("EXPR$1") == 2); // count(*)
    assert (tuple.getDouble("EXPR$2") == 27); // sum(field_i)
    assert (tuple.getDouble("EXPR$3") == 7); // min(field_i)
    assert (tuple.getDouble("EXPR$4") == 20); // max(field_i)
    assert (tuple.getDouble("EXPR$5") == 14); // avg(field_i)

    sParams = mapParams(CommonParams.QT, "/sql", "aggregationMode", "facet",
        "stmt", "select str_s, count(*), sum(field_i), min(field_i), max(field_i), "
            + "cast(avg(1.0 * field_i) as float) from collection1 where (text='XXXX' AND NOT (text='XXXY')) "
            + "group by str_s order by str_s desc");

    solrStream = new SolrStream(jetty.url, sParams);
    tuples = getTuples(solrStream);

    // The sort by and order by match and no limit is applied. All the Tuples should be returned in
    // this scenario.

    assert (tuples.size() == 3);

    tuple = tuples.get(0);
    assert (tuple.get("str_s").equals("c"));
    assert (tuple.getDouble("EXPR$1") == 4); // count(*)
    assert (tuple.getDouble("EXPR$2") == 180); // sum(field_i)
    assert (tuple.getDouble("EXPR$3") == 30); // min(field_i)
    assert (tuple.getDouble("EXPR$4") == 60); // max(field_i)
    assert (tuple.getDouble("EXPR$5") == 45); // avg(field_i)

    tuple = tuples.get(1);
    assert (tuple.get("str_s").equals("b"));
    assert (tuple.getDouble("EXPR$1") == 2); // count(*)
    assert (tuple.getDouble("EXPR$2") == 19); // sum(field_i)
    assert (tuple.getDouble("EXPR$3") == 8); // min(field_i)
    assert (tuple.getDouble("EXPR$4") == 11); // max(field_i)
    assert (tuple.getDouble("EXPR$5") == 9.5D); // avg(field_i)

    tuple = tuples.get(2);
    assert (tuple.get("str_s").equals("a"));
    assert (tuple.getDouble("EXPR$1") == 2); // count(*)
    assert (tuple.getDouble("EXPR$2") == 27); // sum(field_i)
    assert (tuple.getDouble("EXPR$3") == 7); // min(field_i)
    assert (tuple.getDouble("EXPR$4") == 20); // max(field_i)
    assert (tuple.getDouble("EXPR$5") == 13.5D); // avg(field_i)

    sParams = mapParams(CommonParams.QT, "/sql", "aggregationMode", "facet",
        "stmt", "select str_s as myString, count(*), sum(field_i) as mySum, min(field_i), max(field_i), "
            + "cast(avg(1.0 * field_i) as float) from collection1 where (text='XXXX' AND NOT (text='XXXY')) "
            + "group by str_s order by myString desc");

    solrStream = new SolrStream(jetty.url, sParams);
    tuples = getTuples(solrStream);

    // The sort by and order by match and no limit is applied. All the Tuples should be returned in
    // this scenario.

    assert (tuples.size() == 3);

    tuple = tuples.get(0);
    assert (tuple.get("myString").equals("c"));
    assert (tuple.getDouble("EXPR$1") == 4); // count(*)
    assert (tuple.getDouble("mySum") == 180);
    assert (tuple.getDouble("EXPR$3") == 30); // min(field_i)
    assert (tuple.getDouble("EXPR$4") == 60); // max(field_i)
    assert (tuple.getDouble("EXPR$5") == 45); // avg(field_i)

    tuple = tuples.get(1);
    assert (tuple.get("myString").equals("b"));
    assert (tuple.getDouble("EXPR$1") == 2); // count(*)
    assert (tuple.getDouble("mySum") == 19);
    assert (tuple.getDouble("EXPR$3") == 8); // min(field_i)
    assert (tuple.getDouble("EXPR$4") == 11); // max(field_i)
    assert (tuple.getDouble("EXPR$5") == 9.5D); // avg(field_i)

    tuple = tuples.get(2);
    assert (tuple.get("myString").equals("a"));
    assert (tuple.getDouble("EXPR$1") == 2); // count(*)
    assert (tuple.getDouble("mySum") == 27);
    assert (tuple.getDouble("EXPR$3") == 7); // min(field_i)
    assert (tuple.getDouble("EXPR$4") == 20); // max(field_i)
    assert (tuple.getDouble("EXPR$5") == 13.5D); // avg(field_i)

    sParams = mapParams(CommonParams.QT, "/sql", "aggregationMode", "facet",
        "stmt", "select str_s, count(*), sum(field_i), min(field_i), max(field_i), " +
            "cast(avg(1.0 * field_i) as float) from collection1 where text='XXXX' group by str_s having sum(field_i) = 19");

    solrStream = new SolrStream(jetty.url, sParams);
    tuples = getTuples(solrStream);

    assert (tuples.size() == 1);

    tuple = tuples.get(0);
    assert (tuple.get("str_s").equals("b"));
    assert (tuple.getDouble("EXPR$1") == 2); // count(*)
    assert (tuple.getDouble("EXPR$2") == 19); // sum(field_i)
    assert (tuple.getDouble("EXPR$3") == 8); // min(field_i)
    assert (tuple.getDouble("EXPR$4") == 11); // max(field_i)
    assert (tuple.getDouble("EXPR$5") == 9.5D); // avg(field_i)

    sParams = mapParams(CommonParams.QT, "/sql", "aggregationMode", "facet",
        "stmt", "select str_s, count(*), sum(field_i), min(field_i), max(field_i), " +
            "cast(avg(1.0 * field_i) as float) from collection1 where text='XXXX' group by str_s " +
            "having ((sum(field_i) = 19) AND (min(field_i) = 8))");

    solrStream = new SolrStream(jetty.url, sParams);
    tuples = getTuples(solrStream);

    assert (tuples.size() == 1);

    tuple = tuples.get(0);
    assert (tuple.get("str_s").equals("b"));
    assert (tuple.getDouble("EXPR$1") == 2); // count(*)
    assert (tuple.getDouble("EXPR$2") == 19); // sum(field_i)
    assert (tuple.getDouble("EXPR$3") == 8); // min(field_i)
    assert (tuple.getDouble("EXPR$4") == 11); // max(field_i)
    assert (tuple.getDouble("EXPR$5") == 9.5D); // avg(field_i)

    sParams = mapParams(CommonParams.QT, "/sql", "aggregationMode", "facet",
        "stmt", "select str_s, count(*), sum(field_i) as mySum, min(field_i), max(field_i), " +
            "cast(avg(1.0 * field_i) as float) from collection1 where text='XXXX' group by str_s " +
            "having ((sum(field_i) = 19) AND (min(field_i) = 8))");

    solrStream = new SolrStream(jetty.url, sParams);
    tuples = getTuples(solrStream);

    assert (tuples.size() == 1);

    tuple = tuples.get(0);
    assert (tuple.get("str_s").equals("b"));
    assert (tuple.getDouble("EXPR$1") == 2); // count(*)
    assert (tuple.getDouble("mySum") == 19);
    assert (tuple.getDouble("EXPR$3") == 8); // min(field_i)
    assert (tuple.getDouble("EXPR$4") == 11); // max(field_i)
    assert (tuple.getDouble("EXPR$5") == 9.5D); // avg(field_i)

    sParams = mapParams(CommonParams.QT, "/sql", "aggregationMode", "facet",
        "stmt", "select str_s, count(*), sum(field_i), min(field_i), max(field_i), " +
            "cast(avg(1.0 * field_i) as float) from collection1 where text='XXXX' group by str_s " +
            "having ((sum(field_i) = 19) AND (min(field_i) = 100))");

    solrStream = new SolrStream(jetty.url, sParams);
    tuples = getTuples(solrStream);

    assert (tuples.size() == 0);

  }

  private void testParallelBasicGrouping() throws Exception {

    CloudJettyRunner jetty = this.cloudJettys.get(0);

    del("*:*");

    commit();

    indexr("id", "1", "text", "XXXX XXXX", "str_s", "a", "field_i", "7");
    indexr("id", "2", "text", "XXXX XXXX", "str_s", "b", "field_i", "8");
    indexr("id", "3", "text", "XXXX XXXX", "str_s", "a", "field_i", "20");
    indexr("id", "4", "text", "XXXX XXXX", "str_s", "b", "field_i", "11");
    indexr("id", "5", "text", "XXXX XXXX", "str_s", "c", "field_i", "30");
    indexr("id", "6", "text", "XXXX XXXX", "str_s", "c", "field_i", "40");
    indexr("id", "7", "text", "XXXX XXXX", "str_s", "c", "field_i", "50");
    indexr("id", "8", "text", "XXXX XXXX", "str_s", "c", "field_i", "60");
    commit();

    SolrParams sParams = mapParams(CommonParams.QT, "/sql", "numWorkers", "2", "aggregationMode", "map_reduce",
        "stmt", "select str_s, count(*), sum(field_i), min(field_i), max(field_i), " +
            "avg(field_i) from collection1 where text='XXXX' group by str_s " +
            "order by sum(field_i) asc limit 2");

    SolrStream solrStream = new SolrStream(jetty.url, sParams);
    List<Tuple> tuples = getTuples(solrStream);

    // Only two results because of the limit.
    assert (tuples.size() == 2);

    Tuple tuple;

    tuple = tuples.get(0);
    assert (tuple.get("str_s").equals("b"));
    assert (tuple.getDouble("EXPR$1") == 2); // count(*)
    assert (tuple.getDouble("EXPR$2") == 19); // sum(field_i)
    assert (tuple.getDouble("EXPR$3") == 8); // min(field_i)
    assert (tuple.getDouble("EXPR$4") == 11); // max(field_i)
    assert (tuple.getDouble("EXPR$5") == 10); // avg(field_i)

    tuple = tuples.get(1);
    assert (tuple.get("str_s").equals("a"));
    assert (tuple.getDouble("EXPR$1") == 2); // count(*)
    assert (tuple.getDouble("EXPR$2") == 27); // sum(field_i)
    assert (tuple.getDouble("EXPR$3") == 7); // min(field_i)
    assert (tuple.getDouble("EXPR$4") == 20); // max(field_i)
    assert (tuple.getDouble("EXPR$5") == 14); // avg(field_i)

    sParams = mapParams(CommonParams.QT, "/sql", "numWorkers", "2", "aggregationMode", "map_reduce",
        "stmt", "select str_s, count(*), sum(field_i), min(field_i), max(field_i), " +
            "cast(avg(1.0 * field_i) as float) from collection1 where text='XXXX' group by str_s " +
            "order by sum(field_i) asc limit 2");

    solrStream = new SolrStream(jetty.url, sParams);
    tuples = getTuples(solrStream);

    // Only two results because of the limit.
    assert (tuples.size() == 2);

    tuple = tuples.get(0);
    assert (tuple.get("str_s").equals("b"));
    assert (tuple.getDouble("EXPR$1") == 2); // count(*)
    assert (tuple.getDouble("EXPR$2") == 19); // sum(field_i)
    assert (tuple.getDouble("EXPR$3") == 8); // min(field_i)
    assert (tuple.getDouble("EXPR$4") == 11); // max(field_i)
    assert (tuple.getDouble("EXPR$5") == 9.5); // avg(field_i)

    tuple = tuples.get(1);
    assert (tuple.get("str_s").equals("a"));
    assert (tuple.getDouble("EXPR$1") == 2); // count(*)
    assert (tuple.getDouble("EXPR$2") == 27); // sum(field_i)
    assert (tuple.getDouble("EXPR$3") == 7); // min(field_i)
    assert (tuple.getDouble("EXPR$4") == 20); // max(field_i)
    assert (tuple.getDouble("EXPR$5") == 13.5); // avg(field_i)

    sParams = mapParams(CommonParams.QT, "/sql", "numWorkers", "2", "aggregationMode", "map_reduce",
        "stmt", "select str_s, count(*), sum(field_i) as mySum, min(field_i), max(field_i), " +
            "avg(field_i) from collection1 where text='XXXX' group by str_s order by mySum asc limit 2");

    solrStream = new SolrStream(jetty.url, sParams);
    tuples = getTuples(solrStream);

    // Only two results because of the limit.
    assert (tuples.size() == 2);

    tuple = tuples.get(0);
    assert (tuple.get("str_s").equals("b"));
    assert (tuple.getDouble("EXPR$1") == 2); // count(*)
    assert (tuple.getDouble("mySum") == 19);
    assert (tuple.getDouble("EXPR$3") == 8); // min(field_i)
    assert (tuple.getDouble("EXPR$4") == 11); // max(field_i)
    assert (tuple.getDouble("EXPR$5") == 10); // avg(field_i)

    tuple = tuples.get(1);
    assert (tuple.get("str_s").equals("a"));
    assert (tuple.getDouble("EXPR$1") == 2); // count(*)
    assert (tuple.getDouble("mySum") == 27);
    assert (tuple.getDouble("EXPR$3") == 7); // min(field_i)
    assert (tuple.getDouble("EXPR$4") == 20); // max(field_i)
    assert (tuple.getDouble("EXPR$5") == 14); // avg(field_i)

    sParams = mapParams(CommonParams.QT, "/sql", "numWorkers", "2", "aggregationMode", "map_reduce",
        "stmt", "select str_s, count(*), sum(field_i), min(field_i), max(field_i), " +
            "avg(field_i) from collection1 where text='XXXX' group by str_s order by str_s desc");

    solrStream = new SolrStream(jetty.url, sParams);
    tuples = getTuples(solrStream);

    // The sort by and order by match and no limit is applied. All the Tuples should be returned in
    // this scenario.

    assert (tuples.size() == 3);

    tuple = tuples.get(0);
    assert (tuple.get("str_s").equals("c"));
    assert (tuple.getDouble("EXPR$1") == 4); // count(*)
    assert (tuple.getDouble("EXPR$2") == 180); // sum(field_i)
    assert (tuple.getDouble("EXPR$3") == 30); // min(field_i)
    assert (tuple.getDouble("EXPR$4") == 60); // max(field_i)
    assert (tuple.getDouble("EXPR$5") == 45); // avg(field_i)

    tuple = tuples.get(1);
    assert (tuple.get("str_s").equals("b"));
    assert (tuple.getDouble("EXPR$1") == 2); // count(*)
    assert (tuple.getDouble("EXPR$2") == 19); // sum(field_i)
    assert (tuple.getDouble("EXPR$3") == 8); // min(field_i)
    assert (tuple.getDouble("EXPR$4") == 11); // max(field_i)
    assert (tuple.getDouble("EXPR$5") == 10); // avg(field_i)

    tuple = tuples.get(2);
    assert (tuple.get("str_s").equals("a"));
    assert (tuple.getDouble("EXPR$1") == 2); // count(*)
    assert (tuple.getDouble("EXPR$2") == 27); // sum(field_i)
    assert (tuple.getDouble("EXPR$3") == 7); // min(field_i)
    assert (tuple.getDouble("EXPR$4") == 20); // max(field_i)
    assert (tuple.getDouble("EXPR$5") == 14); // avg(field_i)

    sParams = mapParams(CommonParams.QT, "/sql", "numWorkers", "2", "aggregationMode", "map_reduce",
        "stmt", "select str_s as myString, count(*), sum(field_i), min(field_i), max(field_i), " +
            "avg(field_i) from collection1 where text='XXXX' group by str_s order by myString desc");

    solrStream = new SolrStream(jetty.url, sParams);
    tuples = getTuples(solrStream);

    // The sort by and order by match and no limit is applied. All the Tuples should be returned in
    // this scenario.

    assert (tuples.size() == 3);

    tuple = tuples.get(0);
    assert (tuple.get("myString").equals("c"));
    assert (tuple.getDouble("EXPR$1") == 4); // count(*)
    assert (tuple.getDouble("EXPR$2") == 180); // sum(field_i)
    assert (tuple.getDouble("EXPR$3") == 30); // min(field_i)
    assert (tuple.getDouble("EXPR$4") == 60); // max(field_i)
    assert (tuple.getDouble("EXPR$5") == 45); // avg(field_i)

    tuple = tuples.get(1);
    assert (tuple.get("myString").equals("b"));
    assert (tuple.getDouble("EXPR$1") == 2); // count(*)
    assert (tuple.getDouble("EXPR$2") == 19); // sum(field_i)
    assert (tuple.getDouble("EXPR$3") == 8); // min(field_i)
    assert (tuple.getDouble("EXPR$4") == 11); // max(field_i)
    assert (tuple.getDouble("EXPR$5") == 10); // avg(field_i)

    tuple = tuples.get(2);
    assert (tuple.get("myString").equals("a"));
    assert (tuple.getDouble("EXPR$1") == 2); // count(*)
    assert (tuple.getDouble("EXPR$2") == 27); // sum(field_i)
    assert (tuple.getDouble("EXPR$3") == 7); // min(field_i)
    assert (tuple.getDouble("EXPR$4") == 20); // max(field_i)
    assert (tuple.getDouble("EXPR$5") == 14); // avg(field_i)

    sParams = mapParams(CommonParams.QT, "/sql", "numWorkers", "2", "aggregationMode", "map_reduce",
        "stmt", "select str_s, count(*), sum(field_i), min(field_i), max(field_i), " +
            "avg(field_i) from collection1 where text='XXXX' group by str_s having sum(field_i) = 19");

    solrStream = new SolrStream(jetty.url, sParams);
    tuples = getTuples(solrStream);

    assert (tuples.size() == 1);

    tuple = tuples.get(0);
    assert (tuple.get("str_s").equals("b"));
    assert (tuple.getDouble("EXPR$1") == 2); // count(*)
    assert (tuple.getDouble("EXPR$2") == 19); // sum(field_i)
    assert (tuple.getDouble("EXPR$3") == 8); // min(field_i)
    assert (tuple.getDouble("EXPR$4") == 11); // max(field_i)
    assert (tuple.getDouble("EXPR$5") == 10); // avg(field_i)

    sParams = mapParams(CommonParams.QT, "/sql", "numWorkers", "2", "aggregationMode", "map_reduce",
        "stmt", "select str_s, count(*), sum(field_i), min(field_i), max(field_i), " +
            "avg(field_i) from collection1 where text='XXXX' group by str_s " +
            "having ((sum(field_i) = 19) AND (min(field_i) = 8))");

    solrStream = new SolrStream(jetty.url, sParams);
    tuples = getTuples(solrStream);

    assert (tuples.size() == 1);

    tuple = tuples.get(0);
    assert (tuple.get("str_s").equals("b"));
    assert (tuple.getDouble("EXPR$1") == 2); // count(*)
    assert (tuple.getDouble("EXPR$2") == 19); // sum(field_i)
    assert (tuple.getDouble("EXPR$3") == 8); // min(field_i)
    assert (tuple.getDouble("EXPR$4") == 11); // max(field_i)
    assert (tuple.getDouble("EXPR$5") == 10); // avg(field_i)

    sParams = mapParams(CommonParams.QT, "/sql", "numWorkers", "2", "aggregationMode", "map_reduce",
        "stmt", "select str_s, count(*), sum(field_i), min(field_i), max(field_i), " +
            "avg(field_i) from collection1 where text='XXXX' group by str_s " +
            "having ((sum(field_i) = 19) AND (min(field_i) = 100))");

    solrStream = new SolrStream(jetty.url, sParams);
    tuples = getTuples(solrStream);

    assert (tuples.size() == 0);

  }

  private void testAggregatesWithoutGrouping() throws Exception {
    CloudJettyRunner jetty = this.cloudJettys.get(0);

    del("*:*");

    commit();

    indexr(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "1");
    indexr(id, "2", "a_s", "hello0", "a_i", "2", "a_f", "2");
    indexr(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3");
    indexr(id, "4", "a_s", "hello4", "a_i", "4", "a_f", "4");
    indexr(id, "1", "a_s", "hello0", "a_i", "1", "a_f", "5");
    indexr(id, "5", "a_s", "hello3", "a_i", "10", "a_f", "6");
    indexr(id, "6", "a_s", "hello4", "a_i", "11", "a_f", "7");
    indexr(id, "7", "a_s", "hello3", "a_i", "12", "a_f", "8");
    indexr(id, "8", "a_s", "hello3", "a_i", "13", "a_f", "9");
    indexr(id, "9", "a_s", "hello0", "a_i", "14", "a_f", "10");

    commit();

    SolrParams sParams = mapParams(CommonParams.QT, "/sql", "stmt",
        "select count(*), sum(a_i), min(a_i), max(a_i), cast(avg(1.0 * a_i) as float), sum(a_f), " +
            "min(a_f), max(a_f), avg(a_f) from collection1");

    SolrStream solrStream = new SolrStream(jetty.url, sParams);

    List<Tuple> tuples = getTuples(solrStream);

    assert (tuples.size() == 1);

    // Test Long and Double Sums

    Tuple tuple = tuples.get(0);

    Double count = tuple.getDouble("EXPR$0"); // count(*)
    Double sumi = tuple.getDouble("EXPR$1"); // sum(a_i)
    Double mini = tuple.getDouble("EXPR$2"); // min(a_i)
    Double maxi = tuple.getDouble("EXPR$3"); // max(a_i)
    Double avgi = tuple.getDouble("EXPR$4"); // avg(a_i)
    Double sumf = tuple.getDouble("EXPR$5"); // sum(a_f)
    Double minf = tuple.getDouble("EXPR$6"); // min(a_f)
    Double maxf = tuple.getDouble("EXPR$7"); // max(a_f)
    Double avgf = tuple.getDouble("EXPR$8"); // avg(a_f)

    assertTrue(count == 10);
    assertTrue(sumi == 70);
    assertTrue(mini == 0.0D);
    assertTrue(maxi == 14.0D);
    assertTrue(avgi == 7.0D);
    assertTrue(sumf == 55.0D);
    assertTrue(minf == 1.0D);
    assertTrue(maxf == 10.0D);
    assertTrue(avgf == 5.5D);

    sParams = mapParams(CommonParams.QT, "/sql",
        "stmt", "select count(*) as myCount, sum(a_i) as mySum, min(a_i) as myMin, max(a_i) as myMax, " +
            "cast(avg(1.0 * a_i) as float) as myAvg, sum(a_f), min(a_f), max(a_f), avg(a_f) from collection1");

    solrStream = new SolrStream(jetty.url, sParams);

    tuples = getTuples(solrStream);

    assert (tuples.size() == 1);

    // Test Long and Double Sums

    tuple = tuples.get(0);

    count = tuple.getDouble("myCount");
    sumi = tuple.getDouble("mySum");
    mini = tuple.getDouble("myMin");
    maxi = tuple.getDouble("myMax");
    avgi = tuple.getDouble("myAvg");
    sumf = tuple.getDouble("EXPR$5"); // sum(a_f)
    minf = tuple.getDouble("EXPR$6"); // min(a_f)
    maxf = tuple.getDouble("EXPR$7"); // max(a_f)
    avgf = tuple.getDouble("EXPR$8"); // avg(a_f)

    assertTrue(count == 10);
    assertTrue(mini == 0.0D);
    assertTrue(maxi == 14.0D);
    assertTrue(sumi == 70);
    assertTrue(avgi == 7.0D);
    assertTrue(sumf == 55.0D);
    assertTrue(minf == 1.0D);
    assertTrue(maxf == 10.0D);
    assertTrue(avgf == 5.5D);

    // Test without cast on average int field
    sParams = mapParams(CommonParams.QT, "/sql",
        "stmt", "select count(*) as myCount, sum(a_i) as mySum, min(a_i) as myMin, max(a_i) as myMax, " +
            "avg(a_i) as myAvg, sum(a_f), min(a_f), max(a_f), avg(a_f) from collection1");

    solrStream = new SolrStream(jetty.url, sParams);

    tuples = getTuples(solrStream);

    assert (tuples.size() == 1);

    // Test Long and Double Sums

    tuple = tuples.get(0);

    count = tuple.getDouble("myCount");
    sumi = tuple.getDouble("mySum");
    mini = tuple.getDouble("myMin");
    maxi = tuple.getDouble("myMax");
    avgi = tuple.getDouble("myAvg");
    assertTrue(tuple.get("myAvg") instanceof Long);
    sumf = tuple.getDouble("EXPR$5"); // sum(a_f)
    minf = tuple.getDouble("EXPR$6"); // min(a_f)
    maxf = tuple.getDouble("EXPR$7"); // max(a_f)
    avgf = tuple.getDouble("EXPR$8"); // avg(a_f)

    assertTrue(count == 10);
    assertTrue(mini == 0.0D);
    assertTrue(maxi == 14.0D);
    assertTrue(sumi == 70);
    assertTrue(avgi == 7);
    assertTrue(sumf == 55.0D);
    assertTrue(minf == 1.0D);
    assertTrue(maxf == 10.0D);
    assertTrue(avgf == 5.5D);

    // Test where clause hits
    sParams = mapParams(CommonParams.QT, "/sql",
        "stmt", "select count(*), sum(a_i), min(a_i), max(a_i), cast(avg(1.0 * a_i) as float), sum(a_f), " +
            "min(a_f), max(a_f), avg(a_f) from collection1 where id = 2");

    solrStream = new SolrStream(jetty.url, sParams);

    tuples = getTuples(solrStream);

    assert (tuples.size() == 1);

    tuple = tuples.get(0);

    count = tuple.getDouble("EXPR$0"); // count(*)
    sumi = tuple.getDouble("EXPR$1"); // sum(a_i)
    mini = tuple.getDouble("EXPR$2"); // min(a_i)
    maxi = tuple.getDouble("EXPR$3"); // max(a_i)
    avgi = tuple.getDouble("EXPR$4"); // avg(a_i)
    sumf = tuple.getDouble("EXPR$5"); // sum(a_f)
    minf = tuple.getDouble("EXPR$6"); // min(a_f)
    maxf = tuple.getDouble("EXPR$7"); // max(a_f)
    avgf = tuple.getDouble("EXPR$8"); // avg(a_f)

    assertTrue(count == 1);
    assertTrue(sumi == 2);
    assertTrue(mini == 2);
    assertTrue(maxi == 2);
    assertTrue(avgi == 2.0D);
    assertTrue(sumf == 2.0D);
    assertTrue(minf == 2);
    assertTrue(maxf == 2);
    assertTrue(avgf == 2.0);

    // Test zero hits
    sParams = mapParams(CommonParams.QT, "/sql",
        "stmt", "select count(*), sum(a_i), min(a_i), max(a_i), cast(avg(1.0 * a_i) as float), sum(a_f), " +
            "min(a_f), max(a_f), avg(a_f) from collection1 where a_s = 'blah'");

    solrStream = new SolrStream(jetty.url, sParams);

    tuples = getTuples(solrStream);

    assert (tuples.size() == 1);

    tuple = tuples.get(0);

    count = tuple.getDouble("EXPR$0"); // count(*)
    sumi = tuple.getDouble("EXPR$1"); // sum(a_i)
    mini = tuple.getDouble("EXPR$2"); // min(a_i)
    maxi = tuple.getDouble("EXPR$3"); // max(a_i)
    avgi = tuple.getDouble("EXPR$4"); // avg(a_i)
    sumf = tuple.getDouble("EXPR$5"); // sum(a_f)
    minf = tuple.getDouble("EXPR$6"); // min(a_f)
    maxf = tuple.getDouble("EXPR$7"); // max(a_f)
    avgf = tuple.getDouble("EXPR$8"); // avg(a_f)

    assertTrue(count == 0);
    assertTrue(sumi == null);
    assertTrue(mini == null);
    assertTrue(maxi == null);
    assertTrue(avgi == null);
    assertTrue(sumf == null);
    assertTrue(minf == null);
    assertTrue(maxf == null);
    assertTrue(avgf == null);

    del("*:*");
    commit();
  }

  private void testTimeSeriesGrouping() throws Exception {

    CloudJettyRunner jetty = this.cloudJettys.get(0);

    del("*:*");

    commit();

    indexr("id", "1", "year_i", "2015", "month_i", "11", "day_i", "7", "item_i", "5");
    indexr("id", "2", "year_i", "2015", "month_i", "11", "day_i", "7", "item_i", "10");
    indexr("id", "3", "year_i", "2015", "month_i", "11", "day_i", "8", "item_i", "30");
    indexr("id", "4", "year_i", "2015", "month_i", "11", "day_i", "8", "item_i", "12");
    indexr("id", "5", "year_i", "2015", "month_i", "10", "day_i", "1", "item_i", "4");
    indexr("id", "6", "year_i", "2015", "month_i", "10", "day_i", "3", "item_i", "5");
    indexr("id", "7", "year_i", "2014", "month_i", "4", "day_i", "4", "item_i", "6");
    indexr("id", "8", "year_i", "2014", "month_i", "4", "day_i", "2", "item_i", "1");

    commit();

    SolrParams sParams = mapParams(CommonParams.QT, "/sql",
        "stmt", "select year_i, sum(item_i) from collection1 group by year_i order by year_i desc");

    SolrStream solrStream = new SolrStream(jetty.url, sParams);
    List<Tuple> tuples = getTuples(solrStream);

    assert (tuples.size() == 2);

    Tuple tuple;

    tuple = tuples.get(0);
    assert (tuple.getLong("year_i") == 2015);
    assert (tuple.getDouble("EXPR$1") == 66); // sum(item_i)

    tuple = tuples.get(1);
    assert (tuple.getLong("year_i") == 2014);
    assert (tuple.getDouble("EXPR$1") == 7); // sum(item_i)

    sParams = mapParams(CommonParams.QT, "/sql",
        "stmt", "select year_i, month_i, sum(item_i) from collection1 group by year_i, month_i " +
            "order by year_i desc, month_i desc");

    solrStream = new SolrStream(jetty.url, sParams);
    tuples = getTuples(solrStream);

    assert (tuples.size() == 3);

    tuple = tuples.get(0);
    assert (tuple.getLong("year_i") == 2015);
    assert (tuple.getLong("month_i") == 11);
    assert (tuple.getDouble("EXPR$2") == 57); // sum(item_i)

    tuple = tuples.get(1);
    assert (tuple.getLong("year_i") == 2015);
    assert (tuple.getLong("month_i") == 10);
    assert (tuple.getDouble("EXPR$2") == 9); // sum(item_i)

    tuple = tuples.get(2);
    assert (tuple.getLong("year_i") == 2014);
    assert (tuple.getLong("month_i") == 4);
    assert (tuple.getDouble("EXPR$2") == 7); // sum(item_i)

    sParams = mapParams(CommonParams.QT, "/sql",
        "stmt", "select year_i, month_i, day_i, sum(item_i) from collection1 group by year_i, month_i, day_i " +
            "order by year_i desc, month_i desc, day_i desc");

    solrStream = new SolrStream(jetty.url, sParams);
    tuples = getTuples(solrStream);

    assert (tuples.size() == 6);

    tuple = tuples.get(0);
    assert (tuple.getLong("year_i") == 2015);
    assert (tuple.getLong("month_i") == 11);
    assert (tuple.getLong("day_i") == 8);
    assert (tuple.getDouble("EXPR$3") == 42); // sum(item_i)

    tuple = tuples.get(1);
    assert (tuple.getLong("year_i") == 2015);
    assert (tuple.getLong("month_i") == 11);
    assert (tuple.getLong("day_i") == 7);
    assert (tuple.getDouble("EXPR$3") == 15); // sum(item_i)

    tuple = tuples.get(2);
    assert (tuple.getLong("year_i") == 2015);
    assert (tuple.getLong("month_i") == 10);
    assert (tuple.getLong("day_i") == 3);
    assert (tuple.getDouble("EXPR$3") == 5); // sum(item_i)

    tuple = tuples.get(3);
    assert (tuple.getLong("year_i") == 2015);
    assert (tuple.getLong("month_i") == 10);
    assert (tuple.getLong("day_i") == 1);
    assert (tuple.getDouble("EXPR$3") == 4); // sum(item_i)

    tuple = tuples.get(4);
    assert (tuple.getLong("year_i") == 2014);
    assert (tuple.getLong("month_i") == 4);
    assert (tuple.getLong("day_i") == 4);
    assert (tuple.getDouble("EXPR$3") == 6); // sum(item_i)

    tuple = tuples.get(5);
    assert (tuple.getLong("year_i") == 2014);
    assert (tuple.getLong("month_i") == 4);
    assert (tuple.getLong("day_i") == 2);
    assert (tuple.getDouble("EXPR$3") == 1); // sum(item_i)

  }

  private void testTimeSeriesGroupingFacet() throws Exception {

    CloudJettyRunner jetty = this.cloudJettys.get(0);

    del("*:*");

    commit();

    indexr("id", "1", "year_i", "2015", "month_i", "11", "day_i", "7", "item_i", "5");
    indexr("id", "2", "year_i", "2015", "month_i", "11", "day_i", "7", "item_i", "10");
    indexr("id", "3", "year_i", "2015", "month_i", "11", "day_i", "8", "item_i", "30");
    indexr("id", "4", "year_i", "2015", "month_i", "11", "day_i", "8", "item_i", "12");
    indexr("id", "5", "year_i", "2015", "month_i", "10", "day_i", "1", "item_i", "4");
    indexr("id", "6", "year_i", "2015", "month_i", "10", "day_i", "3", "item_i", "5");
    indexr("id", "7", "year_i", "2014", "month_i", "4", "day_i", "4", "item_i", "6");
    indexr("id", "8", "year_i", "2014", "month_i", "4", "day_i", "2", "item_i", "1");

    commit();
    SolrParams sParams = mapParams(CommonParams.QT, "/sql", "aggregationMode", "facet",
        "stmt", "select year_i, sum(item_i) from collection1 group by year_i order by year_i desc");

    SolrStream solrStream = new SolrStream(jetty.url, sParams);
    List<Tuple> tuples = getTuples(solrStream);

    assert (tuples.size() == 2);

    Tuple tuple;

    tuple = tuples.get(0);
    assert (tuple.getLong("year_i") == 2015);
    assert (tuple.getDouble("EXPR$1") == 66); // sum(item_i)

    tuple = tuples.get(1);
    assert (tuple.getLong("year_i") == 2014);
    assert (tuple.getDouble("EXPR$1") == 7); // sum(item_i)

    sParams = mapParams(CommonParams.QT, "/sql", "aggregationMode", "facet",
        "stmt", "select year_i, month_i, sum(item_i) from collection1 group by year_i, month_i " +
            "order by year_i desc, month_i desc");

    solrStream = new SolrStream(jetty.url, sParams);
    tuples = getTuples(solrStream);

    assert (tuples.size() == 3);

    tuple = tuples.get(0);
    assert (tuple.getLong("year_i") == 2015);
    assert (tuple.getLong("month_i") == 11);
    assert (tuple.getDouble("EXPR$2") == 57); // sum(item_i)

    tuple = tuples.get(1);
    assert (tuple.getLong("year_i") == 2015);
    assert (tuple.getLong("month_i") == 10);
    assert (tuple.getDouble("EXPR$2") == 9); // sum(item_i)

    tuple = tuples.get(2);
    assert (tuple.getLong("year_i") == 2014);
    assert (tuple.getLong("month_i") == 4);
    assert (tuple.getDouble("EXPR$2") == 7); // sum(item_i)

    sParams = mapParams(CommonParams.QT, "/sql", "aggregationMode", "facet",
        "stmt", "select year_i, month_i, day_i, sum(item_i) from collection1 group by year_i, month_i, day_i " +
            "order by year_i desc, month_i desc, day_i desc");

    solrStream = new SolrStream(jetty.url, sParams);
    tuples = getTuples(solrStream);

    assert (tuples.size() == 6);

    tuple = tuples.get(0);
    assert (tuple.getLong("year_i") == 2015);
    assert (tuple.getLong("month_i") == 11);
    assert (tuple.getLong("day_i") == 8);
    assert (tuple.getDouble("EXPR$3") == 42); // sum(item_i)

    tuple = tuples.get(1);
    assert (tuple.getLong("year_i") == 2015);
    assert (tuple.getLong("month_i") == 11);
    assert (tuple.getLong("day_i") == 7);
    assert (tuple.getDouble("EXPR$3") == 15); // sum(item_i)

    tuple = tuples.get(2);
    assert (tuple.getLong("year_i") == 2015);
    assert (tuple.getLong("month_i") == 10);
    assert (tuple.getLong("day_i") == 3);
    assert (tuple.getDouble("EXPR$3") == 5); // sum(item_i)

    tuple = tuples.get(3);
    assert (tuple.getLong("year_i") == 2015);
    assert (tuple.getLong("month_i") == 10);
    assert (tuple.getLong("day_i") == 1);
    assert (tuple.getDouble("EXPR$3") == 4); // sum(item_i)

    tuple = tuples.get(4);
    assert (tuple.getLong("year_i") == 2014);
    assert (tuple.getLong("month_i") == 4);
    assert (tuple.getLong("day_i") == 4);
    assert (tuple.getDouble("EXPR$3") == 6); // sum(item_i)

    tuple = tuples.get(5);
    assert (tuple.getLong("year_i") == 2014);
    assert (tuple.getLong("month_i") == 4);
    assert (tuple.getLong("day_i") == 2);
    assert (tuple.getDouble("EXPR$3") == 1); // sum(item_i)

  }

  private void testParallelTimeSeriesGrouping() throws Exception {

    CloudJettyRunner jetty = this.cloudJettys.get(0);

    del("*:*");

    commit();

    indexr("id", "1", "year_i", "2015", "month_i", "11", "day_i", "7", "item_i", "5");
    indexr("id", "2", "year_i", "2015", "month_i", "11", "day_i", "7", "item_i", "10");
    indexr("id", "3", "year_i", "2015", "month_i", "11", "day_i", "8", "item_i", "30");
    indexr("id", "4", "year_i", "2015", "month_i", "11", "day_i", "8", "item_i", "12");
    indexr("id", "5", "year_i", "2015", "month_i", "10", "day_i", "1", "item_i", "4");
    indexr("id", "6", "year_i", "2015", "month_i", "10", "day_i", "3", "item_i", "5");
    indexr("id", "7", "year_i", "2014", "month_i", "4", "day_i", "4", "item_i", "6");
    indexr("id", "8", "year_i", "2014", "month_i", "4", "day_i", "2", "item_i", "1");

    commit();
    SolrParams sParams = mapParams(CommonParams.QT, "/sql", "numWorkers", "2", "aggregationMode", "map_reduce",
        "stmt", "select year_i, sum(item_i) from collection1 group by year_i order by year_i desc");

    SolrStream solrStream = new SolrStream(jetty.url, sParams);
    List<Tuple> tuples = getTuples(solrStream);

    assert (tuples.size() == 2);

    Tuple tuple;

    tuple = tuples.get(0);
    assert (tuple.getLong("year_i") == 2015);
    assert (tuple.get("year_i") instanceof Long); // SOLR-8601, This tests that the bucket is actually a Long and not
                                                  // parsed from a String.
    assert (tuple.getDouble("EXPR$1") == 66); // sum(item_i)

    tuple = tuples.get(1);
    assert (tuple.getLong("year_i") == 2014);
    assert (tuple.getDouble("EXPR$1") == 7); // sum(item_i)

    sParams = mapParams(CommonParams.QT, "/sql", "numWorkers", "2", "aggregationMode", "map_reduce",
        "stmt", "select year_i, month_i, sum(item_i) from collection1 group by year_i, month_i " +
            "order by year_i desc, month_i desc");

    solrStream = new SolrStream(jetty.url, sParams);
    tuples = getTuples(solrStream);

    assert (tuples.size() == 3);

    tuple = tuples.get(0);
    assert (tuple.getLong("year_i") == 2015);
    assert (tuple.getLong("month_i") == 11);
    assert (tuple.get("year_i") instanceof Long);
    assert (tuple.get("month_i") instanceof Long);
    assert (tuple.getDouble("EXPR$2") == 57); // sum(item_i)

    tuple = tuples.get(1);
    assert (tuple.getLong("year_i") == 2015);
    assert (tuple.getLong("month_i") == 10);
    assert (tuple.getDouble("EXPR$2") == 9); // sum(item_i)

    tuple = tuples.get(2);
    assert (tuple.getLong("year_i") == 2014);
    assert (tuple.getLong("month_i") == 4);
    assert (tuple.getDouble("EXPR$2") == 7); // sum(item_i)

    sParams = mapParams(CommonParams.QT, "/sql", "numWorkers", "2", "aggregationMode", "map_reduce",
        "stmt", "select year_i, month_i, day_i, sum(item_i) from collection1 group by year_i, month_i, day_i " +
            "order by year_i desc, month_i desc, day_i desc");

    solrStream = new SolrStream(jetty.url, sParams);
    tuples = getTuples(solrStream);

    assert (tuples.size() == 6);

    tuple = tuples.get(0);
    assert (tuple.getLong("year_i") == 2015);
    assert (tuple.getLong("month_i") == 11);
    assert (tuple.getLong("day_i") == 8);
    assert (tuple.getDouble("EXPR$3") == 42); // sum(item_i)

    tuple = tuples.get(1);
    assert (tuple.getLong("year_i") == 2015);
    assert (tuple.getLong("month_i") == 11);
    assert (tuple.getLong("day_i") == 7);
    assert (tuple.getDouble("EXPR$3") == 15); // sum(item_i)

    tuple = tuples.get(2);
    assert (tuple.getLong("year_i") == 2015);
    assert (tuple.getLong("month_i") == 10);
    assert (tuple.getLong("day_i") == 3);
    assert (tuple.getDouble("EXPR$3") == 5); // sum(item_i)

    tuple = tuples.get(3);
    assert (tuple.getLong("year_i") == 2015);
    assert (tuple.getLong("month_i") == 10);
    assert (tuple.getLong("day_i") == 1);
    assert (tuple.getDouble("EXPR$3") == 4); // sum(item_i)

    tuple = tuples.get(4);
    assert (tuple.getLong("year_i") == 2014);
    assert (tuple.getLong("month_i") == 4);
    assert (tuple.getLong("day_i") == 4);
    assert (tuple.getDouble("EXPR$3") == 6); // sum(item_i)

    tuple = tuples.get(5);
    assert (tuple.getLong("year_i") == 2014);
    assert (tuple.getLong("month_i") == 4);
    assert (tuple.getLong("day_i") == 2);
    assert (tuple.getDouble("EXPR$3") == 1); // sum(item_i)

  }

  protected List<Tuple> getTuples(TupleStream tupleStream) throws IOException {
    tupleStream.open();
    List<Tuple> tuples = new ArrayList<>();
    for (;;) {
      Tuple t = tupleStream.read();
      if (t.EOF) {
        break;
      } else {
        tuples.add(t);
      }
    }
    tupleStream.close();
    return tuples;
  }

  protected Tuple getTuple(TupleStream tupleStream) throws IOException {
    tupleStream.open();
    Tuple t = tupleStream.read();
    tupleStream.close();
    return t;
  }

  public static SolrParams mapParams(String... vals) {
    ModifiableSolrParams params = new ModifiableSolrParams();
    assertEquals("Parameters passed in here must be in pairs!", 0, (vals.length % 2));
    for (int idx = 0; idx < vals.length; idx += 2) {
      params.add(vals[idx], vals[idx + 1]);
    }

    return params;
  }

  public void assertResponseContains(SolrClient server, SolrParams requestParams, String json)
      throws IOException, SolrServerException {
    String p = requestParams.get("qt");
    ModifiableSolrParams modifiableSolrParams = (ModifiableSolrParams) requestParams;
    modifiableSolrParams.set("indent", modifiableSolrParams.get("indent", "off"));
    if (p != null) {
      modifiableSolrParams.remove("qt");
    }

    QueryRequest query = new QueryRequest(modifiableSolrParams);
    query.setPath(p);
    query.setResponseParser(new InputStreamResponseParser("json"));
    query.setMethod(SolrRequest.METHOD.POST);
    NamedList<Object> genericResponse = server.request(query);
    InputStream stream = (InputStream) genericResponse.get("stream");
    InputStreamReader reader = new InputStreamReader(stream, "UTF-8");
    BufferedReader bufferedReader = new BufferedReader(reader);
    String response = bufferedReader.readLine();
    assertTrue(response.contains(json));
  }

}
