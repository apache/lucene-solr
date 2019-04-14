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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.ExceptionStream;
import org.apache.solr.client.solrj.io.stream.SolrStream;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

@Slow
@SolrTestCaseJ4.SuppressSSL
@LuceneTestCase.SuppressCodecs({"Lucene3x", "Lucene40","Lucene41","Lucene42","Lucene45"})
public class TestSQLHandler extends SolrCloudTestCase {

  private static final String COLLECTIONORALIAS = "collection1";
  private static final int TIMEOUT = DEFAULT_TIMEOUT;
  private static final String id = "id";

  private static boolean useAlias;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(4)
        .addConfig("conf", configset("sql"))
        .configure();

    String collection;
    useAlias = random().nextBoolean();
    if (useAlias) {
      collection = COLLECTIONORALIAS + "_collection";
    } else {
      collection = COLLECTIONORALIAS;
    }

    CollectionAdminRequest.createCollection(collection, "conf", 2, 1).process(cluster.getSolrClient());
    cluster.waitForActiveCollection(collection, 2, 2);
    if (useAlias) {
      CollectionAdminRequest.createAlias(COLLECTIONORALIAS, collection).process(cluster.getSolrClient());
    }
  }

  @Before
  public void cleanIndex() throws Exception {
    new UpdateRequest()
        .deleteByQuery("*:*")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);
  }

  @Test
  public void testBasicSelect() throws Exception {


    new UpdateRequest()
        .add("id", "1", "text_t", "XXXX XXXX", "str_s", "a", "field_i", "7", "field_f", "7.5", "field_d", "7.5", "field_l", "7")
        .add("id", "2", "text_t", "XXXX XXXX", "str_s", "b", "field_i", "8", "field_f", "8.5", "field_d", "8.5", "field_l", "8")
        .add("id", "3", "text_t", "XXXX XXXX", "str_s", "a", "field_i", "20", "field_f", "20.5", "field_d", "20.5", "field_l", "20")
        .add("id", "4", "text_t", "XXXX XXXX", "str_s", "b", "field_i", "11", "field_f", "11.5", "field_d", "11.5", "field_l", "11")
        .add("id", "5", "text_t", "XXXX XXXX", "str_s", "c", "field_i", "30", "field_f", "30.5", "field_d", "30.5", "field_l", "30")
        .add("id", "6", "text_t", "XXXX XXXX", "str_s", "c", "field_i", "40", "field_f", "40.5", "field_d", "40.5", "field_l", "40")
        .add("id", "7", "text_t", "XXXX XXXX", "str_s", "c", "field_i", "50", "field_f", "50.5", "field_d", "50.5", "field_l", "50")
        .add("id", "8", "text_t", "XXXX XXXX", "str_s", "c", "field_i", "60", "field_f", "60.5", "field_d", "60.5", "field_l", "60")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    SolrParams sParams = mapParams(CommonParams.QT, "/sql",
        "stmt",
        "select id, field_i, str_s, field_f, field_d, field_l from collection1 where (text_t='(XXXX)' OR text_t='XXXX') AND text_t='XXXX' order by field_i desc");

    String baseUrl = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    List<Tuple> tuples = getTuples(sParams, baseUrl);

    assert (tuples.size() == 8);
    Tuple tuple;

    tuple = tuples.get(0);
    assertEquals(tuple.getLong("id").longValue(), 8);
    assertEquals(tuple.getLong("field_i").longValue(), 60);
    assert (tuple.get("str_s").equals("c"));
    assertEquals(tuple.getLong("field_i").longValue(), 60L);
    assertEquals(tuple.getDouble("field_f"), 60.5, 0.0);
    assertEquals(tuple.getDouble("field_d"), 60.5, 0.0);
    assertEquals(tuple.getLong("field_l").longValue(), 60);

    tuple = tuples.get(1);
    assertEquals(tuple.getLong("id").longValue(), 7);
    assertEquals(tuple.getLong("field_i").longValue(), 50);
    assert (tuple.get("str_s").equals("c"));
    assertEquals(tuple.getLong("field_i").longValue(), 50);
    assertEquals(tuple.getDouble("field_f"), 50.5, 0.0);
    assertEquals(tuple.getDouble("field_d"), 50.5, 0.0);
    assertEquals(tuple.getLong("field_l").longValue(), 50);

    tuple = tuples.get(2);
    assertEquals(tuple.getLong("id").longValue(), 6);
    assertEquals(tuple.getLong("field_i").longValue(), 40);
    assert (tuple.get("str_s").equals("c"));
    assertEquals(tuple.getLong("field_i").longValue(), 40);
    assertEquals(tuple.getDouble("field_f"), 40.5, 0.0);
    assertEquals(tuple.getDouble("field_d"), 40.5, 0.0);
    assertEquals(tuple.getLong("field_l").longValue(), 40);

    tuple = tuples.get(3);
    assertEquals(tuple.getLong("id").longValue(), 5);
    assertEquals(tuple.getLong("field_i").longValue(), 30);
    assert (tuple.get("str_s").equals("c"));
    assertEquals(tuple.getLong("field_i").longValue(), 30);
    assertEquals(tuple.getDouble("field_f"), 30.5, 0.0);
    assertEquals(tuple.getDouble("field_d"), 30.5, 0.0);
    assertEquals(tuple.getLong("field_l").longValue(), 30);

    tuple = tuples.get(4);
    assertEquals(tuple.getLong("id").longValue(), 3);
    assertEquals(tuple.getLong("field_i").longValue(), 20);
    assert (tuple.get("str_s").equals("a"));
    assertEquals(tuple.getLong("field_i").longValue(), 20);
    assertEquals(tuple.getDouble("field_f"), 20.5, 0.0);
    assertEquals(tuple.getDouble("field_d"), 20.5, 0.0);
    assertEquals(tuple.getLong("field_l").longValue(), 20);

    tuple = tuples.get(5);
    assertEquals(tuple.getLong("id").longValue(), 4);
    assertEquals(tuple.getLong("field_i").longValue(), 11);
    assert (tuple.get("str_s").equals("b"));
    assertEquals(tuple.getLong("field_i").longValue(), 11);
    assertEquals(tuple.getDouble("field_f"), 11.5, 0.0);
    assertEquals(tuple.getDouble("field_d"), 11.5, 0.0);
    assertEquals(tuple.getLong("field_l").longValue(), 11);

    tuple = tuples.get(6);
    assertEquals(tuple.getLong("id").longValue(), 2);
    assertEquals(tuple.getLong("field_i").longValue(), 8);
    assert (tuple.get("str_s").equals("b"));
    assertEquals(tuple.getLong("field_i").longValue(), 8);
    assertEquals(tuple.getDouble("field_f"), 8.5, 0.0);
    assertEquals(tuple.getDouble("field_d"), 8.5, 0.0);
    assertEquals(tuple.getLong("field_l").longValue(), 8);

    tuple = tuples.get(7);
    assertEquals(tuple.getLong("id").longValue(), 1);
    assertEquals(tuple.getLong("field_i").longValue(), 7);
    assert (tuple.get("str_s").equals("a"));
    assertEquals(tuple.getLong("field_i").longValue(), 7);
    assertEquals(tuple.getDouble("field_f"), 7.5, 0.0);
    assertEquals(tuple.getDouble("field_d"), 7.5, 0.0);
    assertEquals(tuple.getLong("field_l").longValue(), 7);

    // Assert field order
    //assertResponseContains(clients.get(0), sParams, "{\"docs\":[{\"id\":\"8\",\"field_i\":60,\"str_s\":\"c\",\"field_i\":60,\"field_f\":60.5,\"field_d\":60.5,\"field_l\":60}");


    sParams = mapParams(CommonParams.QT, "/sql", "stmt",
        "select id, field_i, str_s from collection1 where text_t='XXXX' order by id desc");

    tuples = getTuples(sParams, baseUrl);

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
        "stmt", "select id, field_i, str_s from collection1 where text_t='XXXX' order by field_i desc limit 1");

    tuples = getTuples(sParams, baseUrl);

    assert (tuples.size() == 1);

    tuple = tuples.get(0);
    assert (tuple.getLong("id") == 8);
    assert (tuple.getLong("field_i") == 60);
    assert (tuple.get("str_s").equals("c"));

    sParams = mapParams(CommonParams.QT, "/sql", "stmt",
        "select id, field_i, str_s from collection1 where text_t='XXXX' AND id='(1 2 3)' order by field_i desc");

    tuples = getTuples(sParams, baseUrl);

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
        "select id as myId, field_i as myInt, str_s as myString from collection1 where text_t='XXXX' AND id='(1 2 3)' order by myInt desc");

    tuples = getTuples(sParams, baseUrl);

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
        "select id as myId, field_i as myInt, str_s as myString from collection1 where text_t='XXXX' AND id='(1 2 3)' order by field_i desc");

    tuples = getTuples(sParams, baseUrl);

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

    tuples = getTuples(sParams, baseUrl);

    assertEquals(0, tuples.size());

  }


  @Test
  public void testWhere() throws Exception {

    new UpdateRequest()
        .add("id", "1", "text_t", "XXXX XXXX", "str_s", "a", "field_i", "7")
        .add("id", "2", "text_t", "XXXX XXXX", "str_s", "b", "field_i", "8")
        .add("id", "3", "text_t", "XXXX XXXX", "str_s", "a", "field_i", "20")
        .add("id", "4", "text_t", "XXXX XXXX", "str_s", "b", "field_i", "11")
        .add("id", "5", "text_t", "XXXX XXXX", "str_s", "c", "field_i", "30")
        .add("id", "6", "text_t", "XXXX XXXX", "str_s", "c", "field_i", "40")
        .add("id", "7", "text_t", "XXXX XXXX", "str_s", "c", "field_i", "50")
        .add("id", "8", "text_t", "XXXX XXXX", "str_s", "c", "field_i", "60")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    String baseUrl = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;

    // Equals
    SolrParams sParams = mapParams(CommonParams.QT, "/sql",
        "stmt", "select id from collection1 where id = 1 order by id asc");

    List<Tuple> tuples = getTuples(sParams, baseUrl);

    assertEquals(1, tuples.size());

    Tuple tuple = tuples.get(0);
    assertEquals("1", tuple.get("id"));

    // Not Equals <>
    sParams = mapParams(CommonParams.QT, "/sql",
        "stmt", "select id from collection1 where id <> 1 order by id asc limit 10");

    tuples = getTuples(sParams,baseUrl);

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
    // tuples = getTuples(sParams);
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

    tuples = getTuples(sParams, baseUrl);

    assertEquals(1, tuples.size());

    tuple = tuples.get(0);
    assertEquals("1", tuple.get("id"));

    // Less than equal
    sParams = mapParams(CommonParams.QT, "/sql",
        "stmt", "select id from collection1 where id <= 2 order by id asc");

    tuples = getTuples(sParams, baseUrl);

    assertEquals(2, tuples.size());

    tuple = tuples.get(0);
    assertEquals("1", tuple.get("id"));
    tuple = tuples.get(1);
    assertEquals("2", tuple.get("id"));

    // Greater than
    sParams = mapParams(CommonParams.QT, "/sql",
        "stmt", "select id from collection1 where id > 7 order by id asc");

    tuples = getTuples(sParams, baseUrl);

    assertEquals(1, tuples.size());

    tuple = tuples.get(0);
    assertEquals("8", tuple.get("id"));

    // Greater than equal
    sParams = mapParams(CommonParams.QT, "/sql",
        "stmt", "select id from collection1 where id >= 7 order by id asc");

    tuples = getTuples(sParams, baseUrl);

    assertEquals(2, tuples.size());

    tuple = tuples.get(0);
    assertEquals("7", tuple.get("id"));
    tuple = tuples.get(1);
    assertEquals("8", tuple.get("id"));

  }

  @Test
  public void testMixedCaseFields() throws Exception {

    new UpdateRequest()
        .add("id", "1", "Text_t", "XXXX XXXX", "Str_s", "a", "Field_i", "7")
        .add("id", "2", "Text_t", "XXXX XXXX", "Str_s", "b", "Field_i", "8")
        .add("id", "3", "Text_t", "XXXX XXXX", "Str_s", "a", "Field_i", "20")
        .add("id", "4", "Text_t", "XXXX XXXX", "Str_s", "b", "Field_i", "11")
        .add("id", "5", "Text_t", "XXXX XXXX", "Str_s", "c", "Field_i", "30")
        .add("id", "6", "Text_t", "XXXX XXXX", "Str_s", "c", "Field_i", "40")
        .add("id", "7", "Text_t", "XXXX XXXX", "Str_s", "c", "Field_i", "50")
        .add("id", "8", "Text_t", "XXXX XXXX", "Str_s", "c", "Field_i", "60")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    String baseUrl = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;

    SolrParams sParams = mapParams(CommonParams.QT, "/sql", "aggregationMode", "map_reduce",
        "stmt", "select id, Field_i, Str_s from collection1 where Text_t='XXXX' order by Field_i desc");

    List<Tuple> tuples = getTuples(sParams, baseUrl);

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

    tuples = getTuples(sParams, baseUrl);

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

    tuples = getTuples(sParams, baseUrl);

    assert (tuples.size() == 2);

    tuple = tuples.get(0);
    assert (tuple.get("Str_s").equals("c"));
    assert (tuple.getDouble("EXPR$1") == 60);

    tuple = tuples.get(1);
    assert (tuple.get("Str_s").equals("a"));
    assert (tuple.getDouble("EXPR$1") == 7);

  }

  @Test
  public void testSelectDistinctFacets() throws Exception {

    new UpdateRequest()
        .add("id", "1", "text_t", "XXXX XXXX", "str_s", "a", "field_i", "1")
        .add("id", "2", "text_t", "XXXX XXXX", "str_s", "b", "field_i", "2")
        .add("id", "3", "text_t", "XXXX XXXX", "str_s", "a", "field_i", "20")
        .add("id", "4", "text_t", "XXXX XXXX", "str_s", "b", "field_i", "2")
        .add("id", "5", "text_t", "XXXX XXXX", "str_s", "c", "field_i", "30")
        .add("id", "6", "text_t", "XXXX XXXX", "str_s", "c", "field_i", "30")
        .add("id", "7", "text_t", "XXXX XXXX", "str_s", "c", "field_i", "50")
        .add("id", "8", "text_t", "XXXX XXXX", "str_s", "c", "field_i", "60")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    String baseUrl = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;


    SolrParams sParams = mapParams(CommonParams.QT, "/sql", "aggregationMode", "facet",
        "stmt", "select distinct str_s, field_i from collection1 order by str_s asc, field_i asc");


    List<Tuple> tuples = getTuples(sParams, baseUrl);

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

    tuples = getTuples(sParams, baseUrl);

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

    tuples = getTuples(sParams, baseUrl);

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

    tuples = getTuples(sParams, baseUrl);

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

    tuples = getTuples(sParams, baseUrl);

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

    tuples = getTuples(sParams, baseUrl);

    assert (tuples.size() == 2);

    tuple = tuples.get(0);
    assert (tuple.get("str_s").equals("a"));
    assert (tuple.getLong("field_i") == 1);

    tuple = tuples.get(1);
    assert (tuple.get("str_s").equals("a"));
    assert (tuple.getLong("field_i") == 20);

  }


  @Test
  public void testSelectDistinct() throws Exception {

    new UpdateRequest()
        .add("id", "1", "text_t", "XXXX XXXX", "str_s", "a", "field_i", "1")
        .add("id", "2", "text_t", "XXXX XXXX", "str_s", "b", "field_i", "2")
        .add("id", "3", "text_t", "XXXX XXXX", "str_s", "a", "field_i", "20")
        .add("id", "4", "text_t", "XXXX XXXX", "str_s", "b", "field_i", "2")
        .add("id", "5", "text_t", "XXXX XXXX", "str_s", "c", "field_i", "30")
        .add("id", "6", "text_t", "XXXX XXXX", "str_s", "c", "field_i", "30")
        .add("id", "7", "text_t", "XXXX XXXX", "str_s", "c", "field_i", "50")
        .add("id", "8", "text_t", "XXXX XXXX", "str_s", "c", "field_i", "60")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    String baseUrl = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;

    SolrParams sParams = mapParams(CommonParams.QT, "/sql", "aggregationMode", "map_reduce",
        "stmt", "select distinct str_s, field_i from collection1 order by str_s asc, field_i asc");

    List<Tuple> tuples = getTuples(sParams, baseUrl);

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

    tuples = getTuples(sParams, baseUrl);

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

    tuples = getTuples(sParams, baseUrl);

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

    tuples = getTuples(sParams, baseUrl);

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

    tuples = getTuples(sParams, baseUrl);

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

    tuples = getTuples(sParams, baseUrl);

    assert (tuples.size() == 2);

    tuple = tuples.get(0);
    assert (tuple.get("str_s").equals("a"));
    assert (tuple.getLong("field_i") == 1);

    tuple = tuples.get(1);
    assert (tuple.get("str_s").equals("a"));
    assert (tuple.getLong("field_i") == 20);

  }

  @Test
  public void testParallelSelectDistinct() throws Exception {

    new UpdateRequest()
        .add("id", "1", "text_t", "XXXX XXXX", "str_s", "a", "field_i", "1")
        .add("id", "2", "text_t", "XXXX XXXX", "str_s", "b", "field_i", "2")
        .add("id", "3", "text_t", "XXXX XXXX", "str_s", "a", "field_i", "20")
        .add("id", "4", "text_t", "XXXX XXXX", "str_s", "b", "field_i", "2")
        .add("id", "5", "text_t", "XXXX XXXX", "str_s", "c", "field_i", "30")
        .add("id", "6", "text_t", "XXXX XXXX", "str_s", "c", "field_i", "30")
        .add("id", "7", "text_t", "XXXX XXXX", "str_s", "c", "field_i", "50")
        .add("id", "8", "text_t", "XXXX XXXX", "str_s", "c", "field_i", "60")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    String baseUrl = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    SolrParams sParams = mapParams(CommonParams.QT, "/sql", "numWorkers", "2", "aggregationMode", "map_reduce",
        "stmt", "select distinct str_s, field_i from collection1 order by str_s asc, field_i asc");

    List<Tuple> tuples = getTuples(sParams, baseUrl);

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

    tuples = getTuples(sParams, baseUrl);

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

    tuples = getTuples(sParams, baseUrl);

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

    tuples = getTuples(sParams, baseUrl);

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

    tuples = getTuples(sParams, baseUrl);

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

    tuples = getTuples(sParams, baseUrl);

    assert (tuples.size() == 2);

    tuple = tuples.get(0);
    assert (tuple.get("str_s").equals("a"));
    assert (tuple.getLong("field_i") == 1);

    tuple = tuples.get(1);
    assert (tuple.get("str_s").equals("a"));
    assert (tuple.getLong("field_i") == 20);

  }

  @Test
  public void testBasicGroupingFacets() throws Exception {

    new UpdateRequest()
        .add("id", "1", "text_t", "XXXX XXXX", "str_s", "a", "field_i", "7")
        .add("id", "2", "text_t", "XXXX XXXX", "str_s", "b", "field_i", "8")
        .add("id", "3", "text_t", "XXXX XXXX", "str_s", "a", "field_i", "20")
        .add("id", "4", "text_t", "XXXX XXXX", "str_s", "b", "field_i", "11")
        .add("id", "5", "text_t", "XXXX XXXX", "str_s", "c", "field_i", "30")
        .add("id", "6", "text_t", "XXXX XXXX", "str_s", "c", "field_i", "40")
        .add("id", "7", "text_t", "XXXX XXXX", "str_s", "c", "field_i", "50")
        .add("id", "8", "text_t", "XXXX XXXX", "str_s", "c", "field_i", "60")
        .add("id", "9", "text_t", "XXXX XXXY", "str_s", "d", "field_i", "70")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    String baseUrl = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;

    SolrParams sParams = mapParams(CommonParams.QT, "/sql", "aggregationMode", "facet",
        "stmt", "select str_s, count(*), sum(field_i), min(field_i), max(field_i), " +
            "cast(avg(1.0 * field_i) as float) from collection1 where text_t='XXXX' group by str_s " +
            "order by sum(field_i) asc limit 2");

    List<Tuple> tuples = getTuples(sParams, baseUrl);

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
            "avg(field_i) from collection1 where text_t='XXXX' group by str_s " +
            "order by sum(field_i) asc limit 2");

    tuples = getTuples(sParams, baseUrl);

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
            + "cast(avg(1.0 * field_i) as float) from collection1 where (text_t='XXXX' AND NOT (text_t='XXXY')) "
            + "group by str_s order by str_s desc");

    tuples = getTuples(sParams, baseUrl);

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
            + "cast(avg(1.0 * field_i) as float) from collection1 where (text_t='XXXX' AND NOT (text_t='XXXY')) "
            + "group by str_s order by myString desc");

    tuples = getTuples(sParams, baseUrl);

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
            "cast(avg(1.0 * field_i) as float) from collection1 where text_t='XXXX' group by str_s having sum(field_i) = 19");

    tuples = getTuples(sParams, baseUrl);

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
            "cast(avg(1.0 * field_i) as float) from collection1 where text_t='XXXX' group by str_s " +
            "having ((sum(field_i) = 19) AND (min(field_i) = 8))");

    tuples = getTuples(sParams, baseUrl);

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
            "cast(avg(1.0 * field_i) as float) from collection1 where text_t='XXXX' group by str_s " +
            "having ((sum(field_i) = 19) AND (min(field_i) = 8))");

    tuples = getTuples(sParams, baseUrl);

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
            "cast(avg(1.0 * field_i) as float) from collection1 where text_t='XXXX' group by str_s " +
            "having ((sum(field_i) = 19) AND (min(field_i) = 100))");

    tuples = getTuples(sParams, baseUrl);

    assert (tuples.size() == 0);

  }

  @Test
  public void testAggregatesWithoutGrouping() throws Exception {

    new UpdateRequest()
        .add(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "1")
        .add(id, "2", "a_s", "hello0", "a_i", "2", "a_f", "2")
        .add(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3")
        .add(id, "4", "a_s", "hello4", "a_i", "4", "a_f", "4")
        .add(id, "1", "a_s", "hello0", "a_i", "1", "a_f", "5")
        .add(id, "5", "a_s", "hello3", "a_i", "10", "a_f", "6")
        .add(id, "6", "a_s", "hello4", "a_i", "11", "a_f", "7")
        .add(id, "7", "a_s", "hello3", "a_i", "12", "a_f", "8")
        .add(id, "8", "a_s", "hello3", "a_i", "13", "a_f", "9")
        .add(id, "9", "a_s", "hello0", "a_i", "14", "a_f", "10")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    String baseUrl = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;

    SolrParams sParams = mapParams(CommonParams.QT, "/sql", "stmt",
        "select count(*), sum(a_i), min(a_i), max(a_i), cast(avg(1.0 * a_i) as float), sum(a_f), " +
            "min(a_f), max(a_f), avg(a_f) from collection1");


    List<Tuple> tuples = getTuples(sParams, baseUrl);

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


    tuples = getTuples(sParams, baseUrl);

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


    tuples = getTuples(sParams, baseUrl);

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


    tuples = getTuples(sParams, baseUrl);

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


    tuples = getTuples(sParams, baseUrl);

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

  }

  @Test
  public void testTimeSeriesGrouping() throws Exception {


    new UpdateRequest()
        .add(id, "1", "year_i", "2015", "month_i", "11", "day_i", "7", "item_i", "5")
        .add(id, "2", "year_i", "2015", "month_i", "11", "day_i", "7", "item_i", "10")
        .add(id, "3", "year_i", "2015", "month_i", "11", "day_i", "8", "item_i", "30")
        .add(id, "4", "year_i", "2015", "month_i", "11", "day_i", "8", "item_i", "12")
        .add(id, "5", "year_i", "2015", "month_i", "10", "day_i", "1", "item_i", "4")
        .add(id, "6", "year_i", "2015", "month_i", "10", "day_i", "3", "item_i", "5")
        .add(id, "7", "year_i", "2014", "month_i", "4", "day_i", "4", "item_i", "6")
        .add(id, "8", "year_i", "2014", "month_i", "4", "day_i", "2", "item_i", "1")

        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    String baseUrl = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;

    SolrParams sParams = mapParams(CommonParams.QT, "/sql", "aggregationMode", "map_reduce",
        "stmt", "select year_i, sum(item_i) from collection1 group by year_i order by year_i desc");

    List<Tuple> tuples = getTuples(sParams, baseUrl);

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

    tuples = getTuples(sParams, baseUrl);

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

    tuples = getTuples(sParams, baseUrl);

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

  @Test
  public void testSQLException() throws Exception {

    new UpdateRequest()
        .add(id, "1", "text_t", "XXXX XXXX", "str_s", "a", "field_i", "7")
        .add(id, "2", "text_t", "XXXX XXXX", "str_s", "b", "field_i", "8")
        .add(id, "3", "text_t", "XXXX XXXX", "str_s", "a", "field_i", "20")
        .add(id, "4", "text_t", "XXXX XXXX", "str_s", "b", "field_i", "11")
        .add(id, "5", "text_t", "XXXX XXXX", "str_s", "c", "field_i", "30")
        .add(id, "6", "text_t", "XXXX XXXX", "str_s", "c", "field_i", "40")
        .add(id, "7", "text_t", "XXXX XXXX", "str_s", "c", "field_i", "50")
        .add(id, "8", "text_t", "XXXX XXXX", "str_s", "c", "field_i", "60")

        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    String baseUrl = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;

    SolrParams sParams = mapParams(CommonParams.QT, "/sql", "aggregationMode", "map_reduce",
        "stmt", "select id, str_s from collection1 where text_t='XXXX' order by field_iff desc");

    SolrStream solrStream = new SolrStream(baseUrl, sParams);
    Tuple tuple = getTuple(new ExceptionStream(solrStream));
    assert (tuple.EOF);
    assert (tuple.EXCEPTION);
    assert (tuple.getException().contains("Column 'field_iff' not found in any table"));

    sParams = mapParams(CommonParams.QT, "/sql",
        "stmt", "select id, field_iff, str_s from collection1 where text_t='XXXX' order by field_iff desc");

    solrStream = new SolrStream(baseUrl, sParams);
    tuple = getTuple(new ExceptionStream(solrStream));
    assert (tuple.EOF);
    assert (tuple.EXCEPTION);

    assert (tuple.getException().contains("Column 'field_iff' not found in any table"));

    sParams = mapParams(CommonParams.QT, "/sql", "aggregationMode", "map_reduce",
        "stmt",
        "select str_s, count(*), sum(field_iff), min(field_i), max(field_i), cast(avg(1.0 * field_i) as float) from collection1 where text_t='XXXX' group by str_s having ((sum(field_iff) = 19) AND (min(field_i) = 8))");

    solrStream = new SolrStream(baseUrl, sParams);
    tuple = getTuple(new ExceptionStream(solrStream));
    assert (tuple.EOF);
    assert (tuple.EXCEPTION);
    assert (tuple.getException().contains("Column 'field_iff' not found in any table"));

    sParams = mapParams(CommonParams.QT, "/sql", "aggregationMode", "map_reduce",
        "stmt",
        "select str_s, count(*), blah(field_i), min(field_i), max(field_i), cast(avg(1.0 * field_i) as float) from collection1 where text_t='XXXX' group by str_s having ((sum(field_i) = 19) AND (min(field_i) = 8))");

    solrStream = new SolrStream(baseUrl, sParams);
    tuple = getTuple(new ExceptionStream(solrStream));
    assert (tuple.EOF);
    assert (tuple.EXCEPTION);
    assert (tuple.getException().contains("No match found for function signature blah"));
  }


  @Test
  public void testTimeSeriesGroupingFacet() throws Exception {

    new UpdateRequest()
        .add(id, "1", "year_i", "2015", "month_i", "11", "day_i", "7", "item_i", "5")
        .add(id, "2", "year_i", "2015", "month_i", "11", "day_i", "7", "item_i", "10")
        .add(id, "3", "year_i", "2015", "month_i", "11", "day_i", "8", "item_i", "30")
        .add(id, "4", "year_i", "2015", "month_i", "11", "day_i", "8", "item_i", "12")
        .add(id, "5", "year_i", "2015", "month_i", "10", "day_i", "1", "item_i", "4")
        .add(id, "6", "year_i", "2015", "month_i", "10", "day_i", "3", "item_i", "5")
        .add(id, "7", "year_i", "2014", "month_i", "4", "day_i", "4", "item_i", "6")
        .add(id, "8", "year_i", "2014", "month_i", "4", "day_i", "2", "item_i", "1")

        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    String baseUrl = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;

    SolrParams sParams = mapParams(CommonParams.QT, "/sql", "aggregationMode", "facet",
        "stmt", "select year_i, sum(item_i) from collection1 group by year_i order by year_i desc");

    List<Tuple> tuples = getTuples(sParams, baseUrl);

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

    tuples = getTuples(sParams, baseUrl);

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

    tuples = getTuples(sParams, baseUrl);

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

  @Test
  public void testParallelTimeSeriesGrouping() throws Exception {

    new UpdateRequest()
        .add(id, "1", "year_i", "2015", "month_i", "11", "day_i", "7", "item_i", "5")
        .add(id, "2", "year_i", "2015", "month_i", "11", "day_i", "7", "item_i", "10")
        .add(id, "3", "year_i", "2015", "month_i", "11", "day_i", "8", "item_i", "30")
        .add(id, "4", "year_i", "2015", "month_i", "11", "day_i", "8", "item_i", "12")
        .add(id, "5", "year_i", "2015", "month_i", "10", "day_i", "1", "item_i", "4")
        .add(id, "6", "year_i", "2015", "month_i", "10", "day_i", "3", "item_i", "5")
        .add(id, "7", "year_i", "2014", "month_i", "4", "day_i", "4", "item_i", "6")
        .add(id, "8", "year_i", "2014", "month_i", "4", "day_i", "2", "item_i", "1")

        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    String baseUrl = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;

    SolrParams sParams = mapParams(CommonParams.QT, "/sql", "numWorkers", "2", "aggregationMode", "map_reduce",
        "stmt", "select year_i, sum(item_i) from collection1 group by year_i order by year_i desc");

    List<Tuple> tuples = getTuples(sParams, baseUrl);

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

    tuples = getTuples(sParams, baseUrl);

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

    tuples = getTuples(sParams, baseUrl);

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


  public boolean assertLong(Tuple tuple, String fieldName, long l) throws Exception {
    long lv = (long)tuple.get(fieldName);
    if(lv != l) {
      throw new Exception("Longs not equal:"+l+" : "+lv);
    }

    return true;
  }

  public boolean assertString(Tuple tuple, String fieldName, String expected) throws Exception {
    String actual = (String)tuple.get(fieldName);

    if( (null == expected && null != actual) ||
        (null != expected && null == actual) ||
        (null != expected && !expected.equals(actual))){
      throw new Exception("Longs not equal:"+expected+" : "+actual);
    }

    return true;
  }

  public boolean assertDouble(Tuple tuple, String fieldName, double d) throws Exception {
    double dv = tuple.getDouble(fieldName);
    if(dv != d) {
      throw new Exception("Doubles not equal:"+d+" : "+dv);
    }

    return true;
  }

  protected boolean assertMaps(List<Map> maps, int... ids) throws Exception {
    if(maps.size() != ids.length) {
      throw new Exception("Expected id count != actual map count:"+ids.length+":"+maps.size());
    }

    int i=0;
    for(int val : ids) {
      Map t = maps.get(i);
      String tip = (String)t.get("id");
      if(!tip.equals(Integer.toString(val))) {
        throw new Exception("Found value:"+tip+" expecting:"+val);
      }
      ++i;
    }
    return true;
  }

  protected List<Tuple> getTuples(final SolrParams params, String baseUrl) throws IOException {
    //log.info("Tuples from params: {}", params);
    TupleStream tupleStream = new SolrStream(baseUrl, params);

    tupleStream.open();
    List<Tuple> tuples = new ArrayList<>();
    for (;;) {
      Tuple t = tupleStream.read();
      //log.info(" ... {}", t.fields);
      if (t.EOF) {
        break;
      } else {
        tuples.add(t);
      }
    }
    tupleStream.close();
    return tuples;
  }

  public static SolrParams mapParams(String... vals) {
    ModifiableSolrParams params = new ModifiableSolrParams();
    assertEquals("Parameters passed in here must be in pairs!", 0, (vals.length % 2));
    for (int idx = 0; idx < vals.length; idx += 2) {
      params.add(vals[idx], vals[idx + 1]);
    }

    return params;
  }

  protected Tuple getTuple(TupleStream tupleStream) throws IOException {
    tupleStream.open();
    Tuple t = tupleStream.read();
    tupleStream.close();
    return t;
  }

}
