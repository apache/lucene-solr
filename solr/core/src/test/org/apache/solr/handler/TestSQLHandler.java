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
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
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
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

@Slow
@SolrTestCaseJ4.SuppressSSL
@LuceneTestCase.SuppressCodecs({"Lucene3x", "Lucene40", "Lucene41", "Lucene42", "Lucene45"})
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

    CollectionAdminRequest.createCollection(collection, "conf", 2, 1)
        .setPerReplicaState(SolrCloudTestCase.USE_PER_REPLICA_STATE)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(collection, 2, 2);
    if (useAlias) {
      CollectionAdminRequest.createAlias(COLLECTIONORALIAS, collection).process(cluster.getSolrClient());
    }
  }

  public static SolrParams mapParams(String... vals) {
    ModifiableSolrParams params = new ModifiableSolrParams();
    assertEquals("Parameters passed in here must be in pairs!", 0, (vals.length % 2));
    for (int idx = 0; idx < vals.length; idx += 2) {
      params.add(vals[idx], vals[idx + 1]);
    }

    return params;
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

    String baseUrl = cluster.getJettySolrRunners().get(0).getBaseUrl().toString() + "/" + COLLECTIONORALIAS;
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
        .add("id", "5", "text_t", "XXXX XXXX", "str_s", "c", "field_i", "30", "specialchars_s", "witha|pipe")
        .add("id", "6", "text_t", "XXXX XXXX", "str_s", "c", "field_i", "40", "specialchars_s", "witha\\slash")
        .add("id", "7", "text_t", "XXXX XXXX", "str_s", "c", "field_i", "50", "specialchars_s", "witha!bang")
        .add("id", "8", "text_t", "XXXX XXXX", "str_s", "c", "field_i", "60", "specialchars_s", "witha\"quote")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    String baseUrl = cluster.getJettySolrRunners().get(0).getBaseUrl().toString() + "/" + COLLECTIONORALIAS;

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

    tuples = getTuples(sParams, baseUrl);

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

    expectResults("SELECT id FROM $ALIAS WHERE str_s = 'a'", 2);
    expectResults("SELECT id FROM $ALIAS WHERE 'a' = str_s", 2);
    expectResults("SELECT id FROM $ALIAS WHERE str_s <> 'c'", 4);
    expectResults("SELECT id FROM $ALIAS WHERE 'c' <> str_s", 4);
    expectResults("SELECT id FROM $ALIAS WHERE specialchars_s = 'witha\"quote'", 1);
    expectResults("SELECT id FROM $ALIAS WHERE specialchars_s = 'witha|pipe'", 1);
    expectResults("SELECT id FROM $ALIAS WHERE specialchars_s LIKE 'with%'", 4);
    expectResults("SELECT id FROM $ALIAS WHERE specialchars_s LIKE 'witha|%'", 1);
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

    String baseUrl = cluster.getJettySolrRunners().get(0).getBaseUrl().toString() + "/" + COLLECTIONORALIAS;

    SolrParams sParams = mapParams(CommonParams.QT, "/sql", "aggregationMode", "map_reduce",
        "stmt", "select id, Field_i, Str_s from collection1 where Text_t='XXXX' order by Field_i desc");

    List<Tuple> tuples = getTuples(sParams, baseUrl);

    assertEquals(tuples.toString(), 8, tuples.size());

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


    assertEquals(tuples.toString(), 2, tuples.size());

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

    String baseUrl = cluster.getJettySolrRunners().get(0).getBaseUrl().toString() + "/" + COLLECTIONORALIAS;


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

    String baseUrl = cluster.getJettySolrRunners().get(0).getBaseUrl().toString() + "/" + COLLECTIONORALIAS;

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
    Assert.assertEquals (tuples.toString(), 2, tuples.size());

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

    String baseUrl = cluster.getJettySolrRunners().get(0).getBaseUrl().toString() + "/" + COLLECTIONORALIAS;
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

    String baseUrl = cluster.getJettySolrRunners().get(0).getBaseUrl().toString() + "/" + COLLECTIONORALIAS;

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

    assertEquals(tuples.toString(), 3, tuples.size());

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

    String baseUrl = cluster.getJettySolrRunners().get(0).getBaseUrl().toString() + "/" + COLLECTIONORALIAS;

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

    // test bunch of where predicates
    sParams = mapParams(CommonParams.QT, "/sql",
        "stmt", "select count(*), sum(a_i), min(a_i), max(a_i), cast(avg(1.0 * a_i) as float), sum(a_f), " +
            "min(a_f), max(a_f), avg(a_f) from collection1 where id = 2 AND a_s='hello0' AND a_i=2 AND a_f=2");


    tuples = getTuples(sParams, baseUrl);
    assert (tuples.size() == 1);
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

    String baseUrl = cluster.getJettySolrRunners().get(0).getBaseUrl().toString() + "/" + COLLECTIONORALIAS;

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

    String baseUrl = cluster.getJettySolrRunners().get(0).getBaseUrl().toString() + "/" + COLLECTIONORALIAS;

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

    // verify exception message formatting with wildcard query
    sParams = mapParams(CommonParams.QT, "/sql", "aggregationMode", "map_reduce",
        "stmt",
        "select str_s from collection1 where not_a_field LIKE 'foo%'");

    solrStream = new SolrStream(baseUrl, sParams);
    tuple = getTuple(new ExceptionStream(solrStream));
    assert (tuple.EOF);
    assert (tuple.EXCEPTION);
    assert (tuple.getException().contains("Column 'not_a_field' not found in any table"));
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

    String baseUrl = cluster.getJettySolrRunners().get(0).getBaseUrl().toString() + "/" + COLLECTIONORALIAS;

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

    String baseUrl = cluster.getJettySolrRunners().get(0).getBaseUrl().toString() + "/" + COLLECTIONORALIAS;

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
    long lv = (long) tuple.get(fieldName);
    if (lv != l) {
      throw new Exception("Longs not equal:" + l + " : " + lv);
    }

    return true;
  }

  public boolean assertString(Tuple tuple, String fieldName, String expected) throws Exception {
    String actual = (String) tuple.get(fieldName);

    if ((null == expected && null != actual) ||
        (null != expected && null == actual) ||
        (null != expected && !expected.equals(actual))) {
      throw new Exception("Longs not equal:" + expected + " : " + actual);
    }

    return true;
  }

  public boolean assertDouble(Tuple tuple, String fieldName, double d) throws Exception {
    double dv = tuple.getDouble(fieldName);
    if (dv != d) {
      throw new Exception("Doubles not equal:" + d + " : " + dv);
    }

    return true;
  }

  protected boolean assertMaps(@SuppressWarnings({"rawtypes"}) List<Map> maps, int... ids) throws Exception {
    if (maps.size() != ids.length) {
      throw new Exception("Expected id count != actual map count:" + ids.length + ":" + maps.size());
    }

    int i = 0;
    for (int val : ids) {
      @SuppressWarnings({"rawtypes"})
      Map t = maps.get(i);
      String tip = (String) t.get("id");
      if (!tip.equals(Integer.toString(val))) {
        throw new Exception("Found value:" + tip + " expecting:" + val);
      }
      ++i;
    }
    return true;
  }

  protected List<Tuple> getTuples(final SolrParams params, String baseUrl) throws IOException {
    List<Tuple> tuples = new LinkedList<>();
    try (TupleStream tupleStream = new SolrStream(baseUrl, params)) {
      tupleStream.open();
      for (; ; ) {
        Tuple t = tupleStream.read();
        if (t.EOF) {
          break;
        } else {
          tuples.add(t);
        }
      }
    }
    return tuples;
  }

  protected Tuple getTuple(TupleStream tupleStream) throws IOException {
    tupleStream.open();
    Tuple t = tupleStream.read();
    tupleStream.close();
    return t;
  }

  @Test
  public void testIn() throws Exception {
    new UpdateRequest()
        .add("id", "1", "text_t", "foobar", "str_s", "a")
        .add("id", "2", "text_t", "foobaz", "str_s", "b")
        .add("id", "3", "text_t", "foobaz", "str_s", "c")
        .add("id", "4", "text_t", "foobaz", "str_s", "d")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    SolrParams sParams = mapParams(CommonParams.QT, "/sql",
        "stmt",
        "select id from collection1 where str_s IN ('a','b','c')");

    String baseUrl = cluster.getJettySolrRunners().get(0).getBaseUrl().toString() + "/" + COLLECTIONORALIAS;
    List<Tuple> tuples = getTuples(sParams, baseUrl);
    assertEquals(3, tuples.size());
  }

  private String sqlUrl() {
    return cluster.getJettySolrRunners().get(0).getBaseUrl().toString() + "/" + COLLECTIONORALIAS;
  }

  private List<Tuple> expectResults(String sql, final int expectedCount) throws Exception {
    String sqlStmt = sql.replace("$ALIAS", COLLECTIONORALIAS);
    SolrParams params = mapParams(CommonParams.QT, "/sql", "stmt", sqlStmt);
    List<Tuple> tuples = getTuples(params, sqlUrl());
    assertEquals(expectedCount, tuples.size());
    return tuples;
  }

  @Test
  public void testColIsNotNull() throws Exception {
    new UpdateRequest()
        .add("id", "1", "b_s", "foobar")
        .add("id", "2", "b_s", "foobaz")
        .add("id", "3")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    expectResults("SELECT b_s FROM $ALIAS WHERE b_s IS NOT NULL", 2);
  }

  @Test
  public void testColIsNull() throws Exception {
    new UpdateRequest()
        .add("id", "1", "b_s", "foobar")
        .add("id", "2")
        .add("id", "3", "b_s", "foobaz")
        .add("id", "4")
        .add("id", "5", "b_s", "bazbar")
        .add("id", "6")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    expectResults("SELECT id FROM $ALIAS WHERE b_s IS NULL", 3);
  }

  @Test
  public void testLike() throws Exception {
    new UpdateRequest()
        .add("id", "1", "a_s", "hello-1", "b_s", "foo", "c_t", "the quick brown fox jumped over the lazy dog")
        .add("id", "2", "a_s", "world-2", "b_s", "foo", "c_t", "the sly black dog jumped over the sleeping pig")
        .add("id", "3", "a_s", "hello-3", "b_s", "foo", "c_t", "the quick brown fox jumped over the lazy dog")
        .add("id", "4", "a_s", "world-4", "b_s", "foo", "c_t", "the sly black dog jumped over the sleepy pig")
        .add("id", "5", "a_s", "hello-5", "b_s", "foo", "c_t", "the quick brown fox jumped over the lazy dog")
        .add("id", "6", "a_s", "w_orld-6", "b_s", "bar", "c_t", "the sly black dog jumped over the sleepin piglet")
        .add("id", "7", "a_s", "world%_7", "b_s", "zaz", "c_t", "the lazy dog jumped over the quick brown fox")
        .add("id", "8", "a_s", "world'\\9", "b_s", "zaz", "c_t", "the lazy dog ducked over the quick brown fox")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    expectResults("SELECT a_s FROM $ALIAS WHERE a_s LIKE 'h_llo-%'", 3);
    Throwable exception = expectThrows(IOException.class, () -> expectResults("SELECT a_s FROM $ALIAS WHERE a_s LIKE 'world\\'%' ESCAPE '\\'", 1));
    assertTrue(exception.getMessage().contains("parse failed: Lexical error"));
    expectResults("SELECT a_s FROM $ALIAS WHERE a_s LIKE 'world''%'", 1);
    expectResults("SELECT a_s FROM $ALIAS WHERE a_s LIKE 'world''\\\\%' ESCAPE '\\'", 1);
    expectResults("SELECT a_s FROM $ALIAS WHERE a_s LIKE 'w\\_o_ld%' ESCAPE '\\'", 1);
    expectResults("SELECT a_s FROM $ALIAS WHERE a_s LIKE 'world\\%\\__' ESCAPE '\\'", 1);

    // not technically valid SQL but we support it for legacy purposes, see: SOLR-15463
    expectResults("SELECT a_s FROM $ALIAS WHERE a_s='world-*'", 2);

    // no results
    expectResults("SELECT a_s FROM $ALIAS WHERE a_s LIKE '%MATCHNONE%'", 0);

    // like but without wildcard, should still work
    expectResults("SELECT b_s FROM $ALIAS WHERE b_s LIKE 'foo'", 5);

    // NOT LIKE
    expectResults("SELECT b_s FROM $ALIAS WHERE b_s NOT LIKE 'f%'", 3);

    // leading wildcard
    expectResults("SELECT b_s FROM $ALIAS WHERE b_s LIKE '%oo'", 5);

    // user supplied parens around arg, no double-quotes ...
    expectResults("SELECT b_s FROM $ALIAS WHERE b_s LIKE '(fo%)'", 5);

    expectResults("SELECT b_s FROM $ALIAS WHERE b_s LIKE '(ba*)'", 1);

    expectResults("SELECT b_s FROM $ALIAS WHERE c_t LIKE 'fox'", 5);
    expectResults("SELECT b_s FROM $ALIAS WHERE c_t LIKE 'sleep% pig%'", 3);
    expectResults("SELECT b_s FROM $ALIAS WHERE c_t LIKE 'sleep% pigle%'", 1);
    expectResults("SELECT b_s FROM $ALIAS WHERE c_t LIKE 'sleep% piglet'", 1);
    expectResults("SELECT b_s FROM $ALIAS WHERE c_t LIKE 'sleep% pigle%'", 1);
    expectResults("SELECT b_s FROM $ALIAS WHERE c_t LIKE 'sleep% piglet'", 1);

    expectResults("SELECT b_s FROM $ALIAS WHERE c_t LIKE 'jump%'", 7);
    expectResults("SELECT b_s FROM $ALIAS WHERE c_t LIKE '%ump%'", 7);

    expectResults("SELECT b_s FROM $ALIAS WHERE c_t LIKE '(\"dog pig\"~5)'", 2);
    expectResults("SELECT b_s FROM $ALIAS WHERE c_t LIKE 'jumped over'", 8);
    expectResults("SELECT b_s FROM $ALIAS WHERE c_t LIKE 'quick brow%'", 5);
    expectResults("SELECT b_s FROM $ALIAS WHERE c_t LIKE 'quick brow%' AND b_s LIKE 'foo*'", 3);
    expectResults("SELECT b_s FROM $ALIAS WHERE b_s LIKE 'foo*'", 5);
    expectResults("SELECT b_s FROM $ALIAS WHERE c_t LIKE '*og'", 8);
  }

  @Test
  public void testBetween() throws Exception {
    new UpdateRequest()
        .add(withMultiValuedField("b_is", Arrays.asList(1, 5), "id", "1", "a_i", "1"))
        .add(withMultiValuedField("b_is", Arrays.asList(2, 6), "id", "2", "a_i", "2"))
        .add(withMultiValuedField("b_is", Arrays.asList(3, 7), "id", "3", "a_i", "3"))
        .add(withMultiValuedField("b_is", Arrays.asList(4, 8), "id", "4", "a_i", "4"))
        .add(withMultiValuedField("b_is", Arrays.asList(5, 9), "id", "5", "a_i", "5"))
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    expectResults("SELECT a_i FROM $ALIAS WHERE a_i BETWEEN 2 AND 4", 3);
    expectResults("SELECT a_i FROM $ALIAS WHERE a_i NOT BETWEEN 2 AND 4", 2);
    expectResults("SELECT id FROM $ALIAS WHERE b_is BETWEEN 2 AND 4", 3);
    expectResults("SELECT id FROM $ALIAS WHERE b_is BETWEEN 1 AND 9", 5);
    expectResults("SELECT id FROM $ALIAS WHERE b_is BETWEEN 8 AND 10", 2);
    expectResults("SELECT id FROM $ALIAS WHERE b_is >= 2 AND b_is <= 4", 3);
    expectResults("SELECT id FROM $ALIAS WHERE b_is <= 4 AND b_is >= 2", 3);
    expectResults("SELECT id FROM $ALIAS WHERE b_is <= 2 OR b_is >= 8", 4);
    // tricky ~ with Solr, this should return 2 docs, but Calcite short-circuits this query and returns 0
    // Calcite sees the predicate as disjoint from a single-valued field perspective ...
    expectResults("SELECT id FROM $ALIAS WHERE b_is >= 5 AND b_is <= 2", 2);
    // hacky work-around the aforementioned problem ^^
    expectResults("SELECT id FROM $ALIAS WHERE b_is = '(+[5 TO *] +[* TO 2])'", 2);
  }

  private SolrInputDocument withMultiValuedField(String mvField, List<Object> values, String... fields) {
    SolrInputDocument doc = new SolrInputDocument(fields);
    doc.addField(mvField, values);
    return doc;
  }

  @Test
  public void testMultipleFilters() throws Exception {
    new UpdateRequest()
        .add("id", "1", "a_s", "hello-1", "b_s", "foo", "c_s", "bar", "d_s", "x")
        .add("id", "2", "a_s", "world-2", "b_s", "foo", "a_i", "2", "d_s", "a")
        .add("id", "3", "a_s", "hello-3", "b_s", "foo", "c_s", "bar", "d_s", "x")
        .add("id", "4", "a_s", "world-4", "b_s", "foo", "a_i", "3", "d_s", "b")
        .add("id", "5", "a_s", "hello-5", "b_s", "foo", "c_s", "bar", "d_s", "x")
        .add("id", "6", "a_s", "world-6", "b_s", "bar", "a_i", "4", "d_s", "c")
        .add("id", "7", "a_s", "hello-7", "b_s", "foo", "c_s", "baz blah", "d_s", "x")
        .add("id", "8", "a_s", "world-8", "b_s", "bar", "a_i", "5", "d_s", "c")
        .add("id", "9", "a_s", "world-9", "b_s", "bar", "a_i", "1", "d_s", "x")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    List<Tuple> tuples = expectResults("SELECT a_s FROM $ALIAS WHERE a_s LIKE 'world%' AND b_s IS NOT NULL AND c_s IS NULL AND a_i BETWEEN 2 AND 4 AND d_s IN ('a','b','c') ORDER BY id ASC LIMIT 10", 3);

    assertEquals("world-2", tuples.get(0).getString("a_s"));
    assertEquals("world-4", tuples.get(1).getString("a_s"));
    assertEquals("world-6", tuples.get(2).getString("a_s"));

    tuples = expectResults("SELECT a_s FROM $ALIAS WHERE a_s NOT LIKE 'hello%' AND b_s IS NOT NULL AND c_s IS NULL AND a_i NOT BETWEEN 2 AND 4 AND d_s IN ('a','b','c') ORDER BY id ASC LIMIT 10", 1);
    assertEquals("world-8", tuples.get(0).getString("a_s"));
  }

  @Test
  public void testCountWithFilters() throws Exception {
    new UpdateRequest()
        .add("id", "1", "a_s", "hello-1", "b_s", "foo", "c_s", "bar", "d_s", "x")
        .add("id", "2", "a_s", "world-2", "b_s", "foo", "a_i", "2", "d_s", "a")
        .add("id", "3", "a_s", "hello-3", "b_s", "foo", "c_s", "bar", "d_s", "x")
        .add("id", "4", "a_s", "world-4", "b_s", "foo", "a_i", "3", "d_s", "b")
        .add("id", "5", "a_s", "hello-5", "b_s", "foo", "c_s", "bar", "d_s", "x")
        .add("id", "6", "a_s", "world-6", "b_s", "bar", "a_i", "4", "d_s", "c")
        .add("id", "7", "a_s", "hello-7", "b_s", "foo", "c_s", "baz blah", "d_s", "x")
        .add("id", "8", "a_s", "world-8", "b_s", "bar", "a_i", "5", "d_s", "c")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    List<Tuple> tuples = expectResults("SELECT COUNT(1) as `the_count` FROM $ALIAS as `alias` WHERE (`alias`.`b_s`='foo' AND `alias`.`a_s` LIKE 'hell%' AND `alias`.`c_s` IS NOT NULL) HAVING (COUNT(1) > 0)", 1);
    assertEquals(4L, (long) tuples.get(0).getLong("the_count"));
  }

  @Test
  public void testDateHandling() throws Exception {
    new UpdateRequest()
        .add("id", "1", "pdatex", "2021-06-01T00:00:00Z")
        .add("id", "2", "pdatex", "2021-06-02T02:00:00Z")
        .add("id", "3", "pdatex", "2021-06-03T03:00:00Z")
        .add("id", "4", "pdatex", "2021-06-04T04:00:00Z")
        .add("id", "5", "pdatex", "2021-06-01T01:01:00Z")
        .add("id", "6", "pdatex", "2021-06-02T02:02:00Z")
        .add("id", "7", "pdatex", "2021-06-03T03:03:00Z")
        .add("id", "8", "pdatex", "2021-06-04T04:04:00Z")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    expectResults("SELECT id FROM $ALIAS WHERE pdatex IS NULL", 0);
    expectResults("SELECT id, pdatex FROM $ALIAS WHERE pdatex IS NOT NULL", 8);
    expectResults("SELECT id, pdatex FROM $ALIAS WHERE pdatex > '2021-06-02'", 6);
    expectResults("SELECT id, pdatex FROM $ALIAS WHERE pdatex <= '2021-06-01'", 1);
    expectResults("SELECT id, pdatex FROM $ALIAS WHERE pdatex > '2021-06-04 04:00:00'", 1);
    expectResults("SELECT id, pdatex FROM $ALIAS WHERE pdatex = '2021-06-04 04:00:00'", 1);
    expectResults("SELECT id, pdatex FROM $ALIAS WHERE pdatex = CAST('2021-06-04 04:04:00' as TIMESTAMP)", 1);
    expectResults("SELECT id, pdatex FROM $ALIAS WHERE pdatex BETWEEN '2021-06-03' AND '2021-06-05'", 4);
  }

  @Test
  public void testISO8601TimestampFiltering() throws Exception {
    new UpdateRequest()
        .add("id", "1", "pdatex", "2021-07-13T15:12:09.037Z")
        .add("id", "2", "pdatex", "2021-07-13T15:12:10.037Z")
        .add("id", "3", "pdatex", "2021-07-13T15:12:11.037Z")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    expectResults("SELECT id, pdatex FROM $ALIAS WHERE pdatex >= CAST('2021-07-13 15:12:10.037' as TIMESTAMP)", 2);
    expectResults("SELECT id, pdatex FROM $ALIAS WHERE pdatex >= '2021-07-13T15:12:10.037Z'", 2);
    expectResults("SELECT id, pdatex FROM $ALIAS WHERE pdatex < '2021-07-13T15:12:10.037Z'", 1);
    expectResults("SELECT id, pdatex FROM $ALIAS WHERE pdatex = '2021-07-13T15:12:10.037Z'", 1);
    expectResults("SELECT id, pdatex FROM $ALIAS WHERE pdatex <> '2021-07-13T15:12:10.037Z'", 2);
    expectResults("SELECT id, pdatex FROM $ALIAS WHERE pdatex BETWEEN '2021-07-13T15:12:09.037Z' AND '2021-07-13T15:12:10.037Z' ORDER BY pdatex ASC", 2);
    expectResults("SELECT id, pdatex FROM $ALIAS WHERE pdatex >= '2021-07-13T15:12:10.037Z'", 2);
    expectResults("SELECT id, pdatex FROM $ALIAS WHERE pdatex >= '2021-07-13T15:12:10.037Z' ORDER BY pdatex ASC LIMIT 10", 2);
  }

  @Test
  public void testAggsOnCustomFieldType() throws Exception {
    new UpdateRequest()
        .add(withMultiValuedField("pintxs", Arrays.asList(1,5),"id", "1", "tintx", "1", "pintx", "2", "tfloatx", "3.33", "pfloatx", "3.33", "tlongx", "1623875868000", "plongx", "1623875868000", "tdoublex", "3.14159265359", "pdoublex", "3.14159265359", "stringx", "A", "textx", "aaa", "pdatex", "2021-06-17T00:00:00Z"))
        .add(withMultiValuedField("pintxs", Arrays.asList(2,6),"id", "2", "tintx", "2", "pintx", "4", "tfloatx", "4.44", "pfloatx", "4.44", "tlongx", "1723875868000", "plongx", "1723875868000", "tdoublex", "6.14159265359", "pdoublex", "6.14159265359", "stringx", "B", "textx", "bbb", "pdatex", "2021-06-18T00:00:00Z"))
        .add(withMultiValuedField("pintxs", Arrays.asList(3,7),"id", "3", "tintx", "3", "pintx", "6", "tfloatx", "5.55", "pfloatx", "5.55", "tlongx", "1823875868000", "plongx", "1823875868000", "tdoublex", "9.14159265359", "pdoublex", "9.14159265359", "stringx", "C", "textx", "ccc", "pdatex", "2021-06-19T00:00:00Z"))
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    String dateStatsSql = "min(pdatex) as min_pdatex, max(pdatex) as max_pdatex";
    String numTypeStatsSql = toStatsSql(Arrays.asList("intx", "floatx", "longx", "doublex"));
    String sql = "SELECT min(pintxs) as min_pintxs, max(pintxs) as max_pintxs, "+
        min("stringx")+", "+max("stringx")+", "+min("textx")+", "+max("textx")+", "+numTypeStatsSql+", "+dateStatsSql+" FROM $ALIAS";

    List<Tuple> tuples = expectResults(sql, 1);
    Tuple stats = tuples.get(0);
    assertEquals("A", stats.getString("min_stringx"));
    assertEquals("C", stats.getString("max_stringx"));
    assertEquals("aaa", stats.getString("min_textx"));
    assertEquals("ccc", stats.getString("max_textx"));
    assertEquals(1L, (long) stats.getLong("min_tintx"));
    assertEquals(3L, (long) stats.getLong("max_tintx"));
    assertEquals(2L, (long) stats.getLong("min_pintx"));
    assertEquals(6L, (long) stats.getLong("max_pintx"));
    assertEquals(1L, (long) stats.getLong("min_pintxs"));
    assertEquals(7L, (long) stats.getLong("max_pintxs"));
    assertEquals(1623875868000L, (long) stats.getLong("min_tlongx"));
    assertEquals(1823875868000L, (long) stats.getLong("max_tlongx"));
    assertEquals(1623875868000L, (long) stats.getLong("min_plongx"));
    assertEquals(1823875868000L, (long) stats.getLong("max_plongx"));
    final double delta = 0.00001d;
    assertEquals(3.33d, stats.getDouble("min_tfloatx"), delta);
    assertEquals(5.55d, stats.getDouble("max_tfloatx"), delta);
    assertEquals(3.33d, stats.getDouble("min_pfloatx"), delta);
    assertEquals(5.55d, stats.getDouble("max_pfloatx"), delta);
    assertEquals(3.14159265359d, stats.getDouble("min_tdoublex"), delta);
    assertEquals(9.14159265359d, stats.getDouble("max_tdoublex"), delta);
    assertEquals(3.14159265359d, stats.getDouble("min_pdoublex"), delta);
    assertEquals(9.14159265359d, stats.getDouble("max_pdoublex"), delta);
    assertNotNull(stats.getDate("min_pdatex"));
    assertNotNull(stats.getDate("max_pdatex"));
  }

  private String toStatsSql(List<String> types) {
    StringBuilder sb = new StringBuilder();
    for (String type : types) {
      if (sb.length() > 0) {
        sb.append(", ");
      }
      sb.append(min("t"+type)).append(", ").append(min("p"+type));
      sb.append(", ").append(max("t"+type)).append(", ").append(max("p"+type));
    }
    return sb.toString();
  }

  private String min(String type) {
    return String.format(Locale.ROOT, "min(%s) as min_%s", type, type);
  }

  private String max(String type) {
    return String.format(Locale.ROOT, "max(%s) as max_%s", type, type);
  }

  @Test
  public void testOffsetAndFetch() throws Exception {
    new UpdateRequest()
        .add("id", "01")
        .add("id", "02")
        .add("id", "03")
        .add("id", "04")
        .add("id", "05")
        .add("id", "06")
        .add("id", "07")
        .add("id", "08")
        .add("id", "09")
        .add("id", "10")
        .add("id", "11")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    final int numDocs = 11;

    List<Tuple> results = expectResults("SELECT id FROM $ALIAS ORDER BY id DESC OFFSET 0 FETCH NEXT 5 ROWS ONLY", 5);
    assertEquals("11", results.get(0).getString("id"));
    assertEquals("10", results.get(1).getString("id"));
    assertEquals("09", results.get(2).getString("id"));
    assertEquals("08", results.get(3).getString("id"));
    assertEquals("07", results.get(4).getString("id"));

    // no explicit offset, but defaults to 0 if using FETCH!
    results = expectResults("SELECT id FROM $ALIAS ORDER BY id DESC FETCH NEXT 5 ROWS ONLY", 5);
    assertEquals("11", results.get(0).getString("id"));
    assertEquals("10", results.get(1).getString("id"));
    assertEquals("09", results.get(2).getString("id"));
    assertEquals("08", results.get(3).getString("id"));
    assertEquals("07", results.get(4).getString("id"));

    results = expectResults("SELECT id FROM $ALIAS ORDER BY id DESC OFFSET 5 FETCH NEXT 5 ROWS ONLY", 5);
    assertEquals("06", results.get(0).getString("id"));
    assertEquals("05", results.get(1).getString("id"));
    assertEquals("04", results.get(2).getString("id"));
    assertEquals("03", results.get(3).getString("id"));
    assertEquals("02", results.get(4).getString("id"));

    results = expectResults("SELECT id FROM $ALIAS ORDER BY id DESC OFFSET 10 FETCH NEXT 5 ROWS ONLY", 1);
    assertEquals("01", results.get(0).getString("id"));

    expectResults("SELECT id FROM $ALIAS ORDER BY id DESC LIMIT "+numDocs, numDocs);

    for (int i=0; i < numDocs; i++) {
      results = expectResults("SELECT id FROM $ALIAS ORDER BY id ASC OFFSET "+i+" FETCH NEXT 1 ROW ONLY", 1);
      String id = results.get(0).getString("id");
      if (id.startsWith("0")) id = id.substring(1);
      assertEquals(i+1, Integer.parseInt(id));
    }

    // just past the end of the results
    expectResults("SELECT id FROM $ALIAS ORDER BY id DESC OFFSET "+numDocs+" FETCH NEXT 5 ROWS ONLY", 0);

    // Solr doesn't support OFFSET w/o LIMIT
    expectThrows(IOException.class, () -> expectResults("SELECT id FROM $ALIAS ORDER BY id DESC OFFSET 5", 5));
  }

  @Test
  public void testCountDistinct() throws Exception {
    UpdateRequest updateRequest = new UpdateRequest();
    final int cardinality = 5;
    final int maxDocs = 100; // keep this an even # b/c we divide by 2 in this test
    final String padFmt = "%03d";
    for (int i = 0; i < maxDocs; i++) {
      updateRequest = addDocForDistinctTests(i, updateRequest, cardinality, padFmt);
    }
    updateRequest.commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    List<Tuple> tuples = expectResults("SELECT COUNT(1) AS total_rows, COUNT(distinct str_s) AS distinct_str, MIN(str_s) AS min_str, MAX(str_s) AS max_str FROM $ALIAS", 1);
    Tuple firstRow = tuples.get(0);
    assertEquals(maxDocs, (long) firstRow.getLong("total_rows"));
    assertEquals(cardinality, (long) firstRow.getLong("distinct_str"));

    String expectedMin = String.format(Locale.ROOT, padFmt, 0);
    String expectedMax = String.format(Locale.ROOT, padFmt, cardinality - 1); // max is card-1
    assertEquals(expectedMin, firstRow.getString("min_str"));
    assertEquals(expectedMax, firstRow.getString("max_str"));

    tuples = expectResults("SELECT DISTINCT str_s FROM $ALIAS ORDER BY str_s ASC", cardinality);
    for (int t = 0; t < tuples.size(); t++) {
      assertEquals(String.format(Locale.ROOT, padFmt, t), tuples.get(t).getString("str_s"));
    }

    tuples = expectResults("SELECT APPROX_COUNT_DISTINCT(distinct str_s) AS approx_distinct FROM $ALIAS", 1);
    firstRow = tuples.get(0);
    assertEquals(cardinality, (long) firstRow.getLong("approx_distinct"));

    tuples = expectResults("SELECT country_s, COUNT(*) AS count_per_bucket FROM $ALIAS GROUP BY country_s", 2);
    assertEquals(maxDocs/2L, (long)tuples.get(0).getLong("count_per_bucket"));
    assertEquals(maxDocs/2L, (long)tuples.get(1).getLong("count_per_bucket"));
  }

  private UpdateRequest addDocForDistinctTests(int id, UpdateRequest updateRequest, int cardinality, String padFmt) {
    String country = id % 2 == 0 ? "US" : "CA";
    return updateRequest.add("id", String.valueOf(id), "str_s", String.format(Locale.ROOT, padFmt, id % cardinality), "country_s", country);
  }

  @Test
  public void testSelectStarWithLimit() throws Exception {
    new UpdateRequest()
        .add("id", "1", "a_s", "hello-1", "b_s", "foo", "c_s", "bar", "d_s", "x")
        .add("id", "2", "a_s", "world-2", "b_s", "foo", "a_i", "2", "d_s", "a")
        .add("id", "3", "a_s", "hello-3", "b_s", "foo", "c_s", "bar", "d_s", "x")
        .add("id", "4", "a_s", "world-4", "b_s", "foo", "a_i", "3", "d_s", "b")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    expectResults("SELECT * FROM $ALIAS LIMIT 100", 4);
    // select * w/o limit is not supported by Solr SQL
    expectThrows(IOException.class, () -> expectResults("SELECT * FROM $ALIAS", -1));
  }

  @Test
  public void testSelectEmptyField() throws Exception {
    new UpdateRequest()
        .add("id", "01", "notstored", "X", "dvonly", "Y")
        .add("id", "02", "notstored", "X", "dvonly", "Y")
        .add("id", "03", "notstored", "X", "dvonly", "Y")
        .add("id", "04", "notstored", "X", "dvonly", "Y")
        .add("id", "05", "notstored", "X", "dvonly", "Y")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    // stringx is declared in the schema but has no docs
    expectResults("SELECT id, stringx FROM $ALIAS", 5);
    expectResults("SELECT id, stringx FROM $ALIAS LIMIT 10", 5);
    expectResults("SELECT id, stringx, dvonly FROM $ALIAS", 5);
    expectResults("SELECT id, stringx, dvonly FROM $ALIAS LIMIT 10", 5);

    // notafield_i matches a dynamic field pattern but has no docs, so don't allow this
    expectThrows(IOException.class, () -> expectResults("SELECT id, stringx, notafield_i FROM $ALIAS", 5));
    expectThrows(IOException.class, () -> expectResults("SELECT id, stringx, notstored FROM $ALIAS", 5));
  }

  @Test
  public void testMultiValuedFieldHandling() throws Exception {
    List<String> textmv = Arrays.asList("just some text here", "across multiple values", "the quick brown fox jumped over the lazy dog");
    List<String> listOfTimestamps = Arrays.asList("2021-08-06T15:37:52Z", "2021-08-06T15:37:53Z", "2021-08-06T15:37:54Z");
    List<Date> dates = listOfTimestamps.stream().map(ts -> new Date(Instant.parse(ts).toEpochMilli())).collect(Collectors.toList());
    List<String> stringxmv = Arrays.asList("a", "b", "c");
    List<String> stringsx = Arrays.asList("d", "e", "f");
    List<Double> pdoublesx = Arrays.asList(1d, 2d, 3d);
    List<Double> pdoublexmv = Arrays.asList(4d, 5d, 6d);
    List<Boolean> booleans = Arrays.asList(false, true);
    List<Long> evenLongs = Arrays.asList(2L, 4L, 6L);
    List<Long> oddLongs = Arrays.asList(1L, 3L, 5L);

    UpdateRequest update = new UpdateRequest();
    final int maxDocs = 10;
    for (int i = 0; i < maxDocs; i++) {
      SolrInputDocument doc = new SolrInputDocument("id", String.valueOf(i));
      if (i % 2 == 0) {
        doc.setField("stringsx", stringsx);
        doc.setField("pdoublexmv", pdoublexmv);
        doc.setField("longs", evenLongs);
      } else {
        // stringsx & pdoublexmv null
        doc.setField("longs", oddLongs);
      }
      doc.setField("stringxmv", stringxmv);
      doc.setField("pdoublesx", pdoublesx);
      doc.setField("pdatexs", dates);
      doc.setField("textmv", textmv);
      doc.setField("booleans", booleans);
      update.add(doc);
    }
    update.add("id", String.valueOf(maxDocs)); // all multi-valued fields are null
    update.commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    expectResults("SELECT longs, stringsx, booleans FROM $ALIAS WHERE longs = 2 AND longs = 4", 5);
    expectResults(
        "SELECT longs, pdoublexmv, booleans FROM $ALIAS WHERE pdoublexmv = 4.0 AND pdoublexmv = 5.0",
        5);
    expectResults("SELECT longs, pdoublexmv, booleans FROM $ALIAS WHERE pdoublexmv > 4.0", 5);
    expectResults(
        "SELECT stringxmv, stringsx, booleans FROM $ALIAS WHERE stringxmv = 'a' AND stringxmv = 'b' AND stringxmv = 'c'",
        10);

    expectResults("SELECT stringxmv, stringsx, booleans FROM $ALIAS WHERE stringxmv > 'a'", 10);
    expectResults("SELECT stringxmv, stringsx, booleans FROM $ALIAS WHERE stringxmv NOT IN ('a')", 1);
    expectResults("SELECT stringxmv, stringsx, booleans FROM $ALIAS WHERE stringxmv > 'a' LIMIT 10", 10);
    expectResults("SELECT stringxmv, stringsx, booleans FROM $ALIAS WHERE stringxmv NOT IN ('a') LIMIT 10", 1);

    // can't sort by a mv field
    expectThrows(IOException.class,
        () -> expectResults("SELECT stringxmv FROM $ALIAS WHERE stringxmv IS NOT NULL ORDER BY stringxmv ASC LIMIT 10", 0));

    // even id's have these fields, odd's are null ...
    expectListInResults("0", "stringsx", stringsx, -1, 5);
    expectListInResults("0", "pdoublexmv", pdoublexmv, -1, 5);
    expectListInResults("1", "stringsx", null, -1, 0);
    expectListInResults("1", "pdoublexmv", null, -1, 0);
    expectListInResults("2", "stringsx", stringsx, 10, 5);
    expectListInResults("2", "pdoublexmv", pdoublexmv, 10, 5);

    expectListInResults("1", "stringxmv", stringxmv, -1, 10);
    expectListInResults("1", "pdoublesx", pdoublesx, -1, 10);
    expectListInResults("1", "pdatexs", listOfTimestamps, -1, 10);
    expectListInResults("1", "booleans", booleans, -1, 10);
    expectListInResults("1", "longs", oddLongs, -1, 5);

    expectListInResults("2", "stringxmv", stringxmv, 10, 10);
    expectListInResults("2", "pdoublesx", pdoublesx, 10, 10);
    expectListInResults("2", "pdatexs", listOfTimestamps, 10, 10);
    expectListInResults("2", "textmv", textmv, 10, 10);
    expectListInResults("2", "booleans", booleans, 10, 10);
    expectListInResults("2", "longs", evenLongs, 10, 5);

    expectAggCount("stringxmv", 3);
    expectAggCount("stringsx", 3);
    expectAggCount("pdoublesx", 3);
    expectAggCount("pdoublexmv", 3);
    expectAggCount("pdatexs", 3);
    expectAggCount("booleans", 2);
    expectAggCount("longs", 6);
  }

  private void expectListInResults(String id, String mvField, List<?> expected, int limit, int expCount) throws Exception {
    String projection = limit > 0 ? "*" : "id," + mvField;
    String sql = "SELECT " + projection + " FROM $ALIAS WHERE id='" + id + "'";
    if (limit > 0) sql += " LIMIT " + limit;
    List<Tuple> results = expectResults(sql, 1);
    if (expected != null) {
      assertEquals(expected, results.get(0).get(mvField));
    } else {
      assertNull(results.get(0).get(mvField));
    }

    if (expected != null) {
      String crit = "'" + expected.get(0) + "'";
      sql = "SELECT " + projection + " FROM $ALIAS WHERE " + mvField + "=" + crit;
      if (limit > 0) sql += " LIMIT " + limit;
      expectResults(sql, expCount);

      // test "IN" operator but skip for text analyzed fields
      if (!"textmv".equals(mvField)) {
        String inClause = expected.stream().map(o -> "'" + o + "'").collect(Collectors.joining(","));
        sql = "SELECT " + projection + " FROM $ALIAS WHERE " + mvField + " IN (" + inClause + ")";
        if (limit > 0) sql += " LIMIT " + limit;
        expectResults(sql, expCount);
      }
    }
  }

  private void expectAggCount(String mvField, int expCount) throws Exception {
    expectResults("SELECT COUNT(*), " + mvField + " FROM $ALIAS GROUP BY " + mvField, expCount);
  }

  @Test
  public void testManyInValues() throws Exception {
    int maxSize = 1000;
    int width = 4;
    List<String> bigList = new ArrayList<>(maxSize);
    for (int i=0; i < maxSize; i++) {
      bigList.add(StringUtils.leftPad(String.valueOf(i), width, "0"));
    }

    UpdateRequest update = new UpdateRequest();
    final int maxDocs = 10;
    for (int i = 0; i < maxDocs; i++) {
      SolrInputDocument doc = new SolrInputDocument("id", String.valueOf(i));
      doc.setField("stringxmv", bigList);
      update.add(doc);
    }
    update.add("id", String.valueOf(maxDocs)); // no stringxmv

    SolrInputDocument doc = new SolrInputDocument("id", String.valueOf(maxDocs+1));
    doc.setField("stringxmv", Arrays.asList("a", "b", "c"));
    update.add(doc);

    doc = new SolrInputDocument("id", String.valueOf(maxDocs+2));
    doc.setField("stringxmv", Arrays.asList("d", "e", "f"));
    update.add(doc);

    update.commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    expectResults(
        "SELECT id FROM $ALIAS WHERE stringxmv IN ('a') AND stringxmv IN ('b') ORDER BY id ASC", 1);

    int numIn = 200;
    List<String> bigInList = new ArrayList<>(bigList);
    Collections.shuffle(bigInList, random());
    bigInList = bigInList.subList(0, numIn).stream().map(s -> "'"+s+"'").collect(Collectors.toList());
    String inClause = String.join(",", bigInList);
    String sql = "SELECT id FROM $ALIAS WHERE stringxmv IN ("+inClause+") ORDER BY id ASC";
    expectResults(sql, maxDocs);
    sql = "SELECT * FROM $ALIAS WHERE stringxmv IN ("+inClause+") ORDER BY id ASC LIMIT "+maxDocs;
    expectResults(sql, maxDocs);
    sql = "SELECT id FROM $ALIAS WHERE stringxmv NOT IN ("+inClause+") ORDER BY id ASC";
    expectResults(sql, 3);
    sql = "SELECT id FROM $ALIAS WHERE stringxmv IS NOT NULL AND stringxmv NOT IN ("+inClause+") ORDER BY id ASC";
    expectResults(sql, 2);
    sql = "SELECT * FROM $ALIAS WHERE stringxmv IN ('a','d') ORDER BY id ASC LIMIT 10";
    expectResults(sql, 2);
  }

  @Test
  public void testNotAndOrLogic() throws Exception {
    new UpdateRequest()
        .add("id", "1", "a_s", "hello-1", "b_s", "foo", "c_s", "bar", "d_s", "x")
        .add("id", "2", "a_s", "world-2", "b_s", "foo", "a_i", "2", "d_s", "a")
        .add("id", "3", "a_s", "hello-3", "b_s", "foo", "c_s", "bar", "d_s", "x")
        .add("id", "4", "a_s", "world-4", "b_s", "foo", "a_i", "3", "d_s", "b")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    // single NOT clause
    expectResults("SELECT id FROM $ALIAS WHERE a_s <> 'hello-1' ORDER BY id ASC LIMIT 10", 3);
    expectResults("SELECT id FROM $ALIAS WHERE b_s NOT LIKE 'foo' ORDER BY id ASC LIMIT 10", 0);
    expectResults("SELECT id FROM $ALIAS WHERE d_s NOT IN ('x','y') ORDER BY id ASC LIMIT 10", 2);
    expectResults("SELECT id FROM $ALIAS WHERE a_i IS NULL ORDER BY id ASC LIMIT 10", 2);
    expectResults("SELECT id FROM $ALIAS WHERE c_s IS NOT NULL ORDER BY id ASC LIMIT 10", 2);

    expectResults("SELECT * FROM $ALIAS WHERE a_s='hello-1' AND d_s='x' ORDER BY id ASC LIMIT 10", 1);
    expectResults("SELECT id FROM $ALIAS WHERE a_s='hello-1' AND d_s='x'", 1);

    expectResults("SELECT * FROM $ALIAS WHERE a_s <> 'hello-1' AND d_s <> 'x' ORDER BY id ASC LIMIT 10", 2);
    expectResults("SELECT id FROM $ALIAS WHERE a_s <> 'hello-1' AND d_s <> 'x'", 2);

    expectResults("SELECT * FROM $ALIAS WHERE d_s <> 'x' ORDER BY id ASC LIMIT 10", 2);
    expectResults("SELECT id FROM $ALIAS WHERE d_s <> 'x'", 2);

    expectResults("SELECT * FROM $ALIAS WHERE (a_s = 'hello-1' OR a_s = 'hello-3') AND d_s <> 'x' ORDER BY id ASC LIMIT 10", 0);
    expectResults("SELECT id FROM $ALIAS WHERE (a_s = 'hello-1' OR a_s = 'hello-3') AND d_s <> 'x'", 0);

    expectResults("SELECT * FROM $ALIAS WHERE (a_s = 'hello-1' OR a_s = 'hello-3') AND d_s NOT IN ('x') ORDER BY id ASC LIMIT 10", 0);
    expectResults("SELECT id FROM $ALIAS WHERE (a_s = 'hello-1' OR a_s = 'hello-3') AND d_s NOT IN ('x')", 0);
    expectResults("SELECT * FROM $ALIAS WHERE (a_s = 'hello-1' OR a_s = 'hello-3') AND d_s NOT IN ('a') ORDER BY id ASC LIMIT 10", 2);
    expectResults("SELECT id FROM $ALIAS WHERE (a_s = 'hello-1' OR a_s = 'hello-3') AND d_s NOT IN ('a')", 2);

    expectResults("SELECT * FROM $ALIAS WHERE (a_s = 'hello-1' OR a_s = 'hello-3') AND d_s NOT LIKE 'x' ORDER BY id ASC LIMIT 10", 0);
    expectResults("SELECT id FROM $ALIAS WHERE (a_s = 'hello-1' OR a_s = 'hello-3') AND d_s NOT LIKE 'x'", 0);
    expectResults("SELECT * FROM $ALIAS WHERE (a_s = 'hello-1' OR a_s = 'hello-3') AND d_s NOT LIKE 'b' ORDER BY id ASC LIMIT 10", 2);
    expectResults("SELECT id FROM $ALIAS WHERE (a_s = 'hello-1' OR a_s = 'hello-3') AND d_s NOT LIKE 'b'", 2);
    expectResults("SELECT * FROM $ALIAS WHERE (a_s = 'hello-1' OR a_s = 'hello-3') AND d_s LIKE 'x' ORDER BY id ASC LIMIT 10", 2);
    expectResults("SELECT id FROM $ALIAS WHERE (a_s = 'hello-1' OR a_s = 'hello-3') AND d_s LIKE 'x'", 2);

    expectResults("SELECT * FROM $ALIAS WHERE a_s <> 'hello-1' AND b_s='foo' AND d_s IS NOT NULL AND a_i IS NULL AND c_s IN ('bar') ORDER BY id ASC LIMIT 10", 1);
    expectResults("SELECT id FROM $ALIAS WHERE a_s <> 'hello-1' AND b_s='foo' AND d_s IS NOT NULL AND a_i IS NULL AND c_s IN ('bar')", 1);

    // just a bunch of OR's that end up matching all docs
    expectResults("SELECT id FROM $ALIAS WHERE a_s <> 'hello-1' OR a_i <> 2 OR d_s <> 'x' ORDER BY id ASC LIMIT 10", 4);
  }

  @Test
  public void testCountWithManyFilters() throws Exception {
    new UpdateRequest()
        .add("id", "1", "a_s", "hello-1", "b_s", "foo", "c_s", "bar", "d_s", "x")
        .add("id", "2", "a_s", "world-2", "b_s", "foo", "a_i", "2", "d_s", "a")
        .add("id", "3", "a_s", "hello-3", "b_s", "foo", "c_s", "bar", "d_s", "x")
        .add("id", "4", "a_s", "world-4", "b_s", "foo", "a_i", "3", "d_s", "b")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    expectResults("SELECT COUNT(*) as QUERY_COUNT FROM $ALIAS WHERE (id='1')", 1);
    expectResults("SELECT COUNT(*) as QUERY_COUNT FROM $ALIAS WHERE (d_s='x') AND (id='1')", 1);
    expectResults("SELECT COUNT(*) as QUERY_COUNT FROM $ALIAS WHERE (d_s='x') AND (id='1') AND (b_s='foo')", 1);
    expectResults("SELECT COUNT(1) as QUERY_COUNT FROM $ALIAS WHERE (d_s='x') AND (id='1') AND (b_s='foo')", 1);
    expectResults("SELECT COUNT(1) as QUERY_COUNT FROM $ALIAS WHERE (d_s='x') AND (id='1') AND (b_s='foo') AND (a_s='hello-1')", 1);
    expectResults("SELECT COUNT(*) as QUERY_COUNT, max(id) as max_id FROM $ALIAS WHERE (d_s='x') AND (id='1') AND (b_s='foo')", 1);
    expectResults("SELECT COUNT(*) as QUERY_COUNT FROM $ALIAS WHERE (d_s='x') AND (id='1') AND (b_s='foo') HAVING COUNT(*) > 0", 1);
  }

  @Test
  public void testCustomUDFArrayContains() throws Exception {
    new UpdateRequest()
        .add("id", "1", "name_s", "hello-1", "a_i", "1", "stringxmv", "a", "stringxmv", "b", "stringxmv", "c", "pdoublexmv", "1.5", "pdoublexmv", "2.5", "longs", "1", "longs", "2")
        .add("id", "2", "name_s", "hello-2", "a_i", "2", "stringxmv", "c", "stringxmv", "d", "stringxmv", "e", "pdoublexmv", "1.5", "pdoublexmv", "3.5", "longs", "1", "longs", "3")
        .add("id", "3", "name_s", "hello-3", "a_i", "3", "stringxmv", "e", "stringxmv", "\"f\"", "stringxmv", "g\"h", "stringxmv", "a", "pdoublexmv", "2.5", "pdoublexmv", "3.5", "longs", "2", "longs", "3")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    expectResults("select id, pdoublexmv from $ALIAS", 3);
    expectResults("select id, stringxmv from $ALIAS WHERE array_contains_all(pdoublexmv, (1.5, 2.5))", 1);
    expectResults("select id, stringxmv from $ALIAS WHERE array_contains_all(longs, (1, 3))", 1);
    expectResults("select id, stringxmv from $ALIAS WHERE array_contains_all(stringxmv, 'c')", 2);
    expectResults("select id, stringxmv from $ALIAS WHERE array_contains_all(stringxmv, ('c'))", 2);
    expectResults("select id, stringxmv from $ALIAS WHERE array_contains_all(stringxmv, ('a', 'b', 'c'))", 1);
    expectResults("select id, stringxmv from $ALIAS WHERE array_contains_all(stringxmv, ('b', 'c'))", 1);
    expectResults("select id, stringxmv from $ALIAS WHERE array_contains_all(stringxmv, ('c', 'e'))", 1);
    expectResults("select id, stringxmv from $ALIAS WHERE array_contains_all(stringxmv, ('c', 'd', 'e'))", 1);

    expectResults("select id, stringxmv from $ALIAS WHERE array_contains_any(stringxmv, ('\"a\"', '\"e\"'))", 0);
    expectResults("select id, stringxmv from $ALIAS WHERE array_contains_any(stringxmv, ('\"a\"'))", 0);
    expectResults("select id, stringxmv from $ALIAS WHERE array_contains_any(stringxmv, ('\"f\"'))", 1);
    expectResults("select id, stringxmv from $ALIAS WHERE array_contains_any(stringxmv, ('g\"h'))", 1);
    expectResults("select id, stringxmv from $ALIAS WHERE array_contains_any(stringxmv, ('a', 'e', '\"f\"'))", 3);
    expectResults("select id, stringxmv from $ALIAS WHERE array_contains_any(pdoublexmv, (1.5, 2.5))", 3);
    expectResults("select id, stringxmv from $ALIAS WHERE array_contains_any(longs, (1, 3))", 3);
    expectResults("select id, stringxmv from $ALIAS WHERE array_contains_any(stringxmv, ('a'))", 2);
    expectResults("select id, stringxmv from $ALIAS WHERE array_contains_any(stringxmv, ('a', 'b', 'c'))", 3);
    expectResults("select id, stringxmv from $ALIAS WHERE array_contains_any(stringxmv, ('a', 'c'))", 3);
    expectResults("select id, stringxmv from $ALIAS WHERE array_contains_any(stringxmv, ('a', 'e'))", 3);
    expectResults("select id, stringxmv from $ALIAS WHERE array_contains_any(stringxmv, ('a', 'e', 'f'))", 3);
  }
}
