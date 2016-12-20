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
package org.apache.solr.client.solrj.io.stream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.io.ClassificationEvaluation;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.ComparatorOrder;
import org.apache.solr.client.solrj.io.comp.FieldComparator;
import org.apache.solr.client.solrj.io.ops.ConcatOperation;
import org.apache.solr.client.solrj.io.ops.GroupOperation;
import org.apache.solr.client.solrj.io.ops.ReplaceOperation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParser;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.client.solrj.io.stream.metrics.CountMetric;
import org.apache.solr.client.solrj.io.stream.metrics.MaxMetric;
import org.apache.solr.client.solrj.io.stream.metrics.MeanMetric;
import org.apache.solr.client.solrj.io.stream.metrics.MinMetric;
import org.apache.solr.client.solrj.io.stream.metrics.SumMetric;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.AbstractDistribZkTestBase;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *  All base tests will be done with CloudSolrStream. Under the covers CloudSolrStream uses SolrStream so
 *  SolrStream will get fully exercised through these tests.
 *
 **/

@Slow
@LuceneTestCase.SuppressCodecs({"Lucene3x", "Lucene40","Lucene41","Lucene42","Lucene45"})
public class StreamExpressionTest extends SolrCloudTestCase {

  private static final String COLLECTIONORALIAS = "collection1";
  private static final int TIMEOUT = DEFAULT_TIMEOUT;
  private static final String id = "id";

  private static boolean useAlias;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(4)
        .addConfig("conf", getFile("solrj").toPath().resolve("solr").resolve("configsets").resolve("streaming").resolve("conf"))
        .addConfig("ml", getFile("solrj").toPath().resolve("solr").resolve("configsets").resolve("ml").resolve("conf"))
        .configure();

    String collection;
    useAlias = random().nextBoolean();
    if (useAlias) {
      collection = COLLECTIONORALIAS + "_collection";
    } else {
      collection = COLLECTIONORALIAS;
    }
    CollectionAdminRequest.createCollection(collection, "conf", 2, 1).process(cluster.getSolrClient());
    AbstractDistribZkTestBase.waitForRecoveriesToFinish(collection, cluster.getSolrClient().getZkStateReader(),
        false, true, TIMEOUT);
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
  public void testCloudSolrStream() throws Exception {

    new UpdateRequest()
        .add(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "0")
        .add(id, "2", "a_s", "hello2", "a_i", "2", "a_f", "0")
        .add(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3")
        .add(id, "4", "a_s", "hello4", "a_i", "4", "a_f", "4")
        .add(id, "1", "a_s", "hello1", "a_i", "1", "a_f", "1")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    StreamFactory factory = new StreamFactory().withCollectionZkHost(COLLECTIONORALIAS, cluster.getZkServer().getZkAddress());
    StreamExpression expression;
    CloudSolrStream stream;
    List<Tuple> tuples;
    
    // Basic test
    expression = StreamExpressionParser.parse("search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\")");
    stream = new CloudSolrStream(expression, factory);
    tuples = getTuples(stream);

    assert(tuples.size() == 5);
    assertOrder(tuples, 0, 2, 1, 3, 4);
    assertLong(tuples.get(0), "a_i", 0);

    // Basic w/aliases
    expression = StreamExpressionParser.parse("search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\", aliases=\"a_i=alias.a_i, a_s=name\")");
    stream = new CloudSolrStream(expression, factory);
    tuples = getTuples(stream);

    assert(tuples.size() == 5);
    assertOrder(tuples, 0, 2, 1, 3, 4);
    assertLong(tuples.get(0), "alias.a_i", 0);
    assertString(tuples.get(0), "name", "hello0");

    // Basic filtered test
    expression = StreamExpressionParser.parse("search(" + COLLECTIONORALIAS + ", q=\"id:(0 3 4)\", fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\")");
    stream = new CloudSolrStream(expression, factory);
    tuples = getTuples(stream);

    assert(tuples.size() == 3);
    assertOrder(tuples, 0, 3, 4);
    assertLong(tuples.get(1), "a_i", 3);

    try {
      expression = StreamExpressionParser.parse("search(" + COLLECTIONORALIAS + ", fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\")");
      stream = new CloudSolrStream(expression, factory);
      tuples = getTuples(stream);
      throw new Exception("Should be an exception here");
    } catch(Exception e) {
      assertTrue(e.getMessage().contains("q param expected for search function"));
    }

    try {
      expression = StreamExpressionParser.parse("search(" + COLLECTIONORALIAS + ", q=\"blah\", sort=\"a_f asc, a_i asc\")");
      stream = new CloudSolrStream(expression, factory);
      tuples = getTuples(stream);
      throw new Exception("Should be an exception here");
    } catch(Exception e) {
      assertTrue(e.getMessage().contains("fl param expected for search function"));
    }

  }

  @Test
  public void testCloudSolrStreamWithZkHost() throws Exception {

    new UpdateRequest()
        .add(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "0")
        .add(id, "2", "a_s", "hello2", "a_i", "2", "a_f", "0")
        .add(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3")
        .add(id, "4", "a_s", "hello4", "a_i", "4", "a_f", "4")
        .add(id, "1", "a_s", "hello1", "a_i", "1", "a_f", "1")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    StreamFactory factory = new StreamFactory();
    StreamExpression expression;
    CloudSolrStream stream;
    List<Tuple> tuples;
    
    // Basic test
    expression = StreamExpressionParser.parse("search(" + COLLECTIONORALIAS + ", zkHost=" + cluster.getZkServer().getZkAddress() + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\")");
    stream = new CloudSolrStream(expression, factory);
    tuples = getTuples(stream);

    assert(tuples.size() == 5);
    assertOrder(tuples, 0, 2, 1, 3, 4);
    assertLong(tuples.get(0), "a_i", 0);

    // Basic w/aliases
    expression = StreamExpressionParser.parse("search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\", aliases=\"a_i=alias.a_i, a_s=name\", zkHost=" + cluster.getZkServer().getZkAddress() + ")");
    stream = new CloudSolrStream(expression, factory);
    tuples = getTuples(stream);

    assert(tuples.size() == 5);
    assertOrder(tuples, 0, 2, 1, 3, 4);
    assertLong(tuples.get(0), "alias.a_i", 0);
    assertString(tuples.get(0), "name", "hello0");

    // Basic filtered test
    expression = StreamExpressionParser.parse("search(" + COLLECTIONORALIAS + ", q=\"id:(0 3 4)\", fl=\"id,a_s,a_i,a_f\", zkHost="
        + cluster.getZkServer().getZkAddress() + ", sort=\"a_f asc, a_i asc\")");
    stream = new CloudSolrStream(expression, factory);
    tuples = getTuples(stream);

    assert(tuples.size() == 3);
    assertOrder(tuples, 0, 3, 4);
    assertLong(tuples.get(1), "a_i", 3);


    // Test a couple of multile field lists.
    expression = StreamExpressionParser.parse("search(collection1, fq=\"a_s:hello0\", fq=\"a_s:hello1\", q=\"id:(*)\", " +
        "zkHost=" + cluster.getZkServer().getZkAddress()+ ", fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\")");
    stream = new CloudSolrStream(expression, factory);
    tuples = getTuples(stream);

    assertEquals("fq clauses should have prevented any docs from coming back", tuples.size(), 0);


    expression = StreamExpressionParser.parse("search(collection1, fq=\"a_s:(hello0 OR hello1)\", q=\"id:(*)\", " +
        "zkHost=" + cluster.getZkServer().getZkAddress() + ", fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\")");
    stream = new CloudSolrStream(expression, factory);
    tuples = getTuples(stream);

    assertEquals("Combining an f1 clause should show us 2 docs", tuples.size(), 2);
    
        

  }

  @Test
  public void testParameterSubstitution() throws Exception {

    new UpdateRequest()
        .add(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "0")
        .add(id, "2", "a_s", "hello2", "a_i", "2", "a_f", "0")
        .add(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3")
        .add(id, "4", "a_s", "hello4", "a_i", "4", "a_f", "4")
        .add(id, "1", "a_s", "hello1", "a_i", "1", "a_f", "1")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString() + "/" + COLLECTIONORALIAS;
    List<Tuple> tuples;
    TupleStream stream;

    // Basic test
    ModifiableSolrParams sParams = new ModifiableSolrParams();
    sParams.set("expr", "merge("
        + "${q1},"
        + "${q2},"
        + "on=${mySort})");
    sParams.set(CommonParams.QT, "/stream");
    sParams.set("q1", "search(" + COLLECTIONORALIAS + ", q=\"id:(0 3 4)\", fl=\"id,a_s,a_i,a_f\", sort=${mySort})");
    sParams.set("q2", "search(" + COLLECTIONORALIAS + ", q=\"id:(1)\", fl=\"id,a_s,a_i,a_f\", sort=${mySort})");
    sParams.set("mySort", "a_f asc");
    stream = new SolrStream(url, sParams);
    tuples = getTuples(stream);

    assertEquals(4, tuples.size());
    assertOrder(tuples, 0,1,3,4);

    // Basic test desc
    sParams.set("mySort", "a_f desc");
    stream = new SolrStream(url, sParams);
    tuples = getTuples(stream);

    assertEquals(4, tuples.size());
    assertOrder(tuples, 4, 3, 1, 0);

    // Basic w/ multi comp
    sParams.set("q2", "search(" + COLLECTIONORALIAS + ", q=\"id:(1 2)\", fl=\"id,a_s,a_i,a_f\", sort=${mySort})");
    sParams.set("mySort", "\"a_f asc, a_s asc\"");
    stream = new SolrStream(url, sParams);
    tuples = getTuples(stream);

    assertEquals(5, tuples.size());
    assertOrder(tuples, 0, 2, 1, 3, 4);
  }

  @Test
  public void testUniqueStream() throws Exception {

    new UpdateRequest()
        .add(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "0")
        .add(id, "2", "a_s", "hello2", "a_i", "2", "a_f", "0")
        .add(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3")
        .add(id, "4", "a_s", "hello4", "a_i", "4", "a_f", "4")
        .add(id, "1", "a_s", "hello1", "a_i", "1", "a_f", "1")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    StreamExpression expression;
    TupleStream stream;
    List<Tuple> tuples;
    
    StreamFactory factory = new StreamFactory()
      .withCollectionZkHost(COLLECTIONORALIAS, cluster.getZkServer().getZkAddress())
      .withFunctionName("search", CloudSolrStream.class)
      .withFunctionName("unique", UniqueStream.class);
    
    // Basic test
    expression = StreamExpressionParser.parse("unique(search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\"), over=\"a_f\")");
    stream = new UniqueStream(expression, factory);
    tuples = getTuples(stream);
    
    assert(tuples.size() == 4);
    assertOrder(tuples, 0, 1, 3, 4);

    // Basic test desc
    expression = StreamExpressionParser.parse("unique(search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f desc, a_i desc\"), over=\"a_f\")");
    stream = new UniqueStream(expression, factory);
    tuples = getTuples(stream);
    
    assert(tuples.size() == 4);
    assertOrder(tuples, 4,3,1,2);
    
    // Basic w/multi comp
    expression = StreamExpressionParser.parse("unique(search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\"), over=\"a_f, a_i\")");
    stream = new UniqueStream(expression, factory);
    tuples = getTuples(stream);
    
    assert(tuples.size() == 5);
    assertOrder(tuples, 0,2,1,3,4);
    
    // full factory w/multi comp
    stream = factory.constructStream("unique(search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\"), over=\"a_f, a_i\")");
    tuples = getTuples(stream);
    
    assert(tuples.size() == 5);
    assertOrder(tuples, 0, 2, 1, 3, 4);

  }

  @Test
  public void testSortStream() throws Exception {

    new UpdateRequest()
        .add(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "0")
        .add(id, "2", "a_s", "hello2", "a_i", "2", "a_f", "0")
        .add(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3")
        .add(id, "4", "a_s", "hello4", "a_i", "4", "a_f", "4")
        .add(id, "1", "a_s", "hello1", "a_i", "1", "a_f", "1")
        .add(id, "5", "a_s", "hello1", "a_i", "1", "a_f", "2")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    StreamExpression expression;
    TupleStream stream;
    List<Tuple> tuples;
    
    StreamFactory factory = new StreamFactory()
      .withCollectionZkHost(COLLECTIONORALIAS, cluster.getZkServer().getZkAddress())
      .withFunctionName("search", CloudSolrStream.class)
      .withFunctionName("sort", SortStream.class);
    
    // Basic test
    stream = factory.constructStream("sort(search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc\"), by=\"a_i asc\")");
    tuples = getTuples(stream);
    assert(tuples.size() == 6);
    assertOrder(tuples, 0,1,5,2,3,4);

    // Basic test desc
    stream = factory.constructStream("sort(search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc\"), by=\"a_i desc\")");
    tuples = getTuples(stream);
    assert(tuples.size() == 6);
    assertOrder(tuples, 4,3,2,1,5,0);
    
    // Basic w/multi comp
    stream = factory.constructStream("sort(search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc\"), by=\"a_i asc, a_f desc\")");
    tuples = getTuples(stream);
    assert(tuples.size() == 6);
    assertOrder(tuples, 0,5,1,2,3,4);

  }

  @Test
  public void testNulls() throws Exception {

    new UpdateRequest()
        .add(id, "0",                  "a_i", "1", "a_f", "0", "s_multi", "aaa", "s_multi", "bbb", "i_multi", "100", "i_multi", "200")
        .add(id, "2", "a_s", "hello2", "a_i", "3", "a_f", "0")
        .add(id, "3", "a_s", "hello3", "a_i", "4", "a_f", "3")
        .add(id, "4", "a_s", "hello4",             "a_f", "4")
        .add(id, "1", "a_s", "hello1", "a_i", "2", "a_f", "1")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    StreamExpression expression;
    TupleStream stream;
    List<Tuple> tuples;
    Tuple tuple;
    StreamFactory factory = new StreamFactory()
        .withCollectionZkHost(COLLECTIONORALIAS, cluster.getZkServer().getZkAddress())
        .withFunctionName("search", CloudSolrStream.class);
    // Basic test
    expression = StreamExpressionParser.parse("search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f, s_multi, i_multi\", qt=\"/export\", sort=\"a_i asc\")");
    stream = new CloudSolrStream(expression, factory);
    tuples = getTuples(stream);

    assert(tuples.size() == 5);
    assertOrder(tuples, 4, 0, 1, 2, 3);

    tuple = tuples.get(0);
    assertTrue("hello4".equals(tuple.getString("a_s")));
    assertNull(tuple.get("s_multi"));
    assertNull(tuple.get("i_multi"));
    assertNull(tuple.getLong("a_i"));


    tuple = tuples.get(1);
    assertNull(tuple.get("a_s"));
    List<String> strings = tuple.getStrings("s_multi");
    assertNotNull(strings);
    assertEquals("aaa", strings.get(0));
    assertEquals("bbb", strings.get(1));
    List<Long> longs = tuple.getLongs("i_multi");
    assertNotNull(longs);

    //test sort (asc) with null string field. Null should sort to the top.
    expression = StreamExpressionParser.parse("search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f, s_multi, i_multi\", qt=\"/export\", sort=\"a_s asc\")");
    stream = new CloudSolrStream(expression, factory);
    tuples = getTuples(stream);

    assert(tuples.size() == 5);
    assertOrder(tuples, 0, 1, 2, 3, 4);

    //test sort(desc) with null string field.  Null should sort to the bottom.
    expression = StreamExpressionParser.parse("search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f, s_multi, i_multi\", qt=\"/export\", sort=\"a_s desc\")");
    stream = new CloudSolrStream(expression, factory);
    tuples = getTuples(stream);

    assert(tuples.size() == 5);
    assertOrder(tuples, 4, 3, 2, 1, 0);

  }

  @Test
  public void testMergeStream() throws Exception {

    new UpdateRequest()
        .add(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "0")
        .add(id, "2", "a_s", "hello2", "a_i", "2", "a_f", "0")
        .add(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3")
        .add(id, "4", "a_s", "hello4", "a_i", "4", "a_f", "4")
        .add(id, "1", "a_s", "hello1", "a_i", "1", "a_f", "1")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    StreamExpression expression;
    TupleStream stream;
    List<Tuple> tuples;
    
    StreamFactory factory = new StreamFactory()
      .withCollectionZkHost(COLLECTIONORALIAS, cluster.getZkServer().getZkAddress())
      .withFunctionName("search", CloudSolrStream.class)
      .withFunctionName("unique", UniqueStream.class)
      .withFunctionName("merge", MergeStream.class);
    
    // Basic test
    expression = StreamExpressionParser.parse("merge("
        + "search(" + COLLECTIONORALIAS + ", q=\"id:(0 3 4)\", fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc\"),"
        + "search(" + COLLECTIONORALIAS + ", q=\"id:(1)\", fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc\"),"
        + "on=\"a_f asc\")");
    stream = new MergeStream(expression, factory);
    tuples = getTuples(stream);
    
    assert(tuples.size() == 4);
    assertOrder(tuples, 0, 1, 3, 4);

    // Basic test desc
    expression = StreamExpressionParser.parse("merge("
        + "search(" + COLLECTIONORALIAS + ", q=\"id:(0 3 4)\", fl=\"id,a_s,a_i,a_f\", sort=\"a_f desc\"),"
        + "search(" + COLLECTIONORALIAS + ", q=\"id:(1)\", fl=\"id,a_s,a_i,a_f\", sort=\"a_f desc\"),"
        + "on=\"a_f desc\")");
    stream = new MergeStream(expression, factory);
    tuples = getTuples(stream);
    
    assert(tuples.size() == 4);
    assertOrder(tuples, 4, 3, 1, 0);
    
    // Basic w/multi comp
    expression = StreamExpressionParser.parse("merge("
        + "search(" + COLLECTIONORALIAS + ", q=\"id:(0 3 4)\", fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_s asc\"),"
        + "search(" + COLLECTIONORALIAS + ", q=\"id:(1 2)\", fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_s asc\"),"
        + "on=\"a_f asc, a_s asc\")");
    stream = new MergeStream(expression, factory);
    tuples = getTuples(stream);
    
    assert(tuples.size() == 5);
    assertOrder(tuples, 0, 2, 1, 3, 4);
    
    // full factory w/multi comp
    stream = factory.constructStream("merge("
        + "search(" + COLLECTIONORALIAS + ", q=\"id:(0 3 4)\", fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_s asc\"),"
        + "search(" + COLLECTIONORALIAS + ", q=\"id:(1 2)\", fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_s asc\"),"
        + "on=\"a_f asc, a_s asc\")");
    tuples = getTuples(stream);
    
    assert(tuples.size() == 5);
    assertOrder(tuples, 0, 2, 1, 3, 4);
    
    // full factory w/multi streams
    stream = factory.constructStream("merge("
        + "search(" + COLLECTIONORALIAS + ", q=\"id:(0 4)\", fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_s asc\"),"
        + "search(" + COLLECTIONORALIAS + ", q=\"id:(1)\", fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_s asc\"),"
        + "search(" + COLLECTIONORALIAS + ", q=\"id:(2)\", fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_s asc\"),"
        + "on=\"a_f asc\")");
    tuples = getTuples(stream);
    
    assert(tuples.size() == 4);
    assertOrder(tuples, 0, 2, 1, 4);

  }

  @Test
  public void testRankStream() throws Exception {

    new UpdateRequest()
        .add(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "0")
        .add(id, "2", "a_s", "hello2", "a_i", "2", "a_f", "0")
        .add(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3")
        .add(id, "4", "a_s", "hello4", "a_i", "4", "a_f", "4")
        .add(id, "1", "a_s", "hello1", "a_i", "1", "a_f", "1")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    StreamExpression expression;
    TupleStream stream;
    List<Tuple> tuples;
    
    StreamFactory factory = new StreamFactory()
      .withCollectionZkHost(COLLECTIONORALIAS, cluster.getZkServer().getZkAddress())
      .withFunctionName("search", CloudSolrStream.class)
      .withFunctionName("unique", UniqueStream.class)
      .withFunctionName("top", RankStream.class);
    
    // Basic test
    expression = StreamExpressionParser.parse("top("
        + "n=3,"
        + "search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\"),"
        + "sort=\"a_f asc, a_i asc\")");
    stream = new RankStream(expression, factory);
    tuples = getTuples(stream);
    
    assert(tuples.size() == 3);
    assertOrder(tuples, 0, 2, 1);

    // Basic test desc
    expression = StreamExpressionParser.parse("top("
        + "n=2,"
        + "unique("
        + "search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f desc\"),"
        + "over=\"a_f\"),"
        + "sort=\"a_f desc\")");
    stream = new RankStream(expression, factory);
    tuples = getTuples(stream);
    
    assert(tuples.size() == 2);
    assertOrder(tuples, 4, 3);
    
    // full factory
    stream = factory.constructStream("top("
        + "n=4,"
        + "unique("
        + "search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\"),"
        + "over=\"a_f\"),"
        + "sort=\"a_f asc\")");
    tuples = getTuples(stream);
    
    assert(tuples.size() == 4);
    assertOrder(tuples, 0,1,3,4);

    // full factory, switch order
    stream = factory.constructStream("top("
            + "n=4,"
            + "unique("
            +   "search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f desc, a_i desc\"),"
            +   "over=\"a_f\"),"
            + "sort=\"a_f asc\")");
    tuples = getTuples(stream);
    
    assert(tuples.size() == 4);
    assertOrder(tuples, 2,1,3,4);

  }

  @Test
  public void testRandomStream() throws Exception {

    UpdateRequest update = new UpdateRequest();
    for(int idx = 0; idx < 1000; ++idx){
      String idxString = new Integer(idx).toString();
      update.add(id,idxString, "a_s", "hello" + idxString, "a_i", idxString, "a_f", idxString);
    }
    update.commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    StreamExpression expression;
    TupleStream stream;

    StreamFactory factory = new StreamFactory()
        .withCollectionZkHost(COLLECTIONORALIAS, cluster.getZkServer().getZkAddress())
        .withFunctionName("random", RandomStream.class);


    StreamContext context = new StreamContext();
    SolrClientCache cache = new SolrClientCache();
    try {
      context.setSolrClientCache(cache);

      expression = StreamExpressionParser.parse("random(" + COLLECTIONORALIAS + ", q=\"*:*\", rows=\"1000\", fl=\"id, a_i\")");
      stream = factory.constructStream(expression);
      stream.setStreamContext(context);
      List<Tuple> tuples1 = getTuples(stream);
      assert (tuples1.size() == 1000);

      expression = StreamExpressionParser.parse("random(" + COLLECTIONORALIAS + ", q=\"*:*\", rows=\"1000\", fl=\"id, a_i\")");
      stream = factory.constructStream(expression);
      stream.setStreamContext(context);
      List<Tuple> tuples2 = getTuples(stream);
      assert (tuples2.size() == 1000);

      boolean different = false;
      for (int i = 0; i < tuples1.size(); i++) {
        Tuple tuple1 = tuples1.get(i);
        Tuple tuple2 = tuples2.get(i);
        if (!tuple1.get("id").equals(tuple2.get(id))) {
          different = true;
          break;
        }
      }

      assertTrue(different);

      Collections.sort(tuples1, new FieldComparator("id", ComparatorOrder.ASCENDING));
      Collections.sort(tuples2, new FieldComparator("id", ComparatorOrder.ASCENDING));

      for (int i = 0; i < tuples1.size(); i++) {
        Tuple tuple1 = tuples1.get(i);
        Tuple tuple2 = tuples2.get(i);
        if (!tuple1.get("id").equals(tuple2.get(id))) {
          assert(tuple1.getLong("id").equals(tuple2.get("a_i")));
        }
      }

      expression = StreamExpressionParser.parse("random(" + COLLECTIONORALIAS + ", q=\"*:*\", rows=\"1\", fl=\"id, a_i\")");
      stream = factory.constructStream(expression);
      stream.setStreamContext(context);
      List<Tuple> tuples3 = getTuples(stream);
      assert (tuples3.size() == 1);

    } finally {
      cache.close();
    }
  }

  @Test
  public void testReducerStream() throws Exception {

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
    
    StreamExpression expression;
    TupleStream stream;
    List<Tuple> tuples;
    Tuple t0, t1, t2;
    List<Map> maps0, maps1, maps2;
    
    StreamFactory factory = new StreamFactory()
        .withCollectionZkHost(COLLECTIONORALIAS, cluster.getZkServer().getZkAddress())
        .withFunctionName("search", CloudSolrStream.class)
        .withFunctionName("reduce", ReducerStream.class)
        .withFunctionName("group", GroupOperation.class);

    // basic
    expression = StreamExpressionParser.parse("reduce("
        + "search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_s asc, a_f asc\"),"
        + "by=\"a_s\","
        + "group(sort=\"a_f desc\", n=\"4\"))");

    stream = factory.constructStream(expression);
    tuples = getTuples(stream);

    assert(tuples.size() == 3);

    t0 = tuples.get(0);
    maps0 = t0.getMaps("group");
    assertMaps(maps0, 9, 1, 2, 0);

    t1 = tuples.get(1);
    maps1 = t1.getMaps("group");
    assertMaps(maps1, 8, 7, 5, 3);


    t2 = tuples.get(2);
    maps2 = t2.getMaps("group");
    assertMaps(maps2, 6, 4);
    
    // basic w/spaces
    expression = StreamExpressionParser.parse("reduce("
        + "search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_s asc, a_f       asc\"),"
        + "by=\"a_s\"," +
        "group(sort=\"a_i asc\", n=\"2\"))");
    stream = factory.constructStream(expression);
    tuples = getTuples(stream);

    assert(tuples.size() == 3);

    t0 = tuples.get(0);
    maps0 = t0.getMaps("group");
    assert(maps0.size() == 2);

    assertMaps(maps0, 0, 1);

    t1 = tuples.get(1);
    maps1 = t1.getMaps("group");
    assertMaps(maps1, 3, 5);

    t2 = tuples.get(2);
    maps2 = t2.getMaps("group");
    assertMaps(maps2, 4, 6);

  }

  @Test
  public void testFetchStream() throws Exception {

    SolrClientCache solrClientCache = new SolrClientCache();

    new UpdateRequest()
        .add(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "1", "subject", "blah blah blah 0")
        .add(id, "2", "a_s", "hello0", "a_i", "2", "a_f", "2", "subject", "blah blah blah 2")
        .add(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3", "subject", "blah blah blah 3")
        .add(id, "4", "a_s", "hello4", "a_i", "4", "a_f", "4", "subject", "blah blah blah 4")
        .add(id, "1", "a_s", "hello0", "a_i", "1", "a_f", "5", "subject", "blah blah blah 1")
        .add(id, "5", "a_s", "hello3", "a_i", "5", "a_f", "6", "subject", "blah blah blah 5")
        .add(id, "6", "a_s", "hello4", "a_i", "6", "a_f", "7", "subject", "blah blah blah 6")
        .add(id, "7", "a_s", "hello3", "a_i", "7", "a_f", "8", "subject", "blah blah blah 7")
        .add(id, "8", "a_s", "hello3", "a_i", "8", "a_f", "9", "subject", "blah blah blah 8")
        .add(id, "9", "a_s", "hello0", "a_i", "9", "a_f", "10", "subject", "blah blah blah 9")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    TupleStream stream;
    List<Tuple> tuples;

    StreamFactory factory = new StreamFactory()
        .withCollectionZkHost(COLLECTIONORALIAS, cluster.getZkServer().getZkAddress())
        .withFunctionName("search", CloudSolrStream.class)
        .withFunctionName("fetch", FetchStream.class);

    stream = factory.constructStream("fetch("+ COLLECTIONORALIAS +",  search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc\"), on=\"id=a_i\", batchSize=\"2\", fl=\"subject\")");
    StreamContext context = new StreamContext();
    context.setSolrClientCache(solrClientCache);
    stream.setStreamContext(context);
    tuples = getTuples(stream);

    assert(tuples.size() == 10);
    Tuple t = tuples.get(0);
    assertTrue("blah blah blah 0".equals(t.getString("subject")));
    t = tuples.get(1);
    assertTrue("blah blah blah 2".equals(t.getString("subject")));
    t = tuples.get(2);
    assertTrue("blah blah blah 3".equals(t.getString("subject")));
    t = tuples.get(3);
    assertTrue("blah blah blah 4".equals(t.getString("subject")));
    t = tuples.get(4);
    assertTrue("blah blah blah 1".equals(t.getString("subject")));
    t = tuples.get(5);
    assertTrue("blah blah blah 5".equals(t.getString("subject")));
    t = tuples.get(6);
    assertTrue("blah blah blah 6".equals(t.getString("subject")));
    t = tuples.get(7);
    assertTrue("blah blah blah 7".equals(t.getString("subject")));
    t = tuples.get(8);
    assertTrue("blah blah blah 8".equals(t.getString("subject")));
    t = tuples.get(9);
    assertTrue("blah blah blah 9".equals(t.getString("subject")));

    //Change the batch size
    stream = factory.constructStream("fetch("+ COLLECTIONORALIAS +",  search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc\"), on=\"id=a_i\", batchSize=\"3\", fl=\"subject\")");
    context = new StreamContext();
    context.setSolrClientCache(solrClientCache);
    stream.setStreamContext(context);
    tuples = getTuples(stream);

    assert(tuples.size() == 10);
    t = tuples.get(0);
    assertTrue("blah blah blah 0".equals(t.getString("subject")));
    t = tuples.get(1);
    assertTrue("blah blah blah 2".equals(t.getString("subject")));
    t = tuples.get(2);
    assertTrue("blah blah blah 3".equals(t.getString("subject")));
    t = tuples.get(3);
    assertTrue("blah blah blah 4".equals(t.getString("subject")));
    t = tuples.get(4);
    assertTrue("blah blah blah 1".equals(t.getString("subject")));
    t = tuples.get(5);
    assertTrue("blah blah blah 5".equals(t.getString("subject")));
    t = tuples.get(6);
    assertTrue("blah blah blah 6".equals(t.getString("subject")));
    t = tuples.get(7);
    assertTrue("blah blah blah 7".equals(t.getString("subject")));
    t = tuples.get(8);
    assertTrue("blah blah blah 8".equals(t.getString("subject")));
    t = tuples.get(9);
    assertTrue("blah blah blah 9".equals(t.getString("subject")));
    solrClientCache.close();
  }

  @Test
  public void testParallelFetchStream() throws Exception {

    new UpdateRequest()
        .add(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "1", "subject", "blah blah blah 0")
        .add(id, "2", "a_s", "hello0", "a_i", "2", "a_f", "2", "subject", "blah blah blah 2")
        .add(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3", "subject", "blah blah blah 3")
        .add(id, "4", "a_s", "hello4", "a_i", "4", "a_f", "4", "subject", "blah blah blah 4")
        .add(id, "1", "a_s", "hello0", "a_i", "1", "a_f", "5", "subject", "blah blah blah 1")
        .add(id, "5", "a_s", "hello3", "a_i", "5", "a_f", "6", "subject", "blah blah blah 5")
        .add(id, "6", "a_s", "hello4", "a_i", "6", "a_f", "7", "subject", "blah blah blah 6")
        .add(id, "7", "a_s", "hello3", "a_i", "7", "a_f", "8", "subject", "blah blah blah 7")
        .add(id, "8", "a_s", "hello3", "a_i", "8", "a_f", "9", "subject", "blah blah blah 8")
        .add(id, "9", "a_s", "hello0", "a_i", "9", "a_f", "10", "subject", "blah blah blah 9")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    TupleStream stream;
    List<Tuple> tuples;

    StreamFactory factory = new StreamFactory()
        .withCollectionZkHost(COLLECTIONORALIAS, cluster.getZkServer().getZkAddress())
        .withFunctionName("search", CloudSolrStream.class)
        .withFunctionName("parallel", ParallelStream.class)
        .withFunctionName("fetch", FetchStream.class);

    stream = factory.constructStream("parallel(" + COLLECTIONORALIAS + ", workers=2, sort=\"a_f asc\", fetch(" + COLLECTIONORALIAS + ",  search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc\", partitionKeys=\"id\"), on=\"id=a_i\", batchSize=\"2\", fl=\"subject\"))");
    tuples = getTuples(stream);

    assert(tuples.size() == 10);
    Tuple t = tuples.get(0);
    assertTrue("blah blah blah 0".equals(t.getString("subject")));
    t = tuples.get(1);
    assertTrue("blah blah blah 2".equals(t.getString("subject")));
    t = tuples.get(2);
    assertTrue("blah blah blah 3".equals(t.getString("subject")));
    t = tuples.get(3);
    assertTrue("blah blah blah 4".equals(t.getString("subject")));
    t = tuples.get(4);
    assertTrue("blah blah blah 1".equals(t.getString("subject")));
    t = tuples.get(5);
    assertTrue("blah blah blah 5".equals(t.getString("subject")));
    t = tuples.get(6);
    assertTrue("blah blah blah 6".equals(t.getString("subject")));
    t = tuples.get(7);
    assertTrue("blah blah blah 7".equals(t.getString("subject")));
    t = tuples.get(8);
    assertTrue("blah blah blah 8".equals(t.getString("subject")));
    t = tuples.get(9);
    assertTrue("blah blah blah 9".equals(t.getString("subject")));


    stream = factory.constructStream("parallel(" + COLLECTIONORALIAS + ", workers=2, sort=\"a_f asc\", fetch(" + COLLECTIONORALIAS + ",  search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc\", partitionKeys=\"id\"), on=\"id=a_i\", batchSize=\"3\", fl=\"subject\"))");
    tuples = getTuples(stream);

    assert(tuples.size() == 10);
    t = tuples.get(0);
    assertTrue("blah blah blah 0".equals(t.getString("subject")));
    t = tuples.get(1);
    assertTrue("blah blah blah 2".equals(t.getString("subject")));
    t = tuples.get(2);
    assertTrue("blah blah blah 3".equals(t.getString("subject")));
    t = tuples.get(3);
    assertTrue("blah blah blah 4".equals(t.getString("subject")));
    t = tuples.get(4);
    assertTrue("blah blah blah 1".equals(t.getString("subject")));
    t = tuples.get(5);
    assertTrue("blah blah blah 5".equals(t.getString("subject")));
    t = tuples.get(6);
    assertTrue("blah blah blah 6".equals(t.getString("subject")));
    t = tuples.get(7);
    assertTrue("blah blah blah 7".equals(t.getString("subject")));
    t = tuples.get(8);
    assertTrue("blah blah blah 8".equals(t.getString("subject")));
    t = tuples.get(9);
    assertTrue("blah blah blah 9".equals(t.getString("subject")));

  }





  @Test
  public void testDaemonStream() throws Exception {

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

    StreamFactory factory = new StreamFactory()
        .withCollectionZkHost(COLLECTIONORALIAS, cluster.getZkServer().getZkAddress())
        .withFunctionName("search", CloudSolrStream.class)
        .withFunctionName("rollup", RollupStream.class)
        .withFunctionName("sum", SumMetric.class)
        .withFunctionName("min", MinMetric.class)
        .withFunctionName("max", MaxMetric.class)
        .withFunctionName("avg", MeanMetric.class)
        .withFunctionName("count", CountMetric.class)
        .withFunctionName("daemon", DaemonStream.class);

    StreamExpression expression;
    DaemonStream daemonStream;

    expression = StreamExpressionParser.parse("daemon(rollup("
        + "search(" + COLLECTIONORALIAS + ", q=\"*:*\", fl=\"a_i,a_s\", sort=\"a_s asc\"),"
        + "over=\"a_s\","
        + "sum(a_i)"
        + "), id=\"test\", runInterval=\"1000\", queueSize=\"9\")");
    daemonStream = (DaemonStream)factory.constructStream(expression);


    //Test Long and Double Sums

    daemonStream.open(); // This will start the daemon thread

    for(int i=0; i<4; i++) {
      Tuple tuple = daemonStream.read(); // Reads from the queue
      String bucket = tuple.getString("a_s");
      Double sumi = tuple.getDouble("sum(a_i)");

      //System.out.println("#################################### Bucket 1:"+bucket);
      assertTrue(bucket.equals("hello0"));
      assertTrue(sumi.doubleValue() == 17.0D);

      tuple = daemonStream.read();
      bucket = tuple.getString("a_s");
      sumi = tuple.getDouble("sum(a_i)");

      //System.out.println("#################################### Bucket 2:"+bucket);
      assertTrue(bucket.equals("hello3"));
      assertTrue(sumi.doubleValue() == 38.0D);

      tuple = daemonStream.read();
      bucket = tuple.getString("a_s");
      sumi = tuple.getDouble("sum(a_i)");
      //System.out.println("#################################### Bucket 3:"+bucket);
      assertTrue(bucket.equals("hello4"));
      assertTrue(sumi.longValue() == 15);
    }

    //Now lets wait until the internal queue fills up

    while(daemonStream.remainingCapacity() > 0) {
      try {
        Thread.sleep(1000);
      } catch (Exception e) {

      }
    }

    //OK capacity is full, let's index a new doc

    new UpdateRequest()
        .add(id, "10", "a_s", "hello0", "a_i", "1", "a_f", "10")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    //Now lets clear the existing docs in the queue 9, plus 3 more to get passed the run that was blocked. The next run should
    //have the tuples with the updated count.
    for(int i=0; i<12;i++) {
      daemonStream.read();
    }

    //And rerun the loop. It should have a new count for hello0
    for(int i=0; i<4; i++) {
      Tuple tuple = daemonStream.read(); // Reads from the queue
      String bucket = tuple.getString("a_s");
      Double sumi = tuple.getDouble("sum(a_i)");

      //System.out.println("#################################### Bucket 1:"+bucket);
      assertTrue(bucket.equals("hello0"));
      assertTrue(sumi.doubleValue() == 18.0D);

      tuple = daemonStream.read();
      bucket = tuple.getString("a_s");
      sumi = tuple.getDouble("sum(a_i)");

      //System.out.println("#################################### Bucket 2:"+bucket);
      assertTrue(bucket.equals("hello3"));
      assertTrue(sumi.doubleValue() == 38.0D);

      tuple = daemonStream.read();
      bucket = tuple.getString("a_s");
      sumi = tuple.getDouble("sum(a_i)");
      //System.out.println("#################################### Bucket 3:"+bucket);
      assertTrue(bucket.equals("hello4"));
      assertTrue(sumi.longValue() == 15);
    }

    daemonStream.close(); //This should stop the daemon thread

  }


  @Test
  public void testTerminatingDaemonStream() throws Exception {
    Assume.assumeTrue(!useAlias);

    new UpdateRequest()
        .add(id, "0", "a_s", "hello", "a_i", "0", "a_f", "1")
        .add(id, "2", "a_s", "hello", "a_i", "2", "a_f", "2")
        .add(id, "3", "a_s", "hello", "a_i", "3", "a_f", "3")
        .add(id, "4", "a_s", "hello", "a_i", "4", "a_f", "4")
        .add(id, "1", "a_s", "hello", "a_i", "1", "a_f", "5")
        .add(id, "5", "a_s", "hello", "a_i", "10", "a_f", "6")
        .add(id, "6", "a_s", "hello", "a_i", "11", "a_f", "7")
        .add(id, "7", "a_s", "hello", "a_i", "12", "a_f", "8")
        .add(id, "8", "a_s", "hello", "a_i", "13", "a_f", "9")
        .add(id, "9", "a_s", "hello", "a_i", "14", "a_f", "10")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    StreamFactory factory = new StreamFactory()
        .withCollectionZkHost(COLLECTIONORALIAS, cluster.getZkServer().getZkAddress())
        .withFunctionName("topic", TopicStream.class)
        .withFunctionName("daemon", DaemonStream.class);

    StreamExpression expression;
    DaemonStream daemonStream;

    SolrClientCache cache = new SolrClientCache();
    StreamContext context = new StreamContext();
    context.setSolrClientCache(cache);
    expression = StreamExpressionParser.parse("daemon(topic("+ COLLECTIONORALIAS +","+ COLLECTIONORALIAS +", q=\"a_s:hello\", initialCheckpoint=0, id=\"topic1\", rows=2, fl=\"id\""
        + "), id=test, runInterval=1000, terminate=true, queueSize=50)");
    daemonStream = (DaemonStream)factory.constructStream(expression);
    daemonStream.setStreamContext(context);

    List<Tuple> tuples = getTuples(daemonStream);
    assertTrue(tuples.size() == 10);
    cache.close();
  }


  @Test
  public void testRollupStream() throws Exception {

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

    StreamFactory factory = new StreamFactory()
      .withCollectionZkHost(COLLECTIONORALIAS, cluster.getZkServer().getZkAddress())
      .withFunctionName("search", CloudSolrStream.class)
      .withFunctionName("rollup", RollupStream.class)
      .withFunctionName("sum", SumMetric.class)
      .withFunctionName("min", MinMetric.class)
      .withFunctionName("max", MaxMetric.class)
      .withFunctionName("avg", MeanMetric.class)
      .withFunctionName("count", CountMetric.class);     
    
    StreamExpression expression;
    TupleStream stream;
    List<Tuple> tuples;

    expression = StreamExpressionParser.parse("rollup("
                                              + "search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"a_s,a_i,a_f\", sort=\"a_s asc\"),"
                                              + "over=\"a_s\","
                                              + "sum(a_i),"
                                              + "sum(a_f),"
                                              + "min(a_i),"
                                              + "min(a_f),"
                                              + "max(a_i),"
                                              + "max(a_f),"
                                              + "avg(a_i),"
                                              + "avg(a_f),"
                                              + "count(*),"
                                              + ")");
    stream = factory.constructStream(expression);
    tuples = getTuples(stream);

    assert(tuples.size() == 3);

    //Test Long and Double Sums

    Tuple tuple = tuples.get(0);
    String bucket = tuple.getString("a_s");
    Double sumi = tuple.getDouble("sum(a_i)");
    Double sumf = tuple.getDouble("sum(a_f)");
    Double mini = tuple.getDouble("min(a_i)");
    Double minf = tuple.getDouble("min(a_f)");
    Double maxi = tuple.getDouble("max(a_i)");
    Double maxf = tuple.getDouble("max(a_f)");
    Double avgi = tuple.getDouble("avg(a_i)");
    Double avgf = tuple.getDouble("avg(a_f)");
    Double count = tuple.getDouble("count(*)");

    assertTrue(bucket.equals("hello0"));
    assertTrue(sumi.doubleValue() == 17.0D);
    assertTrue(sumf.doubleValue() == 18.0D);
    assertTrue(mini.doubleValue() == 0.0D);
    assertTrue(minf.doubleValue() == 1.0D);
    assertTrue(maxi.doubleValue() == 14.0D);
    assertTrue(maxf.doubleValue() == 10.0D);
    assertTrue(avgi.doubleValue() == 4.25D);
    assertTrue(avgf.doubleValue() == 4.5D);
    assertTrue(count.doubleValue() == 4);

    tuple = tuples.get(1);
    bucket = tuple.getString("a_s");
    sumi = tuple.getDouble("sum(a_i)");
    sumf = tuple.getDouble("sum(a_f)");
    mini = tuple.getDouble("min(a_i)");
    minf = tuple.getDouble("min(a_f)");
    maxi = tuple.getDouble("max(a_i)");
    maxf = tuple.getDouble("max(a_f)");
    avgi = tuple.getDouble("avg(a_i)");
    avgf = tuple.getDouble("avg(a_f)");
    count = tuple.getDouble("count(*)");

    assertTrue(bucket.equals("hello3"));
    assertTrue(sumi.doubleValue() == 38.0D);
    assertTrue(sumf.doubleValue() == 26.0D);
    assertTrue(mini.doubleValue() == 3.0D);
    assertTrue(minf.doubleValue() == 3.0D);
    assertTrue(maxi.doubleValue() == 13.0D);
    assertTrue(maxf.doubleValue() == 9.0D);
    assertTrue(avgi.doubleValue() == 9.5D);
    assertTrue(avgf.doubleValue() == 6.5D);
    assertTrue(count.doubleValue() == 4);

    tuple = tuples.get(2);
    bucket = tuple.getString("a_s");
    sumi = tuple.getDouble("sum(a_i)");
    sumf = tuple.getDouble("sum(a_f)");
    mini = tuple.getDouble("min(a_i)");
    minf = tuple.getDouble("min(a_f)");
    maxi = tuple.getDouble("max(a_i)");
    maxf = tuple.getDouble("max(a_f)");
    avgi = tuple.getDouble("avg(a_i)");
    avgf = tuple.getDouble("avg(a_f)");
    count = tuple.getDouble("count(*)");

    assertTrue(bucket.equals("hello4"));
    assertTrue(sumi.longValue() == 15);
    assertTrue(sumf.doubleValue() == 11.0D);
    assertTrue(mini.doubleValue() == 4.0D);
    assertTrue(minf.doubleValue() == 4.0D);
    assertTrue(maxi.doubleValue() == 11.0D);
    assertTrue(maxf.doubleValue() == 7.0D);
    assertTrue(avgi.doubleValue() == 7.5D);
    assertTrue(avgf.doubleValue() == 5.5D);
    assertTrue(count.doubleValue() == 2);

  }

  @Test
  public void testStatsStream() throws Exception {

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

    StreamFactory factory = new StreamFactory()
    .withCollectionZkHost(COLLECTIONORALIAS, cluster.getZkServer().getZkAddress())
    .withFunctionName("stats", StatsStream.class)
    .withFunctionName("sum", SumMetric.class)
    .withFunctionName("min", MinMetric.class)
    .withFunctionName("max", MaxMetric.class)
    .withFunctionName("avg", MeanMetric.class)
    .withFunctionName("count", CountMetric.class);     
  
    StreamExpression expression;
    TupleStream stream;
    List<Tuple> tuples;
  
    expression = StreamExpressionParser.parse("stats(collection1, q=*:*, sum(a_i), sum(a_f), min(a_i), min(a_f), max(a_i), max(a_f), avg(a_i), avg(a_f), count(*))");
    stream = factory.constructStream(expression);

    tuples = getTuples(stream);

    assert(tuples.size() == 1);

    //Test Long and Double Sums

    Tuple tuple = tuples.get(0);

    Double sumi = tuple.getDouble("sum(a_i)");
    Double sumf = tuple.getDouble("sum(a_f)");
    Double mini = tuple.getDouble("min(a_i)");
    Double minf = tuple.getDouble("min(a_f)");
    Double maxi = tuple.getDouble("max(a_i)");
    Double maxf = tuple.getDouble("max(a_f)");
    Double avgi = tuple.getDouble("avg(a_i)");
    Double avgf = tuple.getDouble("avg(a_f)");
    Double count = tuple.getDouble("count(*)");

    assertTrue(sumi.longValue() == 70);
    assertTrue(sumf.doubleValue() == 55.0D);
    assertTrue(mini.doubleValue() == 0.0D);
    assertTrue(minf.doubleValue() == 1.0D);
    assertTrue(maxi.doubleValue() == 14.0D);
    assertTrue(maxf.doubleValue() == 10.0D);
    assertTrue(avgi.doubleValue() == 7.0D);
    assertTrue(avgf.doubleValue() == 5.5D);
    assertTrue(count.doubleValue() == 10);

  }

  @Test
  public void testParallelUniqueStream() throws Exception {

    new UpdateRequest()
        .add(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "0")
        .add(id, "2", "a_s", "hello2", "a_i", "2", "a_f", "0")
        .add(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3")
        .add(id, "4", "a_s", "hello4", "a_i", "4", "a_f", "4")
        .add(id, "1", "a_s", "hello1", "a_i", "1", "a_f", "1")
        .add(id, "5", "a_s", "hello1", "a_i", "10", "a_f", "1")
        .add(id, "6", "a_s", "hello1", "a_i", "11", "a_f", "5")
        .add(id, "7", "a_s", "hello1", "a_i", "12", "a_f", "5")
        .add(id, "8", "a_s", "hello1", "a_i", "13", "a_f", "4")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    String zkHost = cluster.getZkServer().getZkAddress();
    StreamFactory streamFactory = new StreamFactory().withCollectionZkHost(COLLECTIONORALIAS, zkHost)
        .withFunctionName("search", CloudSolrStream.class)
        .withFunctionName("unique", UniqueStream.class)
        .withFunctionName("top", RankStream.class)
        .withFunctionName("group", ReducerStream.class)
        .withFunctionName("parallel", ParallelStream.class);

    ParallelStream pstream = (ParallelStream)streamFactory.constructStream("parallel(" + COLLECTIONORALIAS + ", unique(search(collection1, q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\", partitionKeys=\"a_f\"), over=\"a_f\"), workers=\"2\", zkHost=\""+zkHost+"\", sort=\"a_f asc\")");

    List<Tuple> tuples = getTuples(pstream);
    assert(tuples.size() == 5);
    assertOrder(tuples, 0,1,3,4,6);

    //Test the eofTuples

    Map<String,Tuple> eofTuples = pstream.getEofTuples();
    assert(eofTuples.size() == 2); //There should be an EOF tuple for each worker.

  }

  @Test
  public void testParallelReducerStream() throws Exception {

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

    String zkHost = cluster.getZkServer().getZkAddress();
    StreamFactory streamFactory = new StreamFactory().withCollectionZkHost(COLLECTIONORALIAS, zkHost)
        .withFunctionName("search", CloudSolrStream.class)
        .withFunctionName("group", GroupOperation.class)
        .withFunctionName("reduce", ReducerStream.class)
        .withFunctionName("parallel", ParallelStream.class);

    ParallelStream pstream = (ParallelStream)streamFactory.constructStream("parallel(" + COLLECTIONORALIAS + ", " +
                                                                                    "reduce(" +
                                                                                              "search(" + COLLECTIONORALIAS + ", q=\"*:*\", fl=\"id,a_s,a_i,a_f\", sort=\"a_s asc,a_f asc\", partitionKeys=\"a_s\"), " +
                                                                                              "by=\"a_s\"," +
                                                                                              "group(sort=\"a_i asc\", n=\"5\")), " +
                                                                                    "workers=\"2\", zkHost=\""+zkHost+"\", sort=\"a_s asc\")");

    List<Tuple> tuples = getTuples(pstream);

    assert(tuples.size() == 3);

    Tuple t0 = tuples.get(0);
    List<Map> maps0 = t0.getMaps("group");
    assertMaps(maps0, 0, 1, 2, 9);

    Tuple t1 = tuples.get(1);
    List<Map> maps1 = t1.getMaps("group");
    assertMaps(maps1, 3, 5, 7, 8);

    Tuple t2 = tuples.get(2);
    List<Map> maps2 = t2.getMaps("group");
    assertMaps(maps2, 4, 6);


    pstream = (ParallelStream)streamFactory.constructStream("parallel(" + COLLECTIONORALIAS + ", " +
                                                                      "reduce(" +
                                                                              "search(" + COLLECTIONORALIAS + ", q=\"*:*\", fl=\"id,a_s,a_i,a_f\", sort=\"a_s desc,a_f asc\", partitionKeys=\"a_s\"), " +
                                                                              "by=\"a_s\", " +
                                                                              "group(sort=\"a_i desc\", n=\"5\")),"+
                                                                      "workers=\"2\", zkHost=\""+zkHost+"\", sort=\"a_s desc\")");

    tuples = getTuples(pstream);

    assert(tuples.size() == 3);

    t0 = tuples.get(0);
    maps0 = t0.getMaps("group");
    assertMaps(maps0, 6, 4);


    t1 = tuples.get(1);
    maps1 = t1.getMaps("group");
    assertMaps(maps1, 8, 7, 5, 3);


    t2 = tuples.get(2);
    maps2 = t2.getMaps("group");
    assertMaps(maps2, 9, 2, 1, 0);

  }

  @Test
  public void testParallelRankStream() throws Exception {

    new UpdateRequest()
        .add(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "0")
        .add(id, "2", "a_s", "hello2", "a_i", "2", "a_f", "0")
        .add(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3")
        .add(id, "4", "a_s", "hello4", "a_i", "4", "a_f", "4")
        .add(id, "5", "a_s", "hello1", "a_i", "5", "a_f", "1")
        .add(id, "6", "a_s", "hello1", "a_i", "6", "a_f", "1")
        .add(id, "7", "a_s", "hello1", "a_i", "7", "a_f", "1")
        .add(id, "8", "a_s", "hello1", "a_i", "8", "a_f", "1")
        .add(id, "9", "a_s", "hello1", "a_i", "9", "a_f", "1")
        .add(id, "10", "a_s", "hello1", "a_i", "10", "a_f", "1")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    String zkHost = cluster.getZkServer().getZkAddress();
    StreamFactory streamFactory = new StreamFactory().withCollectionZkHost(COLLECTIONORALIAS, zkHost)
        .withFunctionName("search", CloudSolrStream.class)
        .withFunctionName("unique", UniqueStream.class)
        .withFunctionName("top", RankStream.class)
        .withFunctionName("group", ReducerStream.class)
        .withFunctionName("parallel", ParallelStream.class);

    ParallelStream pstream = (ParallelStream)streamFactory.constructStream("parallel("
        + COLLECTIONORALIAS + ", "
        + "top("
          + "search(" + COLLECTIONORALIAS + ", q=\"*:*\", fl=\"id,a_s,a_i\", sort=\"a_i asc\", partitionKeys=\"a_i\"), "
          + "n=\"11\", "
          + "sort=\"a_i desc\"), workers=\"2\", zkHost=\""+zkHost+"\", sort=\"a_i desc\")");

    List<Tuple> tuples = getTuples(pstream);

    assert(tuples.size() == 10);
    assertOrder(tuples, 10,9,8,7,6,5,4,3,2,0);

  }

  @Test
  public void testParallelMergeStream() throws Exception {

    new UpdateRequest()
        .add(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "0")
        .add(id, "2", "a_s", "hello2", "a_i", "2", "a_f", "0")
        .add(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3")
        .add(id, "4", "a_s", "hello4", "a_i", "4", "a_f", "4")
        .add(id, "1", "a_s", "hello1", "a_i", "1", "a_f", "1")
        .add(id, "5", "a_s", "hello0", "a_i", "10", "a_f", "0")
        .add(id, "6", "a_s", "hello2", "a_i", "8", "a_f", "0")
        .add(id, "7", "a_s", "hello3", "a_i", "7", "a_f", "3")
        .add(id, "8", "a_s", "hello4", "a_i", "11", "a_f", "4")
        .add(id, "9", "a_s", "hello1", "a_i", "100", "a_f", "1")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    String zkHost = cluster.getZkServer().getZkAddress();
    StreamFactory streamFactory = new StreamFactory().withCollectionZkHost(COLLECTIONORALIAS, zkHost)
        .withFunctionName("search", CloudSolrStream.class)
        .withFunctionName("unique", UniqueStream.class)
        .withFunctionName("top", RankStream.class)
        .withFunctionName("group", ReducerStream.class)
        .withFunctionName("merge", MergeStream.class)
        .withFunctionName("parallel", ParallelStream.class);

    //Test ascending
    ParallelStream pstream = (ParallelStream)streamFactory.constructStream("parallel(" + COLLECTIONORALIAS + ", merge(search(" + COLLECTIONORALIAS + ", q=\"id:(4 1 8 7 9)\", fl=\"id,a_s,a_i\", sort=\"a_i asc\", partitionKeys=\"a_i\"), search(" + COLLECTIONORALIAS + ", q=\"id:(0 2 3 6)\", fl=\"id,a_s,a_i\", sort=\"a_i asc\", partitionKeys=\"a_i\"), on=\"a_i asc\"), workers=\"2\", zkHost=\""+zkHost+"\", sort=\"a_i asc\")");

    List<Tuple> tuples = getTuples(pstream);



    assert(tuples.size() == 9);
    assertOrder(tuples, 0, 1, 2, 3, 4, 7, 6, 8, 9);

    //Test descending

    pstream = (ParallelStream)streamFactory.constructStream("parallel(" + COLLECTIONORALIAS + ", merge(search(" + COLLECTIONORALIAS + ", q=\"id:(4 1 8 9)\", fl=\"id,a_s,a_i\", sort=\"a_i desc\", partitionKeys=\"a_i\"), search(" + COLLECTIONORALIAS + ", q=\"id:(0 2 3 6)\", fl=\"id,a_s,a_i\", sort=\"a_i desc\", partitionKeys=\"a_i\"), on=\"a_i desc\"), workers=\"2\", zkHost=\""+zkHost+"\", sort=\"a_i desc\")");

    tuples = getTuples(pstream);

    assert(tuples.size() == 8);
    assertOrder(tuples, 9, 8, 6, 4, 3, 2, 1, 0);

  }

  @Test
  public void testParallelRollupStream() throws Exception {

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

    StreamFactory factory = new StreamFactory()
      .withCollectionZkHost(COLLECTIONORALIAS, cluster.getZkServer().getZkAddress())
      .withFunctionName("search", CloudSolrStream.class)
      .withFunctionName("parallel", ParallelStream.class)
      .withFunctionName("rollup", RollupStream.class)
      .withFunctionName("sum", SumMetric.class)
      .withFunctionName("min", MinMetric.class)
      .withFunctionName("max", MaxMetric.class)
      .withFunctionName("avg", MeanMetric.class)
      .withFunctionName("count", CountMetric.class);     
    
    StreamExpression expression;
    TupleStream stream;
    List<Tuple> tuples;

    expression = StreamExpressionParser.parse("parallel(" + COLLECTIONORALIAS + ","
                                              + "rollup("
                                                + "search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"a_s,a_i,a_f\", sort=\"a_s asc\", partitionKeys=\"a_s\"),"
                                                + "over=\"a_s\","
                                                + "sum(a_i),"
                                                + "sum(a_f),"
                                                + "min(a_i),"
                                                + "min(a_f),"
                                                + "max(a_i),"
                                                + "max(a_f),"
                                                + "avg(a_i),"
                                                + "avg(a_f),"
                                                + "count(*)"
                                              + "),"
                                              + "workers=\"2\", zkHost=\""+cluster.getZkServer().getZkAddress()+"\", sort=\"a_s asc\")"
                                              );
    stream = factory.constructStream(expression);
    tuples = getTuples(stream);

    assert(tuples.size() == 3);

    //Test Long and Double Sums

    Tuple tuple = tuples.get(0);
    String bucket = tuple.getString("a_s");
    Double sumi = tuple.getDouble("sum(a_i)");
    Double sumf = tuple.getDouble("sum(a_f)");
    Double mini = tuple.getDouble("min(a_i)");
    Double minf = tuple.getDouble("min(a_f)");
    Double maxi = tuple.getDouble("max(a_i)");
    Double maxf = tuple.getDouble("max(a_f)");
    Double avgi = tuple.getDouble("avg(a_i)");
    Double avgf = tuple.getDouble("avg(a_f)");
    Double count = tuple.getDouble("count(*)");

    assertTrue(bucket.equals("hello0"));
    assertTrue(sumi.doubleValue() == 17.0D);
    assertTrue(sumf.doubleValue() == 18.0D);
    assertTrue(mini.doubleValue() == 0.0D);
    assertTrue(minf.doubleValue() == 1.0D);
    assertTrue(maxi.doubleValue() == 14.0D);
    assertTrue(maxf.doubleValue() == 10.0D);
    assertTrue(avgi.doubleValue() == 4.25D);
    assertTrue(avgf.doubleValue() == 4.5D);
    assertTrue(count.doubleValue() == 4);

    tuple = tuples.get(1);
    bucket = tuple.getString("a_s");
    sumi = tuple.getDouble("sum(a_i)");
    sumf = tuple.getDouble("sum(a_f)");
    mini = tuple.getDouble("min(a_i)");
    minf = tuple.getDouble("min(a_f)");
    maxi = tuple.getDouble("max(a_i)");
    maxf = tuple.getDouble("max(a_f)");
    avgi = tuple.getDouble("avg(a_i)");
    avgf = tuple.getDouble("avg(a_f)");
    count = tuple.getDouble("count(*)");

    assertTrue(bucket.equals("hello3"));
    assertTrue(sumi.doubleValue() == 38.0D);
    assertTrue(sumf.doubleValue() == 26.0D);
    assertTrue(mini.doubleValue() == 3.0D);
    assertTrue(minf.doubleValue() == 3.0D);
    assertTrue(maxi.doubleValue() == 13.0D);
    assertTrue(maxf.doubleValue() == 9.0D);
    assertTrue(avgi.doubleValue() == 9.5D);
    assertTrue(avgf.doubleValue() == 6.5D);
    assertTrue(count.doubleValue() == 4);

    tuple = tuples.get(2);
    bucket = tuple.getString("a_s");
    sumi = tuple.getDouble("sum(a_i)");
    sumf = tuple.getDouble("sum(a_f)");
    mini = tuple.getDouble("min(a_i)");
    minf = tuple.getDouble("min(a_f)");
    maxi = tuple.getDouble("max(a_i)");
    maxf = tuple.getDouble("max(a_f)");
    avgi = tuple.getDouble("avg(a_i)");
    avgf = tuple.getDouble("avg(a_f)");
    count = tuple.getDouble("count(*)");

    assertTrue(bucket.equals("hello4"));
    assertTrue(sumi.longValue() == 15);
    assertTrue(sumf.doubleValue() == 11.0D);
    assertTrue(mini.doubleValue() == 4.0D);
    assertTrue(minf.doubleValue() == 4.0D);
    assertTrue(maxi.doubleValue() == 11.0D);
    assertTrue(maxf.doubleValue() == 7.0D);
    assertTrue(avgi.doubleValue() == 7.5D);
    assertTrue(avgf.doubleValue() == 5.5D);
    assertTrue(count.doubleValue() == 2);

  }

  @Test
  public void testInnerJoinStream() throws Exception {

    new UpdateRequest()
        .add(id, "1", "side_s", "left", "join1_i", "0", "join2_s", "a", "ident_s", "left_1") // 8, 9
        .add(id, "15", "side_s", "left", "join1_i", "0", "join2_s", "a", "ident_s", "left_1") // 8, 9
        .add(id, "2", "side_s", "left", "join1_i", "0", "join2_s", "b", "ident_s", "left_2")
        .add(id, "3", "side_s", "left", "join1_i", "1", "join2_s", "a", "ident_s", "left_3") // 10
        .add(id, "4", "side_s", "left", "join1_i", "1", "join2_s", "b", "ident_s", "left_4") // 11
        .add(id, "5", "side_s", "left", "join1_i", "1", "join2_s", "c", "ident_s", "left_5") // 12
        .add(id, "6", "side_s", "left", "join1_i", "2", "join2_s", "d", "ident_s", "left_6")
        .add(id, "7", "side_s", "left", "join1_i", "3", "join2_s", "e", "ident_s", "left_7") // 14

        .add(id, "8", "side_s", "right", "join1_i", "0", "join2_s", "a", "ident_s", "right_1", "join3_i", "0") // 1,15
        .add(id, "9", "side_s", "right", "join1_i", "0", "join2_s", "a", "ident_s", "right_2", "join3_i", "0") // 1,15
        .add(id, "10", "side_s", "right", "join1_i", "1", "join2_s", "a", "ident_s", "right_3", "join3_i", "1") // 3
        .add(id, "11", "side_s", "right", "join1_i", "1", "join2_s", "b", "ident_s", "right_4", "join3_i", "1") // 4
        .add(id, "12", "side_s", "right", "join1_i", "1", "join2_s", "c", "ident_s", "right_5", "join3_i", "1") // 5
        .add(id, "13", "side_s", "right", "join1_i", "2", "join2_s", "dad", "ident_s", "right_6", "join3_i", "2")
        .add(id, "14", "side_s", "right", "join1_i", "3", "join2_s", "e", "ident_s", "right_7", "join3_i", "3") // 7
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    StreamExpression expression;
    TupleStream stream;
    List<Tuple> tuples;
    
    StreamFactory factory = new StreamFactory()
      .withCollectionZkHost(COLLECTIONORALIAS, cluster.getZkServer().getZkAddress())
      .withFunctionName("search", CloudSolrStream.class)
      .withFunctionName("innerJoin", InnerJoinStream.class);
    
    // Basic test
    expression = StreamExpressionParser.parse("innerJoin("
                                                + "search(" + COLLECTIONORALIAS + ", q=\"side_s:left\", fl=\"id,join1_i,join2_s,ident_s\", sort=\"join1_i asc, join2_s asc, id asc\"),"
                                                + "search(" + COLLECTIONORALIAS + ", q=\"side_s:right\", fl=\"join1_i,join2_s,ident_s\", sort=\"join1_i asc, join2_s asc\"),"
                                                + "on=\"join1_i=join1_i, join2_s=join2_s\")");
    stream = new InnerJoinStream(expression, factory);
    tuples = getTuples(stream);    
    assert(tuples.size() == 8);
    assertOrder(tuples, 1,1,15,15,3,4,5,7);

    // Basic desc
    expression = StreamExpressionParser.parse("innerJoin("
                                                + "search(" + COLLECTIONORALIAS + ", q=\"side_s:left\", fl=\"id,join1_i,join2_s,ident_s\", sort=\"join1_i desc, join2_s asc\"),"
                                                + "search(" + COLLECTIONORALIAS + ", q=\"side_s:right\", fl=\"join1_i,join2_s,ident_s\", sort=\"join1_i desc, join2_s asc\"),"
                                                + "on=\"join1_i=join1_i, join2_s=join2_s\")");
    stream = new InnerJoinStream(expression, factory);
    tuples = getTuples(stream);    
    assert(tuples.size() == 8);
    assertOrder(tuples, 7,3,4,5,1,1,15,15);
    
    // Results in both searches, no join matches
    expression = StreamExpressionParser.parse("innerJoin("
                                                + "search(" + COLLECTIONORALIAS + ", q=\"side_s:left\", fl=\"id,join1_i,join2_s,ident_s\", sort=\"ident_s asc\"),"
                                                + "search(" + COLLECTIONORALIAS + ", q=\"side_s:right\", fl=\"id,join1_i,join2_s,ident_s\", sort=\"ident_s asc\", aliases=\"id=right.id, join1_i=right.join1_i, join2_s=right.join2_s, ident_s=right.ident_s\"),"
                                                + "on=\"ident_s=right.ident_s\")");
    stream = new InnerJoinStream(expression, factory);
    tuples = getTuples(stream);    
    assert(tuples.size() == 0);
    
    // Differing field names
    expression = StreamExpressionParser.parse("innerJoin("
                                                + "search(" + COLLECTIONORALIAS + ", q=\"side_s:left\", fl=\"id,join1_i,join2_s,ident_s\", sort=\"join1_i asc, join2_s asc, id asc\"),"
                                                + "search(" + COLLECTIONORALIAS + ", q=\"side_s:right\", fl=\"join3_i,join2_s,ident_s\", sort=\"join3_i asc, join2_s asc\", aliases=\"join3_i=aliasesField\"),"
                                                + "on=\"join1_i=aliasesField, join2_s=join2_s\")");
    stream = new InnerJoinStream(expression, factory);
    tuples = getTuples(stream);
    
    assert(tuples.size() == 8);
    assertOrder(tuples, 1,1,15,15,3,4,5,7);

  }

  @Test
  public void testLeftOuterJoinStream() throws Exception {

    new UpdateRequest()
        .add(id, "1", "side_s", "left", "join1_i", "0", "join2_s", "a", "ident_s", "left_1") // 8, 9
        .add(id, "15", "side_s", "left", "join1_i", "0", "join2_s", "a", "ident_s", "left_1") // 8, 9
        .add(id, "2", "side_s", "left", "join1_i", "0", "join2_s", "b", "ident_s", "left_2")
        .add(id, "3", "side_s", "left", "join1_i", "1", "join2_s", "a", "ident_s", "left_3") // 10
        .add(id, "4", "side_s", "left", "join1_i", "1", "join2_s", "b", "ident_s", "left_4") // 11
        .add(id, "5", "side_s", "left", "join1_i", "1", "join2_s", "c", "ident_s", "left_5") // 12
        .add(id, "6", "side_s", "left", "join1_i", "2", "join2_s", "d", "ident_s", "left_6")
        .add(id, "7", "side_s", "left", "join1_i", "3", "join2_s", "e", "ident_s", "left_7") // 14

        .add(id, "8", "side_s", "right", "join1_i", "0", "join2_s", "a", "ident_s", "right_1", "join3_i", "0") // 1,15
        .add(id, "9", "side_s", "right", "join1_i", "0", "join2_s", "a", "ident_s", "right_2", "join3_i", "0") // 1,15
        .add(id, "10", "side_s", "right", "join1_i", "1", "join2_s", "a", "ident_s", "right_3", "join3_i", "1") // 3
        .add(id, "11", "side_s", "right", "join1_i", "1", "join2_s", "b", "ident_s", "right_4", "join3_i", "1") // 4
        .add(id, "12", "side_s", "right", "join1_i", "1", "join2_s", "c", "ident_s", "right_5", "join3_i", "1") // 5
        .add(id, "13", "side_s", "right", "join1_i", "2", "join2_s", "dad", "ident_s", "right_6", "join3_i", "2")
        .add(id, "14", "side_s", "right", "join1_i", "3", "join2_s", "e", "ident_s", "right_7", "join3_i", "3") // 7
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    StreamExpression expression;
    TupleStream stream;
    List<Tuple> tuples;
    
    StreamFactory factory = new StreamFactory()
      .withCollectionZkHost(COLLECTIONORALIAS, cluster.getZkServer().getZkAddress())
      .withFunctionName("search", CloudSolrStream.class)
      .withFunctionName("leftOuterJoin", LeftOuterJoinStream.class);
    
    // Basic test
    expression = StreamExpressionParser.parse("leftOuterJoin("
                                                + "search(" + COLLECTIONORALIAS + ", q=\"side_s:left\", fl=\"id,join1_i,join2_s,ident_s\", sort=\"join1_i asc, join2_s asc, id asc\"),"
                                                + "search(" + COLLECTIONORALIAS + ", q=\"side_s:right\", fl=\"join1_i,join2_s,ident_s\", sort=\"join1_i asc, join2_s asc\"),"
                                                + "on=\"join1_i=join1_i, join2_s=join2_s\")");
    stream = new LeftOuterJoinStream(expression, factory);
    tuples = getTuples(stream);    
    assert(tuples.size() == 10);
    assertOrder(tuples, 1,1,15,15,2,3,4,5,6,7);

    // Basic desc
    expression = StreamExpressionParser.parse("leftOuterJoin("
                                                + "search(" + COLLECTIONORALIAS + ", q=\"side_s:left\", fl=\"id,join1_i,join2_s,ident_s\", sort=\"join1_i desc, join2_s asc\"),"
                                                + "search(" + COLLECTIONORALIAS + ", q=\"side_s:right\", fl=\"join1_i,join2_s,ident_s\", sort=\"join1_i desc, join2_s asc\"),"
                                                + "on=\"join1_i=join1_i, join2_s=join2_s\")");
    stream = new LeftOuterJoinStream(expression, factory);
    tuples = getTuples(stream);    
    assert(tuples.size() == 10);
    assertOrder(tuples, 7, 6, 3, 4, 5, 1, 1, 15, 15, 2);
    
    // Results in both searches, no join matches
    expression = StreamExpressionParser.parse("leftOuterJoin("
                                                + "search(" + COLLECTIONORALIAS + ", q=\"side_s:left\", fl=\"id,join1_i,join2_s,ident_s\", sort=\"ident_s asc\"),"
                                                + "search(" + COLLECTIONORALIAS + ", q=\"side_s:right\", fl=\"id,join1_i,join2_s,ident_s\", sort=\"ident_s asc\", aliases=\"id=right.id, join1_i=right.join1_i, join2_s=right.join2_s, ident_s=right.ident_s\"),"
                                                + "on=\"ident_s=right.ident_s\")");
    stream = new LeftOuterJoinStream(expression, factory);
    tuples = getTuples(stream);    
    assert(tuples.size() == 8);
    assertOrder(tuples, 1, 15, 2, 3, 4, 5, 6, 7);
    
    // Differing field names
    expression = StreamExpressionParser.parse("leftOuterJoin("
                                                + "search(" + COLLECTIONORALIAS + ", q=\"side_s:left\", fl=\"id,join1_i,join2_s,ident_s\", sort=\"join1_i asc, join2_s asc, id asc\"),"
                                                + "search(" + COLLECTIONORALIAS + ", q=\"side_s:right\", fl=\"join3_i,join2_s,ident_s\", sort=\"join3_i asc, join2_s asc\", aliases=\"join3_i=aliasesField\"),"
                                                + "on=\"join1_i=aliasesField, join2_s=join2_s\")");
    stream = new LeftOuterJoinStream(expression, factory);
    tuples = getTuples(stream);
    assert(tuples.size() == 10);
    assertOrder(tuples, 1, 1, 15, 15, 2, 3, 4, 5, 6, 7);

  }

  @Test
  public void testHashJoinStream() throws Exception {

    new UpdateRequest()
        .add(id, "1", "side_s", "left", "join1_i", "0", "join2_s", "a", "ident_s", "left_1") // 8, 9
        .add(id, "15", "side_s", "left", "join1_i", "0", "join2_s", "a", "ident_s", "left_1") // 8, 9
        .add(id, "2", "side_s", "left", "join1_i", "0", "join2_s", "b", "ident_s", "left_2")
        .add(id, "3", "side_s", "left", "join1_i", "1", "join2_s", "a", "ident_s", "left_3") // 10
        .add(id, "4", "side_s", "left", "join1_i", "1", "join2_s", "b", "ident_s", "left_4") // 11
        .add(id, "5", "side_s", "left", "join1_i", "1", "join2_s", "c", "ident_s", "left_5") // 12
        .add(id, "6", "side_s", "left", "join1_i", "2", "join2_s", "d", "ident_s", "left_6")
        .add(id, "7", "side_s", "left", "join1_i", "3", "join2_s", "e", "ident_s", "left_7") // 14

        .add(id, "8", "side_s", "right", "join1_i", "0", "join2_s", "a", "ident_s", "right_1", "join3_i", "0") // 1,15
        .add(id, "9", "side_s", "right", "join1_i", "0", "join2_s", "a", "ident_s", "right_2", "join3_i", "0") // 1,15
        .add(id, "10", "side_s", "right", "join1_i", "1", "join2_s", "a", "ident_s", "right_3", "join3_i", "1") // 3
        .add(id, "11", "side_s", "right", "join1_i", "1", "join2_s", "b", "ident_s", "right_4", "join3_i", "1") // 4
        .add(id, "12", "side_s", "right", "join1_i", "1", "join2_s", "c", "ident_s", "right_5", "join3_i", "1") // 5
        .add(id, "13", "side_s", "right", "join1_i", "2", "join2_s", "dad", "ident_s", "right_6", "join3_i", "2")
        .add(id, "14", "side_s", "right", "join1_i", "3", "join2_s", "e", "ident_s", "right_7", "join3_i", "3") // 7
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    StreamExpression expression;
    TupleStream stream;
    List<Tuple> tuples;
    
    StreamFactory factory = new StreamFactory()
      .withCollectionZkHost(COLLECTIONORALIAS, cluster.getZkServer().getZkAddress())
      .withFunctionName("search", CloudSolrStream.class)
      .withFunctionName("hashJoin", HashJoinStream.class);
    
    // Basic test
    expression = StreamExpressionParser.parse("hashJoin("
                                                + "search(collection1, q=\"side_s:left\", fl=\"id,join1_i,join2_s,ident_s\", sort=\"join1_i asc, join2_s asc, id asc\"),"
                                                + "hashed=search(collection1, q=\"side_s:right\", fl=\"join1_i,join2_s,ident_s\", sort=\"join1_i asc, join2_s asc\"),"
                                                + "on=\"join1_i, join2_s\")");
    stream = new HashJoinStream(expression, factory);
    tuples = getTuples(stream);    
    assert(tuples.size() == 8);
    assertOrder(tuples, 1,1,15,15,3,4,5,7);

    // Basic desc
    expression = StreamExpressionParser.parse("hashJoin("
                                                + "search(collection1, q=\"side_s:left\", fl=\"id,join1_i,join2_s,ident_s\", sort=\"join1_i desc, join2_s asc\"),"
                                                + "hashed=search(collection1, q=\"side_s:right\", fl=\"join1_i,join2_s,ident_s\", sort=\"join1_i desc, join2_s asc\"),"
                                                + "on=\"join1_i, join2_s\")");
    stream = new HashJoinStream(expression, factory);
    tuples = getTuples(stream);    
    assert(tuples.size() == 8);
    assertOrder(tuples, 7,3,4,5,1,1,15,15);
    
    // Results in both searches, no join matches
    expression = StreamExpressionParser.parse("hashJoin("
                                                + "search(collection1, q=\"side_s:left\", fl=\"id,join1_i,join2_s,ident_s\", sort=\"ident_s asc\"),"
                                                + "hashed=search(collection1, q=\"side_s:right\", fl=\"id,join1_i,join2_s,ident_s\", sort=\"ident_s asc\"),"
                                                + "on=\"ident_s\")");
    stream = new HashJoinStream(expression, factory);
    tuples = getTuples(stream);    
    assert(tuples.size() == 0);
    
    // Basic test with "on" mapping
    expression = StreamExpressionParser.parse("hashJoin("
                                                + "search(collection1, q=\"side_s:left\", fl=\"id,join1_i,join3_i,ident_s\", sort=\"join1_i asc, join3_i asc, id asc\"),"
                                                + "hashed=search(collection1, q=\"side_s:right\", fl=\"join1_i,join3_i,ident_s\", sort=\"join1_i asc, join3_i asc\"),"
                                                + "on=\"join1_i=join3_i\")");
    stream = new HashJoinStream(expression, factory);
    tuples = getTuples(stream);
    assertEquals(17, tuples.size());

    //Does a lexical sort
    assertOrder(tuples, 1, 1, 15, 15, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 5, 6, 7);

  }

  @Test
  public void testOuterHashJoinStream() throws Exception {

    new UpdateRequest()
        .add(id, "1", "side_s", "left", "join1_i", "0", "join2_s", "a", "ident_s", "left_1") // 8, 9
        .add(id, "15", "side_s", "left", "join1_i", "0", "join2_s", "a", "ident_s", "left_1") // 8, 9
        .add(id, "2", "side_s", "left", "join1_i", "0", "join2_s", "b", "ident_s", "left_2")
        .add(id, "3", "side_s", "left", "join1_i", "1", "join2_s", "a", "ident_s", "left_3") // 10
        .add(id, "4", "side_s", "left", "join1_i", "1", "join2_s", "b", "ident_s", "left_4") // 11
        .add(id, "5", "side_s", "left", "join1_i", "1", "join2_s", "c", "ident_s", "left_5") // 12
        .add(id, "6", "side_s", "left", "join1_i", "2", "join2_s", "d", "ident_s", "left_6")
        .add(id, "7", "side_s", "left", "join1_i", "3", "join2_s", "e", "ident_s", "left_7") // 14

        .add(id, "8", "side_s", "right", "join1_i", "0", "join2_s", "a", "ident_s", "right_1", "join3_i", "0") // 1,15
        .add(id, "9", "side_s", "right", "join1_i", "0", "join2_s", "a", "ident_s", "right_2", "join3_i", "0") // 1,15
        .add(id, "10", "side_s", "right", "join1_i", "1", "join2_s", "a", "ident_s", "right_3", "join3_i", "1") // 3
        .add(id, "11", "side_s", "right", "join1_i", "1", "join2_s", "b", "ident_s", "right_4", "join3_i", "1") // 4
        .add(id, "12", "side_s", "right", "join1_i", "1", "join2_s", "c", "ident_s", "right_5", "join3_i", "1") // 5
        .add(id, "13", "side_s", "right", "join1_i", "2", "join2_s", "dad", "ident_s", "right_6", "join3_i", "2")
        .add(id, "14", "side_s", "right", "join1_i", "3", "join2_s", "e", "ident_s", "right_7", "join3_i", "3") // 7
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    StreamExpression expression;
    TupleStream stream;
    List<Tuple> tuples;
    
    StreamFactory factory = new StreamFactory()
        .withCollectionZkHost("collection1", cluster.getZkServer().getZkAddress())
      .withFunctionName("search", CloudSolrStream.class)
      .withFunctionName("outerHashJoin", OuterHashJoinStream.class);
    
    // Basic test
    expression = StreamExpressionParser.parse("outerHashJoin("
                                                + "search(collection1, q=\"side_s:left\", fl=\"id,join1_i,join2_s,ident_s\", sort=\"join1_i asc, join2_s asc, id asc\"),"
                                                + "hashed=search(collection1, q=\"side_s:right\", fl=\"join1_i,join2_s,ident_s\", sort=\"join1_i asc, join2_s asc\"),"
                                                + "on=\"join1_i, join2_s\")");
    stream = new OuterHashJoinStream(expression, factory);
    tuples = getTuples(stream);    
    assert(tuples.size() == 10);
    assertOrder(tuples, 1, 1, 15, 15, 2, 3, 4, 5, 6, 7);

    // Basic desc
    expression = StreamExpressionParser.parse("outerHashJoin("
        + "search(collection1, q=\"side_s:left\", fl=\"id,join1_i,join2_s,ident_s\", sort=\"join1_i desc, join2_s asc\"),"
        + "hashed=search(collection1, q=\"side_s:right\", fl=\"join1_i,join2_s,ident_s\", sort=\"join1_i desc, join2_s asc\"),"
        + "on=\"join1_i, join2_s\")");
    stream = new OuterHashJoinStream(expression, factory);
    tuples = getTuples(stream);    
    assert(tuples.size() == 10);
    assertOrder(tuples, 7,6,3,4,5,1,1,15,15,2);
    
    // Results in both searches, no join matches
    expression = StreamExpressionParser.parse("outerHashJoin("
        + "search(collection1, q=\"side_s:left\", fl=\"id,join1_i,join2_s,ident_s\", sort=\"ident_s asc\"),"
        + "hashed=search(collection1, q=\"side_s:right\", fl=\"id,join1_i,join2_s,ident_s\", sort=\"ident_s asc\"),"
        + "on=\"ident_s\")");
    stream = new OuterHashJoinStream(expression, factory);
    tuples = getTuples(stream);    
    assert(tuples.size() == 8);
    assertOrder(tuples, 1,15,2,3,4,5,6,7);

    // Basic test
    expression = StreamExpressionParser.parse("outerHashJoin("
                                                + "search(collection1, q=\"side_s:left\", fl=\"id,join1_i,join2_s,ident_s\", sort=\"join1_i asc, join2_s asc, id asc\"),"
                                                + "hashed=search(collection1, q=\"side_s:right\", fl=\"join3_i,join2_s,ident_s\", sort=\"join2_s asc\"),"
                                                + "on=\"join1_i=join3_i, join2_s\")");
    stream = new OuterHashJoinStream(expression, factory);
    tuples = getTuples(stream);
    assert(tuples.size() == 10);
    assertOrder(tuples, 1, 1, 15, 15, 2, 3, 4, 5, 6, 7);
  }

  @Test
  public void testSelectStream() throws Exception {

    new UpdateRequest()
        .add(id, "1", "side_s", "left", "join1_i", "0", "join2_s", "a", "ident_s", "left_1") // 8, 9
        .add(id, "15", "side_s", "left", "join1_i", "0", "join2_s", "a", "ident_s", "left_1") // 8, 9
        .add(id, "2", "side_s", "left", "join1_i", "0", "join2_s", "b", "ident_s", "left_2")
        .add(id, "3", "side_s", "left", "join1_i", "1", "join2_s", "a", "ident_s", "left_3") // 10
        .add(id, "4", "side_s", "left", "join1_i", "1", "join2_s", "b", "ident_s", "left_4") // 11
        .add(id, "5", "side_s", "left", "join1_i", "1", "join2_s", "c", "ident_s", "left_5") // 12
        .add(id, "6", "side_s", "left", "join1_i", "2", "join2_s", "d", "ident_s", "left_6")
        .add(id, "7", "side_s", "left", "join1_i", "3", "join2_s", "e", "ident_s", "left_7") // 14

        .add(id, "8", "side_s", "right", "join1_i", "0", "join2_s", "a", "ident_s", "right_1", "join3_i", "0") // 1,15
        .add(id, "9", "side_s", "right", "join1_i", "0", "join2_s", "a", "ident_s", "right_2", "join3_i", "0") // 1,15
        .add(id, "10", "side_s", "right", "join1_i", "1", "join2_s", "a", "ident_s", "right_3", "join3_i", "1") // 3
        .add(id, "11", "side_s", "right", "join1_i", "1", "join2_s", "b", "ident_s", "right_4", "join3_i", "1") // 4
        .add(id, "12", "side_s", "right", "join1_i", "1", "join2_s", "c", "ident_s", "right_5", "join3_i", "1") // 5
        .add(id, "13", "side_s", "right", "join1_i", "2", "join2_s", "dad", "ident_s", "right_6", "join3_i", "2")
        .add(id, "14", "side_s", "right", "join1_i", "3", "join2_s", "e", "ident_s", "right_7", "join3_i", "3") // 7
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    String clause;
    TupleStream stream;
    List<Tuple> tuples;
    
    StreamFactory factory = new StreamFactory()
      .withCollectionZkHost("collection1", cluster.getZkServer().getZkAddress())
      .withFunctionName("search", CloudSolrStream.class)
      .withFunctionName("innerJoin", InnerJoinStream.class)
      .withFunctionName("select", SelectStream.class)
      .withFunctionName("replace", ReplaceOperation.class)
      .withFunctionName("concat", ConcatOperation.class)
      ;
    
    // Basic test
    clause = "select("
            +   "id, join1_i as join1, join2_s as join2, ident_s as identity,"
            +   "search(collection1, q=\"side_s:left\", fl=\"id,join1_i,join2_s,ident_s\", sort=\"join1_i asc, join2_s asc, id asc\")"
            + ")";
    stream = factory.constructStream(clause);
    tuples = getTuples(stream);
    assertFields(tuples, "id", "join1", "join2", "identity");
    assertNotFields(tuples, "join1_i", "join2_s", "ident_s");

    // Basic with replacements test
    clause = "select("
            +   "id, join1_i as join1, join2_s as join2, ident_s as identity,"
            +   "replace(join1, 0, withValue=12), replace(join1, 3, withValue=12), replace(join1, 2, withField=join2),"
            +   "search(collection1, q=\"side_s:left\", fl=\"id,join1_i,join2_s,ident_s\", sort=\"join1_i asc, join2_s asc, id asc\")"
            + ")";
    stream = factory.constructStream(clause);
    tuples = getTuples(stream);
    assertFields(tuples, "id", "join1", "join2", "identity");
    assertNotFields(tuples, "join1_i", "join2_s", "ident_s");
    assertLong(tuples.get(0), "join1", 12);
    assertLong(tuples.get(1), "join1", 12);
    assertLong(tuples.get(2), "join1", 12);
    assertLong(tuples.get(7), "join1", 12);
    assertString(tuples.get(6), "join1", "d");
    

    // Basic with replacements and concat test
    clause = "select("
            +   "id, join1_i as join1, join2_s as join2, ident_s as identity,"
            +   "replace(join1, 0, withValue=12), replace(join1, 3, withValue=12), replace(join1, 2, withField=join2),"
            +   "concat(fields=\"identity,join1\", as=\"newIdentity\",delim=\"-\"),"
            +   "search(collection1, q=\"side_s:left\", fl=\"id,join1_i,join2_s,ident_s\", sort=\"join1_i asc, join2_s asc, id asc\")"
            + ")";
    stream = factory.constructStream(clause);
    tuples = getTuples(stream);
    assertFields(tuples, "id", "join1", "join2", "identity", "newIdentity");
    assertNotFields(tuples, "join1_i", "join2_s", "ident_s");
    assertLong(tuples.get(0), "join1", 12);
    assertString(tuples.get(0), "newIdentity", "left_1-12");
    assertLong(tuples.get(1), "join1", 12); 
    assertString(tuples.get(1), "newIdentity", "left_1-12");
    assertLong(tuples.get(2), "join1", 12); 
    assertString(tuples.get(2), "newIdentity", "left_2-12");
    assertLong(tuples.get(7), "join1", 12); 
    assertString(tuples.get(7), "newIdentity", "left_7-12");
    assertString(tuples.get(6), "join1", "d");
    assertString(tuples.get(6), "newIdentity", "left_6-d");
    
    // Inner stream test
    clause = "innerJoin("
            +   "select("
            +     "id, join1_i as left.join1, join2_s as left.join2, ident_s as left.ident,"
            +     "search(collection1, q=\"side_s:left\", fl=\"id,join1_i,join2_s,ident_s\", sort=\"join1_i asc, join2_s asc, id asc\")"
            +   "),"
            +   "select("
            +     "join3_i as right.join1, join2_s as right.join2, ident_s as right.ident,"
            +     "search(collection1, q=\"side_s:right\", fl=\"join3_i,join2_s,ident_s\", sort=\"join3_i asc, join2_s asc\"),"
            +   "),"
            +   "on=\"left.join1=right.join1, left.join2=right.join2\""
            + ")";
    stream = factory.constructStream(clause);
    tuples = getTuples(stream);
    assertFields(tuples, "id", "left.join1", "left.join2", "left.ident", "right.join1", "right.join2", "right.ident");
    
    // Wrapped select test
    clause = "select("
            +   "id, left.ident, right.ident,"
            +   "innerJoin("
            +     "select("
            +       "id, join1_i as left.join1, join2_s as left.join2, ident_s as left.ident,"
            +       "search(collection1, q=\"side_s:left\", fl=\"id,join1_i,join2_s,ident_s\", sort=\"join1_i asc, join2_s asc, id asc\")"
            +     "),"
            +     "select("
            +       "join3_i as right.join1, join2_s as right.join2, ident_s as right.ident,"
            +       "search(collection1, q=\"side_s:right\", fl=\"join3_i,join2_s,ident_s\", sort=\"join3_i asc, join2_s asc\"),"
            +     "),"
            +     "on=\"left.join1=right.join1, left.join2=right.join2\""
            +   ")"
            + ")";
    stream = factory.constructStream(clause);
    tuples = getTuples(stream);
    assertFields(tuples, "id", "left.ident", "right.ident");
    assertNotFields(tuples, "left.join1", "left.join2", "right.join1", "right.join2");

  }

  @Test
  public void testFacetStream() throws Exception {

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
    
    String clause;
    TupleStream stream;
    List<Tuple> tuples;
    
    StreamFactory factory = new StreamFactory()
      .withCollectionZkHost("collection1", cluster.getZkServer().getZkAddress())
      .withFunctionName("facet", FacetStream.class)
      .withFunctionName("sum", SumMetric.class)
      .withFunctionName("min", MinMetric.class)
      .withFunctionName("max", MaxMetric.class)
      .withFunctionName("avg", MeanMetric.class)
      .withFunctionName("count", CountMetric.class);
    
    // Basic test
    clause = "facet("
              +   "collection1, "
              +   "q=\"*:*\", "
              +   "fl=\"a_s,a_i,a_f\", "
              +   "sort=\"a_s asc\", "
              +   "buckets=\"a_s\", "
              +   "bucketSorts=\"sum(a_i) asc\", "
              +   "bucketSizeLimit=100, "
              +   "sum(a_i), sum(a_f), "
              +   "min(a_i), min(a_f), "
              +   "max(a_i), max(a_f), "
              +   "avg(a_i), avg(a_f), "
              +   "count(*)"
              + ")";
    
    stream = factory.constructStream(clause);
    tuples = getTuples(stream);

    assert(tuples.size() == 3);

    //Test Long and Double Sums

    Tuple tuple = tuples.get(0);
    String bucket = tuple.getString("a_s");
    Double sumi = tuple.getDouble("sum(a_i)");
    Double sumf = tuple.getDouble("sum(a_f)");
    Double mini = tuple.getDouble("min(a_i)");
    Double minf = tuple.getDouble("min(a_f)");
    Double maxi = tuple.getDouble("max(a_i)");
    Double maxf = tuple.getDouble("max(a_f)");
    Double avgi = tuple.getDouble("avg(a_i)");
    Double avgf = tuple.getDouble("avg(a_f)");
    Double count = tuple.getDouble("count(*)");

    assertTrue(bucket.equals("hello4"));
    assertTrue(sumi.longValue() == 15);
    assertTrue(sumf.doubleValue() == 11.0D);
    assertTrue(mini.doubleValue() == 4.0D);
    assertTrue(minf.doubleValue() == 4.0D);
    assertTrue(maxi.doubleValue() == 11.0D);
    assertTrue(maxf.doubleValue() == 7.0D);
    assertTrue(avgi.doubleValue() == 7.5D);
    assertTrue(avgf.doubleValue() == 5.5D);
    assertTrue(count.doubleValue() == 2);

    tuple = tuples.get(1);
    bucket = tuple.getString("a_s");
    sumi = tuple.getDouble("sum(a_i)");
    sumf = tuple.getDouble("sum(a_f)");
    mini = tuple.getDouble("min(a_i)");
    minf = tuple.getDouble("min(a_f)");
    maxi = tuple.getDouble("max(a_i)");
    maxf = tuple.getDouble("max(a_f)");
    avgi = tuple.getDouble("avg(a_i)");
    avgf = tuple.getDouble("avg(a_f)");
    count = tuple.getDouble("count(*)");

    assertTrue(bucket.equals("hello0"));
    assertTrue(sumi.doubleValue() == 17.0D);
    assertTrue(sumf.doubleValue() == 18.0D);
    assertTrue(mini.doubleValue() == 0.0D);
    assertTrue(minf.doubleValue() == 1.0D);
    assertTrue(maxi.doubleValue() == 14.0D);
    assertTrue(maxf.doubleValue() == 10.0D);
    assertTrue(avgi.doubleValue() == 4.25D);
    assertTrue(avgf.doubleValue() == 4.5D);
    assertTrue(count.doubleValue() == 4);

    tuple = tuples.get(2);
    bucket = tuple.getString("a_s");
    sumi = tuple.getDouble("sum(a_i)");
    sumf = tuple.getDouble("sum(a_f)");
    mini = tuple.getDouble("min(a_i)");
    minf = tuple.getDouble("min(a_f)");
    maxi = tuple.getDouble("max(a_i)");
    maxf = tuple.getDouble("max(a_f)");
    avgi = tuple.getDouble("avg(a_i)");
    avgf = tuple.getDouble("avg(a_f)");
    count = tuple.getDouble("count(*)");

    assertTrue(bucket.equals("hello3"));
    assertTrue(sumi.doubleValue() == 38.0D);
    assertTrue(sumf.doubleValue() == 26.0D);
    assertTrue(mini.doubleValue() == 3.0D);
    assertTrue(minf.doubleValue() == 3.0D);
    assertTrue(maxi.doubleValue() == 13.0D);
    assertTrue(maxf.doubleValue() == 9.0D);
    assertTrue(avgi.doubleValue() == 9.5D);
    assertTrue(avgf.doubleValue() == 6.5D);
    assertTrue(count.doubleValue() == 4);


    //Reverse the Sort.

    clause = "facet("
        +   "collection1, "
        +   "q=\"*:*\", "
        +   "fl=\"a_s,a_i,a_f\", "
        +   "sort=\"a_s asc\", "
        +   "buckets=\"a_s\", "
        +   "bucketSorts=\"sum(a_i) desc\", "
        +   "bucketSizeLimit=100, "
        +   "sum(a_i), sum(a_f), "
        +   "min(a_i), min(a_f), "
        +   "max(a_i), max(a_f), "
        +   "avg(a_i), avg(a_f), "
        +   "count(*)"
        + ")";

    stream = factory.constructStream(clause);
    tuples = getTuples(stream);


    //Test Long and Double Sums

    tuple = tuples.get(0);
    bucket = tuple.getString("a_s");
    sumi = tuple.getDouble("sum(a_i)");
    sumf = tuple.getDouble("sum(a_f)");
    mini = tuple.getDouble("min(a_i)");
    minf = tuple.getDouble("min(a_f)");
    maxi = tuple.getDouble("max(a_i)");
    maxf = tuple.getDouble("max(a_f)");
    avgi = tuple.getDouble("avg(a_i)");
    avgf = tuple.getDouble("avg(a_f)");
    count = tuple.getDouble("count(*)");

    assertTrue(bucket.equals("hello3"));
    assertTrue(sumi.doubleValue() == 38.0D);
    assertTrue(sumf.doubleValue() == 26.0D);
    assertTrue(mini.doubleValue() == 3.0D);
    assertTrue(minf.doubleValue() == 3.0D);
    assertTrue(maxi.doubleValue() == 13.0D);
    assertTrue(maxf.doubleValue() == 9.0D);
    assertTrue(avgi.doubleValue() == 9.5D);
    assertTrue(avgf.doubleValue() == 6.5D);
    assertTrue(count.doubleValue() == 4);

    tuple = tuples.get(1);
    bucket = tuple.getString("a_s");
    sumi = tuple.getDouble("sum(a_i)");
    sumf = tuple.getDouble("sum(a_f)");
    mini = tuple.getDouble("min(a_i)");
    minf = tuple.getDouble("min(a_f)");
    maxi = tuple.getDouble("max(a_i)");
    maxf = tuple.getDouble("max(a_f)");
    avgi = tuple.getDouble("avg(a_i)");
    avgf = tuple.getDouble("avg(a_f)");
    count = tuple.getDouble("count(*)");

    assertTrue(bucket.equals("hello0"));
    assertTrue(sumi.doubleValue() == 17.0D);
    assertTrue(sumf.doubleValue() == 18.0D);
    assertTrue(mini.doubleValue() == 0.0D);
    assertTrue(minf.doubleValue() == 1.0D);
    assertTrue(maxi.doubleValue() == 14.0D);
    assertTrue(maxf.doubleValue() == 10.0D);
    assertTrue(avgi.doubleValue() == 4.25D);
    assertTrue(avgf.doubleValue() == 4.5D);
    assertTrue(count.doubleValue() == 4);

    tuple = tuples.get(2);
    bucket = tuple.getString("a_s");
    sumi = tuple.getDouble("sum(a_i)");
    sumf = tuple.getDouble("sum(a_f)");
    mini = tuple.getDouble("min(a_i)");
    minf = tuple.getDouble("min(a_f)");
    maxi = tuple.getDouble("max(a_i)");
    maxf = tuple.getDouble("max(a_f)");
    avgi = tuple.getDouble("avg(a_i)");
    avgf = tuple.getDouble("avg(a_f)");
    count = tuple.getDouble("count(*)");

    assertTrue(bucket.equals("hello4"));
    assertTrue(sumi.longValue() == 15);
    assertTrue(sumf.doubleValue() == 11.0D);
    assertTrue(mini.doubleValue() == 4.0D);
    assertTrue(minf.doubleValue() == 4.0D);
    assertTrue(maxi.doubleValue() == 11.0D);
    assertTrue(maxf.doubleValue() == 7.0D);
    assertTrue(avgi.doubleValue() == 7.5D);
    assertTrue(avgf.doubleValue() == 5.5D);
    assertTrue(count.doubleValue() == 2);


    //Test index sort
    clause = "facet("
        +   "collection1, "
        +   "q=\"*:*\", "
        +   "fl=\"a_s,a_i,a_f\", "
        +   "sort=\"a_s asc\", "
        +   "buckets=\"a_s\", "
        +   "bucketSorts=\"a_s desc\", "
        +   "bucketSizeLimit=100, "
        +   "sum(a_i), sum(a_f), "
        +   "min(a_i), min(a_f), "
        +   "max(a_i), max(a_f), "
        +   "avg(a_i), avg(a_f), "
        +   "count(*)"
        + ")";

    stream = factory.constructStream(clause);
    tuples = getTuples(stream);

    assert(tuples.size() == 3);


    tuple = tuples.get(0);
    bucket = tuple.getString("a_s");
    sumi = tuple.getDouble("sum(a_i)");
    sumf = tuple.getDouble("sum(a_f)");
    mini = tuple.getDouble("min(a_i)");
    minf = tuple.getDouble("min(a_f)");
    maxi = tuple.getDouble("max(a_i)");
    maxf = tuple.getDouble("max(a_f)");
    avgi = tuple.getDouble("avg(a_i)");
    avgf = tuple.getDouble("avg(a_f)");
    count = tuple.getDouble("count(*)");


    assertTrue(bucket.equals("hello4"));
    assertTrue(sumi.longValue() == 15);
    assertTrue(sumf.doubleValue() == 11.0D);
    assertTrue(mini.doubleValue() == 4.0D);
    assertTrue(minf.doubleValue() == 4.0D);
    assertTrue(maxi.doubleValue() == 11.0D);
    assertTrue(maxf.doubleValue() == 7.0D);
    assertTrue(avgi.doubleValue() == 7.5D);
    assertTrue(avgf.doubleValue() == 5.5D);
    assertTrue(count.doubleValue() == 2);


    tuple = tuples.get(1);
    bucket = tuple.getString("a_s");
    sumi = tuple.getDouble("sum(a_i)");
    sumf = tuple.getDouble("sum(a_f)");
    mini = tuple.getDouble("min(a_i)");
    minf = tuple.getDouble("min(a_f)");
    maxi = tuple.getDouble("max(a_i)");
    maxf = tuple.getDouble("max(a_f)");
    avgi = tuple.getDouble("avg(a_i)");
    avgf = tuple.getDouble("avg(a_f)");
    count = tuple.getDouble("count(*)");

    assertTrue(bucket.equals("hello3"));
    assertTrue(sumi.doubleValue() == 38.0D);
    assertTrue(sumf.doubleValue() == 26.0D);
    assertTrue(mini.doubleValue() == 3.0D);
    assertTrue(minf.doubleValue() == 3.0D);
    assertTrue(maxi.doubleValue() == 13.0D);
    assertTrue(maxf.doubleValue() == 9.0D);
    assertTrue(avgi.doubleValue() == 9.5D);
    assertTrue(avgf.doubleValue() == 6.5D);
    assertTrue(count.doubleValue() == 4);


    tuple = tuples.get(2);
    bucket = tuple.getString("a_s");
    sumi = tuple.getDouble("sum(a_i)");
    sumf = tuple.getDouble("sum(a_f)");
    mini = tuple.getDouble("min(a_i)");
    minf = tuple.getDouble("min(a_f)");
    maxi = tuple.getDouble("max(a_i)");
    maxf = tuple.getDouble("max(a_f)");
    avgi = tuple.getDouble("avg(a_i)");
    avgf = tuple.getDouble("avg(a_f)");
    count = tuple.getDouble("count(*)");

    assertTrue(bucket.equals("hello0"));
    assertTrue(sumi.doubleValue() == 17.0D);
    assertTrue(sumf.doubleValue() == 18.0D);
    assertTrue(mini.doubleValue() == 0.0D);
    assertTrue(minf.doubleValue() == 1.0D);
    assertTrue(maxi.doubleValue() == 14.0D);
    assertTrue(maxf.doubleValue() == 10.0D);
    assertTrue(avgi.doubleValue() == 4.25D);
    assertTrue(avgf.doubleValue() == 4.5D);
    assertTrue(count.doubleValue() == 4);

    //Test index sort

    clause = "facet("
        +   "collection1, "
        +   "q=\"*:*\", "
        +   "fl=\"a_s,a_i,a_f\", "
        +   "sort=\"a_s asc\", "
        +   "buckets=\"a_s\", "
        +   "bucketSorts=\"a_s asc\", "
        +   "bucketSizeLimit=100, "
        +   "sum(a_i), sum(a_f), "
        +   "min(a_i), min(a_f), "
        +   "max(a_i), max(a_f), "
        +   "avg(a_i), avg(a_f), "
        +   "count(*)"
        + ")";

    stream = factory.constructStream(clause);
    tuples = getTuples(stream);

    assert(tuples.size() == 3);


    tuple = tuples.get(0);
    bucket = tuple.getString("a_s");
    sumi = tuple.getDouble("sum(a_i)");
    sumf = tuple.getDouble("sum(a_f)");
    mini = tuple.getDouble("min(a_i)");
    minf = tuple.getDouble("min(a_f)");
    maxi = tuple.getDouble("max(a_i)");
    maxf = tuple.getDouble("max(a_f)");
    avgi = tuple.getDouble("avg(a_i)");
    avgf = tuple.getDouble("avg(a_f)");
    count = tuple.getDouble("count(*)");

    assertTrue(bucket.equals("hello0"));
    assertTrue(sumi.doubleValue() == 17.0D);
    assertTrue(sumf.doubleValue() == 18.0D);
    assertTrue(mini.doubleValue() == 0.0D);
    assertTrue(minf.doubleValue() == 1.0D);
    assertTrue(maxi.doubleValue() == 14.0D);
    assertTrue(maxf.doubleValue() == 10.0D);
    assertTrue(avgi.doubleValue() == 4.25D);
    assertTrue(avgf.doubleValue() == 4.5D);
    assertTrue(count.doubleValue() == 4);


    tuple = tuples.get(1);
    bucket = tuple.getString("a_s");
    sumi = tuple.getDouble("sum(a_i)");
    sumf = tuple.getDouble("sum(a_f)");
    mini = tuple.getDouble("min(a_i)");
    minf = tuple.getDouble("min(a_f)");
    maxi = tuple.getDouble("max(a_i)");
    maxf = tuple.getDouble("max(a_f)");
    avgi = tuple.getDouble("avg(a_i)");
    avgf = tuple.getDouble("avg(a_f)");
    count = tuple.getDouble("count(*)");

    assertTrue(bucket.equals("hello3"));
    assertTrue(sumi.doubleValue() == 38.0D);
    assertTrue(sumf.doubleValue() == 26.0D);
    assertTrue(mini.doubleValue() == 3.0D);
    assertTrue(minf.doubleValue() == 3.0D);
    assertTrue(maxi.doubleValue() == 13.0D);
    assertTrue(maxf.doubleValue() == 9.0D);
    assertTrue(avgi.doubleValue() == 9.5D);
    assertTrue(avgf.doubleValue() == 6.5D);
    assertTrue(count.doubleValue() == 4);


    tuple = tuples.get(2);
    bucket = tuple.getString("a_s");
    sumi = tuple.getDouble("sum(a_i)");
    sumf = tuple.getDouble("sum(a_f)");
    mini = tuple.getDouble("min(a_i)");
    minf = tuple.getDouble("min(a_f)");
    maxi = tuple.getDouble("max(a_i)");
    maxf = tuple.getDouble("max(a_f)");
    avgi = tuple.getDouble("avg(a_i)");
    avgf = tuple.getDouble("avg(a_f)");
    count = tuple.getDouble("count(*)");

    assertTrue(bucket.equals("hello4"));
    assertTrue(sumi.longValue() == 15);
    assertTrue(sumf.doubleValue() == 11.0D);
    assertTrue(mini.doubleValue() == 4.0D);
    assertTrue(minf.doubleValue() == 4.0D);
    assertTrue(maxi.doubleValue() == 11.0D);
    assertTrue(maxf.doubleValue() == 7.0D);
    assertTrue(avgi.doubleValue() == 7.5D);
    assertTrue(avgf.doubleValue() == 5.5D);
    assertTrue(count.doubleValue() == 2);

    //Test zero result facets
    clause = "facet("
        +   "collection1, "
        +   "q=\"blahhh\", "
        +   "fl=\"a_s,a_i,a_f\", "
        +   "sort=\"a_s asc\", "
        +   "buckets=\"a_s\", "
        +   "bucketSorts=\"a_s asc\", "
        +   "bucketSizeLimit=100, "
        +   "sum(a_i), sum(a_f), "
        +   "min(a_i), min(a_f), "
        +   "max(a_i), max(a_f), "
        +   "avg(a_i), avg(a_f), "
        +   "count(*)"
        + ")";

    stream = factory.constructStream(clause);
    tuples = getTuples(stream);

    assert(tuples.size() == 0);

  }

  @Test
  public void testSubFacetStream() throws Exception {

    new UpdateRequest()
        .add(id, "0", "level1_s", "hello0", "level2_s", "a", "a_i", "0", "a_f", "1")
        .add(id, "2", "level1_s", "hello0", "level2_s", "a", "a_i", "2", "a_f", "2")
        .add(id, "3", "level1_s", "hello3", "level2_s", "a", "a_i", "3", "a_f", "3")
        .add(id, "4", "level1_s", "hello4", "level2_s", "a", "a_i", "4", "a_f", "4")
        .add(id, "1", "level1_s", "hello0", "level2_s", "b", "a_i", "1", "a_f", "5")
        .add(id, "5", "level1_s", "hello3", "level2_s", "b", "a_i", "10", "a_f", "6")
        .add(id, "6", "level1_s", "hello4", "level2_s", "b", "a_i", "11", "a_f", "7")
        .add(id, "7", "level1_s", "hello3", "level2_s", "b", "a_i", "12", "a_f", "8")
        .add(id, "8", "level1_s", "hello3", "level2_s", "b", "a_i", "13", "a_f", "9")
        .add(id, "9", "level1_s", "hello0", "level2_s", "b", "a_i", "14", "a_f", "10")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    String clause;
    TupleStream stream;
    List<Tuple> tuples;
    
    StreamFactory factory = new StreamFactory()
      .withCollectionZkHost("collection1", cluster.getZkServer().getZkAddress())
      .withFunctionName("facet", FacetStream.class)
      .withFunctionName("sum", SumMetric.class)
      .withFunctionName("min", MinMetric.class)
      .withFunctionName("max", MaxMetric.class)
      .withFunctionName("avg", MeanMetric.class)
      .withFunctionName("count", CountMetric.class);
    
    // Basic test
    clause = "facet("
              +   "collection1, "
              +   "q=\"*:*\", "
              +   "buckets=\"level1_s, level2_s\", "
              +   "bucketSorts=\"sum(a_i) desc, sum(a_i) desc)\", "
              +   "bucketSizeLimit=100, "
              +   "sum(a_i), count(*)"
              + ")";
    
    stream = factory.constructStream(clause);
    tuples = getTuples(stream);

    assert(tuples.size() == 6);

    Tuple tuple = tuples.get(0);
    String bucket1 = tuple.getString("level1_s");
    String bucket2 = tuple.getString("level2_s");
    Double sumi = tuple.getDouble("sum(a_i)");
    Double count = tuple.getDouble("count(*)");

    assertTrue(bucket1.equals("hello3"));
    assertTrue(bucket2.equals("b"));
    assertTrue(sumi.longValue() == 35);
    assertTrue(count.doubleValue() == 3);

    tuple = tuples.get(1);
    bucket1 = tuple.getString("level1_s");
    bucket2 = tuple.getString("level2_s");
    sumi = tuple.getDouble("sum(a_i)");
    count = tuple.getDouble("count(*)");

    assertTrue(bucket1.equals("hello0"));
    assertTrue(bucket2.equals("b"));
    assertTrue(sumi.longValue() == 15);
    assertTrue(count.doubleValue() == 2);

    tuple = tuples.get(2);
    bucket1 = tuple.getString("level1_s");
    bucket2 = tuple.getString("level2_s");
    sumi = tuple.getDouble("sum(a_i)");
    count = tuple.getDouble("count(*)");

    assertTrue(bucket1.equals("hello4"));
    assertTrue(bucket2.equals("b"));
    assertTrue(sumi.longValue() == 11);
    assertTrue(count.doubleValue() == 1);

    tuple = tuples.get(3);
    bucket1 = tuple.getString("level1_s");
    bucket2 = tuple.getString("level2_s");
    sumi = tuple.getDouble("sum(a_i)");
    count = tuple.getDouble("count(*)");

    assertTrue(bucket1.equals("hello4"));
    assertTrue(bucket2.equals("a"));
    assertTrue(sumi.longValue() == 4);
    assertTrue(count.doubleValue() == 1);

    tuple = tuples.get(4);
    bucket1 = tuple.getString("level1_s");
    bucket2 = tuple.getString("level2_s");
    sumi = tuple.getDouble("sum(a_i)");
    count = tuple.getDouble("count(*)");

    assertTrue(bucket1.equals("hello3"));
    assertTrue(bucket2.equals("a"));
    assertTrue(sumi.longValue() == 3);
    assertTrue(count.doubleValue() == 1);

    tuple = tuples.get(5);
    bucket1 = tuple.getString("level1_s");
    bucket2 = tuple.getString("level2_s");
    sumi = tuple.getDouble("sum(a_i)");
    count = tuple.getDouble("count(*)");

    assertTrue(bucket1.equals("hello0"));
    assertTrue(bucket2.equals("a"));
    assertTrue(sumi.longValue() == 2);
    assertTrue(count.doubleValue() == 2);

    clause = "facet("
        +   "collection1, "
        +   "q=\"*:*\", "
        +   "buckets=\"level1_s, level2_s\", "
        +   "bucketSorts=\"level1_s desc, level2_s desc)\", "
        +   "bucketSizeLimit=100, "
        +   "sum(a_i), count(*)"
        + ")";

    stream = factory.constructStream(clause);
    tuples = getTuples(stream);

    assert(tuples.size() == 6);

    tuple = tuples.get(0);
    bucket1 = tuple.getString("level1_s");
    bucket2 = tuple.getString("level2_s");
    sumi = tuple.getDouble("sum(a_i)");
    count = tuple.getDouble("count(*)");

    assertTrue(bucket1.equals("hello4"));
    assertTrue(bucket2.equals("b"));
    assertTrue(sumi.longValue() == 11);
    assertTrue(count.doubleValue() == 1);

    tuple = tuples.get(1);
    bucket1 = tuple.getString("level1_s");
    bucket2 = tuple.getString("level2_s");
    sumi = tuple.getDouble("sum(a_i)");
    count = tuple.getDouble("count(*)");

    assertTrue(bucket1.equals("hello4"));
    assertTrue(bucket2.equals("a"));
    assertTrue(sumi.longValue() == 4);
    assertTrue(count.doubleValue() == 1);

    tuple = tuples.get(2);
    bucket1 = tuple.getString("level1_s");
    bucket2 = tuple.getString("level2_s");
    sumi = tuple.getDouble("sum(a_i)");
    count = tuple.getDouble("count(*)");

    assertTrue(bucket1.equals("hello3"));
    assertTrue(bucket2.equals("b"));
    assertTrue(sumi.longValue() == 35);
    assertTrue(count.doubleValue() == 3);

    tuple = tuples.get(3);
    bucket1 = tuple.getString("level1_s");
    bucket2 = tuple.getString("level2_s");
    sumi = tuple.getDouble("sum(a_i)");
    count = tuple.getDouble("count(*)");

    assertTrue(bucket1.equals("hello3"));
    assertTrue(bucket2.equals("a"));
    assertTrue(sumi.longValue() == 3);
    assertTrue(count.doubleValue() == 1);

    tuple = tuples.get(4);
    bucket1 = tuple.getString("level1_s");
    bucket2 = tuple.getString("level2_s");
    sumi = tuple.getDouble("sum(a_i)");
    count = tuple.getDouble("count(*)");

    assertTrue(bucket1.equals("hello0"));
    assertTrue(bucket2.equals("b"));
    assertTrue(sumi.longValue() == 15);
    assertTrue(count.doubleValue() == 2);

    tuple = tuples.get(5);
    bucket1 = tuple.getString("level1_s");
    bucket2 = tuple.getString("level2_s");
    sumi = tuple.getDouble("sum(a_i)");
    count = tuple.getDouble("count(*)");

    assertTrue(bucket1.equals("hello0"));
    assertTrue(bucket2.equals("a"));
    assertTrue(sumi.longValue() == 2);
    assertTrue(count.doubleValue() == 2);
  }

  @Test
  public void testTopicStream() throws Exception {
    Assume.assumeTrue(!useAlias);

    new UpdateRequest()
        .add(id, "0", "a_s", "hello", "a_i", "0", "a_f", "1")
        .add(id, "2", "a_s", "hello", "a_i", "2", "a_f", "2")
        .add(id, "3", "a_s", "hello", "a_i", "3", "a_f", "3")
        .add(id, "4", "a_s", "hello", "a_i", "4", "a_f", "4")
        .add(id, "1", "a_s", "hello", "a_i", "1", "a_f", "5")
        .add(id, "5", "a_s", "hello", "a_i", "10", "a_f", "6")
        .add(id, "6", "a_s", "hello", "a_i", "11", "a_f", "7")
        .add(id, "7", "a_s", "hello", "a_i", "12", "a_f", "8")
        .add(id, "8", "a_s", "hello", "a_i", "13", "a_f", "9")
        .add(id, "9", "a_s", "hello", "a_i", "14", "a_f", "10")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    StreamFactory factory = new StreamFactory()
        .withCollectionZkHost("collection1", cluster.getZkServer().getZkAddress())
        .withFunctionName("topic", TopicStream.class)
        .withFunctionName("search", CloudSolrStream.class)
        .withFunctionName("daemon", DaemonStream.class);

    StreamExpression expression;
    TupleStream stream;
    List<Tuple> tuples;

    SolrClientCache cache = new SolrClientCache();

    try {
      //Store checkpoints in the same index as the main documents. This perfectly valid
      expression = StreamExpressionParser.parse("topic(collection1, collection1, q=\"a_s:hello\", fl=\"id\", id=\"1000000\", checkpointEvery=3)");

      stream = factory.constructStream(expression);
      StreamContext context = new StreamContext();
      context.setSolrClientCache(cache);
      stream.setStreamContext(context);
      tuples = getTuples(stream);

      //Should be zero because the checkpoints will be set to the highest vesion on the shards.
      assertEquals(tuples.size(), 0);

      cluster.getSolrClient().commit("collection1");
      //Now check to see if the checkpoints are present

              expression = StreamExpressionParser.parse("search(collection1, q=\"id:1000000\", fl=\"id, checkpoint_ss, _version_\", sort=\"id asc\")");
              stream = factory.constructStream(expression);
              context = new StreamContext();
              context.setSolrClientCache(cache);
              stream.setStreamContext(context);
              tuples = getTuples(stream);
              assertEquals(tuples.size(), 1);
              List<String> checkpoints = tuples.get(0).getStrings("checkpoint_ss");
              assertEquals(checkpoints.size(), 2);
              Long version1 = tuples.get(0).getLong("_version_");

      //Index a few more documents
      new UpdateRequest()
          .add(id, "10", "a_s", "hello", "a_i", "13", "a_f", "9")
          .add(id, "11", "a_s", "hello", "a_i", "14", "a_f", "10")
          .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

      expression = StreamExpressionParser.parse("topic(collection1, collection1, fl=\"id\", q=\"a_s:hello\", id=\"1000000\", checkpointEvery=2)");

      stream = factory.constructStream(expression);
      context = new StreamContext();
      context.setSolrClientCache(cache);
      stream.setStreamContext(context);

      try {
        stream.open();
        Tuple tuple1 = stream.read();
        assertEquals((long) tuple1.getLong("id"), 10l);
        cluster.getSolrClient().commit("collection1");

                // Checkpoint should not have changed.
                expression = StreamExpressionParser.parse("search(collection1, q=\"id:1000000\", fl=\"id, checkpoint_ss, _version_\", sort=\"id asc\")");
                TupleStream cstream = factory.constructStream(expression);
                context = new StreamContext();
                context.setSolrClientCache(cache);
                cstream.setStreamContext(context);
                tuples = getTuples(cstream);

                assertEquals(tuples.size(), 1);
                checkpoints = tuples.get(0).getStrings("checkpoint_ss");
                assertEquals(checkpoints.size(), 2);
                Long version2 = tuples.get(0).getLong("_version_");
                assertEquals(version1, version2);

        Tuple tuple2 = stream.read();
        cluster.getSolrClient().commit("collection1");
        assertEquals((long) tuple2.getLong("id"), 11l);

                //Checkpoint should have changed.
                expression = StreamExpressionParser.parse("search(collection1, q=\"id:1000000\", fl=\"id, checkpoint_ss, _version_\", sort=\"id asc\")");
                cstream = factory.constructStream(expression);
                context = new StreamContext();
                context.setSolrClientCache(cache);
                cstream.setStreamContext(context);
                tuples = getTuples(cstream);

                assertEquals(tuples.size(), 1);
                checkpoints = tuples.get(0).getStrings("checkpoint_ss");
                assertEquals(checkpoints.size(), 2);
                Long version3 = tuples.get(0).getLong("_version_");
                assertTrue(version3 > version2);

        Tuple tuple3 = stream.read();
        assertTrue(tuple3.EOF);
      } finally {
        stream.close();
      }

      //Test with the DaemonStream

      DaemonStream dstream = null;
      try {
        expression = StreamExpressionParser.parse("daemon(topic(collection1, collection1, fl=\"id\", q=\"a_s:hello\", id=\"1000000\", checkpointEvery=2), id=\"test\", runInterval=\"1000\", queueSize=\"9\")");
        dstream = (DaemonStream) factory.constructStream(expression);
        context = new StreamContext();
        context.setSolrClientCache(cache);
        dstream.setStreamContext(context);

        //Index a few more documents
        new UpdateRequest()
            .add(id, "12", "a_s", "hello", "a_i", "13", "a_f", "9")
            .add(id, "13", "a_s", "hello", "a_i", "14", "a_f", "10")
            .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

        //Start reading from the DaemonStream
        Tuple tuple = null;

        dstream.open();
        tuple = dstream.read();
        assertEquals(12, (long) tuple.getLong(id));
        tuple = dstream.read();
        assertEquals(13, (long) tuple.getLong(id));
        cluster.getSolrClient().commit("collection1"); // We want to see if the version has been updated after reading two tuples

        //Index a few more documents
        new UpdateRequest()
            .add(id, "14", "a_s", "hello", "a_i", "13", "a_f", "9")
            .add(id, "15", "a_s", "hello", "a_i", "14", "a_f", "10")
            .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

        //Read from the same DaemonStream stream

        tuple = dstream.read();
        assertEquals(14, (long) tuple.getLong(id));
        tuple = dstream.read(); // This should trigger a checkpoint as it's the 4th read from the stream.
        assertEquals(15, (long) tuple.getLong(id));

        dstream.shutdown();
        tuple = dstream.read();
        assertTrue(tuple.EOF);
      } finally {
        dstream.close();
      }
    } finally {
      cache.close();
    }
  }

  @Test
  public void testParallelTopicStream() throws Exception {

    Assume.assumeTrue(!useAlias);

    new UpdateRequest()
        .add(id, "0", "a_s", "hello", "a_i", "0", "a_f", "1", "subject", "ha ha bla blah0")
        .add(id, "2", "a_s", "hello", "a_i", "2", "a_f", "2", "subject", "ha ha bla blah2")
        .add(id, "3", "a_s", "hello", "a_i", "3", "a_f", "3", "subject", "ha ha bla blah3")
        .add(id, "4", "a_s", "hello", "a_i", "4", "a_f", "4", "subject", "ha ha bla blah4")
        .add(id, "1", "a_s", "hello", "a_i", "1", "a_f", "5", "subject", "ha ha bla blah5")
        .add(id, "5", "a_s", "hello", "a_i", "10", "a_f", "6","subject", "ha ha bla blah6")
        .add(id, "6", "a_s", "hello", "a_i", "11", "a_f", "7","subject", "ha ha bla blah7")
        .add(id, "7", "a_s", "hello", "a_i", "12", "a_f", "8", "subject", "ha ha bla blah8")
        .add(id, "8", "a_s", "hello", "a_i", "13", "a_f", "9", "subject", "ha ha bla blah9")
        .add(id, "9", "a_s", "hello", "a_i", "14", "a_f", "10", "subject", "ha ha bla blah10")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    StreamFactory factory = new StreamFactory()
        .withCollectionZkHost("collection1", cluster.getZkServer().getZkAddress())
        .withFunctionName("topic", TopicStream.class)
        .withFunctionName("search", CloudSolrStream.class)
        .withFunctionName("parallel", ParallelStream.class)
        .withFunctionName("daemon", DaemonStream.class);

    StreamExpression expression;
    TupleStream stream;
    List<Tuple> tuples;

    SolrClientCache cache = new SolrClientCache();

    try {
      //Store checkpoints in the same index as the main documents. This is perfectly valid
      expression = StreamExpressionParser.parse("parallel(collection1, " +
                                                         "workers=\"2\", " +
                                                         "sort=\"_version_ asc\"," +
                                                         "topic(collection1, " +
                                                               "collection1, " +
                                                               "q=\"a_s:hello\", " +
                                                               "fl=\"id\", " +
                                                               "id=\"1000000\", " +
                                                               "partitionKeys=\"id\"))");

      stream = factory.constructStream(expression);
      StreamContext context = new StreamContext();
      context.setSolrClientCache(cache);
      stream.setStreamContext(context);
      tuples = getTuples(stream);

      //Should be zero because the checkpoints will be set to the highest version on the shards.
      assertEquals(tuples.size(), 0);

      cluster.getSolrClient().commit("collection1");
      //Now check to see if the checkpoints are present

      expression = StreamExpressionParser.parse("search(collection1, q=\"id:1000000*\", fl=\"id, checkpoint_ss, _version_\", sort=\"id asc\")");

      stream = factory.constructStream(expression);
      context = new StreamContext();
      context.setSolrClientCache(cache);
      stream.setStreamContext(context);
      tuples = getTuples(stream);
      assertEquals(tuples.size(), 2);
      List<String> checkpoints = tuples.get(0).getStrings("checkpoint_ss");
      assertEquals(checkpoints.size(), 2);
      String id1 = tuples.get(0).getString("id");
      String id2 = tuples.get(1).getString("id");
      assertTrue(id1.equals("1000000_0"));
      assertTrue(id2.equals("1000000_1"));

      //Index a few more documents
      new UpdateRequest()
          .add(id, "10", "a_s", "hello", "a_i", "13", "a_f", "9")
          .add(id, "11", "a_s", "hello", "a_i", "14", "a_f", "10")
          .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

      expression = StreamExpressionParser.parse("parallel(collection1, " +
          "workers=\"2\", " +
          "sort=\"_version_ asc\"," +
          "topic(collection1, " +
          "collection1, " +
          "q=\"a_s:hello\", " +
          "fl=\"id\", " +
          "id=\"1000000\", " +
          "partitionKeys=\"id\"))");

      stream = factory.constructStream(expression);
      context = new StreamContext();
      context.setSolrClientCache(cache);
      stream.setStreamContext(context);

      assertTopicRun(stream, "10", "11");

      //Test will initial checkpoint. This should pull all

      expression = StreamExpressionParser.parse("parallel(collection1, " +
          "workers=\"2\", " +
          "sort=\"_version_ asc\"," +
          "topic(collection1, " +
          "collection1, " +
          "q=\"a_s:hello\", " +
          "fl=\"id\", " +
          "id=\"2000000\", " +
          "initialCheckpoint=\"0\", " +
          "partitionKeys=\"id\"))");

      stream = factory.constructStream(expression);
      context = new StreamContext();
      context.setSolrClientCache(cache);
      stream.setStreamContext(context);
      assertTopicRun(stream, "0","1","2","3","4","5","6","7","8","9","10","11");

      //Add more documents
      //Index a few more documents
      new UpdateRequest()
          .add(id, "12", "a_s", "hello", "a_i", "13", "a_f", "9")
          .add(id, "13", "a_s", "hello", "a_i", "14", "a_f", "10")
          .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

      //Run the same topic again including the initialCheckpoint. It should start where it left off.
      //initialCheckpoint should be ignored for all but the first run.
      stream = factory.constructStream(expression);
      context = new StreamContext();
      context.setSolrClientCache(cache);
      stream.setStreamContext(context);
      assertTopicRun(stream, "12","13");

      //Test text extraction

      expression = StreamExpressionParser.parse("parallel(collection1, " +
          "workers=\"2\", " +
          "sort=\"_version_ asc\"," +
          "topic(collection1, " +
          "collection1, " +
          "q=\"subject:bla\", " +
          "fl=\"subject\", " +
          "id=\"3000000\", " +
          "initialCheckpoint=\"0\", " +
          "partitionKeys=\"id\"))");

      stream = factory.constructStream(expression);
      context = new StreamContext();
      context.setSolrClientCache(cache);
      stream.setStreamContext(context);

      assertTopicSubject(stream, "ha ha bla blah0",
          "ha ha bla blah1",
          "ha ha bla blah2",
          "ha ha bla blah3",
          "ha ha bla blah4",
          "ha ha bla blah5",
          "ha ha bla blah6",
          "ha ha bla blah7",
          "ha ha bla blah8",
          "ha ha bla blah9",
          "ha ha bla blah10");

    } finally {
      cache.close();
    }
  }



  @Test
  public void testUpdateStream() throws Exception {

    CollectionAdminRequest.createCollection("destinationCollection", "conf", 2, 1).process(cluster.getSolrClient());
    AbstractDistribZkTestBase.waitForRecoveriesToFinish("destinationCollection", cluster.getSolrClient().getZkStateReader(),
        false, true, TIMEOUT);

    new UpdateRequest()
        .add(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "0", "s_multi", "aaaa",  "s_multi", "bbbb",  "i_multi", "4", "i_multi", "7")
        .add(id, "2", "a_s", "hello2", "a_i", "2", "a_f", "0", "s_multi", "aaaa1", "s_multi", "bbbb1", "i_multi", "44", "i_multi", "77")
        .add(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3", "s_multi", "aaaa2", "s_multi", "bbbb2", "i_multi", "444", "i_multi", "777")
        .add(id, "4", "a_s", "hello4", "a_i", "4", "a_f", "4", "s_multi", "aaaa3", "s_multi", "bbbb3", "i_multi", "4444", "i_multi", "7777")
        .add(id, "1", "a_s", "hello1", "a_i", "1", "a_f", "1", "s_multi", "aaaa4", "s_multi", "bbbb4", "i_multi", "44444", "i_multi", "77777")
        .commit(cluster.getSolrClient(), "collection1");
    
    StreamExpression expression;
    TupleStream stream;
    Tuple t;
    
    StreamFactory factory = new StreamFactory()
      .withCollectionZkHost("collection1", cluster.getZkServer().getZkAddress())
      .withCollectionZkHost("destinationCollection", cluster.getZkServer().getZkAddress())
      .withFunctionName("search", CloudSolrStream.class)
      .withFunctionName("update", UpdateStream.class);
    
    //Copy all docs to destinationCollection
    expression = StreamExpressionParser.parse("update(destinationCollection, batchSize=5, search(collection1, q=*:*, fl=\"id,a_s,a_i,a_f,s_multi,i_multi\", sort=\"a_f asc, a_i asc\"))");
    stream = new UpdateStream(expression, factory);
    List<Tuple> tuples = getTuples(stream);
    cluster.getSolrClient().commit("destinationCollection");
    
    //Ensure that all UpdateStream tuples indicate the correct number of copied/indexed docs
    assert(tuples.size() == 1);
    t = tuples.get(0);
    assert(t.EOF == false);
    assertEquals(5, t.get("batchIndexed"));
    
    //Ensure that destinationCollection actually has the new docs.
    expression = StreamExpressionParser.parse("search(destinationCollection, q=*:*, fl=\"id,a_s,a_i,a_f,s_multi,i_multi\", sort=\"a_i asc\")");
    stream = new CloudSolrStream(expression, factory);
    tuples = getTuples(stream);
    assertEquals(5, tuples.size());

    Tuple tuple = tuples.get(0);
    assert(tuple.getLong("id") == 0);
    assert(tuple.get("a_s").equals("hello0"));
    assert(tuple.getLong("a_i") == 0);
    assert(tuple.getDouble("a_f") == 0.0);
    assertList(tuple.getStrings("s_multi"), "aaaa", "bbbb");
    assertList(tuple.getLongs("i_multi"), Long.parseLong("4"), Long.parseLong("7"));

    tuple = tuples.get(1);
    assert(tuple.getLong("id") == 1);
    assert(tuple.get("a_s").equals("hello1"));
    assert(tuple.getLong("a_i") == 1);
    assert(tuple.getDouble("a_f") == 1.0);
    assertList(tuple.getStrings("s_multi"), "aaaa4", "bbbb4");
    assertList(tuple.getLongs("i_multi"), Long.parseLong("44444"), Long.parseLong("77777"));

    tuple = tuples.get(2);
    assert(tuple.getLong("id") == 2);
    assert(tuple.get("a_s").equals("hello2"));
    assert(tuple.getLong("a_i") == 2);
    assert(tuple.getDouble("a_f") == 0.0);
    assertList(tuple.getStrings("s_multi"), "aaaa1", "bbbb1");
    assertList(tuple.getLongs("i_multi"), Long.parseLong("44"), Long.parseLong("77"));

    tuple = tuples.get(3);
    assert(tuple.getLong("id") == 3);
    assert(tuple.get("a_s").equals("hello3"));
    assert(tuple.getLong("a_i") == 3);
    assert(tuple.getDouble("a_f") == 3.0);
    assertList(tuple.getStrings("s_multi"), "aaaa2", "bbbb2");
    assertList(tuple.getLongs("i_multi"), Long.parseLong("444"), Long.parseLong("777"));

    tuple = tuples.get(4);
    assert(tuple.getLong("id") == 4);
    assert(tuple.get("a_s").equals("hello4"));
    assert(tuple.getLong("a_i") == 4);
    assert(tuple.getDouble("a_f") == 4.0);
    assertList(tuple.getStrings("s_multi"), "aaaa3", "bbbb3");
    assertList(tuple.getLongs("i_multi"), Long.parseLong("4444"), Long.parseLong("7777"));

    CollectionAdminRequest.deleteCollection("destinationCollection").process(cluster.getSolrClient());
  }

  @Test
  public void testParallelUpdateStream() throws Exception {

    CollectionAdminRequest.createCollection("parallelDestinationCollection", "conf", 2, 1).process(cluster.getSolrClient());
    AbstractDistribZkTestBase.waitForRecoveriesToFinish("parallelDestinationCollection", cluster.getSolrClient().getZkStateReader(),
        false, true, TIMEOUT);

    new UpdateRequest()
        .add(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "0", "s_multi", "aaaa",  "s_multi", "bbbb",  "i_multi", "4", "i_multi", "7")
        .add(id, "2", "a_s", "hello2", "a_i", "2", "a_f", "0", "s_multi", "aaaa1", "s_multi", "bbbb1", "i_multi", "44", "i_multi", "77")
        .add(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3", "s_multi", "aaaa2", "s_multi", "bbbb2", "i_multi", "444", "i_multi", "777")
        .add(id, "4", "a_s", "hello4", "a_i", "4", "a_f", "4", "s_multi", "aaaa3", "s_multi", "bbbb3", "i_multi", "4444", "i_multi", "7777")
        .add(id, "1", "a_s", "hello1", "a_i", "1", "a_f", "1", "s_multi", "aaaa4", "s_multi", "bbbb4", "i_multi", "44444", "i_multi", "77777")
        .commit(cluster.getSolrClient(), "collection1");
    
    StreamExpression expression;
    TupleStream stream;
    Tuple t;
    
    String zkHost = cluster.getZkServer().getZkAddress();
    StreamFactory factory = new StreamFactory()
      .withCollectionZkHost("collection1", cluster.getZkServer().getZkAddress())
      .withCollectionZkHost("parallelDestinationCollection", cluster.getZkServer().getZkAddress())
      .withFunctionName("search", CloudSolrStream.class)
      .withFunctionName("update", UpdateStream.class)
      .withFunctionName("parallel", ParallelStream.class);

    //Copy all docs to destinationCollection
    String updateExpression = "update(parallelDestinationCollection, batchSize=2, search(collection1, q=*:*, fl=\"id,a_s,a_i,a_f,s_multi,i_multi\", sort=\"a_f asc, a_i asc\", partitionKeys=\"a_f\"))";
    TupleStream parallelUpdateStream = factory.constructStream("parallel(collection1, " + updateExpression + ", workers=\"2\", zkHost=\""+zkHost+"\", sort=\"batchNumber asc\")");
    List<Tuple> tuples = getTuples(parallelUpdateStream);
    cluster.getSolrClient().commit("parallelDestinationCollection");
    
    //Ensure that all UpdateStream tuples indicate the correct number of copied/indexed docs
    long count = 0;

    for(Tuple tuple : tuples) {
      count+=tuple.getLong("batchIndexed");
    }

    assert(count == 5);

    //Ensure that destinationCollection actually has the new docs.
    expression = StreamExpressionParser.parse("search(parallelDestinationCollection, q=*:*, fl=\"id,a_s,a_i,a_f,s_multi,i_multi\", sort=\"a_i asc\")");
    stream = new CloudSolrStream(expression, factory);
    tuples = getTuples(stream);
    assertEquals(5, tuples.size());

    Tuple tuple = tuples.get(0);
    assert(tuple.getLong("id") == 0);
    assert(tuple.get("a_s").equals("hello0"));
    assert(tuple.getLong("a_i") == 0);
    assert(tuple.getDouble("a_f") == 0.0);
    assertList(tuple.getStrings("s_multi"), "aaaa", "bbbb");
    assertList(tuple.getLongs("i_multi"), Long.parseLong("4"), Long.parseLong("7"));

    tuple = tuples.get(1);
    assert(tuple.getLong("id") == 1);
    assert(tuple.get("a_s").equals("hello1"));
    assert(tuple.getLong("a_i") == 1);
    assert(tuple.getDouble("a_f") == 1.0);
    assertList(tuple.getStrings("s_multi"), "aaaa4", "bbbb4");
    assertList(tuple.getLongs("i_multi"), Long.parseLong("44444"), Long.parseLong("77777"));

    tuple = tuples.get(2);
    assert(tuple.getLong("id") == 2);
    assert(tuple.get("a_s").equals("hello2"));
    assert(tuple.getLong("a_i") == 2);
    assert(tuple.getDouble("a_f") == 0.0);
    assertList(tuple.getStrings("s_multi"), "aaaa1", "bbbb1");
    assertList(tuple.getLongs("i_multi"), Long.parseLong("44"), Long.parseLong("77"));

    tuple = tuples.get(3);
    assert(tuple.getLong("id") == 3);
    assert(tuple.get("a_s").equals("hello3"));
    assert(tuple.getLong("a_i") == 3);
    assert(tuple.getDouble("a_f") == 3.0);
    assertList(tuple.getStrings("s_multi"), "aaaa2", "bbbb2");
    assertList(tuple.getLongs("i_multi"), Long.parseLong("444"), Long.parseLong("777"));

    tuple = tuples.get(4);
    assert(tuple.getLong("id") == 4);
    assert(tuple.get("a_s").equals("hello4"));
    assert(tuple.getLong("a_i") == 4);
    assert(tuple.getDouble("a_f") == 4.0);
    assertList(tuple.getStrings("s_multi"), "aaaa3", "bbbb3");
    assertList(tuple.getLongs("i_multi"), Long.parseLong("4444"), Long.parseLong("7777"));

    CollectionAdminRequest.deleteCollection("parallelDestinationCollection").process(cluster.getSolrClient());
  }

  @Test
  public void testParallelDaemonUpdateStream() throws Exception {

    CollectionAdminRequest.createCollection("parallelDestinationCollection1", "conf", 2, 1).process(cluster.getSolrClient());
    AbstractDistribZkTestBase.waitForRecoveriesToFinish("parallelDestinationCollection1", cluster.getSolrClient().getZkStateReader(),
        false, true, TIMEOUT);

    new UpdateRequest()
        .add(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "0", "s_multi", "aaaa",  "s_multi", "bbbb",  "i_multi", "4", "i_multi", "7")
        .add(id, "2", "a_s", "hello2", "a_i", "2", "a_f", "0", "s_multi", "aaaa1", "s_multi", "bbbb1", "i_multi", "44", "i_multi", "77")
        .add(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3", "s_multi", "aaaa2", "s_multi", "bbbb2", "i_multi", "444", "i_multi", "777")
        .add(id, "4", "a_s", "hello4", "a_i", "4", "a_f", "4", "s_multi", "aaaa3", "s_multi", "bbbb3", "i_multi", "4444", "i_multi", "7777")
        .add(id, "1", "a_s", "hello1", "a_i", "1", "a_f", "1", "s_multi", "aaaa4", "s_multi", "bbbb4", "i_multi", "44444", "i_multi", "77777")
        .commit(cluster.getSolrClient(), "collection1");

    StreamExpression expression;
    TupleStream stream;
    Tuple t;

    String zkHost = cluster.getZkServer().getZkAddress();
    StreamFactory factory = new StreamFactory()
        .withCollectionZkHost("collection1", cluster.getZkServer().getZkAddress())
        .withCollectionZkHost("parallelDestinationCollection1", cluster.getZkServer().getZkAddress())
        .withFunctionName("search", CloudSolrStream.class)
        .withFunctionName("update", UpdateStream.class)
        .withFunctionName("parallel", ParallelStream.class)
        .withFunctionName("daemon", DaemonStream.class);

    //Copy all docs to destinationCollection
    String updateExpression = "daemon(update(parallelDestinationCollection1, batchSize=2, search(collection1, q=*:*, fl=\"id,a_s,a_i,a_f,s_multi,i_multi\", sort=\"a_f asc, a_i asc\", partitionKeys=\"a_f\")), runInterval=\"1000\", id=\"test\")";
    TupleStream parallelUpdateStream = factory.constructStream("parallel(collection1, " + updateExpression + ", workers=\"2\", zkHost=\""+zkHost+"\", sort=\"batchNumber asc\")");
    List<Tuple> tuples = getTuples(parallelUpdateStream);
    assert(tuples.size() == 2);

    //Lets sleep long enough for daemon updates to run.
    //Lets stop the daemons
    ModifiableSolrParams sParams = new ModifiableSolrParams(StreamingTest.mapParams(CommonParams.QT, "/stream", "action", "list"));

    int workersComplete = 0;
    for(JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      int iterations = 0;
      INNER:
      while(iterations == 0) {
        SolrStream solrStream = new SolrStream(jetty.getBaseUrl().toString() + "/collection1", sParams);
        solrStream.open();
        Tuple tupleResponse = solrStream.read();
        if (tupleResponse.EOF) {
          solrStream.close();
          break INNER;
        } else {
          long l = tupleResponse.getLong("iterations");
          if(l > 0) {
            ++workersComplete;
          } else {
            try {
              Thread.sleep(1000);
            } catch(Exception e) {

            }
          }
          iterations = (int) l;
          solrStream.close();
        }
      }
    }

    assertEquals(cluster.getJettySolrRunners().size(), workersComplete);

    cluster.getSolrClient().commit("parallelDestinationCollection1");

    //Lets stop the daemons
    sParams = new ModifiableSolrParams();
    sParams.set(CommonParams.QT, "/stream");
    sParams.set("action", "stop");
    sParams.set("id", "test");
    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      SolrStream solrStream = new SolrStream(jetty.getBaseUrl() + "/collection1", sParams);
      solrStream.open();
      Tuple tupleResponse = solrStream.read();
      solrStream.close();
    }

    sParams = new ModifiableSolrParams();
    sParams.set(CommonParams.QT, "/stream");
    sParams.set("action", "list");

    workersComplete = 0;
    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      long stopTime = 0;
      INNER:
      while(stopTime == 0) {
        SolrStream solrStream = new SolrStream(jetty.getBaseUrl() + "/collection1", sParams);
        solrStream.open();
        Tuple tupleResponse = solrStream.read();
        if (tupleResponse.EOF) {
          solrStream.close();
          break INNER;
        } else {
          stopTime = tupleResponse.getLong("stopTime");
          if (stopTime > 0) {
            ++workersComplete;
          } else {
            try {
              Thread.sleep(1000);
            } catch(Exception e) {

            }
          }
          solrStream.close();
        }
      }
    }

    assertEquals(cluster.getJettySolrRunners().size(), workersComplete);
    //Ensure that destinationCollection actually has the new docs.
    expression = StreamExpressionParser.parse("search(parallelDestinationCollection1, q=*:*, fl=\"id,a_s,a_i,a_f,s_multi,i_multi\", sort=\"a_i asc\")");
    stream = new CloudSolrStream(expression, factory);
    tuples = getTuples(stream);
    assertEquals(5, tuples.size());

    Tuple tuple = tuples.get(0);
    assert(tuple.getLong("id") == 0);
    assert(tuple.get("a_s").equals("hello0"));
    assert(tuple.getLong("a_i") == 0);
    assert(tuple.getDouble("a_f") == 0.0);
    assertList(tuple.getStrings("s_multi"), "aaaa", "bbbb");
    assertList(tuple.getLongs("i_multi"), Long.parseLong("4"), Long.parseLong("7"));

    tuple = tuples.get(1);
    assert(tuple.getLong("id") == 1);
    assert(tuple.get("a_s").equals("hello1"));
    assert(tuple.getLong("a_i") == 1);
    assert(tuple.getDouble("a_f") == 1.0);
    assertList(tuple.getStrings("s_multi"), "aaaa4", "bbbb4");
    assertList(tuple.getLongs("i_multi"), Long.parseLong("44444"), Long.parseLong("77777"));

    tuple = tuples.get(2);
    assert(tuple.getLong("id") == 2);
    assert(tuple.get("a_s").equals("hello2"));
    assert(tuple.getLong("a_i") == 2);
    assert(tuple.getDouble("a_f") == 0.0);
    assertList(tuple.getStrings("s_multi"), "aaaa1", "bbbb1");
    assertList(tuple.getLongs("i_multi"), Long.parseLong("44"), Long.parseLong("77"));

    tuple = tuples.get(3);
    assert(tuple.getLong("id") == 3);
    assert(tuple.get("a_s").equals("hello3"));
    assert(tuple.getLong("a_i") == 3);
    assert(tuple.getDouble("a_f") == 3.0);
    assertList(tuple.getStrings("s_multi"), "aaaa2", "bbbb2");
    assertList(tuple.getLongs("i_multi"), Long.parseLong("444"), Long.parseLong("777"));

    tuple = tuples.get(4);
    assert(tuple.getLong("id") == 4);
    assert(tuple.get("a_s").equals("hello4"));
    assert(tuple.getLong("a_i") == 4);
    assert(tuple.getDouble("a_f") == 4.0);
    assertList(tuple.getStrings("s_multi"), "aaaa3", "bbbb3");
    assertList(tuple.getLongs("i_multi"), Long.parseLong("4444"), Long.parseLong("7777"));

    CollectionAdminRequest.deleteCollection("parallelDestinationCollection1").process(cluster.getSolrClient());
  }


  @Test
  public void testParallelTerminatingDaemonUpdateStream() throws Exception {
    Assume.assumeTrue(!useAlias);

    CollectionAdminRequest.createCollection("parallelDestinationCollection1", "conf", 2, 1).process(cluster.getSolrClient());
    AbstractDistribZkTestBase.waitForRecoveriesToFinish("parallelDestinationCollection1", cluster.getSolrClient().getZkStateReader(),
        false, true, TIMEOUT);

    new UpdateRequest()
        .add(id, "0", "a_s", "hello", "a_i", "0", "a_f", "0", "s_multi", "aaaa",  "s_multi", "bbbb",  "i_multi", "4", "i_multi", "7")
        .add(id, "2", "a_s", "hello", "a_i", "2", "a_f", "0", "s_multi", "aaaa1", "s_multi", "bbbb1", "i_multi", "44", "i_multi", "77")
        .add(id, "3", "a_s", "hello", "a_i", "3", "a_f", "3", "s_multi", "aaaa2", "s_multi", "bbbb2", "i_multi", "444", "i_multi", "777")
        .add(id, "4", "a_s", "hello", "a_i", "4", "a_f", "4", "s_multi", "aaaa3", "s_multi", "bbbb3", "i_multi", "4444", "i_multi", "7777")
        .add(id, "1", "a_s", "hello", "a_i", "1", "a_f", "1", "s_multi", "aaaa4", "s_multi", "bbbb4", "i_multi", "44444", "i_multi", "77777")
        .commit(cluster.getSolrClient(), "collection1");

    StreamExpression expression;
    TupleStream stream;
    Tuple t;

    String zkHost = cluster.getZkServer().getZkAddress();
    StreamFactory factory = new StreamFactory()
        .withCollectionZkHost("collection1", cluster.getZkServer().getZkAddress())
        .withCollectionZkHost("parallelDestinationCollection1", cluster.getZkServer().getZkAddress())
        .withFunctionName("topic", TopicStream.class)
        .withFunctionName("update", UpdateStream.class)
        .withFunctionName("parallel", ParallelStream.class)
        .withFunctionName("daemon", DaemonStream.class);

    //Copy all docs to destinationCollection
    String updateExpression = "daemon(update(parallelDestinationCollection1, batchSize=2, topic(collection1, collection1, q=\"a_s:hello\", fl=\"id,a_s,a_i,a_f,s_multi,i_multi\", partitionKeys=\"a_f\", initialCheckpoint=0, id=\"topic1\")), terminate=true, runInterval=\"1000\", id=\"test\")";
    TupleStream parallelUpdateStream = factory.constructStream("parallel(collection1, " + updateExpression + ", workers=\"2\", zkHost=\""+zkHost+"\", sort=\"batchNumber asc\")");
    List<Tuple> tuples = getTuples(parallelUpdateStream);
    assert(tuples.size() == 2);


    ModifiableSolrParams sParams = new ModifiableSolrParams(StreamingTest.mapParams(CommonParams.QT, "/stream", "action", "list"));

    int workersComplete = 0;

    //Daemons should terminate after the topic is completed
    //Loop through all shards and wait for the daemons to be gone from the listing.
    for(JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      INNER:
      while(true) {
        SolrStream solrStream = new SolrStream(jetty.getBaseUrl().toString() + "/collection1", sParams);
        solrStream.open();
        Tuple tupleResponse = solrStream.read();
        if (tupleResponse.EOF) {
          solrStream.close();
          ++workersComplete;
          break INNER;
        } else {
          solrStream.close();
          Thread.sleep(1000);
        }
      }
    }

    assertEquals(cluster.getJettySolrRunners().size(), workersComplete);

    cluster.getSolrClient().commit("parallelDestinationCollection1");

    //Ensure that destinationCollection actually has the new docs.
    expression = StreamExpressionParser.parse("search(parallelDestinationCollection1, q=*:*, fl=\"id,a_s,a_i,a_f,s_multi,i_multi\", sort=\"a_i asc\")");
    stream = new CloudSolrStream(expression, factory);
    tuples = getTuples(stream);
    assertEquals(5, tuples.size());

    Tuple tuple = tuples.get(0);
    assert(tuple.getLong("id") == 0);
    assert(tuple.get("a_s").equals("hello"));
    assert(tuple.getLong("a_i") == 0);
    assert(tuple.getDouble("a_f") == 0.0);
    assertList(tuple.getStrings("s_multi"), "aaaa", "bbbb");
    assertList(tuple.getLongs("i_multi"), Long.parseLong("4"), Long.parseLong("7"));

    tuple = tuples.get(1);
    assert(tuple.getLong("id") == 1);
    assert(tuple.get("a_s").equals("hello"));
    assert(tuple.getLong("a_i") == 1);
    assert(tuple.getDouble("a_f") == 1.0);
    assertList(tuple.getStrings("s_multi"), "aaaa4", "bbbb4");
    assertList(tuple.getLongs("i_multi"), Long.parseLong("44444"), Long.parseLong("77777"));

    tuple = tuples.get(2);
    assert(tuple.getLong("id") == 2);
    assert(tuple.get("a_s").equals("hello"));
    assert(tuple.getLong("a_i") == 2);
    assert(tuple.getDouble("a_f") == 0.0);
    assertList(tuple.getStrings("s_multi"), "aaaa1", "bbbb1");
    assertList(tuple.getLongs("i_multi"), Long.parseLong("44"), Long.parseLong("77"));

    tuple = tuples.get(3);
    assert(tuple.getLong("id") == 3);
    assert(tuple.get("a_s").equals("hello"));
    assert(tuple.getLong("a_i") == 3);
    assert(tuple.getDouble("a_f") == 3.0);
    assertList(tuple.getStrings("s_multi"), "aaaa2", "bbbb2");
    assertList(tuple.getLongs("i_multi"), Long.parseLong("444"), Long.parseLong("777"));

    tuple = tuples.get(4);
    assert(tuple.getLong("id") == 4);
    assert(tuple.get("a_s").equals("hello"));
    assert(tuple.getLong("a_i") == 4);
    assert(tuple.getDouble("a_f") == 4.0);
    assertList(tuple.getStrings("s_multi"), "aaaa3", "bbbb3");
    assertList(tuple.getLongs("i_multi"), Long.parseLong("4444"), Long.parseLong("7777"));

    CollectionAdminRequest.deleteCollection("parallelDestinationCollection1").process(cluster.getSolrClient());
  }



  ////////////////////////////////////////////
  @Test
  public void testCommitStream() throws Exception {

    CollectionAdminRequest.createCollection("destinationCollection", "conf", 2, 1).process(cluster.getSolrClient());
    AbstractDistribZkTestBase.waitForRecoveriesToFinish("destinationCollection", cluster.getSolrClient().getZkStateReader(),
        false, true, TIMEOUT);

    new UpdateRequest()
        .add(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "0", "s_multi", "aaaa",  "s_multi", "bbbb",  "i_multi", "4", "i_multi", "7")
        .add(id, "2", "a_s", "hello2", "a_i", "2", "a_f", "0", "s_multi", "aaaa1", "s_multi", "bbbb1", "i_multi", "44", "i_multi", "77")
        .add(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3", "s_multi", "aaaa2", "s_multi", "bbbb2", "i_multi", "444", "i_multi", "777")
        .add(id, "4", "a_s", "hello4", "a_i", "4", "a_f", "4", "s_multi", "aaaa3", "s_multi", "bbbb3", "i_multi", "4444", "i_multi", "7777")
        .add(id, "1", "a_s", "hello1", "a_i", "1", "a_f", "1", "s_multi", "aaaa4", "s_multi", "bbbb4", "i_multi", "44444", "i_multi", "77777")
        .commit(cluster.getSolrClient(), "collection1");
    
    StreamExpression expression;
    TupleStream stream;
    Tuple t;
    
    StreamFactory factory = new StreamFactory()
      .withCollectionZkHost("collection1", cluster.getZkServer().getZkAddress())
      .withCollectionZkHost("destinationCollection", cluster.getZkServer().getZkAddress())
      .withFunctionName("search", CloudSolrStream.class)
      .withFunctionName("update", UpdateStream.class)
      .withFunctionName("commit", CommitStream.class);
    
    //Copy all docs to destinationCollection
    expression = StreamExpressionParser.parse("commit(destinationCollection, batchSize=2, update(destinationCollection, batchSize=5, search(collection1, q=*:*, fl=\"id,a_s,a_i,a_f,s_multi,i_multi\", sort=\"a_f asc, a_i asc\")))");
    stream = factory.constructStream(expression);
    List<Tuple> tuples = getTuples(stream);
    
    //Ensure that all CommitStream tuples indicate the correct number of copied/indexed docs
    assert(tuples.size() == 1);
    t = tuples.get(0);
    assert(t.EOF == false);
    assertEquals(5, t.get("batchIndexed"));
    
    //Ensure that destinationCollection actually has the new docs.
    expression = StreamExpressionParser.parse("search(destinationCollection, q=*:*, fl=\"id,a_s,a_i,a_f,s_multi,i_multi\", sort=\"a_i asc\")");
    stream = new CloudSolrStream(expression, factory);
    tuples = getTuples(stream);
    assertEquals(5, tuples.size());

    Tuple tuple = tuples.get(0);
    assert(tuple.getLong("id") == 0);
    assert(tuple.get("a_s").equals("hello0"));
    assert(tuple.getLong("a_i") == 0);
    assert(tuple.getDouble("a_f") == 0.0);
    assertList(tuple.getStrings("s_multi"), "aaaa", "bbbb");
    assertList(tuple.getLongs("i_multi"), Long.parseLong("4"), Long.parseLong("7"));

    tuple = tuples.get(1);
    assert(tuple.getLong("id") == 1);
    assert(tuple.get("a_s").equals("hello1"));
    assert(tuple.getLong("a_i") == 1);
    assert(tuple.getDouble("a_f") == 1.0);
    assertList(tuple.getStrings("s_multi"), "aaaa4", "bbbb4");
    assertList(tuple.getLongs("i_multi"), Long.parseLong("44444"), Long.parseLong("77777"));

    tuple = tuples.get(2);
    assert(tuple.getLong("id") == 2);
    assert(tuple.get("a_s").equals("hello2"));
    assert(tuple.getLong("a_i") == 2);
    assert(tuple.getDouble("a_f") == 0.0);
    assertList(tuple.getStrings("s_multi"), "aaaa1", "bbbb1");
    assertList(tuple.getLongs("i_multi"), Long.parseLong("44"), Long.parseLong("77"));

    tuple = tuples.get(3);
    assert(tuple.getLong("id") == 3);
    assert(tuple.get("a_s").equals("hello3"));
    assert(tuple.getLong("a_i") == 3);
    assert(tuple.getDouble("a_f") == 3.0);
    assertList(tuple.getStrings("s_multi"), "aaaa2", "bbbb2");
    assertList(tuple.getLongs("i_multi"), Long.parseLong("444"), Long.parseLong("777"));

    tuple = tuples.get(4);
    assert(tuple.getLong("id") == 4);
    assert(tuple.get("a_s").equals("hello4"));
    assert(tuple.getLong("a_i") == 4);
    assert(tuple.getDouble("a_f") == 4.0);
    assertList(tuple.getStrings("s_multi"), "aaaa3", "bbbb3");
    assertList(tuple.getLongs("i_multi"), Long.parseLong("4444"), Long.parseLong("7777"));

    CollectionAdminRequest.deleteCollection("destinationCollection").process(cluster.getSolrClient());
  }

  @Test
  public void testParallelCommitStream() throws Exception {

    CollectionAdminRequest.createCollection("parallelDestinationCollection", "conf", 2, 1).process(cluster.getSolrClient());
    AbstractDistribZkTestBase.waitForRecoveriesToFinish("parallelDestinationCollection", cluster.getSolrClient().getZkStateReader(),
        false, true, TIMEOUT);

    new UpdateRequest()
        .add(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "0", "s_multi", "aaaa",  "s_multi", "bbbb",  "i_multi", "4", "i_multi", "7")
        .add(id, "2", "a_s", "hello2", "a_i", "2", "a_f", "0", "s_multi", "aaaa1", "s_multi", "bbbb1", "i_multi", "44", "i_multi", "77")
        .add(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3", "s_multi", "aaaa2", "s_multi", "bbbb2", "i_multi", "444", "i_multi", "777")
        .add(id, "4", "a_s", "hello4", "a_i", "4", "a_f", "4", "s_multi", "aaaa3", "s_multi", "bbbb3", "i_multi", "4444", "i_multi", "7777")
        .add(id, "1", "a_s", "hello1", "a_i", "1", "a_f", "1", "s_multi", "aaaa4", "s_multi", "bbbb4", "i_multi", "44444", "i_multi", "77777")
        .commit(cluster.getSolrClient(), "collection1");
    
    StreamExpression expression;
    TupleStream stream;
    Tuple t;
    
    String zkHost = cluster.getZkServer().getZkAddress();
    StreamFactory factory = new StreamFactory()
      .withCollectionZkHost("collection1", cluster.getZkServer().getZkAddress())
      .withCollectionZkHost("parallelDestinationCollection", cluster.getZkServer().getZkAddress())
      .withFunctionName("search", CloudSolrStream.class)
      .withFunctionName("update", UpdateStream.class)
      .withFunctionName("commit", CommitStream.class)
      .withFunctionName("parallel", ParallelStream.class);

    //Copy all docs to destinationCollection
    String updateExpression = "commit(parallelDestinationCollection, batchSize=0, zkHost=\"" + cluster.getZkServer().getZkAddress() + "\", update(parallelDestinationCollection, batchSize=2, search(collection1, q=*:*, fl=\"id,a_s,a_i,a_f,s_multi,i_multi\", sort=\"a_f asc, a_i asc\", partitionKeys=\"a_f\")))";
    TupleStream parallelUpdateStream = factory.constructStream("parallel(collection1, " + updateExpression + ", workers=\"2\", zkHost=\""+zkHost+"\", sort=\"batchNumber asc\")");
    List<Tuple> tuples = getTuples(parallelUpdateStream);
    
    //Ensure that all UpdateStream tuples indicate the correct number of copied/indexed docs
    long count = 0;

    for(Tuple tuple : tuples) {
      count+=tuple.getLong("batchIndexed");
    }

    assert(count == 5);

    //Ensure that destinationCollection actually has the new docs.
    expression = StreamExpressionParser.parse("search(parallelDestinationCollection, q=*:*, fl=\"id,a_s,a_i,a_f,s_multi,i_multi\", sort=\"a_i asc\")");
    stream = new CloudSolrStream(expression, factory);
    tuples = getTuples(stream);
    assertEquals(5, tuples.size());

    Tuple tuple = tuples.get(0);
    assert(tuple.getLong("id") == 0);
    assert(tuple.get("a_s").equals("hello0"));
    assert(tuple.getLong("a_i") == 0);
    assert(tuple.getDouble("a_f") == 0.0);
    assertList(tuple.getStrings("s_multi"), "aaaa", "bbbb");
    assertList(tuple.getLongs("i_multi"), Long.parseLong("4"), Long.parseLong("7"));

    tuple = tuples.get(1);
    assert(tuple.getLong("id") == 1);
    assert(tuple.get("a_s").equals("hello1"));
    assert(tuple.getLong("a_i") == 1);
    assert(tuple.getDouble("a_f") == 1.0);
    assertList(tuple.getStrings("s_multi"), "aaaa4", "bbbb4");
    assertList(tuple.getLongs("i_multi"), Long.parseLong("44444"), Long.parseLong("77777"));

    tuple = tuples.get(2);
    assert(tuple.getLong("id") == 2);
    assert(tuple.get("a_s").equals("hello2"));
    assert(tuple.getLong("a_i") == 2);
    assert(tuple.getDouble("a_f") == 0.0);
    assertList(tuple.getStrings("s_multi"), "aaaa1", "bbbb1");
    assertList(tuple.getLongs("i_multi"), Long.parseLong("44"), Long.parseLong("77"));

    tuple = tuples.get(3);
    assert(tuple.getLong("id") == 3);
    assert(tuple.get("a_s").equals("hello3"));
    assert(tuple.getLong("a_i") == 3);
    assert(tuple.getDouble("a_f") == 3.0);
    assertList(tuple.getStrings("s_multi"), "aaaa2", "bbbb2");
    assertList(tuple.getLongs("i_multi"), Long.parseLong("444"), Long.parseLong("777"));

    tuple = tuples.get(4);
    assert(tuple.getLong("id") == 4);
    assert(tuple.get("a_s").equals("hello4"));
    assert(tuple.getLong("a_i") == 4);
    assert(tuple.getDouble("a_f") == 4.0);
    assertList(tuple.getStrings("s_multi"), "aaaa3", "bbbb3");
    assertList(tuple.getLongs("i_multi"), Long.parseLong("4444"), Long.parseLong("7777"));

    CollectionAdminRequest.deleteCollection("parallelDestinationCollection").process(cluster.getSolrClient());
  }

  @Test
  public void testParallelDaemonCommitStream() throws Exception {

    CollectionAdminRequest.createCollection("parallelDestinationCollection1", "conf", 2, 1).process(cluster.getSolrClient());
    AbstractDistribZkTestBase.waitForRecoveriesToFinish("parallelDestinationCollection1", cluster.getSolrClient().getZkStateReader(),
        false, true, TIMEOUT);

    new UpdateRequest()
        .add(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "0", "s_multi", "aaaa",  "s_multi", "bbbb",  "i_multi", "4", "i_multi", "7")
        .add(id, "2", "a_s", "hello2", "a_i", "2", "a_f", "0", "s_multi", "aaaa1", "s_multi", "bbbb1", "i_multi", "44", "i_multi", "77")
        .add(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3", "s_multi", "aaaa2", "s_multi", "bbbb2", "i_multi", "444", "i_multi", "777")
        .add(id, "4", "a_s", "hello4", "a_i", "4", "a_f", "4", "s_multi", "aaaa3", "s_multi", "bbbb3", "i_multi", "4444", "i_multi", "7777")
        .add(id, "1", "a_s", "hello1", "a_i", "1", "a_f", "1", "s_multi", "aaaa4", "s_multi", "bbbb4", "i_multi", "44444", "i_multi", "77777")
        .commit(cluster.getSolrClient(), "collection1");

    StreamExpression expression;
    TupleStream stream;
    Tuple t;

    String zkHost = cluster.getZkServer().getZkAddress();
    StreamFactory factory = new StreamFactory()
        .withCollectionZkHost("collection1", cluster.getZkServer().getZkAddress())
        .withCollectionZkHost("parallelDestinationCollection1", cluster.getZkServer().getZkAddress())
        .withFunctionName("search", CloudSolrStream.class)
        .withFunctionName("update", UpdateStream.class)
        .withFunctionName("commit", CommitStream.class)
        .withFunctionName("parallel", ParallelStream.class)
        .withFunctionName("daemon", DaemonStream.class);

    //Copy all docs to destinationCollection
    String updateExpression = "daemon(commit(parallelDestinationCollection1, batchSize=0, zkHost=\"" + cluster.getZkServer().getZkAddress() + "\", update(parallelDestinationCollection1, batchSize=2, search(collection1, q=*:*, fl=\"id,a_s,a_i,a_f,s_multi,i_multi\", sort=\"a_f asc, a_i asc\", partitionKeys=\"a_f\"))), runInterval=\"1000\", id=\"test\")";
    TupleStream parallelUpdateStream = factory.constructStream("parallel(collection1, " + updateExpression + ", workers=\"2\", zkHost=\""+zkHost+"\", sort=\"batchNumber asc\")");
    List<Tuple> tuples = getTuples(parallelUpdateStream);
    assert(tuples.size() == 2);

    //Lets sleep long enough for daemon updates to run.
    //Lets stop the daemons
    ModifiableSolrParams sParams = new ModifiableSolrParams(StreamingTest.mapParams(CommonParams.QT, "/stream", "action", "list"));

    int workersComplete = 0;
    for(JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      int iterations = 0;
      INNER:
      while(iterations == 0) {
        SolrStream solrStream = new SolrStream(jetty.getBaseUrl().toString() + "/collection1", sParams);
        solrStream.open();
        Tuple tupleResponse = solrStream.read();
        if (tupleResponse.EOF) {
          solrStream.close();
          break INNER;
        } else {
          long l = tupleResponse.getLong("iterations");
          if(l > 0) {
            ++workersComplete;
          } else {
            try {
              Thread.sleep(1000);
            } catch(Exception e) {
            }
          }
          iterations = (int) l;
          solrStream.close();
        }
      }
    }

    assertEquals(cluster.getJettySolrRunners().size(), workersComplete);

    //Lets stop the daemons
    sParams = new ModifiableSolrParams();
    sParams.set(CommonParams.QT, "/stream");
    sParams.set("action", "stop");
    sParams.set("id", "test");
    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      SolrStream solrStream = new SolrStream(jetty.getBaseUrl() + "/collection1", sParams);
      solrStream.open();
      Tuple tupleResponse = solrStream.read();
      solrStream.close();
    }

    sParams = new ModifiableSolrParams();
    sParams.set(CommonParams.QT, "/stream");
    sParams.set("action", "list");

    workersComplete = 0;
    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      long stopTime = 0;
      INNER:
      while(stopTime == 0) {
        SolrStream solrStream = new SolrStream(jetty.getBaseUrl() + "/collection1", sParams);
        solrStream.open();
        Tuple tupleResponse = solrStream.read();
        if (tupleResponse.EOF) {
          solrStream.close();
          break INNER;
        } else {
          stopTime = tupleResponse.getLong("stopTime");
          if (stopTime > 0) {
            ++workersComplete;
          } else {
            try {
              Thread.sleep(1000);
            } catch(Exception e) {

            }
          }
          solrStream.close();
        }
      }
    }

    assertEquals(cluster.getJettySolrRunners().size(), workersComplete);
    //Ensure that destinationCollection actually has the new docs.
    expression = StreamExpressionParser.parse("search(parallelDestinationCollection1, q=*:*, fl=\"id,a_s,a_i,a_f,s_multi,i_multi\", sort=\"a_i asc\")");
    stream = new CloudSolrStream(expression, factory);
    tuples = getTuples(stream);
    assertEquals(5, tuples.size());

    Tuple tuple = tuples.get(0);
    assert(tuple.getLong("id") == 0);
    assert(tuple.get("a_s").equals("hello0"));
    assert(tuple.getLong("a_i") == 0);
    assert(tuple.getDouble("a_f") == 0.0);
    assertList(tuple.getStrings("s_multi"), "aaaa", "bbbb");
    assertList(tuple.getLongs("i_multi"), Long.parseLong("4"), Long.parseLong("7"));

    tuple = tuples.get(1);
    assert(tuple.getLong("id") == 1);
    assert(tuple.get("a_s").equals("hello1"));
    assert(tuple.getLong("a_i") == 1);
    assert(tuple.getDouble("a_f") == 1.0);
    assertList(tuple.getStrings("s_multi"), "aaaa4", "bbbb4");
    assertList(tuple.getLongs("i_multi"), Long.parseLong("44444"), Long.parseLong("77777"));

    tuple = tuples.get(2);
    assert(tuple.getLong("id") == 2);
    assert(tuple.get("a_s").equals("hello2"));
    assert(tuple.getLong("a_i") == 2);
    assert(tuple.getDouble("a_f") == 0.0);
    assertList(tuple.getStrings("s_multi"), "aaaa1", "bbbb1");
    assertList(tuple.getLongs("i_multi"), Long.parseLong("44"), Long.parseLong("77"));

    tuple = tuples.get(3);
    assert(tuple.getLong("id") == 3);
    assert(tuple.get("a_s").equals("hello3"));
    assert(tuple.getLong("a_i") == 3);
    assert(tuple.getDouble("a_f") == 3.0);
    assertList(tuple.getStrings("s_multi"), "aaaa2", "bbbb2");
    assertList(tuple.getLongs("i_multi"), Long.parseLong("444"), Long.parseLong("777"));

    tuple = tuples.get(4);
    assert(tuple.getLong("id") == 4);
    assert(tuple.get("a_s").equals("hello4"));
    assert(tuple.getLong("a_i") == 4);
    assert(tuple.getDouble("a_f") == 4.0);
    assertList(tuple.getStrings("s_multi"), "aaaa3", "bbbb3");
    assertList(tuple.getLongs("i_multi"), Long.parseLong("4444"), Long.parseLong("7777"));

    CollectionAdminRequest.deleteCollection("parallelDestinationCollection1").process(cluster.getSolrClient());
  }
  ////////////////////////////////////////////  
  
  @Test
  public void testIntersectStream() throws Exception {

    new UpdateRequest()
        .add(id, "0", "a_s", "setA", "a_i", "0")
        .add(id, "2", "a_s", "setA", "a_i", "1")
        .add(id, "3", "a_s", "setA", "a_i", "2")
        .add(id, "4", "a_s", "setA", "a_i", "3")

        .add(id, "5", "a_s", "setB", "a_i", "2")
        .add(id, "6", "a_s", "setB", "a_i", "3")

        .add(id, "7", "a_s", "setAB", "a_i", "0")
        .add(id, "8", "a_s", "setAB", "a_i", "6")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);
    
    StreamExpression expression;
    TupleStream stream;
    List<Tuple> tuples;
    
    StreamFactory factory = new StreamFactory()
      .withCollectionZkHost("collection1", cluster.getZkServer().getZkAddress())
      .withFunctionName("search", CloudSolrStream.class)
      .withFunctionName("intersect", IntersectStream.class);
      
    // basic
    expression = StreamExpressionParser.parse("intersect("
                                              + "search(collection1, q=a_s:(setA || setAB), fl=\"id,a_s,a_i\", sort=\"a_i asc, a_s asc\"),"
                                              + "search(collection1, q=a_s:(setB || setAB), fl=\"id,a_s,a_i\", sort=\"a_i asc\"),"
                                              + "on=\"a_i\")");
    stream = new IntersectStream(expression, factory);
    tuples = getTuples(stream);
    
    assert(tuples.size() == 5);
    assertOrder(tuples, 0, 7, 3, 4, 8);

  }

  @Test
  public void testClassifyStream() throws Exception {
    Assume.assumeTrue(!useAlias);

    CollectionAdminRequest.createCollection("modelCollection", "ml", 2, 1).process(cluster.getSolrClient());
    AbstractDistribZkTestBase.waitForRecoveriesToFinish("modelCollection", cluster.getSolrClient().getZkStateReader(),
        false, true, TIMEOUT);
    CollectionAdminRequest.createCollection("uknownCollection", "ml", 2, 1).process(cluster.getSolrClient());
    AbstractDistribZkTestBase.waitForRecoveriesToFinish("uknownCollection", cluster.getSolrClient().getZkStateReader(),
        false, true, TIMEOUT);
    CollectionAdminRequest.createCollection("checkpointCollection", "ml", 2, 1).process(cluster.getSolrClient());
    AbstractDistribZkTestBase.waitForRecoveriesToFinish("checkpointCollection", cluster.getSolrClient().getZkStateReader(),
        false, true, TIMEOUT);

    UpdateRequest updateRequest = new UpdateRequest();

    for (int i = 0; i < 500; i+=2) {
      updateRequest.add(id, String.valueOf(i), "tv_text", "a b c c d", "out_i", "1");
      updateRequest.add(id, String.valueOf(i+1), "tv_text", "a b e e f", "out_i", "0");
    }

    updateRequest.commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    updateRequest = new UpdateRequest();
    updateRequest.add(id, String.valueOf(0), "text_s", "a b c c d");
    updateRequest.add(id, String.valueOf(1), "text_s", "a b e e f");
    updateRequest.commit(cluster.getSolrClient(), "uknownCollection");

    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString() + "/" + COLLECTIONORALIAS;
    TupleStream updateTrainModelStream;
    ModifiableSolrParams paramsLoc;

    StreamFactory factory = new StreamFactory()
        .withCollectionZkHost("collection1", cluster.getZkServer().getZkAddress())
        .withCollectionZkHost("modelCollection", cluster.getZkServer().getZkAddress())
        .withCollectionZkHost("uknownCollection", cluster.getZkServer().getZkAddress())
        .withFunctionName("features", FeaturesSelectionStream.class)
        .withFunctionName("train", TextLogitStream.class)
        .withFunctionName("search", CloudSolrStream.class)
        .withFunctionName("update", UpdateStream.class);

    // train the model
    String textLogitExpression = "train(" +
        "collection1, " +
        "features(collection1, q=\"*:*\", featureSet=\"first\", field=\"tv_text\", outcome=\"out_i\", numTerms=4),"+
        "q=\"*:*\", " +
        "name=\"model\", " +
        "field=\"tv_text\", " +
        "outcome=\"out_i\", " +
        "maxIterations=100)";
    updateTrainModelStream = factory.constructStream("update(modelCollection, batchSize=5, "+textLogitExpression+")");
    getTuples(updateTrainModelStream);
    cluster.getSolrClient().commit("modelCollection");

    // classify unknown documents
    String expr = "classify(" +
        "model(modelCollection, id=\"model\", cacheMillis=5000)," +
        "topic(checkpointCollection, uknownCollection, q=\"*:*\", fl=\"text_s, id\", id=\"1000000\", initialCheckpoint=\"0\")," +
        "field=\"text_s\"," +
        "analyzerField=\"tv_text\")";

    paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", expr);
    paramsLoc.set("qt", "/stream");
    SolrStream classifyStream = new SolrStream(url, paramsLoc);
    Map<String, Double> idToLabel = getIdToLabel(classifyStream, "probability_d");
    assertEquals(idToLabel.size(), 2);
    assertEquals(1.0, idToLabel.get("0"), 0.001);
    assertEquals(0, idToLabel.get("1"), 0.001);

    // Add more documents and classify it
    updateRequest = new UpdateRequest();
    updateRequest.add(id, String.valueOf(2), "text_s", "a b c c d");
    updateRequest.add(id, String.valueOf(3), "text_s", "a b e e f");
    updateRequest.commit(cluster.getSolrClient(), "uknownCollection");

    classifyStream = new SolrStream(url, paramsLoc);
    idToLabel = getIdToLabel(classifyStream, "probability_d");
    assertEquals(idToLabel.size(), 2);
    assertEquals(1.0, idToLabel.get("2"), 0.001);
    assertEquals(0, idToLabel.get("3"), 0.001);


    // Train another model
    updateRequest = new UpdateRequest();
    updateRequest.deleteByQuery("*:*");
    updateRequest.commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    updateRequest = new UpdateRequest();
    for (int i = 0; i < 500; i+=2) {
      updateRequest.add(id, String.valueOf(i), "tv_text", "a b c c d", "out_i", "0");
      updateRequest.add(id, String.valueOf(i+1), "tv_text", "a b e e f", "out_i", "1");
    }
    updateRequest.commit(cluster.getSolrClient(), COLLECTIONORALIAS);
    updateTrainModelStream = factory.constructStream("update(modelCollection, batchSize=5, "+textLogitExpression+")");
    getTuples(updateTrainModelStream);
    cluster.getSolrClient().commit("modelCollection");

    // Add more documents and classify it
    updateRequest = new UpdateRequest();
    updateRequest.add(id, String.valueOf(4), "text_s", "a b c c d");
    updateRequest.add(id, String.valueOf(5), "text_s", "a b e e f");
    updateRequest.commit(cluster.getSolrClient(), "uknownCollection");

    //Sleep for 5 seconds to let model cache expire
    Thread.sleep(5100);

    classifyStream = new SolrStream(url, paramsLoc);
    idToLabel = getIdToLabel(classifyStream, "probability_d");
    assertEquals(idToLabel.size(), 2);
    assertEquals(0, idToLabel.get("4"), 0.001);
    assertEquals(1.0, idToLabel.get("5"), 0.001);

    //Classify in parallel

    // classify unknown documents

    expr = "parallel(collection1, workers=2, sort=\"_version_ asc\", classify(" +
           "model(modelCollection, id=\"model\")," +
           "topic(checkpointCollection, uknownCollection, q=\"id:(4 5)\", fl=\"text_s, id, _version_\", id=\"2000000\", partitionKeys=\"id\", initialCheckpoint=\"0\")," +
           "field=\"text_s\"," +
           "analyzerField=\"tv_text\"))";

    paramsLoc.set("expr", expr);
    classifyStream = new SolrStream(url, paramsLoc);
    idToLabel = getIdToLabel(classifyStream, "probability_d");
    assertEquals(idToLabel.size(), 2);
    assertEquals(0, idToLabel.get("4"), 0.001);
    assertEquals(1.0, idToLabel.get("5"), 0.001);

    CollectionAdminRequest.deleteCollection("modelCollection").process(cluster.getSolrClient());
    CollectionAdminRequest.deleteCollection("uknownCollection").process(cluster.getSolrClient());
    CollectionAdminRequest.deleteCollection("checkpointCollection").process(cluster.getSolrClient());
  }

  @Test
  public void testExecutorStream() throws Exception {
    CollectionAdminRequest.createCollection("workQueue", "conf", 2, 1).process(cluster.getSolrClient());
    AbstractDistribZkTestBase.waitForRecoveriesToFinish("workQueue", cluster.getSolrClient().getZkStateReader(),
        false, true, TIMEOUT);
    CollectionAdminRequest.createCollection("mainCorpus", "conf", 2, 1).process(cluster.getSolrClient());
    AbstractDistribZkTestBase.waitForRecoveriesToFinish("mainCorpus", cluster.getSolrClient().getZkStateReader(),
        false, true, TIMEOUT);
    CollectionAdminRequest.createCollection("destination", "conf", 2, 1).process(cluster.getSolrClient());
    AbstractDistribZkTestBase.waitForRecoveriesToFinish("destination", cluster.getSolrClient().getZkStateReader(),
        false, true, TIMEOUT);

    UpdateRequest workRequest = new UpdateRequest();
    UpdateRequest dataRequest = new UpdateRequest();


    for (int i = 0; i < 500; i++) {
      workRequest.add(id, String.valueOf(i), "expr_s", "update(destination, batchSize=50, search(mainCorpus, q=id:"+i+", rows=1, sort=\"id asc\", fl=\"id, body_t, field_i\"))");
      dataRequest.add(id, String.valueOf(i), "body_t", "hello world "+i, "field_i", Integer.toString(i));
    }

    workRequest.commit(cluster.getSolrClient(), "workQueue");
    dataRequest.commit(cluster.getSolrClient(), "mainCorpus");

    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString() + "/destination";
    TupleStream executorStream;
    ModifiableSolrParams paramsLoc;

    StreamFactory factory = new StreamFactory()
        .withCollectionZkHost("workQueue", cluster.getZkServer().getZkAddress())
        .withCollectionZkHost("mainCorpus", cluster.getZkServer().getZkAddress())
        .withCollectionZkHost("destination", cluster.getZkServer().getZkAddress())
        .withFunctionName("search", CloudSolrStream.class)
        .withFunctionName("executor", ExecutorStream.class)
        .withFunctionName("update", UpdateStream.class);

    String executorExpression = "executor(threads=3, search(workQueue, q=\"*:*\", fl=\"id, expr_s\", rows=1000, sort=\"id desc\"))";
    executorStream = factory.constructStream(executorExpression);

    StreamContext context = new StreamContext();
    SolrClientCache clientCache = new SolrClientCache();
    context.setSolrClientCache(clientCache);
    executorStream.setStreamContext(context);
    getTuples(executorStream);
    //Destination collection should now contain all the records in the main corpus.
    cluster.getSolrClient().commit("destination");
    paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", "search(destination, q=\"*:*\", fl=\"id, body_t, field_i\", rows=1000, sort=\"field_i asc\")");
    paramsLoc.set("qt","/stream");

    SolrStream solrStream = new SolrStream(url, paramsLoc);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 500);
    for(int i=0; i<500; i++) {
      Tuple tuple = tuples.get(i);
      long ivalue = tuple.getLong("field_i");
      String body = tuple.getString("body_t");
      assertTrue(ivalue == i);
      assertTrue(body.equals("hello world "+i));
    }

    solrStream.close();
    clientCache.close();
    CollectionAdminRequest.deleteCollection("workQueue").process(cluster.getSolrClient());
    CollectionAdminRequest.deleteCollection("mainCorpus").process(cluster.getSolrClient());
    CollectionAdminRequest.deleteCollection("destination").process(cluster.getSolrClient());
  }


  @Test
  public void testParallelExecutorStream() throws Exception {
    CollectionAdminRequest.createCollection("workQueue", "conf", 2, 1).process(cluster.getSolrClient());
    AbstractDistribZkTestBase.waitForRecoveriesToFinish("workQueue", cluster.getSolrClient().getZkStateReader(),
        false, true, TIMEOUT);
    CollectionAdminRequest.createCollection("mainCorpus", "conf", 2, 1).process(cluster.getSolrClient());
    AbstractDistribZkTestBase.waitForRecoveriesToFinish("mainCorpus", cluster.getSolrClient().getZkStateReader(),
        false, true, TIMEOUT);
    CollectionAdminRequest.createCollection("destination", "conf", 2, 1).process(cluster.getSolrClient());
    AbstractDistribZkTestBase.waitForRecoveriesToFinish("destination", cluster.getSolrClient().getZkStateReader(),
        false, true, TIMEOUT);

    UpdateRequest workRequest = new UpdateRequest();
    UpdateRequest dataRequest = new UpdateRequest();


    for (int i = 0; i < 500; i++) {
      workRequest.add(id, String.valueOf(i), "expr_s", "update(destination, batchSize=50, search(mainCorpus, q=id:"+i+", rows=1, sort=\"id asc\", fl=\"id, body_t, field_i\"))");
      dataRequest.add(id, String.valueOf(i), "body_t", "hello world "+i, "field_i", Integer.toString(i));
    }

    workRequest.commit(cluster.getSolrClient(), "workQueue");
    dataRequest.commit(cluster.getSolrClient(), "mainCorpus");

    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString() + "/destination";
    TupleStream executorStream;
    ModifiableSolrParams paramsLoc;

    StreamFactory factory = new StreamFactory()
        .withCollectionZkHost("workQueue", cluster.getZkServer().getZkAddress())
        .withCollectionZkHost("mainCorpus", cluster.getZkServer().getZkAddress())
        .withCollectionZkHost("destination", cluster.getZkServer().getZkAddress())
        .withFunctionName("search", CloudSolrStream.class)
        .withFunctionName("executor", ExecutorStream.class)
        .withFunctionName("parallel", ParallelStream.class)
        .withFunctionName("update", UpdateStream.class);

    String executorExpression = "parallel(workQueue, workers=2, sort=\"EOF asc\", executor(threads=3, queueSize=100, search(workQueue, q=\"*:*\", fl=\"id, expr_s\", rows=1000, partitionKeys=id, sort=\"id desc\")))";
    executorStream = factory.constructStream(executorExpression);

    StreamContext context = new StreamContext();
    SolrClientCache clientCache = new SolrClientCache();
    context.setSolrClientCache(clientCache);
    executorStream.setStreamContext(context);
    getTuples(executorStream);
    //Destination collection should now contain all the records in the main corpus.
    cluster.getSolrClient().commit("destination");
    paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", "search(destination, q=\"*:*\", fl=\"id, body_t, field_i\", rows=1000, sort=\"field_i asc\")");
    paramsLoc.set("qt","/stream");

    SolrStream solrStream = new SolrStream(url, paramsLoc);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 500);
    for(int i=0; i<500; i++) {
      Tuple tuple = tuples.get(i);
      long ivalue = tuple.getLong("field_i");
      String body = tuple.getString("body_t");
      assertTrue(ivalue == i);
      assertTrue(body.equals("hello world " + i));
    }

    solrStream.close();
    clientCache.close();
    CollectionAdminRequest.deleteCollection("workQueue").process(cluster.getSolrClient());
    CollectionAdminRequest.deleteCollection("mainCorpus").process(cluster.getSolrClient());
    CollectionAdminRequest.deleteCollection("destination").process(cluster.getSolrClient());
  }



  private Map<String,Double> getIdToLabel(TupleStream stream, String outField) throws IOException {
    Map<String, Double> idToLabel = new HashMap<>();
    List<Tuple> tuples = getTuples(stream);
    for (Tuple tuple : tuples) {
      idToLabel.put(tuple.getString("id"), tuple.getDouble(outField));
    }
    return idToLabel;
  }


  @Test
  public void testBasicTextLogitStream() throws Exception {
    Assume.assumeTrue(!useAlias);

    CollectionAdminRequest.createCollection("destinationCollection", "ml", 2, 1).process(cluster.getSolrClient());
    AbstractDistribZkTestBase.waitForRecoveriesToFinish("destinationCollection", cluster.getSolrClient().getZkStateReader(),
        false, true, TIMEOUT);

    UpdateRequest updateRequest = new UpdateRequest();
    for (int i = 0; i < 5000; i+=2) {
      updateRequest.add(id, String.valueOf(i), "tv_text", "a b c c d", "out_i", "1");
      updateRequest.add(id, String.valueOf(i+1), "tv_text", "a b e e f", "out_i", "0");
    }
    updateRequest.commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    StreamExpression expression;
    TupleStream stream;
    List<Tuple> tuples;

    StreamFactory factory = new StreamFactory()
        .withCollectionZkHost("collection1", cluster.getZkServer().getZkAddress())
        .withCollectionZkHost("destinationCollection", cluster.getZkServer().getZkAddress())
        .withFunctionName("features", FeaturesSelectionStream.class)
        .withFunctionName("train", TextLogitStream.class)
        .withFunctionName("search", CloudSolrStream.class)
        .withFunctionName("update", UpdateStream.class);

    expression = StreamExpressionParser.parse("features(collection1, q=\"*:*\", featureSet=\"first\", field=\"tv_text\", outcome=\"out_i\", numTerms=4)");
    stream = new FeaturesSelectionStream(expression, factory);
    tuples = getTuples(stream);

    assert(tuples.size() == 4);
    HashSet<String> terms = new HashSet<>();
    for (Tuple tuple : tuples) {
      terms.add((String) tuple.get("term_s"));
    }
    assertTrue(terms.contains("d"));
    assertTrue(terms.contains("c"));
    assertTrue(terms.contains("e"));
    assertTrue(terms.contains("f"));

    String textLogitExpression = "train(" +
        "collection1, " +
        "features(collection1, q=\"*:*\", featureSet=\"first\", field=\"tv_text\", outcome=\"out_i\", numTerms=4),"+
        "q=\"*:*\", " +
        "name=\"model\", " +
        "field=\"tv_text\", " +
        "outcome=\"out_i\", " +
        "maxIterations=100)";
    stream = factory.constructStream(textLogitExpression);
    tuples = getTuples(stream);
    Tuple lastTuple = tuples.get(tuples.size() - 1);
    List<Double> lastWeights = lastTuple.getDoubles("weights_ds");
    Double[] lastWeightsArray = lastWeights.toArray(new Double[lastWeights.size()]);

    // first feature is bias value
    Double[] testRecord = {1.0, 1.17, 0.691, 0.0, 0.0};
    double d = sum(multiply(testRecord, lastWeightsArray));
    double prob = sigmoid(d);
    assertEquals(prob, 1.0, 0.1);

    // first feature is bias value
    Double[] testRecord2 = {1.0, 0.0, 0.0, 1.17, 0.691};
    d = sum(multiply(testRecord2, lastWeightsArray));
    prob = sigmoid(d);
    assertEquals(prob, 0, 0.1);

    stream = factory.constructStream("update(destinationCollection, batchSize=5, "+textLogitExpression+")");
    getTuples(stream);
    cluster.getSolrClient().commit("destinationCollection");

    stream = factory.constructStream("search(destinationCollection, " +
        "q=*:*, " +
        "fl=\"iteration_i,* \", " +
        "rows=100, " +
        "sort=\"iteration_i desc\")");
    tuples = getTuples(stream);
    assertEquals(100, tuples.size());
    Tuple lastModel = tuples.get(0);
    ClassificationEvaluation evaluation = ClassificationEvaluation.create(lastModel.fields);
    assertTrue(evaluation.getF1() >= 1.0);
    assertEquals(Math.log( 5000.0 / (2500 + 1)), lastModel.getDoubles("idfs_ds").get(0), 0.0001);
    // make sure the tuples is retrieved in correct order
    Tuple firstTuple = tuples.get(99);
    assertEquals(1L, (long) firstTuple.getLong("iteration_i"));

    CollectionAdminRequest.deleteCollection("destinationCollection").process(cluster.getSolrClient());
  }

  private double sigmoid(double in) {

    double d = 1.0 / (1+Math.exp(-in));
    return d;
  }

  private double[] multiply(Double[] vec1, Double[] vec2) {
    double[] working = new double[vec1.length];
    for(int i=0; i<vec1.length; i++) {
      working[i]= vec1[i]*vec2[i];
    }

    return working;
  }


  private double sum(double[] vec) {
    double d = 0.0;

    for(double v : vec) {
      d += v;
    }

    return d;
  }


  @Test
  public void testParallelIntersectStream() throws Exception {

    new UpdateRequest()
        .add(id, "0", "a_s", "setA", "a_i", "0")
        .add(id, "2", "a_s", "setA", "a_i", "1")
        .add(id, "3", "a_s", "setA", "a_i", "2")
        .add(id, "4", "a_s", "setA", "a_i", "3")

        .add(id, "5", "a_s", "setB", "a_i", "2")
        .add(id, "6", "a_s", "setB", "a_i", "3")

        .add(id, "7", "a_s", "setAB", "a_i", "0")
        .add(id, "8", "a_s", "setAB", "a_i", "6")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);
    
    StreamFactory streamFactory = new StreamFactory()
      .withCollectionZkHost("collection1", cluster.getZkServer().getZkAddress())
      .withFunctionName("search", CloudSolrStream.class)
      .withFunctionName("intersect", IntersectStream.class)
      .withFunctionName("parallel", ParallelStream.class);
    // basic
    
    String zkHost = cluster.getZkServer().getZkAddress();
    final TupleStream stream = streamFactory.constructStream("parallel("
        + "collection1, "
        + "intersect("
          + "search(collection1, q=a_s:(setA || setAB), fl=\"id,a_s,a_i\", sort=\"a_i asc, a_s asc\", partitionKeys=\"a_i\"),"
          + "search(collection1, q=a_s:(setB || setAB), fl=\"id,a_s,a_i\", sort=\"a_i asc\", partitionKeys=\"a_i\"),"
          + "on=\"a_i\"),"
        + "workers=\"2\", zkHost=\"" + zkHost + "\", sort=\"a_i asc\")");
    final List<Tuple> tuples = getTuples(stream);
    
    assert(tuples.size() == 5);
    assertOrder(tuples, 0,7,3,4,8);

  }

  @Test
  public void testFeaturesSelectionStream() throws Exception {
    Assume.assumeTrue(!useAlias);

    CollectionAdminRequest.createCollection("destinationCollection", "ml", 2, 1).process(cluster.getSolrClient());
    AbstractDistribZkTestBase.waitForRecoveriesToFinish("destinationCollection", cluster.getSolrClient().getZkStateReader(),
        false, true, TIMEOUT);

    UpdateRequest updateRequest = new UpdateRequest();
    for (int i = 0; i < 5000; i+=2) {
      updateRequest.add(id, String.valueOf(i), "whitetok", "a b c d", "out_i", "1");
      updateRequest.add(id, String.valueOf(i+1), "whitetok", "a b e f", "out_i", "0");
    }
    updateRequest.commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    StreamExpression expression;
    TupleStream stream;
    List<Tuple> tuples;

    StreamFactory factory = new StreamFactory()
        .withCollectionZkHost("collection1", cluster.getZkServer().getZkAddress())
        .withCollectionZkHost("destinationCollection", cluster.getZkServer().getZkAddress())
        .withFunctionName("featuresSelection", FeaturesSelectionStream.class)
        .withFunctionName("search", CloudSolrStream.class)
        .withFunctionName("update", UpdateStream.class);

    String featuresExpression = "featuresSelection(collection1, q=\"*:*\", featureSet=\"first\", field=\"whitetok\", outcome=\"out_i\", numTerms=4)";
    // basic
    expression = StreamExpressionParser.parse(featuresExpression);
    stream = new FeaturesSelectionStream(expression, factory);
    tuples = getTuples(stream);

    assert(tuples.size() == 4);

    assertTrue(tuples.get(0).get("term_s").equals("c"));
    assertTrue(tuples.get(1).get("term_s").equals("d"));
    assertTrue(tuples.get(2).get("term_s").equals("e"));
    assertTrue(tuples.get(3).get("term_s").equals("f"));

    // update
    expression = StreamExpressionParser.parse("update(destinationCollection, batchSize=5, "+featuresExpression+")");
    stream = new UpdateStream(expression, factory);
    getTuples(stream);
    cluster.getSolrClient().commit("destinationCollection");

    expression = StreamExpressionParser.parse("search(destinationCollection, q=featureSet_s:first, fl=\"index_i, term_s\", sort=\"index_i asc\")");
    stream = new CloudSolrStream(expression, factory);
    tuples = getTuples(stream);
    assertEquals(4, tuples.size());
    assertTrue(tuples.get(0).get("term_s").equals("c"));
    assertTrue(tuples.get(1).get("term_s").equals("d"));
    assertTrue(tuples.get(2).get("term_s").equals("e"));
    assertTrue(tuples.get(3).get("term_s").equals("f"));

    CollectionAdminRequest.deleteCollection("destinationCollection").process(cluster.getSolrClient());
  }

  @Test
  public void testComplementStream() throws Exception {

    new UpdateRequest()
        .add(id, "0", "a_s", "setA", "a_i", "0")
        .add(id, "2", "a_s", "setA", "a_i", "1")
        .add(id, "3", "a_s", "setA", "a_i", "2")
        .add(id, "4", "a_s", "setA", "a_i", "3")

        .add(id, "5", "a_s", "setB", "a_i", "2")
        .add(id, "6", "a_s", "setB", "a_i", "3")
        .add(id, "9", "a_s", "setB", "a_i", "5")

        .add(id, "7", "a_s", "setAB", "a_i", "0")
        .add(id, "8", "a_s", "setAB", "a_i", "6")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);
    
    StreamExpression expression;
    TupleStream stream;
    List<Tuple> tuples;
    
    StreamFactory factory = new StreamFactory()
      .withCollectionZkHost("collection1", cluster.getZkServer().getZkAddress())
      .withFunctionName("search", CloudSolrStream.class)
      .withFunctionName("complement", ComplementStream.class);
      
    // basic
    expression = StreamExpressionParser.parse("complement("
                                              + "search(collection1, q=a_s:(setA || setAB), fl=\"id,a_s,a_i\", sort=\"a_i asc, a_s asc\"),"
                                              + "search(collection1, q=a_s:(setB || setAB), fl=\"id,a_s,a_i\", sort=\"a_i asc\"),"
                                              + "on=\"a_i\")");
    stream = new ComplementStream(expression, factory);
    tuples = getTuples(stream);
   
    assert(tuples.size() == 1);
    assertOrder(tuples, 2);

  }

  @Test
  public void testParallelComplementStream() throws Exception {

    new UpdateRequest()
        .add(id, "0", "a_s", "setA", "a_i", "0")
        .add(id, "2", "a_s", "setA", "a_i", "1")
        .add(id, "3", "a_s", "setA", "a_i", "2")
        .add(id, "4", "a_s", "setA", "a_i", "3")

        .add(id, "5", "a_s", "setB", "a_i", "2")
        .add(id, "6", "a_s", "setB", "a_i", "3")
        .add(id, "9", "a_s", "setB", "a_i", "5")

        .add(id, "7", "a_s", "setAB", "a_i", "0")
        .add(id, "8", "a_s", "setAB", "a_i", "6")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);
    
    StreamFactory streamFactory = new StreamFactory()
      .withCollectionZkHost("collection1", cluster.getZkServer().getZkAddress())
      .withFunctionName("search", CloudSolrStream.class)
      .withFunctionName("complement", ComplementStream.class)
      .withFunctionName("parallel", ParallelStream.class);
    
    final String zkHost = cluster.getZkServer().getZkAddress();
    final TupleStream stream = streamFactory.constructStream("parallel("
      + "collection1, "
      + "complement("
        + "search(collection1, q=a_s:(setA || setAB), fl=\"id,a_s,a_i\", sort=\"a_i asc, a_s asc\", partitionKeys=\"a_i\"),"
        + "search(collection1, q=a_s:(setB || setAB), fl=\"id,a_s,a_i\", sort=\"a_i asc\", partitionKeys=\"a_i\"),"
        + "on=\"a_i\"),"
      + "workers=\"2\", zkHost=\"" + zkHost + "\", sort=\"a_i asc\")");
    final List<Tuple> tuples = getTuples(stream);
    
    assert(tuples.size() == 1);
    assertOrder(tuples, 2);

  }

  protected List<Tuple> getTuples(TupleStream tupleStream) throws IOException {
    tupleStream.open();
    List<Tuple> tuples = new ArrayList<Tuple>();
    for(Tuple t = tupleStream.read(); !t.EOF; t = tupleStream.read()) {
      tuples.add(t);
    }
    tupleStream.close();
    return tuples;
  }
  protected boolean assertOrder(List<Tuple> tuples, int... ids) throws Exception {
    return assertOrderOf(tuples, "id", ids);
  }
  protected boolean assertOrderOf(List<Tuple> tuples, String fieldName, int... ids) throws Exception {
    int i = 0;
    for(int val : ids) {
      Tuple t = tuples.get(i);
      String tip = t.getString(fieldName);
      if(!tip.equals(Integer.toString(val))) {
        throw new Exception("Found value:"+tip+" expecting:"+val);
      }
      ++i;
    }
    return true;
  }

  protected boolean assertMapOrder(List<Tuple> tuples, int... ids) throws Exception {
    int i = 0;
    for(int val : ids) {
      Tuple t = tuples.get(i);
      List<Map> tip = t.getMaps("group");
      int id = (int)tip.get(0).get("id");
      if(id != val) {
        throw new Exception("Found value:"+id+" expecting:"+val);
      }
      ++i;
    }
    return true;
  }


  protected boolean assertFields(List<Tuple> tuples, String ... fields) throws Exception{
    for(Tuple tuple : tuples){
      for(String field : fields){
        if(!tuple.fields.containsKey(field)){
          throw new Exception(String.format(Locale.ROOT, "Expected field '%s' not found", field));
        }
      }
    }
    return true;
  }
  protected boolean assertNotFields(List<Tuple> tuples, String ... fields) throws Exception{
    for(Tuple tuple : tuples){
      for(String field : fields){
        if(tuple.fields.containsKey(field)){
          throw new Exception(String.format(Locale.ROOT, "Unexpected field '%s' found", field));
        }
      }
    }
    return true;
  }  

  protected boolean assertGroupOrder(Tuple tuple, int... ids) throws Exception {
    List<?> group = (List<?>)tuple.get("tuples");
    int i=0;
    for(int val : ids) {
      Map<?,?> t = (Map<?,?>)group.get(i);
      Long tip = (Long)t.get("id");
      if(tip.intValue() != val) {
        throw new Exception("Found value:"+tip.intValue()+" expecting:"+val);
      }
      ++i;
    }
    return true;
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

  private boolean assertList(List list, Object... vals) throws Exception {

    if(list.size() != vals.length) {
      throw new Exception("Lists are not the same size:"+list.size() +" : "+vals.length);
    }

    for(int i=0; i<list.size(); i++) {
      Object a = list.get(i);
      Object b = vals[i];
      if(!a.equals(b)) {
        throw new Exception("List items not equals:"+a+" : "+b);
      }
    }

    return true;
  }

  private void assertTopicRun(TupleStream stream, String... idArray) throws Exception {
    long version = -1;
    int count = 0;
    List<String> ids = new ArrayList();
    for(String id : idArray) {
      ids.add(id);
    }

    try {
      stream.open();
      while (true) {
        Tuple tuple = stream.read();
        if (tuple.EOF) {
          break;
        } else {
          ++count;
          String id = tuple.getString("id");
          if (!ids.contains(id)) {
            throw new Exception("Expecting id in topic run not found:" + id);
          }

          long v = tuple.getLong("_version_");
          if (v < version) {
            throw new Exception("Out of order version in topic run:" + v);
          }
        }
      }
    } finally {
      stream.close();
    }

    if(count != ids.size()) {
      throw new Exception("Wrong count in topic run:"+count);
    }
  }

  private void assertTopicSubject(TupleStream stream, String... textArray) throws Exception {
    long version = -1;
    int count = 0;
    List<String> texts = new ArrayList();
    for(String text : textArray) {
      texts.add(text);
    }

    try {
      stream.open();
      while (true) {
        Tuple tuple = stream.read();
        if (tuple.EOF) {
          break;
        } else {
          ++count;
          String subject = tuple.getString("subject");
          if (!texts.contains(subject)) {
            throw new Exception("Expecting subject in topic run not found:" + subject);
          }
        }
      }
    } finally {
      stream.close();
    }
  }

}
