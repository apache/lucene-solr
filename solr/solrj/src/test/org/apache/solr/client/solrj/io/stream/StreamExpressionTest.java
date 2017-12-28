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

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.stat.inference.KolmogorovSmirnovTest;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.io.ClassificationEvaluation;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.ComparatorOrder;
import org.apache.solr.client.solrj.io.comp.FieldComparator;
import org.apache.solr.client.solrj.io.eval.AddEvaluator;
import org.apache.solr.client.solrj.io.eval.AndEvaluator;
import org.apache.solr.client.solrj.io.eval.EqualToEvaluator;
import org.apache.solr.client.solrj.io.eval.GreaterThanEqualToEvaluator;
import org.apache.solr.client.solrj.io.eval.GreaterThanEvaluator;
import org.apache.solr.client.solrj.io.eval.IfThenElseEvaluator;
import org.apache.solr.client.solrj.io.eval.LessThanEqualToEvaluator;
import org.apache.solr.client.solrj.io.eval.LessThanEvaluator;
import org.apache.solr.client.solrj.io.eval.NotEvaluator;
import org.apache.solr.client.solrj.io.eval.OrEvaluator;
import org.apache.solr.client.solrj.io.eval.RawValueEvaluator;
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
    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);

    try {
      // Basic test
      expression = StreamExpressionParser.parse("search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\")");
      stream = new CloudSolrStream(expression, factory);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);

      assert (tuples.size() == 5);
      assertOrder(tuples, 0, 2, 1, 3, 4);
      assertLong(tuples.get(0), "a_i", 0);

      // Basic w/aliases
      expression = StreamExpressionParser.parse("search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\", aliases=\"a_i=alias.a_i, a_s=name\")");
      stream = new CloudSolrStream(expression, factory);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);

      assert (tuples.size() == 5);
      assertOrder(tuples, 0, 2, 1, 3, 4);
      assertLong(tuples.get(0), "alias.a_i", 0);
      assertString(tuples.get(0), "name", "hello0");

      // Basic filtered test
      expression = StreamExpressionParser.parse("search(" + COLLECTIONORALIAS + ", q=\"id:(0 3 4)\", fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\")");
      stream = new CloudSolrStream(expression, factory);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);

      assert (tuples.size() == 3);
      assertOrder(tuples, 0, 3, 4);
      assertLong(tuples.get(1), "a_i", 3);

      try {
        expression = StreamExpressionParser.parse("search(" + COLLECTIONORALIAS + ", fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\")");
        stream = new CloudSolrStream(expression, factory);
        stream.setStreamContext(streamContext);
        tuples = getTuples(stream);
        throw new Exception("Should be an exception here");
      } catch (Exception e) {
        assertTrue(e.getMessage().contains("q param expected for search function"));
      }

      try {
        expression = StreamExpressionParser.parse("search(" + COLLECTIONORALIAS + ", q=\"blah\", sort=\"a_f asc, a_i asc\")");
        stream = new CloudSolrStream(expression, factory);
        stream.setStreamContext(streamContext);
        tuples = getTuples(stream);
        throw new Exception("Should be an exception here");
      } catch (Exception e) {
        assertTrue(e.getMessage().contains("fl param expected for search function"));
      }

      try {
        expression = StreamExpressionParser.parse("search(" + COLLECTIONORALIAS + ", q=\"blah\", fl=\"id, a_f\", sort=\"a_f\")");
        stream = new CloudSolrStream(expression, factory);
        stream.setStreamContext(streamContext);
        tuples = getTuples(stream);
        throw new Exception("Should be an exception here");
      } catch (Exception e) {
        assertTrue(e.getMessage().contains("Invalid sort spec"));
      }

      // Test with shards param

      List<String> shardUrls = TupleStream.getShards(cluster.getZkServer().getZkAddress(), COLLECTIONORALIAS, streamContext);

      Map<String, List<String>> shardsMap = new HashMap();
      shardsMap.put("myCollection", shardUrls);
      StreamContext context = new StreamContext();
      context.put("shards", shardsMap);
      context.setSolrClientCache(solrClientCache);

      // Basic test
      expression = StreamExpressionParser.parse("search(myCollection, q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\")");
      stream = new CloudSolrStream(expression, factory);
      stream.setStreamContext(context);
      tuples = getTuples(stream);

      assert (tuples.size() == 5);
      assertOrder(tuples, 0, 2, 1, 3, 4);
      assertLong(tuples.get(0), "a_i", 0);


      //Execersise the /stream hander

      //Add the shards http parameter for the myCollection
      StringBuilder buf = new StringBuilder();
      for (String shardUrl : shardUrls) {
        if (buf.length() > 0) {
          buf.append(",");
        }
        buf.append(shardUrl);
      }

      ModifiableSolrParams solrParams = new ModifiableSolrParams();
      solrParams.add("qt", "/stream");
      solrParams.add("expr", "search(myCollection, q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\")");
      solrParams.add("myCollection.shards", buf.toString());
      SolrStream solrStream = new SolrStream(shardUrls.get(0), solrParams);
      stream.setStreamContext(context);
      tuples = getTuples(stream);

      assert (tuples.size() == 5);
      assertOrder(tuples, 0, 2, 1, 3, 4);
      assertLong(tuples.get(0), "a_i", 0);

    } finally {
      solrClientCache.close();
    }
  }

  @Test
  public void testSqlStream() throws Exception {

    new UpdateRequest()
        .add(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "0")
        .add(id, "2", "a_s", "hello2", "a_i", "2", "a_f", "0")
        .add(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3")
        .add(id, "4", "a_s", "hello4", "a_i", "4", "a_f", "4")
        .add(id, "1", "a_s", "hello1", "a_i", "1", "a_f", "1")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    List<Tuple> tuples;
    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);
    List<String> shardUrls = TupleStream.getShards(cluster.getZkServer().getZkAddress(), COLLECTIONORALIAS, streamContext);

    try {
      StringBuilder buf = new StringBuilder();
      for (String shardUrl : shardUrls) {
        if (buf.length() > 0) {
          buf.append(",");
        }
        buf.append(shardUrl);
      }

      ModifiableSolrParams solrParams = new ModifiableSolrParams();
      solrParams.add("qt", "/stream");
      solrParams.add("expr", "sql("+COLLECTIONORALIAS+", stmt=\"select id from collection1 order by a_i asc\")");
      SolrStream solrStream = new SolrStream(shardUrls.get(0), solrParams);
      solrStream.setStreamContext(streamContext);
      tuples = getTuples(solrStream);
      assert (tuples.size() == 5);
      assertOrder(tuples, 0, 1, 2, 3, 4);

      //Test with using the default collection
      solrParams = new ModifiableSolrParams();
      solrParams.add("qt", "/stream");
      solrParams.add("expr", "sql(stmt=\"select id from collection1 order by a_i asc\")");
      solrStream = new SolrStream(shardUrls.get(0), solrParams);
      solrStream.setStreamContext(streamContext);
      tuples = getTuples(solrStream);
      assert (tuples.size() == 5);
      assertOrder(tuples, 0, 1, 2, 3, 4);

    } finally {
      solrClientCache.close();
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
    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);
    List<Tuple> tuples;

    try {
      // Basic test
      expression = StreamExpressionParser.parse("search(" + COLLECTIONORALIAS + ", zkHost=" + cluster.getZkServer().getZkAddress() + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\")");
      stream = new CloudSolrStream(expression, factory);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);

      assert (tuples.size() == 5);
      assertOrder(tuples, 0, 2, 1, 3, 4);
      assertLong(tuples.get(0), "a_i", 0);

      // Basic w/aliases
      expression = StreamExpressionParser.parse("search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\", aliases=\"a_i=alias.a_i, a_s=name\", zkHost=" + cluster.getZkServer().getZkAddress() + ")");
      stream = new CloudSolrStream(expression, factory);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);

      assert (tuples.size() == 5);
      assertOrder(tuples, 0, 2, 1, 3, 4);
      assertLong(tuples.get(0), "alias.a_i", 0);
      assertString(tuples.get(0), "name", "hello0");

      // Basic filtered test
      expression = StreamExpressionParser.parse("search(" + COLLECTIONORALIAS + ", q=\"id:(0 3 4)\", fl=\"id,a_s,a_i,a_f\", zkHost="
          + cluster.getZkServer().getZkAddress() + ", sort=\"a_f asc, a_i asc\")");
      stream = new CloudSolrStream(expression, factory);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);

      assert (tuples.size() == 3);
      assertOrder(tuples, 0, 3, 4);
      assertLong(tuples.get(1), "a_i", 3);


      // Test a couple of multile field lists.
      expression = StreamExpressionParser.parse("search(collection1, fq=\"a_s:hello0\", fq=\"a_s:hello1\", q=\"id:(*)\", " +
          "zkHost=" + cluster.getZkServer().getZkAddress() + ", fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\")");
      stream = new CloudSolrStream(expression, factory);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);

      assertEquals("fq clauses should have prevented any docs from coming back", tuples.size(), 0);


      expression = StreamExpressionParser.parse("search(collection1, fq=\"a_s:(hello0 OR hello1)\", q=\"id:(*)\", " +
          "zkHost=" + cluster.getZkServer().getZkAddress() + ", fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\")");
      stream = new CloudSolrStream(expression, factory);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);

      assertEquals("Combining an f1 clause should show us 2 docs", tuples.size(), 2);

    } finally {
      solrClientCache.close();
    }

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
    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);

    StreamFactory factory = new StreamFactory()
      .withCollectionZkHost(COLLECTIONORALIAS, cluster.getZkServer().getZkAddress())
      .withFunctionName("search", CloudSolrStream.class)
      .withFunctionName("unique", UniqueStream.class);

    try {
      // Basic test
      expression = StreamExpressionParser.parse("unique(search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\"), over=\"a_f\")");
      stream = new UniqueStream(expression, factory);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);

      assert (tuples.size() == 4);
      assertOrder(tuples, 0, 1, 3, 4);

      // Basic test desc
      expression = StreamExpressionParser.parse("unique(search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f desc, a_i desc\"), over=\"a_f\")");
      stream = new UniqueStream(expression, factory);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);

      assert (tuples.size() == 4);
      assertOrder(tuples, 4, 3, 1, 2);

      // Basic w/multi comp
      expression = StreamExpressionParser.parse("unique(search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\"), over=\"a_f, a_i\")");
      stream = new UniqueStream(expression, factory);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);

      assert (tuples.size() == 5);
      assertOrder(tuples, 0, 2, 1, 3, 4);

      // full factory w/multi comp
      stream = factory.constructStream("unique(search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\"), over=\"a_f, a_i\")");
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);

      assert (tuples.size() == 5);
      assertOrder(tuples, 0, 2, 1, 3, 4);
    } finally {
      solrClientCache.close();
    }
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
    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);
    try {
      StreamFactory factory = new StreamFactory()
          .withCollectionZkHost(COLLECTIONORALIAS, cluster.getZkServer().getZkAddress())
          .withFunctionName("search", CloudSolrStream.class)
          .withFunctionName("sort", SortStream.class);

      // Basic test
      stream = factory.constructStream("sort(search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc\"), by=\"a_i asc\")");
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);
      assert (tuples.size() == 6);
      assertOrder(tuples, 0, 1, 5, 2, 3, 4);

      // Basic test desc
      stream = factory.constructStream("sort(search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc\"), by=\"a_i desc\")");
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);
      assert (tuples.size() == 6);
      assertOrder(tuples, 4, 3, 2, 1, 5, 0);

      // Basic w/multi comp
      stream = factory.constructStream("sort(search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc\"), by=\"a_i asc, a_f desc\")");
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);
      assert (tuples.size() == 6);
      assertOrder(tuples, 0, 5, 1, 2, 3, 4);
    } finally {
      solrClientCache.close();
    }
  }


  @Test
  public void testNullStream() throws Exception {

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
    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);
    StreamFactory factory = new StreamFactory()
        .withCollectionZkHost(COLLECTIONORALIAS, cluster.getZkServer().getZkAddress())
        .withFunctionName("search", CloudSolrStream.class)
        .withFunctionName("null", NullStream.class);

    try {
      // Basic test
      stream = factory.constructStream("null(search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc\"), by=\"a_i asc\")");
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);
      assertTrue(tuples.size() == 1);
      assertTrue(tuples.get(0).getLong("nullCount") == 6);
    } finally {
      solrClientCache.close();
    }
  }


  @Test
  public void testParallelNullStream() throws Exception {

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
    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);

    StreamFactory factory = new StreamFactory()
        .withCollectionZkHost(COLLECTIONORALIAS, cluster.getZkServer().getZkAddress())
        .withFunctionName("search", CloudSolrStream.class)
        .withFunctionName("null", NullStream.class)
        .withFunctionName("parallel", ParallelStream.class);

    try {

      // Basic test
      stream = factory.constructStream("parallel(" + COLLECTIONORALIAS + ", workers=2, sort=\"nullCount desc\", null(search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc\", partitionKeys=id), by=\"a_i asc\"))");
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);
      assertTrue(tuples.size() == 2);
      long nullCount = 0;
      for (Tuple t : tuples) {
        nullCount += t.getLong("nullCount");
      }

      assertEquals(nullCount, 6L);
    } finally {
      solrClientCache.close();
    }
  }

  @Test
  public void testNulls() throws Exception {

    new UpdateRequest()
        .add(id, "0", "a_i", "1", "a_f", "0", "s_multi", "aaa", "s_multi", "bbb", "i_multi", "100", "i_multi", "200")
        .add(id, "2", "a_s", "hello2", "a_i", "3", "a_f", "0")
        .add(id, "3", "a_s", "hello3", "a_i", "4", "a_f", "3")
        .add(id, "4", "a_s", "hello4", "a_f", "4")
        .add(id, "1", "a_s", "hello1", "a_i", "2", "a_f", "1")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    StreamExpression expression;
    TupleStream stream;
    List<Tuple> tuples;
    Tuple tuple;
    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);

    StreamFactory factory = new StreamFactory()
        .withCollectionZkHost(COLLECTIONORALIAS, cluster.getZkServer().getZkAddress())
        .withFunctionName("search", CloudSolrStream.class);
    try {
      // Basic test
      expression = StreamExpressionParser.parse("search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f, s_multi, i_multi\", qt=\"/export\", sort=\"a_i asc\")");
      stream = new CloudSolrStream(expression, factory);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);

      assert (tuples.size() == 5);
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
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);

      assert (tuples.size() == 5);
      assertOrder(tuples, 0, 1, 2, 3, 4);

      //test sort(desc) with null string field.  Null should sort to the bottom.
      expression = StreamExpressionParser.parse("search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f, s_multi, i_multi\", qt=\"/export\", sort=\"a_s desc\")");
      stream = new CloudSolrStream(expression, factory);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);

      assert (tuples.size() == 5);
      assertOrder(tuples, 4, 3, 2, 1, 0);
    } finally {
      solrClientCache.close();
    }
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
    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);
    try {
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);

      assert (tuples.size() == 4);
      assertOrder(tuples, 0, 1, 3, 4);

      // Basic test desc
      expression = StreamExpressionParser.parse("merge("
          + "search(" + COLLECTIONORALIAS + ", q=\"id:(0 3 4)\", fl=\"id,a_s,a_i,a_f\", sort=\"a_f desc\"),"
          + "search(" + COLLECTIONORALIAS + ", q=\"id:(1)\", fl=\"id,a_s,a_i,a_f\", sort=\"a_f desc\"),"
          + "on=\"a_f desc\")");
      stream = new MergeStream(expression, factory);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);

      assert (tuples.size() == 4);
      assertOrder(tuples, 4, 3, 1, 0);

      // Basic w/multi comp
      expression = StreamExpressionParser.parse("merge("
          + "search(" + COLLECTIONORALIAS + ", q=\"id:(0 3 4)\", fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_s asc\"),"
          + "search(" + COLLECTIONORALIAS + ", q=\"id:(1 2)\", fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_s asc\"),"
          + "on=\"a_f asc, a_s asc\")");
      stream = new MergeStream(expression, factory);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);

      assert (tuples.size() == 5);
      assertOrder(tuples, 0, 2, 1, 3, 4);

      // full factory w/multi comp
      stream = factory.constructStream("merge("
          + "search(" + COLLECTIONORALIAS + ", q=\"id:(0 3 4)\", fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_s asc\"),"
          + "search(" + COLLECTIONORALIAS + ", q=\"id:(1 2)\", fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_s asc\"),"
          + "on=\"a_f asc, a_s asc\")");
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);

      assert (tuples.size() == 5);
      assertOrder(tuples, 0, 2, 1, 3, 4);

      // full factory w/multi streams
      stream = factory.constructStream("merge("
          + "search(" + COLLECTIONORALIAS + ", q=\"id:(0 4)\", fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_s asc\"),"
          + "search(" + COLLECTIONORALIAS + ", q=\"id:(1)\", fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_s asc\"),"
          + "search(" + COLLECTIONORALIAS + ", q=\"id:(2)\", fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_s asc\"),"
          + "on=\"a_f asc\")");
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);

      assert (tuples.size() == 4);
      assertOrder(tuples, 0, 2, 1, 4);
    } finally {
      solrClientCache.close();
    }
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
    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);

    StreamFactory factory = new StreamFactory()
      .withCollectionZkHost(COLLECTIONORALIAS, cluster.getZkServer().getZkAddress())
      .withFunctionName("search", CloudSolrStream.class)
      .withFunctionName("unique", UniqueStream.class)
      .withFunctionName("top", RankStream.class);
    try {
      // Basic test
      expression = StreamExpressionParser.parse("top("
          + "n=3,"
          + "search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\"),"
          + "sort=\"a_f asc, a_i asc\")");
      stream = new RankStream(expression, factory);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);

      assert (tuples.size() == 3);
      assertOrder(tuples, 0, 2, 1);

      // Basic test desc
      expression = StreamExpressionParser.parse("top("
          + "n=2,"
          + "unique("
          + "search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f desc\"),"
          + "over=\"a_f\"),"
          + "sort=\"a_f desc\")");
      stream = new RankStream(expression, factory);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);

      assert (tuples.size() == 2);
      assertOrder(tuples, 4, 3);

      // full factory
      stream = factory.constructStream("top("
          + "n=4,"
          + "unique("
          + "search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\"),"
          + "over=\"a_f\"),"
          + "sort=\"a_f asc\")");
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);

      assert (tuples.size() == 4);
      assertOrder(tuples, 0, 1, 3, 4);

      // full factory, switch order
      stream = factory.constructStream("top("
          + "n=4,"
          + "unique("
          + "search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f desc, a_i desc\"),"
          + "over=\"a_f\"),"
          + "sort=\"a_f asc\")");
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);

      assert (tuples.size() == 4);
      assertOrder(tuples, 2, 1, 3, 4);
    } finally {
      solrClientCache.close();
    }
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


      //Exercise the /stream handler
      ModifiableSolrParams sParams = new ModifiableSolrParams(StreamingTest.mapParams(CommonParams.QT, "/stream"));
      sParams.add("expr", "random(" + COLLECTIONORALIAS + ", q=\"*:*\", rows=\"1\", fl=\"id, a_i\")");
      JettySolrRunner jetty = cluster.getJettySolrRunner(0);
      SolrStream solrStream = new SolrStream(jetty.getBaseUrl().toString() + "/collection1", sParams);
      List<Tuple> tuples4 = getTuples(solrStream);
      assert (tuples4.size() == 1);

    } finally {
      cache.close();
    }
  }

  @Test
  public void testKnnStream() throws Exception {

    UpdateRequest update = new UpdateRequest();
    update.add(id, "1", "a_t", "hello world have a very nice day blah");
    update.add(id, "4", "a_t", "hello world have a very streaming is fun");
    update.add(id, "3", "a_t", "hello world have a very nice bug out");
    update.add(id, "2", "a_t", "hello world have a very nice day fancy sky");
    update.commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    StreamContext context = new StreamContext();
    SolrClientCache cache = new SolrClientCache();
    try {
      context.setSolrClientCache(cache);
      ModifiableSolrParams sParams = new ModifiableSolrParams(StreamingTest.mapParams(CommonParams.QT, "/stream"));
      sParams.add("expr", "knn(" + COLLECTIONORALIAS + ", id=\"1\", qf=\"a_t\", rows=\"4\", fl=\"id, score\", mintf=\"1\")");
      JettySolrRunner jetty = cluster.getJettySolrRunner(0);
      SolrStream solrStream = new SolrStream(jetty.getBaseUrl().toString() + "/collection1", sParams);
      List<Tuple> tuples = getTuples(solrStream);
      assertTrue(tuples.size() == 3);
      assertOrder(tuples, 2, 3, 4);

      sParams = new ModifiableSolrParams(StreamingTest.mapParams(CommonParams.QT, "/stream"));
      sParams.add("expr", "knn(" + COLLECTIONORALIAS + ", id=\"1\", qf=\"a_t\", k=\"2\", fl=\"id, score\", mintf=\"1\")");
      solrStream = new SolrStream(jetty.getBaseUrl().toString() + "/collection1", sParams);
      tuples = getTuples(solrStream);
      assertTrue(tuples.size() == 2);
      assertOrder(tuples, 2, 3);

      sParams = new ModifiableSolrParams(StreamingTest.mapParams(CommonParams.QT, "/stream"));
      sParams.add("expr", "knn(" + COLLECTIONORALIAS + ", id=\"1\", qf=\"a_t\", rows=\"4\", fl=\"id, score\", mintf=\"1\", maxdf=\"0\")");
      solrStream = new SolrStream(jetty.getBaseUrl().toString() + "/collection1", sParams);
      tuples = getTuples(solrStream);
      assertTrue(tuples.size() == 0);

      sParams = new ModifiableSolrParams(StreamingTest.mapParams(CommonParams.QT, "/stream"));
      sParams.add("expr", "knn(" + COLLECTIONORALIAS + ", id=\"1\", qf=\"a_t\", rows=\"4\", fl=\"id, score\", mintf=\"1\", maxwl=\"1\")");
      solrStream = new SolrStream(jetty.getBaseUrl().toString() + "/collection1", sParams);
      tuples = getTuples(solrStream);
      assertTrue(tuples.size() == 0);

      sParams = new ModifiableSolrParams(StreamingTest.mapParams(CommonParams.QT, "/stream"));
      sParams.add("expr", "knn(" + COLLECTIONORALIAS + ", id=\"1\", qf=\"a_t\", rows=\"2\", fl=\"id, score\", mintf=\"1\", minwl=\"20\")");
      solrStream = new SolrStream(jetty.getBaseUrl().toString() + "/collection1", sParams);
      tuples = getTuples(solrStream);
      assertTrue(tuples.size() == 0);

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
    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);

    StreamFactory factory = new StreamFactory()
        .withCollectionZkHost(COLLECTIONORALIAS, cluster.getZkServer().getZkAddress())
        .withFunctionName("search", CloudSolrStream.class)
        .withFunctionName("reduce", ReducerStream.class)
        .withFunctionName("group", GroupOperation.class);

    try {
      // basic
      expression = StreamExpressionParser.parse("reduce("
          + "search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_s asc, a_f asc\"),"
          + "by=\"a_s\","
          + "group(sort=\"a_f desc\", n=\"4\"))");

      stream = factory.constructStream(expression);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);

      assert (tuples.size() == 3);

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
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);

      assert (tuples.size() == 3);

      t0 = tuples.get(0);
      maps0 = t0.getMaps("group");
      assert (maps0.size() == 2);

      assertMaps(maps0, 0, 1);

      t1 = tuples.get(1);
      maps1 = t1.getMaps("group");
      assertMaps(maps1, 3, 5);

      t2 = tuples.get(2);
      maps2 = t2.getMaps("group");
      assertMaps(maps2, 4, 6);
    } finally {
      solrClientCache.close();
    }
  }


  @Test
  public void testHavingStream() throws Exception {

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
        .withFunctionName("having", HavingStream.class)
        .withFunctionName("rollup", RollupStream.class)
        .withFunctionName("sum", SumMetric.class)
        .withFunctionName("and", AndEvaluator.class)
        .withFunctionName("or", OrEvaluator.class)
        .withFunctionName("not", NotEvaluator.class)
        .withFunctionName("gt", GreaterThanEvaluator.class)
        .withFunctionName("lt", LessThanEvaluator.class)
        .withFunctionName("eq", EqualToEvaluator.class)
        .withFunctionName("lteq", LessThanEqualToEvaluator.class)
        .withFunctionName("gteq", GreaterThanEqualToEvaluator.class);

    stream = factory.constructStream("having(search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc\"), eq(a_i, 9))");
    StreamContext context = new StreamContext();
    context.setSolrClientCache(solrClientCache);
    stream.setStreamContext(context);
    tuples = getTuples(stream);

    assert(tuples.size() == 1);
    Tuple t = tuples.get(0);
    assertTrue(t.getString("id").equals("9"));

    stream = factory.constructStream("having(search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc\"), and(eq(a_i, 9),lt(a_i, 10)))");
    context = new StreamContext();
    context.setSolrClientCache(solrClientCache);
    stream.setStreamContext(context);
    tuples = getTuples(stream);

    assert(tuples.size() == 1);
    t = tuples.get(0);
    assertTrue(t.getString("id").equals("9"));

    stream = factory.constructStream("having(search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc\"), or(eq(a_i, 9),eq(a_i, 8)))");
    context = new StreamContext();
    context.setSolrClientCache(solrClientCache);
    stream.setStreamContext(context);
    tuples = getTuples(stream);

    assert(tuples.size() == 2);
    t = tuples.get(0);
    assertTrue(t.getString("id").equals("8"));

    t = tuples.get(1);
    assertTrue(t.getString("id").equals("9"));


    stream = factory.constructStream("having(search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc\"), and(eq(a_i, 9),not(eq(a_i, 9))))");
    context = new StreamContext();
    context.setSolrClientCache(solrClientCache);
    stream.setStreamContext(context);
    tuples = getTuples(stream);

    assert(tuples.size() == 0);


    stream = factory.constructStream("having(search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc\"), and(lteq(a_i, 9), gteq(a_i, 8)))");
    context = new StreamContext();
    context.setSolrClientCache(solrClientCache);
    stream.setStreamContext(context);
    tuples = getTuples(stream);

    assert(tuples.size() == 2);

    t = tuples.get(0);
    assertTrue(t.getString("id").equals("8"));

    t = tuples.get(1);
    assertTrue(t.getString("id").equals("9"));

    stream = factory.constructStream("having(rollup(over=a_f, sum(a_i), search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc\")), and(eq(sum(a_i), 9),eq(sum(a_i), 9)))");
    context = new StreamContext();
    context.setSolrClientCache(solrClientCache);
    stream.setStreamContext(context);
    tuples = getTuples(stream);

    assert(tuples.size() == 1);
    t = tuples.get(0);
    assertTrue(t.getDouble("a_f") == 10.0D);

    solrClientCache.close();
  }


  @Test
  public void testParallelHavingStream() throws Exception {

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
        .withFunctionName("having", HavingStream.class)
        .withFunctionName("rollup", RollupStream.class)
        .withFunctionName("sum", SumMetric.class)
        .withFunctionName("and", AndEvaluator.class)
        .withFunctionName("or", OrEvaluator.class)
        .withFunctionName("not", NotEvaluator.class)
        .withFunctionName("gt", GreaterThanEvaluator.class)
        .withFunctionName("lt", LessThanEvaluator.class)
        .withFunctionName("eq", EqualToEvaluator.class)
        .withFunctionName("lteq", LessThanEqualToEvaluator.class)
        .withFunctionName("gteq", GreaterThanEqualToEvaluator.class)
        .withFunctionName("val", RawValueEvaluator.class)
        .withFunctionName("parallel", ParallelStream.class);

    stream = factory.constructStream("parallel(" + COLLECTIONORALIAS + ", workers=2, sort=\"a_f asc\", having(search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc\", partitionKeys=id), eq(a_i, 9)))");
    StreamContext context = new StreamContext();
    context.setSolrClientCache(solrClientCache);
    stream.setStreamContext(context);
    tuples = getTuples(stream);

    assert(tuples.size() == 1);
    Tuple t = tuples.get(0);
    assertTrue(t.getString("id").equals("9"));

    stream = factory.constructStream("parallel(" + COLLECTIONORALIAS + ", workers=2, sort=\"a_f asc\", having(search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc\", partitionKeys=id), and(eq(a_i, 9),lt(a_i, 10))))");
    context = new StreamContext();
    context.setSolrClientCache(solrClientCache);
    stream.setStreamContext(context);
    tuples = getTuples(stream);

    assert(tuples.size() == 1);
    t = tuples.get(0);
    assertTrue(t.getString("id").equals("9"));

    stream = factory.constructStream("parallel(" + COLLECTIONORALIAS + ", workers=2, sort=\"a_f asc\",having(search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc\", partitionKeys=id), or(eq(a_i, 9),eq(a_i, 8))))");
    context = new StreamContext();
    context.setSolrClientCache(solrClientCache);
    stream.setStreamContext(context);
    tuples = getTuples(stream);

    assert(tuples.size() == 2);
    t = tuples.get(0);
    assertTrue(t.getString("id").equals("8"));

    t = tuples.get(1);
    assertTrue(t.getString("id").equals("9"));


    stream = factory.constructStream("parallel(" + COLLECTIONORALIAS + ", workers=2, sort=\"a_f asc\", having(search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc\", partitionKeys=id), and(eq(a_i, 9),not(eq(a_i, 9)))))");
    context = new StreamContext();
    context.setSolrClientCache(solrClientCache);
    stream.setStreamContext(context);
    tuples = getTuples(stream);

    assert(tuples.size() == 0);


    stream = factory.constructStream("parallel(" + COLLECTIONORALIAS + ", workers=2, sort=\"a_f asc\",having(search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc\", partitionKeys=id), and(lteq(a_i, 9), gteq(a_i, 8))))");
    context = new StreamContext();
    context.setSolrClientCache(solrClientCache);
    stream.setStreamContext(context);
    tuples = getTuples(stream);

    assert(tuples.size() == 2);

    t = tuples.get(0);
    assertTrue(t.getString("id").equals("8"));

    t = tuples.get(1);
    assertTrue(t.getString("id").equals("9"));

    stream = factory.constructStream("parallel("+COLLECTIONORALIAS+", workers=2, sort=\"a_f asc\", having(rollup(over=a_f, sum(a_i), search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc\", partitionKeys=a_f)), and(eq(sum(a_i), 9),eq(sum(a_i),9))))");
    context = new StreamContext();
    context.setSolrClientCache(solrClientCache);
    stream.setStreamContext(context);
    tuples = getTuples(stream);

    assert(tuples.size() == 1);

    t = tuples.get(0);
    assertTrue(t.getDouble("a_f") == 10.0D);

    solrClientCache.close();
  }

  @Test
  public void testFetchStream() throws Exception {

    SolrClientCache solrClientCache = new SolrClientCache();//TODO share in @Before ; close in @After ?

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
    stream = factory.constructStream("fetch(" + COLLECTIONORALIAS + ",  search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc\"), on=\"id=a_i\", batchSize=\"3\", fl=\"subject\")");
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

    // SOLR-10404 test that "hello 99" as a value gets escaped
    new UpdateRequest()
        .add(id, "99", "a1_s", "hello 99", "a2_s", "hello 99", "subject", "blah blah blah 99")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    stream = factory.constructStream("fetch("+ COLLECTIONORALIAS +",  search(" + COLLECTIONORALIAS + ", q=" + id + ":99, fl=\"id,a1_s\", sort=\"id asc\"), on=\"a1_s=a2_s\", fl=\"subject\")");
    context = new StreamContext();
    context.setSolrClientCache(solrClientCache);
    stream.setStreamContext(context);
    tuples = getTuples(stream);

    assertEquals(1, tuples.size());
    t = tuples.get(0);
    assertTrue("blah blah blah 99".equals(t.getString("subject")));

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

    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);

    TupleStream stream;
    List<Tuple> tuples;

    StreamFactory factory = new StreamFactory()
        .withCollectionZkHost(COLLECTIONORALIAS, cluster.getZkServer().getZkAddress())
        .withFunctionName("search", CloudSolrStream.class)
        .withFunctionName("parallel", ParallelStream.class)
        .withFunctionName("fetch", FetchStream.class);

    try {

      stream = factory.constructStream("parallel(" + COLLECTIONORALIAS + ", workers=2, sort=\"a_f asc\", fetch(" + COLLECTIONORALIAS + ",  search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc\", partitionKeys=\"id\"), on=\"id=a_i\", batchSize=\"2\", fl=\"subject\"))");
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);

      assert (tuples.size() == 10);
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
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);

      assert (tuples.size() == 10);
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
    } finally {
      solrClientCache.close();
    }
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
    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);
    daemonStream.setStreamContext(streamContext);
    try {
      //Test Long and Double Sums

      daemonStream.open(); // This will start the daemon thread

      for (int i = 0; i < 4; i++) {
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

      while (daemonStream.remainingCapacity() > 0) {
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
      for (int i = 0; i < 12; i++) {
        daemonStream.read();
      }

      //And rerun the loop. It should have a new count for hello0
      for (int i = 0; i < 4; i++) {
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
    } finally {
      daemonStream.close(); //This should stop the daemon thread
      solrClientCache.close();
    }
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
    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);
    try {
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
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);

      assert (tuples.size() == 3);

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

    } finally {
      solrClientCache.close();
    }
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
    StreamContext streamContext = new StreamContext();
    SolrClientCache cache = new SolrClientCache();
    try {
      streamContext.setSolrClientCache(cache);
      String expr = "stats(" + COLLECTIONORALIAS + ", q=*:*, sum(a_i), sum(a_f), min(a_i), min(a_f), max(a_i), max(a_f), avg(a_i), avg(a_f), count(*))";
      expression = StreamExpressionParser.parse(expr);
      stream = factory.constructStream(expression);
      stream.setStreamContext(streamContext);

      tuples = getTuples(stream);

      assert (tuples.size() == 1);

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


      //Test with shards parameter
      List<String> shardUrls = TupleStream.getShards(cluster.getZkServer().getZkAddress(), COLLECTIONORALIAS, streamContext);
      expr = "stats(myCollection, q=*:*, sum(a_i), sum(a_f), min(a_i), min(a_f), max(a_i), max(a_f), avg(a_i), avg(a_f), count(*))";
      Map<String, List<String>> shardsMap = new HashMap();
      shardsMap.put("myCollection", shardUrls);
      StreamContext context = new StreamContext();
      context.put("shards", shardsMap);
      context.setSolrClientCache(cache);
      stream = factory.constructStream(expr);
      stream.setStreamContext(context);

      tuples = getTuples(stream);

      assert (tuples.size() == 1);

      //Test Long and Double Sums

      tuple = tuples.get(0);

      sumi = tuple.getDouble("sum(a_i)");
      sumf = tuple.getDouble("sum(a_f)");
      mini = tuple.getDouble("min(a_i)");
      minf = tuple.getDouble("min(a_f)");
      maxi = tuple.getDouble("max(a_i)");
      maxf = tuple.getDouble("max(a_f)");
      avgi = tuple.getDouble("avg(a_i)");
      avgf = tuple.getDouble("avg(a_f)");
      count = tuple.getDouble("count(*)");

      assertTrue(sumi.longValue() == 70);
      assertTrue(sumf.doubleValue() == 55.0D);
      assertTrue(mini.doubleValue() == 0.0D);
      assertTrue(minf.doubleValue() == 1.0D);
      assertTrue(maxi.doubleValue() == 14.0D);
      assertTrue(maxf.doubleValue() == 10.0D);
      assertTrue(avgi.doubleValue() == 7.0D);
      assertTrue(avgf.doubleValue() == 5.5D);
      assertTrue(count.doubleValue() == 10);

      //Execersise the /stream hander

      //Add the shards http parameter for the myCollection
      StringBuilder buf = new StringBuilder();
      for (String shardUrl : shardUrls) {
        if (buf.length() > 0) {
          buf.append(",");
        }
        buf.append(shardUrl);
      }

      ModifiableSolrParams solrParams = new ModifiableSolrParams();
      solrParams.add("qt", "/stream");
      solrParams.add("expr", expr);
      solrParams.add("myCollection.shards", buf.toString());
      SolrStream solrStream = new SolrStream(shardUrls.get(0), solrParams);
      tuples = getTuples(solrStream);
      assert (tuples.size() == 1);

      tuple =tuples.get(0);

      sumi = tuple.getDouble("sum(a_i)");
      sumf = tuple.getDouble("sum(a_f)");
      mini = tuple.getDouble("min(a_i)");
      minf = tuple.getDouble("min(a_f)");
      maxi = tuple.getDouble("max(a_i)");
      maxf = tuple.getDouble("max(a_f)");
      avgi = tuple.getDouble("avg(a_i)");
      avgf = tuple.getDouble("avg(a_f)");
      count = tuple.getDouble("count(*)");

      assertTrue(sumi.longValue() == 70);
      assertTrue(sumf.doubleValue() == 55.0D);
      assertTrue(mini.doubleValue() == 0.0D);
      assertTrue(minf.doubleValue() == 1.0D);
      assertTrue(maxi.doubleValue() == 14.0D);
      assertTrue(maxf.doubleValue() == 10.0D);
      assertTrue(avgi.doubleValue() == 7.0D);
      assertTrue(avgf.doubleValue() == 5.5D);
      assertTrue(count.doubleValue() == 10);
      //Add a negative test to prove that it cannot find slices if shards parameter is removed

      try {
        ModifiableSolrParams solrParamsBad = new ModifiableSolrParams();
        solrParamsBad.add("qt", "/stream");
        solrParamsBad.add("expr", expr);
        solrStream = new SolrStream(shardUrls.get(0), solrParamsBad);
        tuples = getTuples(solrStream);
        throw new Exception("Exception should have been thrown above");
      } catch (IOException e) {
        assertTrue(e.getMessage().contains("Collection not found: myCollection"));
      }
    } finally {
      cache.close();
    }
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
    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);



    try {

      ParallelStream pstream = (ParallelStream) streamFactory.constructStream("parallel(" + COLLECTIONORALIAS + ", unique(search(collection1, q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\", partitionKeys=\"a_f\"), over=\"a_f\"), workers=\"2\", zkHost=\"" + zkHost + "\", sort=\"a_f asc\")");
      pstream.setStreamContext(streamContext);
      List<Tuple> tuples = getTuples(pstream);
      assert (tuples.size() == 5);
      assertOrder(tuples, 0, 1, 3, 4, 6);

      //Test the eofTuples

      Map<String, Tuple> eofTuples = pstream.getEofTuples();
      assert (eofTuples.size() == 2); //There should be an EOF tuple for each worker.
    } finally {
      solrClientCache.close();
    }
  }

  @Test
  public void testParallelShuffleStream() throws Exception {

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
        .add(id, "9", "a_s", "hello1", "a_i", "13", "a_f", "4")
        .add(id, "10", "a_s", "hello1", "a_i", "13", "a_f", "4")
        .add(id, "11", "a_s", "hello1", "a_i", "13", "a_f", "4")
        .add(id, "12", "a_s", "hello1", "a_i", "13", "a_f", "4")
        .add(id, "13", "a_s", "hello1", "a_i", "13", "a_f", "4")
        .add(id, "14", "a_s", "hello1", "a_i", "13", "a_f", "4")
        .add(id, "15", "a_s", "hello1", "a_i", "13", "a_f", "4")
        .add(id, "16", "a_s", "hello1", "a_i", "13", "a_f", "4")
        .add(id, "17", "a_s", "hello1", "a_i", "13", "a_f", "4")
        .add(id, "18", "a_s", "hello1", "a_i", "13", "a_f", "4")
        .add(id, "19", "a_s", "hello1", "a_i", "13", "a_f", "4")
        .add(id, "20", "a_s", "hello1", "a_i", "13", "a_f", "4")
        .add(id, "21", "a_s", "hello1", "a_i", "13", "a_f", "4")
        .add(id, "22", "a_s", "hello1", "a_i", "13", "a_f", "4")
        .add(id, "23", "a_s", "hello1", "a_i", "13", "a_f", "4")
        .add(id, "24", "a_s", "hello1", "a_i", "13", "a_f", "4")
        .add(id, "25", "a_s", "hello1", "a_i", "13", "a_f", "4")
        .add(id, "26", "a_s", "hello1", "a_i", "13", "a_f", "4")
        .add(id, "27", "a_s", "hello1", "a_i", "13", "a_f", "4")
        .add(id, "28", "a_s", "hello1", "a_i", "13", "a_f", "4")
        .add(id, "29", "a_s", "hello1", "a_i", "13", "a_f", "4")
        .add(id, "30", "a_s", "hello1", "a_i", "13", "a_f", "4")
        .add(id, "31", "a_s", "hello1", "a_i", "13", "a_f", "4")
        .add(id, "32", "a_s", "hello1", "a_i", "13", "a_f", "4")
        .add(id, "33", "a_s", "hello1", "a_i", "13", "a_f", "4")
        .add(id, "34", "a_s", "hello1", "a_i", "13", "a_f", "4")
        .add(id, "35", "a_s", "hello1", "a_i", "13", "a_f", "4")
        .add(id, "36", "a_s", "hello1", "a_i", "13", "a_f", "4")
        .add(id, "37", "a_s", "hello1", "a_i", "13", "a_f", "4")
        .add(id, "38", "a_s", "hello1", "a_i", "13", "a_f", "4")
        .add(id, "39", "a_s", "hello1", "a_i", "13", "a_f", "4")
        .add(id, "40", "a_s", "hello1", "a_i", "13", "a_f", "4")
        .add(id, "41", "a_s", "hello1", "a_i", "13", "a_f", "4")
        .add(id, "42", "a_s", "hello1", "a_i", "13", "a_f", "4")
        .add(id, "43", "a_s", "hello1", "a_i", "13", "a_f", "4")
        .add(id, "44", "a_s", "hello1", "a_i", "13", "a_f", "4")
        .add(id, "45", "a_s", "hello1", "a_i", "13", "a_f", "4")
        .add(id, "46", "a_s", "hello1", "a_i", "13", "a_f", "4")
        .add(id, "47", "a_s", "hello1", "a_i", "13", "a_f", "4")
        .add(id, "48", "a_s", "hello1", "a_i", "13", "a_f", "4")
        .add(id, "49", "a_s", "hello1", "a_i", "13", "a_f", "4")
        .add(id, "50", "a_s", "hello1", "a_i", "13", "a_f", "4")
        .add(id, "51", "a_s", "hello1", "a_i", "13", "a_f", "4")
        .add(id, "52", "a_s", "hello1", "a_i", "13", "a_f", "4")
        .add(id, "53", "a_s", "hello1", "a_i", "13", "a_f", "4")
        .add(id, "54", "a_s", "hello1", "a_i", "13", "a_f", "4")
        .add(id, "55", "a_s", "hello1", "a_i", "13", "a_f", "4")
        .add(id, "56", "a_s", "hello1", "a_i", "13", "a_f", "1000")

        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);

    String zkHost = cluster.getZkServer().getZkAddress();
    StreamFactory streamFactory = new StreamFactory().withCollectionZkHost(COLLECTIONORALIAS, zkHost)
        .withFunctionName("shuffle", ShuffleStream.class)
        .withFunctionName("unique", UniqueStream.class)
        .withFunctionName("parallel", ParallelStream.class);

    try {
      ParallelStream pstream = (ParallelStream) streamFactory.constructStream("parallel(" + COLLECTIONORALIAS + ", unique(shuffle(collection1, q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\", partitionKeys=\"a_f\"), over=\"a_f\"), workers=\"2\", zkHost=\"" + zkHost + "\", sort=\"a_f asc\")");
      pstream.setStreamFactory(streamFactory);
      pstream.setStreamContext(streamContext);
      List<Tuple> tuples = getTuples(pstream);
      assert (tuples.size() == 6);
      assertOrder(tuples, 0, 1, 3, 4, 6, 56);

      //Test the eofTuples

      Map<String, Tuple> eofTuples = pstream.getEofTuples();
      assert (eofTuples.size() == 2); //There should be an EOF tuple for each worker.
      assert (pstream.toExpression(streamFactory).toString().contains("shuffle"));
    } finally {
      solrClientCache.close();
    }
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

    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);


    String zkHost = cluster.getZkServer().getZkAddress();
    StreamFactory streamFactory = new StreamFactory().withCollectionZkHost(COLLECTIONORALIAS, zkHost)
        .withFunctionName("search", CloudSolrStream.class)
        .withFunctionName("group", GroupOperation.class)
        .withFunctionName("reduce", ReducerStream.class)
        .withFunctionName("parallel", ParallelStream.class);


    try {
      ParallelStream pstream = (ParallelStream) streamFactory.constructStream("parallel(" + COLLECTIONORALIAS + ", " +
          "reduce(" +
          "search(" + COLLECTIONORALIAS + ", q=\"*:*\", fl=\"id,a_s,a_i,a_f\", sort=\"a_s asc,a_f asc\", partitionKeys=\"a_s\"), " +
          "by=\"a_s\"," +
          "group(sort=\"a_i asc\", n=\"5\")), " +
          "workers=\"2\", zkHost=\"" + zkHost + "\", sort=\"a_s asc\")");

      pstream.setStreamContext(streamContext);

      List<Tuple> tuples = getTuples(pstream);

      assert (tuples.size() == 3);

      Tuple t0 = tuples.get(0);
      List<Map> maps0 = t0.getMaps("group");
      assertMaps(maps0, 0, 1, 2, 9);

      Tuple t1 = tuples.get(1);
      List<Map> maps1 = t1.getMaps("group");
      assertMaps(maps1, 3, 5, 7, 8);

      Tuple t2 = tuples.get(2);
      List<Map> maps2 = t2.getMaps("group");
      assertMaps(maps2, 4, 6);


      pstream = (ParallelStream) streamFactory.constructStream("parallel(" + COLLECTIONORALIAS + ", " +
          "reduce(" +
          "search(" + COLLECTIONORALIAS + ", q=\"*:*\", fl=\"id,a_s,a_i,a_f\", sort=\"a_s desc,a_f asc\", partitionKeys=\"a_s\"), " +
          "by=\"a_s\", " +
          "group(sort=\"a_i desc\", n=\"5\"))," +
          "workers=\"2\", zkHost=\"" + zkHost + "\", sort=\"a_s desc\")");

      pstream.setStreamContext(streamContext);
      tuples = getTuples(pstream);

      assert (tuples.size() == 3);

      t0 = tuples.get(0);
      maps0 = t0.getMaps("group");
      assertMaps(maps0, 6, 4);


      t1 = tuples.get(1);
      maps1 = t1.getMaps("group");
      assertMaps(maps1, 8, 7, 5, 3);


      t2 = tuples.get(2);
      maps2 = t2.getMaps("group");
      assertMaps(maps2, 9, 2, 1, 0);
    } finally {
      solrClientCache.close();
    }

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

    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);
    try {
      ParallelStream pstream = (ParallelStream) streamFactory.constructStream("parallel("
          + COLLECTIONORALIAS + ", "
          + "top("
          + "search(" + COLLECTIONORALIAS + ", q=\"*:*\", fl=\"id,a_s,a_i\", sort=\"a_i asc\", partitionKeys=\"a_i\"), "
          + "n=\"11\", "
          + "sort=\"a_i desc\"), workers=\"2\", zkHost=\"" + zkHost + "\", sort=\"a_i desc\")");
      pstream.setStreamContext(streamContext);
      List<Tuple> tuples = getTuples(pstream);

      assert (tuples.size() == 10);
      assertOrder(tuples, 10, 9, 8, 7, 6, 5, 4, 3, 2, 0);
    } finally {
      solrClientCache.close();
    }

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

    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);
    try {
      //Test ascending
      ParallelStream pstream = (ParallelStream) streamFactory.constructStream("parallel(" + COLLECTIONORALIAS + ", merge(search(" + COLLECTIONORALIAS + ", q=\"id:(4 1 8 7 9)\", fl=\"id,a_s,a_i\", sort=\"a_i asc\", partitionKeys=\"a_i\"), search(" + COLLECTIONORALIAS + ", q=\"id:(0 2 3 6)\", fl=\"id,a_s,a_i\", sort=\"a_i asc\", partitionKeys=\"a_i\"), on=\"a_i asc\"), workers=\"2\", zkHost=\"" + zkHost + "\", sort=\"a_i asc\")");
      pstream.setStreamContext(streamContext);
      List<Tuple> tuples = getTuples(pstream);

      assert (tuples.size() == 9);
      assertOrder(tuples, 0, 1, 2, 3, 4, 7, 6, 8, 9);

      //Test descending

      pstream = (ParallelStream) streamFactory.constructStream("parallel(" + COLLECTIONORALIAS + ", merge(search(" + COLLECTIONORALIAS + ", q=\"id:(4 1 8 9)\", fl=\"id,a_s,a_i\", sort=\"a_i desc\", partitionKeys=\"a_i\"), search(" + COLLECTIONORALIAS + ", q=\"id:(0 2 3 6)\", fl=\"id,a_s,a_i\", sort=\"a_i desc\", partitionKeys=\"a_i\"), on=\"a_i desc\"), workers=\"2\", zkHost=\"" + zkHost + "\", sort=\"a_i desc\")");
      pstream.setStreamContext(streamContext);
      tuples = getTuples(pstream);

      assert (tuples.size() == 8);
      assertOrder(tuples, 9, 8, 6, 4, 3, 2, 1, 0);
    } finally {
      solrClientCache.close();
    }

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


    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);

    StreamExpression expression;
    TupleStream stream;
    List<Tuple> tuples;

    try {
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
              + "workers=\"2\", zkHost=\"" + cluster.getZkServer().getZkAddress() + "\", sort=\"a_s asc\")"
      );


      stream = factory.constructStream(expression);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);

      assert (tuples.size() == 3);

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
    } finally {
      solrClientCache.close();
    }
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
    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);
    
    StreamFactory factory = new StreamFactory()
      .withCollectionZkHost(COLLECTIONORALIAS, cluster.getZkServer().getZkAddress())
      .withFunctionName("search", CloudSolrStream.class)
      .withFunctionName("innerJoin", InnerJoinStream.class);

    try {
      // Basic test
      expression = StreamExpressionParser.parse("innerJoin("
          + "search(" + COLLECTIONORALIAS + ", q=\"side_s:left\", fl=\"id,join1_i,join2_s,ident_s\", sort=\"join1_i asc, join2_s asc, id asc\"),"
          + "search(" + COLLECTIONORALIAS + ", q=\"side_s:right\", fl=\"join1_i,join2_s,ident_s\", sort=\"join1_i asc, join2_s asc\"),"
          + "on=\"join1_i=join1_i, join2_s=join2_s\")");
      stream = new InnerJoinStream(expression, factory);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);
      assert (tuples.size() == 8);
      assertOrder(tuples, 1, 1, 15, 15, 3, 4, 5, 7);

      // Basic desc
      expression = StreamExpressionParser.parse("innerJoin("
          + "search(" + COLLECTIONORALIAS + ", q=\"side_s:left\", fl=\"id,join1_i,join2_s,ident_s\", sort=\"join1_i desc, join2_s asc\"),"
          + "search(" + COLLECTIONORALIAS + ", q=\"side_s:right\", fl=\"join1_i,join2_s,ident_s\", sort=\"join1_i desc, join2_s asc\"),"
          + "on=\"join1_i=join1_i, join2_s=join2_s\")");
      stream = new InnerJoinStream(expression, factory);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);
      assert (tuples.size() == 8);
      assertOrder(tuples, 7, 3, 4, 5, 1, 1, 15, 15);

      // Results in both searches, no join matches
      expression = StreamExpressionParser.parse("innerJoin("
          + "search(" + COLLECTIONORALIAS + ", q=\"side_s:left\", fl=\"id,join1_i,join2_s,ident_s\", sort=\"ident_s asc\"),"
          + "search(" + COLLECTIONORALIAS + ", q=\"side_s:right\", fl=\"id,join1_i,join2_s,ident_s\", sort=\"ident_s asc\", aliases=\"id=right.id, join1_i=right.join1_i, join2_s=right.join2_s, ident_s=right.ident_s\"),"
          + "on=\"ident_s=right.ident_s\")");
      stream = new InnerJoinStream(expression, factory);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);
      assert (tuples.size() == 0);

      // Differing field names
      expression = StreamExpressionParser.parse("innerJoin("
          + "search(" + COLLECTIONORALIAS + ", q=\"side_s:left\", fl=\"id,join1_i,join2_s,ident_s\", sort=\"join1_i asc, join2_s asc, id asc\"),"
          + "search(" + COLLECTIONORALIAS + ", q=\"side_s:right\", fl=\"join3_i,join2_s,ident_s\", sort=\"join3_i asc, join2_s asc\", aliases=\"join3_i=aliasesField\"),"
          + "on=\"join1_i=aliasesField, join2_s=join2_s\")");
      stream = new InnerJoinStream(expression, factory);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);

      assert (tuples.size() == 8);
      assertOrder(tuples, 1, 1, 15, 15, 3, 4, 5, 7);
    } finally {
      solrClientCache.close();
    }
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
    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);
    
    StreamFactory factory = new StreamFactory()
      .withCollectionZkHost(COLLECTIONORALIAS, cluster.getZkServer().getZkAddress())
      .withFunctionName("search", CloudSolrStream.class)
      .withFunctionName("leftOuterJoin", LeftOuterJoinStream.class);
    
    // Basic test
    try {
      expression = StreamExpressionParser.parse("leftOuterJoin("
          + "search(" + COLLECTIONORALIAS + ", q=\"side_s:left\", fl=\"id,join1_i,join2_s,ident_s\", sort=\"join1_i asc, join2_s asc, id asc\"),"
          + "search(" + COLLECTIONORALIAS + ", q=\"side_s:right\", fl=\"join1_i,join2_s,ident_s\", sort=\"join1_i asc, join2_s asc\"),"
          + "on=\"join1_i=join1_i, join2_s=join2_s\")");
      stream = new LeftOuterJoinStream(expression, factory);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);
      assert (tuples.size() == 10);
      assertOrder(tuples, 1, 1, 15, 15, 2, 3, 4, 5, 6, 7);

      // Basic desc
      expression = StreamExpressionParser.parse("leftOuterJoin("
          + "search(" + COLLECTIONORALIAS + ", q=\"side_s:left\", fl=\"id,join1_i,join2_s,ident_s\", sort=\"join1_i desc, join2_s asc\"),"
          + "search(" + COLLECTIONORALIAS + ", q=\"side_s:right\", fl=\"join1_i,join2_s,ident_s\", sort=\"join1_i desc, join2_s asc\"),"
          + "on=\"join1_i=join1_i, join2_s=join2_s\")");
      stream = new LeftOuterJoinStream(expression, factory);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);
      assert (tuples.size() == 10);
      assertOrder(tuples, 7, 6, 3, 4, 5, 1, 1, 15, 15, 2);

      // Results in both searches, no join matches
      expression = StreamExpressionParser.parse("leftOuterJoin("
          + "search(" + COLLECTIONORALIAS + ", q=\"side_s:left\", fl=\"id,join1_i,join2_s,ident_s\", sort=\"ident_s asc\"),"
          + "search(" + COLLECTIONORALIAS + ", q=\"side_s:right\", fl=\"id,join1_i,join2_s,ident_s\", sort=\"ident_s asc\", aliases=\"id=right.id, join1_i=right.join1_i, join2_s=right.join2_s, ident_s=right.ident_s\"),"
          + "on=\"ident_s=right.ident_s\")");
      stream = new LeftOuterJoinStream(expression, factory);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);
      assert (tuples.size() == 8);
      assertOrder(tuples, 1, 15, 2, 3, 4, 5, 6, 7);

      // Differing field names
      expression = StreamExpressionParser.parse("leftOuterJoin("
          + "search(" + COLLECTIONORALIAS + ", q=\"side_s:left\", fl=\"id,join1_i,join2_s,ident_s\", sort=\"join1_i asc, join2_s asc, id asc\"),"
          + "search(" + COLLECTIONORALIAS + ", q=\"side_s:right\", fl=\"join3_i,join2_s,ident_s\", sort=\"join3_i asc, join2_s asc\", aliases=\"join3_i=aliasesField\"),"
          + "on=\"join1_i=aliasesField, join2_s=join2_s\")");
      stream = new LeftOuterJoinStream(expression, factory);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);
      assert (tuples.size() == 10);
      assertOrder(tuples, 1, 1, 15, 15, 2, 3, 4, 5, 6, 7);
    } finally {
      solrClientCache.close();
    }
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
    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);
    
    StreamFactory factory = new StreamFactory()
      .withCollectionZkHost(COLLECTIONORALIAS, cluster.getZkServer().getZkAddress())
      .withFunctionName("search", CloudSolrStream.class)
      .withFunctionName("hashJoin", HashJoinStream.class);
    try {
      // Basic test
      expression = StreamExpressionParser.parse("hashJoin("
          + "search(collection1, q=\"side_s:left\", fl=\"id,join1_i,join2_s,ident_s\", sort=\"join1_i asc, join2_s asc, id asc\"),"
          + "hashed=search(collection1, q=\"side_s:right\", fl=\"join1_i,join2_s,ident_s\", sort=\"join1_i asc, join2_s asc\"),"
          + "on=\"join1_i, join2_s\")");
      stream = new HashJoinStream(expression, factory);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);
      assert (tuples.size() == 8);
      assertOrder(tuples, 1, 1, 15, 15, 3, 4, 5, 7);

      // Basic desc
      expression = StreamExpressionParser.parse("hashJoin("
          + "search(collection1, q=\"side_s:left\", fl=\"id,join1_i,join2_s,ident_s\", sort=\"join1_i desc, join2_s asc\"),"
          + "hashed=search(collection1, q=\"side_s:right\", fl=\"join1_i,join2_s,ident_s\", sort=\"join1_i desc, join2_s asc\"),"
          + "on=\"join1_i, join2_s\")");
      stream = new HashJoinStream(expression, factory);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);
      assert (tuples.size() == 8);
      assertOrder(tuples, 7, 3, 4, 5, 1, 1, 15, 15);

      // Results in both searches, no join matches
      expression = StreamExpressionParser.parse("hashJoin("
          + "search(collection1, q=\"side_s:left\", fl=\"id,join1_i,join2_s,ident_s\", sort=\"ident_s asc\"),"
          + "hashed=search(collection1, q=\"side_s:right\", fl=\"id,join1_i,join2_s,ident_s\", sort=\"ident_s asc\"),"
          + "on=\"ident_s\")");
      stream = new HashJoinStream(expression, factory);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);
      assert (tuples.size() == 0);

      // Basic test with "on" mapping
      expression = StreamExpressionParser.parse("hashJoin("
          + "search(collection1, q=\"side_s:left\", fl=\"id,join1_i,join3_i,ident_s\", sort=\"join1_i asc, join3_i asc, id asc\"),"
          + "hashed=search(collection1, q=\"side_s:right\", fl=\"join1_i,join3_i,ident_s\", sort=\"join1_i asc, join3_i asc\"),"
          + "on=\"join1_i=join3_i\")");
      stream = new HashJoinStream(expression, factory);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);
      assertEquals(17, tuples.size());

      //Does a lexical sort
      assertOrder(tuples, 1, 1, 15, 15, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 5, 6, 7);
    } finally {
      solrClientCache.close();
    }
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
    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);
    
    StreamFactory factory = new StreamFactory()
        .withCollectionZkHost("collection1", cluster.getZkServer().getZkAddress())
      .withFunctionName("search", CloudSolrStream.class)
      .withFunctionName("outerHashJoin", OuterHashJoinStream.class);
    try {
      // Basic test
      expression = StreamExpressionParser.parse("outerHashJoin("
          + "search(collection1, q=\"side_s:left\", fl=\"id,join1_i,join2_s,ident_s\", sort=\"join1_i asc, join2_s asc, id asc\"),"
          + "hashed=search(collection1, q=\"side_s:right\", fl=\"join1_i,join2_s,ident_s\", sort=\"join1_i asc, join2_s asc\"),"
          + "on=\"join1_i, join2_s\")");
      stream = new OuterHashJoinStream(expression, factory);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);
      assert (tuples.size() == 10);
      assertOrder(tuples, 1, 1, 15, 15, 2, 3, 4, 5, 6, 7);

      // Basic desc
      expression = StreamExpressionParser.parse("outerHashJoin("
          + "search(collection1, q=\"side_s:left\", fl=\"id,join1_i,join2_s,ident_s\", sort=\"join1_i desc, join2_s asc\"),"
          + "hashed=search(collection1, q=\"side_s:right\", fl=\"join1_i,join2_s,ident_s\", sort=\"join1_i desc, join2_s asc\"),"
          + "on=\"join1_i, join2_s\")");
      stream = new OuterHashJoinStream(expression, factory);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);
      assert (tuples.size() == 10);
      assertOrder(tuples, 7, 6, 3, 4, 5, 1, 1, 15, 15, 2);

      // Results in both searches, no join matches
      expression = StreamExpressionParser.parse("outerHashJoin("
          + "search(collection1, q=\"side_s:left\", fl=\"id,join1_i,join2_s,ident_s\", sort=\"ident_s asc\"),"
          + "hashed=search(collection1, q=\"side_s:right\", fl=\"id,join1_i,join2_s,ident_s\", sort=\"ident_s asc\"),"
          + "on=\"ident_s\")");
      stream = new OuterHashJoinStream(expression, factory);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);
      assert (tuples.size() == 8);
      assertOrder(tuples, 1, 15, 2, 3, 4, 5, 6, 7);

      // Basic test
      expression = StreamExpressionParser.parse("outerHashJoin("
          + "search(collection1, q=\"side_s:left\", fl=\"id,join1_i,join2_s,ident_s\", sort=\"join1_i asc, join2_s asc, id asc\"),"
          + "hashed=search(collection1, q=\"side_s:right\", fl=\"join3_i,join2_s,ident_s\", sort=\"join2_s asc\"),"
          + "on=\"join1_i=join3_i, join2_s\")");
      stream = new OuterHashJoinStream(expression, factory);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);
      assert (tuples.size() == 10);
      assertOrder(tuples, 1, 1, 15, 15, 2, 3, 4, 5, 6, 7);
    } finally {
      solrClientCache.close();
    }
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

    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);

    StreamFactory factory = new StreamFactory()
      .withCollectionZkHost("collection1", cluster.getZkServer().getZkAddress())
      .withFunctionName("search", CloudSolrStream.class)
      .withFunctionName("innerJoin", InnerJoinStream.class)
      .withFunctionName("select", SelectStream.class)
      .withFunctionName("replace", ReplaceOperation.class)
      .withFunctionName("concat", ConcatOperation.class)
      .withFunctionName("add", AddEvaluator.class)
      .withFunctionName("if", IfThenElseEvaluator.class)
      .withFunctionName("gt", GreaterThanEvaluator.class)
      ;

    try {
      // Basic test
      clause = "select("
          + "id, join1_i as join1, join2_s as join2, ident_s as identity,"
          + "search(collection1, q=\"side_s:left\", fl=\"id,join1_i,join2_s,ident_s\", sort=\"join1_i asc, join2_s asc, id asc\")"
          + ")";

      stream = factory.constructStream(clause);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);
      assertFields(tuples, "id", "join1", "join2", "identity");
      assertNotFields(tuples, "join1_i", "join2_s", "ident_s");

      // Basic with replacements test
      clause = "select("
          + "id, join1_i as join1, join2_s as join2, ident_s as identity,"
          + "replace(join1, 0, withValue=12), replace(join1, 3, withValue=12), replace(join1, 2, withField=join2),"
          + "search(collection1, q=\"side_s:left\", fl=\"id,join1_i,join2_s,ident_s\", sort=\"join1_i asc, join2_s asc, id asc\")"
          + ")";
      stream = factory.constructStream(clause);
      stream.setStreamContext(streamContext);
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
          + "id, join1_i as join1, join2_s as join2, ident_s as identity,"
          + "replace(join1, 0, withValue=12), replace(join1, 3, withValue=12), replace(join1, 2, withField=join2),"
          + "concat(fields=\"identity,join1\", as=\"newIdentity\",delim=\"-\"),"
          + "search(collection1, q=\"side_s:left\", fl=\"id,join1_i,join2_s,ident_s\", sort=\"join1_i asc, join2_s asc, id asc\")"
          + ")";
      stream = factory.constructStream(clause);
      stream.setStreamContext(streamContext);
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
          + "select("
          + "id, join1_i as left.join1, join2_s as left.join2, ident_s as left.ident,"
          + "search(collection1, q=\"side_s:left\", fl=\"id,join1_i,join2_s,ident_s\", sort=\"join1_i asc, join2_s asc, id asc\")"
          + "),"
          + "select("
          + "join3_i as right.join1, join2_s as right.join2, ident_s as right.ident,"
          + "search(collection1, q=\"side_s:right\", fl=\"join3_i,join2_s,ident_s\", sort=\"join3_i asc, join2_s asc\"),"
          + "),"
          + "on=\"left.join1=right.join1, left.join2=right.join2\""
          + ")";
      stream = factory.constructStream(clause);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);
      assertFields(tuples, "id", "left.join1", "left.join2", "left.ident", "right.join1", "right.join2", "right.ident");

      // Wrapped select test
      clause = "select("
          + "id, left.ident, right.ident,"
          + "innerJoin("
          + "select("
          + "id, join1_i as left.join1, join2_s as left.join2, ident_s as left.ident,"
          + "search(collection1, q=\"side_s:left\", fl=\"id,join1_i,join2_s,ident_s\", sort=\"join1_i asc, join2_s asc, id asc\")"
          + "),"
          + "select("
          + "join3_i as right.join1, join2_s as right.join2, ident_s as right.ident,"
          + "search(collection1, q=\"side_s:right\", fl=\"join3_i,join2_s,ident_s\", sort=\"join3_i asc, join2_s asc\"),"
          + "),"
          + "on=\"left.join1=right.join1, left.join2=right.join2\""
          + ")"
          + ")";
      stream = factory.constructStream(clause);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);
      assertFields(tuples, "id", "left.ident", "right.ident");
      assertNotFields(tuples, "left.join1", "left.join2", "right.join1", "right.join2");
    } finally {
      solrClientCache.close();
    }
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
  public void testPriorityStream() throws Exception {
    Assume.assumeTrue(!useAlias);

    new UpdateRequest()
        .add(id, "0", "a_s", "hello1", "a_i", "0", "a_f", "1")
        .add(id, "2", "a_s", "hello1", "a_i", "2", "a_f", "2")
        .add(id, "3", "a_s", "hello1", "a_i", "3", "a_f", "3")
        .add(id, "4", "a_s", "hello1", "a_i", "4", "a_f", "4")
        .add(id, "1", "a_s", "hello1", "a_i", "1", "a_f", "5")
        .add(id, "5", "a_s", "hello", "a_i", "10", "a_f", "6")
        .add(id, "6", "a_s", "hello", "a_i", "11", "a_f", "7")
        .add(id, "7", "a_s", "hello", "a_i", "12", "a_f", "8")
        .add(id, "8", "a_s", "hello", "a_i", "13", "a_f", "9")
        .add(id, "9", "a_s", "hello1", "a_i", "14", "a_f", "10")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    StreamFactory factory = new StreamFactory()
        .withCollectionZkHost("collection1", cluster.getZkServer().getZkAddress())
        .withFunctionName("topic", TopicStream.class)
        .withFunctionName("priority", PriorityStream.class);

    StreamExpression expression;
    TupleStream stream;
    List<Tuple> tuples;

    SolrClientCache cache = new SolrClientCache();

    try {
      FieldComparator comp = new FieldComparator("a_i", ComparatorOrder.ASCENDING);

      expression = StreamExpressionParser.parse("priority(topic(collection1, collection1, q=\"a_s:hello\", fl=\"id,a_i\", id=1000000, initialCheckpoint=0)," +
          "topic(collection1, collection1, q=\"a_s:hello1\", fl=\"id,a_i\", id=2000000, initialCheckpoint=0))");
      stream = factory.constructStream(expression);
      StreamContext context = new StreamContext();
      context.setSolrClientCache(cache);
      stream.setStreamContext(context);
      tuples = getTuples(stream);

      Collections.sort(tuples, comp);
      //The tuples from the first topic (high priority) should be returned.

      assertEquals(tuples.size(), 4);
      assertOrder(tuples, 5, 6, 7, 8);

      expression = StreamExpressionParser.parse("priority(topic(collection1, collection1, q=\"a_s:hello\", fl=\"id,a_i\", id=1000000, initialCheckpoint=0)," +
          "topic(collection1, collection1, q=\"a_s:hello1\", fl=\"id,a_i\", id=2000000, initialCheckpoint=0))");
      stream = factory.constructStream(expression);
      context = new StreamContext();
      context.setSolrClientCache(cache);
      stream.setStreamContext(context);
      tuples = getTuples(stream);
      Collections.sort(tuples, comp);

      //The Tuples from the second topic (Low priority) should be returned.
      assertEquals(tuples.size(), 6);
      assertOrder(tuples, 0, 1, 2, 3, 4, 9);

      expression = StreamExpressionParser.parse("priority(topic(collection1, collection1, q=\"a_s:hello\", fl=\"id,a_i\", id=1000000, initialCheckpoint=0)," +
          "topic(collection1, collection1, q=\"a_s:hello1\", fl=\"id,a_i\", id=2000000, initialCheckpoint=0))");
      stream = factory.constructStream(expression);
      context = new StreamContext();
      context.setSolrClientCache(cache);
      stream.setStreamContext(context);
      tuples = getTuples(stream);

      //Both queus are empty.
      assertEquals(tuples.size(), 0);

    } finally {
      cache.close();
    }
  }

  @Test
  public void testParallelPriorityStream() throws Exception {
    Assume.assumeTrue(!useAlias);

    new UpdateRequest()
        .add(id, "0", "a_s", "hello1", "a_i", "0", "a_f", "1")
        .add(id, "2", "a_s", "hello1", "a_i", "2", "a_f", "2")
        .add(id, "3", "a_s", "hello1", "a_i", "3", "a_f", "3")
        .add(id, "4", "a_s", "hello1", "a_i", "4", "a_f", "4")
        .add(id, "1", "a_s", "hello1", "a_i", "1", "a_f", "5")
        .add(id, "5", "a_s", "hello", "a_i", "10", "a_f", "6")
        .add(id, "6", "a_s", "hello", "a_i", "11", "a_f", "7")
        .add(id, "7", "a_s", "hello", "a_i", "12", "a_f", "8")
        .add(id, "8", "a_s", "hello", "a_i", "13", "a_f", "9")
        .add(id, "9", "a_s", "hello1", "a_i", "14", "a_f", "10")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    StreamFactory factory = new StreamFactory()
        .withCollectionZkHost("collection1", cluster.getZkServer().getZkAddress())
        .withFunctionName("topic", TopicStream.class)
        .withFunctionName("parallel", ParallelStream.class)
        .withFunctionName("priority", PriorityStream.class);

    StreamExpression expression;
    TupleStream stream;
    List<Tuple> tuples;

    SolrClientCache cache = new SolrClientCache();

    try {
      FieldComparator comp = new FieldComparator("a_i", ComparatorOrder.ASCENDING);

      expression = StreamExpressionParser.parse("parallel(collection1, workers=2, sort=\"_version_ asc\", priority(topic(collection1, collection1, q=\"a_s:hello\", fl=\"id,a_i\", id=1000000, initialCheckpoint=0, partitionKeys=id)," +
          "topic(collection1, collection1, q=\"a_s:hello1\", fl=\"id,a_i\", id=2000000, initialCheckpoint=0, partitionKeys=id)))");
      stream = factory.constructStream(expression);
      StreamContext context = new StreamContext();
      context.setSolrClientCache(cache);
      stream.setStreamContext(context);
      tuples = getTuples(stream);

      Collections.sort(tuples, comp);
      //The tuples from the first topic (high priority) should be returned.

      assertEquals(tuples.size(), 4);
      assertOrder(tuples, 5, 6, 7, 8);

      expression = StreamExpressionParser.parse("parallel(collection1, workers=2, sort=\"_version_ asc\", priority(topic(collection1, collection1, q=\"a_s:hello\", fl=\"id,a_i\", id=1000000, initialCheckpoint=0, partitionKeys=id)," +
          "topic(collection1, collection1, q=\"a_s:hello1\", fl=\"id,a_i\", id=2000000, initialCheckpoint=0, partitionKeys=id)))");
      stream = factory.constructStream(expression);
      context = new StreamContext();
      context.setSolrClientCache(cache);
      stream.setStreamContext(context);
      tuples = getTuples(stream);
      Collections.sort(tuples, comp);

      //The Tuples from the second topic (Low priority) should be returned.
      assertEquals(tuples.size(), 6);
      assertOrder(tuples, 0, 1, 2, 3, 4, 9);

      expression = StreamExpressionParser.parse("parallel(collection1, workers=2, sort=\"_version_ asc\", priority(topic(collection1, collection1, q=\"a_s:hello\", fl=\"id,a_i\", id=1000000, initialCheckpoint=0, partitionKeys=id)," +
          "topic(collection1, collection1, q=\"a_s:hello1\", fl=\"id,a_i\", id=2000000, initialCheckpoint=0, partitionKeys=id)))");
      stream = factory.constructStream(expression);
      context = new StreamContext();
      context.setSolrClientCache(cache);
      stream.setStreamContext(context);
      tuples = getTuples(stream);

      //Both queus are empty.
      assertEquals(tuples.size(), 0);

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
        .add(id, "5", "a_s", "hello", "a_i", "10", "a_f", "6", "subject", "ha ha bla blah6")
        .add(id, "6", "a_s", "hello", "a_i", "11", "a_f", "7", "subject", "ha ha bla blah7")
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
      assertTopicRun(stream, "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11");

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
      assertTopicRun(stream, "12", "13");

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
        .add(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "0", "s_multi", "aaaa", "s_multi", "bbbb", "i_multi", "4", "i_multi", "7")
        .add(id, "2", "a_s", "hello2", "a_i", "2", "a_f", "0", "s_multi", "aaaa1", "s_multi", "bbbb1", "i_multi", "44", "i_multi", "77")
        .add(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3", "s_multi", "aaaa2", "s_multi", "bbbb2", "i_multi", "444", "i_multi", "777")
        .add(id, "4", "a_s", "hello4", "a_i", "4", "a_f", "4", "s_multi", "aaaa3", "s_multi", "bbbb3", "i_multi", "4444", "i_multi", "7777")
        .add(id, "1", "a_s", "hello1", "a_i", "1", "a_f", "1", "s_multi", "aaaa4", "s_multi", "bbbb4", "i_multi", "44444", "i_multi", "77777")
        .commit(cluster.getSolrClient(), "collection1");
    
    StreamExpression expression;
    TupleStream stream;
    Tuple t;
    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);

    StreamFactory factory = new StreamFactory()
      .withCollectionZkHost("collection1", cluster.getZkServer().getZkAddress())
      .withCollectionZkHost("destinationCollection", cluster.getZkServer().getZkAddress())
      .withFunctionName("search", CloudSolrStream.class)
      .withFunctionName("update", UpdateStream.class);

    try {
      //Copy all docs to destinationCollection
      expression = StreamExpressionParser.parse("update(destinationCollection, batchSize=5, search(collection1, q=*:*, fl=\"id,a_s,a_i,a_f,s_multi,i_multi\", sort=\"a_f asc, a_i asc\"))");
      stream = new UpdateStream(expression, factory);
      stream.setStreamContext(streamContext);
      List<Tuple> tuples = getTuples(stream);
      cluster.getSolrClient().commit("destinationCollection");

      //Ensure that all UpdateStream tuples indicate the correct number of copied/indexed docs
      assert (tuples.size() == 1);
      t = tuples.get(0);
      assert (t.EOF == false);
      assertEquals(5, t.get("batchIndexed"));

      //Ensure that destinationCollection actually has the new docs.
      expression = StreamExpressionParser.parse("search(destinationCollection, q=*:*, fl=\"id,a_s,a_i,a_f,s_multi,i_multi\", sort=\"a_i asc\")");
      stream = new CloudSolrStream(expression, factory);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);
      assertEquals(5, tuples.size());

      Tuple tuple = tuples.get(0);
      assert (tuple.getLong("id") == 0);
      assert (tuple.get("a_s").equals("hello0"));
      assert (tuple.getLong("a_i") == 0);
      assert (tuple.getDouble("a_f") == 0.0);
      assertList(tuple.getStrings("s_multi"), "aaaa", "bbbb");
      assertList(tuple.getLongs("i_multi"), Long.parseLong("4"), Long.parseLong("7"));

      tuple = tuples.get(1);
      assert (tuple.getLong("id") == 1);
      assert (tuple.get("a_s").equals("hello1"));
      assert (tuple.getLong("a_i") == 1);
      assert (tuple.getDouble("a_f") == 1.0);
      assertList(tuple.getStrings("s_multi"), "aaaa4", "bbbb4");
      assertList(tuple.getLongs("i_multi"), Long.parseLong("44444"), Long.parseLong("77777"));

      tuple = tuples.get(2);
      assert (tuple.getLong("id") == 2);
      assert (tuple.get("a_s").equals("hello2"));
      assert (tuple.getLong("a_i") == 2);
      assert (tuple.getDouble("a_f") == 0.0);
      assertList(tuple.getStrings("s_multi"), "aaaa1", "bbbb1");
      assertList(tuple.getLongs("i_multi"), Long.parseLong("44"), Long.parseLong("77"));

      tuple = tuples.get(3);
      assert (tuple.getLong("id") == 3);
      assert (tuple.get("a_s").equals("hello3"));
      assert (tuple.getLong("a_i") == 3);
      assert (tuple.getDouble("a_f") == 3.0);
      assertList(tuple.getStrings("s_multi"), "aaaa2", "bbbb2");
      assertList(tuple.getLongs("i_multi"), Long.parseLong("444"), Long.parseLong("777"));

      tuple = tuples.get(4);
      assert (tuple.getLong("id") == 4);
      assert (tuple.get("a_s").equals("hello4"));
      assert (tuple.getLong("a_i") == 4);
      assert (tuple.getDouble("a_f") == 4.0);
      assertList(tuple.getStrings("s_multi"), "aaaa3", "bbbb3");
      assertList(tuple.getLongs("i_multi"), Long.parseLong("4444"), Long.parseLong("7777"));
    } finally {
      CollectionAdminRequest.deleteCollection("destinationCollection").process(cluster.getSolrClient());
      solrClientCache.close();
    }
  }

  @Test
  public void testParallelUpdateStream() throws Exception {

    CollectionAdminRequest.createCollection("parallelDestinationCollection", "conf", 2, 1).process(cluster.getSolrClient());
    AbstractDistribZkTestBase.waitForRecoveriesToFinish("parallelDestinationCollection", cluster.getSolrClient().getZkStateReader(),
        false, true, TIMEOUT);

    new UpdateRequest()
        .add(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "0", "s_multi", "aaaa", "s_multi", "bbbb", "i_multi", "4", "i_multi", "7")
        .add(id, "2", "a_s", "hello2", "a_i", "2", "a_f", "0", "s_multi", "aaaa1", "s_multi", "bbbb1", "i_multi", "44", "i_multi", "77")
        .add(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3", "s_multi", "aaaa2", "s_multi", "bbbb2", "i_multi", "444", "i_multi", "777")
        .add(id, "4", "a_s", "hello4", "a_i", "4", "a_f", "4", "s_multi", "aaaa3", "s_multi", "bbbb3", "i_multi", "4444", "i_multi", "7777")
        .add(id, "1", "a_s", "hello1", "a_i", "1", "a_f", "1", "s_multi", "aaaa4", "s_multi", "bbbb4", "i_multi", "44444", "i_multi", "77777")
        .commit(cluster.getSolrClient(), "collection1");
    
    StreamExpression expression;
    TupleStream stream;
    Tuple t;
    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);
    
    String zkHost = cluster.getZkServer().getZkAddress();
    StreamFactory factory = new StreamFactory()
      .withCollectionZkHost("collection1", cluster.getZkServer().getZkAddress())
      .withCollectionZkHost("parallelDestinationCollection", cluster.getZkServer().getZkAddress())
      .withFunctionName("search", CloudSolrStream.class)
      .withFunctionName("update", UpdateStream.class)
      .withFunctionName("parallel", ParallelStream.class);

    try {
      //Copy all docs to destinationCollection
      String updateExpression = "update(parallelDestinationCollection, batchSize=2, search(collection1, q=*:*, fl=\"id,a_s,a_i,a_f,s_multi,i_multi\", sort=\"a_f asc, a_i asc\", partitionKeys=\"a_f\"))";
      TupleStream parallelUpdateStream = factory.constructStream("parallel(collection1, " + updateExpression + ", workers=\"2\", zkHost=\"" + zkHost + "\", sort=\"batchNumber asc\")");
      parallelUpdateStream.setStreamContext(streamContext);
      List<Tuple> tuples = getTuples(parallelUpdateStream);
      cluster.getSolrClient().commit("parallelDestinationCollection");

      //Ensure that all UpdateStream tuples indicate the correct number of copied/indexed docs
      long count = 0;

      for (Tuple tuple : tuples) {
        count += tuple.getLong("batchIndexed");
      }

      assert (count == 5);

      //Ensure that destinationCollection actually has the new docs.
      expression = StreamExpressionParser.parse("search(parallelDestinationCollection, q=*:*, fl=\"id,a_s,a_i,a_f,s_multi,i_multi\", sort=\"a_i asc\")");
      stream = new CloudSolrStream(expression, factory);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);
      assertEquals(5, tuples.size());

      Tuple tuple = tuples.get(0);
      assert (tuple.getLong("id") == 0);
      assert (tuple.get("a_s").equals("hello0"));
      assert (tuple.getLong("a_i") == 0);
      assert (tuple.getDouble("a_f") == 0.0);
      assertList(tuple.getStrings("s_multi"), "aaaa", "bbbb");
      assertList(tuple.getLongs("i_multi"), Long.parseLong("4"), Long.parseLong("7"));

      tuple = tuples.get(1);
      assert (tuple.getLong("id") == 1);
      assert (tuple.get("a_s").equals("hello1"));
      assert (tuple.getLong("a_i") == 1);
      assert (tuple.getDouble("a_f") == 1.0);
      assertList(tuple.getStrings("s_multi"), "aaaa4", "bbbb4");
      assertList(tuple.getLongs("i_multi"), Long.parseLong("44444"), Long.parseLong("77777"));

      tuple = tuples.get(2);
      assert (tuple.getLong("id") == 2);
      assert (tuple.get("a_s").equals("hello2"));
      assert (tuple.getLong("a_i") == 2);
      assert (tuple.getDouble("a_f") == 0.0);
      assertList(tuple.getStrings("s_multi"), "aaaa1", "bbbb1");
      assertList(tuple.getLongs("i_multi"), Long.parseLong("44"), Long.parseLong("77"));

      tuple = tuples.get(3);
      assert (tuple.getLong("id") == 3);
      assert (tuple.get("a_s").equals("hello3"));
      assert (tuple.getLong("a_i") == 3);
      assert (tuple.getDouble("a_f") == 3.0);
      assertList(tuple.getStrings("s_multi"), "aaaa2", "bbbb2");
      assertList(tuple.getLongs("i_multi"), Long.parseLong("444"), Long.parseLong("777"));

      tuple = tuples.get(4);
      assert (tuple.getLong("id") == 4);
      assert (tuple.get("a_s").equals("hello4"));
      assert (tuple.getLong("a_i") == 4);
      assert (tuple.getDouble("a_f") == 4.0);
      assertList(tuple.getStrings("s_multi"), "aaaa3", "bbbb3");
      assertList(tuple.getLongs("i_multi"), Long.parseLong("4444"), Long.parseLong("7777"));
    } finally {
      CollectionAdminRequest.deleteCollection("parallelDestinationCollection").process(cluster.getSolrClient());
      solrClientCache.close();
    }
  }

  @Test
  public void testParallelDaemonUpdateStream() throws Exception {

    CollectionAdminRequest.createCollection("parallelDestinationCollection1", "conf", 2, 1).process(cluster.getSolrClient());
    AbstractDistribZkTestBase.waitForRecoveriesToFinish("parallelDestinationCollection1", cluster.getSolrClient().getZkStateReader(),
        false, true, TIMEOUT);

    new UpdateRequest()
        .add(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "0", "s_multi", "aaaa", "s_multi", "bbbb", "i_multi", "4", "i_multi", "7")
        .add(id, "2", "a_s", "hello2", "a_i", "2", "a_f", "0", "s_multi", "aaaa1", "s_multi", "bbbb1", "i_multi", "44", "i_multi", "77")
        .add(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3", "s_multi", "aaaa2", "s_multi", "bbbb2", "i_multi", "444", "i_multi", "777")
        .add(id, "4", "a_s", "hello4", "a_i", "4", "a_f", "4", "s_multi", "aaaa3", "s_multi", "bbbb3", "i_multi", "4444", "i_multi", "7777")
        .add(id, "1", "a_s", "hello1", "a_i", "1", "a_f", "1", "s_multi", "aaaa4", "s_multi", "bbbb4", "i_multi", "44444", "i_multi", "77777")
        .commit(cluster.getSolrClient(), "collection1");

    StreamExpression expression;
    TupleStream stream;
    Tuple t;
    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);

    String zkHost = cluster.getZkServer().getZkAddress();
    StreamFactory factory = new StreamFactory()
        .withCollectionZkHost("collection1", cluster.getZkServer().getZkAddress())
        .withCollectionZkHost("parallelDestinationCollection1", cluster.getZkServer().getZkAddress())
        .withFunctionName("search", CloudSolrStream.class)
        .withFunctionName("update", UpdateStream.class)
        .withFunctionName("parallel", ParallelStream.class)
        .withFunctionName("daemon", DaemonStream.class);

    try {
      //Copy all docs to destinationCollection
      String updateExpression = "daemon(update(parallelDestinationCollection1, batchSize=2, search(collection1, q=*:*, fl=\"id,a_s,a_i,a_f,s_multi,i_multi\", sort=\"a_f asc, a_i asc\", partitionKeys=\"a_f\")), runInterval=\"1000\", id=\"test\")";
      TupleStream parallelUpdateStream = factory.constructStream("parallel(collection1, " + updateExpression + ", workers=\"2\", zkHost=\"" + zkHost + "\", sort=\"batchNumber asc\")");
      parallelUpdateStream.setStreamContext(streamContext);
      List<Tuple> tuples = getTuples(parallelUpdateStream);
      assert (tuples.size() == 2);

      //Lets sleep long enough for daemon updates to run.
      //Lets stop the daemons
      ModifiableSolrParams sParams = new ModifiableSolrParams(StreamingTest.mapParams(CommonParams.QT, "/stream", "action", "list"));

      int workersComplete = 0;
      for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
        int iterations = 0;
        INNER:
        while (iterations == 0) {
          SolrStream solrStream = new SolrStream(jetty.getBaseUrl().toString() + "/collection1", sParams);
          solrStream.setStreamContext(streamContext);
          solrStream.open();
          Tuple tupleResponse = solrStream.read();
          if (tupleResponse.EOF) {
            solrStream.close();
            break INNER;
          } else {
            long l = tupleResponse.getLong("iterations");
            if (l > 0) {
              ++workersComplete;
            } else {
              try {
                Thread.sleep(1000);
              } catch (Exception e) {

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
        solrStream.setStreamContext(streamContext);
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
        while (stopTime == 0) {
          SolrStream solrStream = new SolrStream(jetty.getBaseUrl() + "/collection1", sParams);
          solrStream.setStreamContext(streamContext);
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
              } catch (Exception e) {

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
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);
      assertEquals(5, tuples.size());

      Tuple tuple = tuples.get(0);
      assert (tuple.getLong("id") == 0);
      assert (tuple.get("a_s").equals("hello0"));
      assert (tuple.getLong("a_i") == 0);
      assert (tuple.getDouble("a_f") == 0.0);
      assertList(tuple.getStrings("s_multi"), "aaaa", "bbbb");
      assertList(tuple.getLongs("i_multi"), Long.parseLong("4"), Long.parseLong("7"));

      tuple = tuples.get(1);
      assert (tuple.getLong("id") == 1);
      assert (tuple.get("a_s").equals("hello1"));
      assert (tuple.getLong("a_i") == 1);
      assert (tuple.getDouble("a_f") == 1.0);
      assertList(tuple.getStrings("s_multi"), "aaaa4", "bbbb4");
      assertList(tuple.getLongs("i_multi"), Long.parseLong("44444"), Long.parseLong("77777"));

      tuple = tuples.get(2);
      assert (tuple.getLong("id") == 2);
      assert (tuple.get("a_s").equals("hello2"));
      assert (tuple.getLong("a_i") == 2);
      assert (tuple.getDouble("a_f") == 0.0);
      assertList(tuple.getStrings("s_multi"), "aaaa1", "bbbb1");
      assertList(tuple.getLongs("i_multi"), Long.parseLong("44"), Long.parseLong("77"));

      tuple = tuples.get(3);
      assert (tuple.getLong("id") == 3);
      assert (tuple.get("a_s").equals("hello3"));
      assert (tuple.getLong("a_i") == 3);
      assert (tuple.getDouble("a_f") == 3.0);
      assertList(tuple.getStrings("s_multi"), "aaaa2", "bbbb2");
      assertList(tuple.getLongs("i_multi"), Long.parseLong("444"), Long.parseLong("777"));

      tuple = tuples.get(4);
      assert (tuple.getLong("id") == 4);
      assert (tuple.get("a_s").equals("hello4"));
      assert (tuple.getLong("a_i") == 4);
      assert (tuple.getDouble("a_f") == 4.0);
      assertList(tuple.getStrings("s_multi"), "aaaa3", "bbbb3");
      assertList(tuple.getLongs("i_multi"), Long.parseLong("4444"), Long.parseLong("7777"));
    } finally {
      CollectionAdminRequest.deleteCollection("parallelDestinationCollection1").process(cluster.getSolrClient());
      solrClientCache.close();
    }
  }


  @Test
  public void testParallelTerminatingDaemonUpdateStream() throws Exception {
    Assume.assumeTrue(!useAlias);

    CollectionAdminRequest.createCollection("parallelDestinationCollection1", "conf", 2, 1).process(cluster.getSolrClient());
    AbstractDistribZkTestBase.waitForRecoveriesToFinish("parallelDestinationCollection1", cluster.getSolrClient().getZkStateReader(),
        false, true, TIMEOUT);

    new UpdateRequest()
        .add(id, "0", "a_s", "hello", "a_i", "0", "a_f", "0", "s_multi", "aaaa", "s_multi", "bbbb", "i_multi", "4", "i_multi", "7")
        .add(id, "2", "a_s", "hello", "a_i", "2", "a_f", "0", "s_multi", "aaaa1", "s_multi", "bbbb1", "i_multi", "44", "i_multi", "77")
        .add(id, "3", "a_s", "hello", "a_i", "3", "a_f", "3", "s_multi", "aaaa2", "s_multi", "bbbb2", "i_multi", "444", "i_multi", "777")
        .add(id, "4", "a_s", "hello", "a_i", "4", "a_f", "4", "s_multi", "aaaa3", "s_multi", "bbbb3", "i_multi", "4444", "i_multi", "7777")
        .add(id, "1", "a_s", "hello", "a_i", "1", "a_f", "1", "s_multi", "aaaa4", "s_multi", "bbbb4", "i_multi", "44444", "i_multi", "77777")
        .commit(cluster.getSolrClient(), "collection1");

    StreamExpression expression;
    TupleStream stream;
    Tuple t;
    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);

    String zkHost = cluster.getZkServer().getZkAddress();
    StreamFactory factory = new StreamFactory()
        .withCollectionZkHost("collection1", cluster.getZkServer().getZkAddress())
        .withCollectionZkHost("parallelDestinationCollection1", cluster.getZkServer().getZkAddress())
        .withFunctionName("topic", TopicStream.class)
        .withFunctionName("update", UpdateStream.class)
        .withFunctionName("parallel", ParallelStream.class)
        .withFunctionName("daemon", DaemonStream.class);

    try {
      //Copy all docs to destinationCollection
      String updateExpression = "daemon(update(parallelDestinationCollection1, batchSize=2, topic(collection1, collection1, q=\"a_s:hello\", fl=\"id,a_s,a_i,a_f,s_multi,i_multi\", partitionKeys=\"a_f\", initialCheckpoint=0, id=\"topic1\")), terminate=true, runInterval=\"1000\", id=\"test\")";
      TupleStream parallelUpdateStream = factory.constructStream("parallel(collection1, " + updateExpression + ", workers=\"2\", zkHost=\"" + zkHost + "\", sort=\"batchNumber asc\")");
      parallelUpdateStream.setStreamContext(streamContext);
      List<Tuple> tuples = getTuples(parallelUpdateStream);
      assert (tuples.size() == 2);


      ModifiableSolrParams sParams = new ModifiableSolrParams(StreamingTest.mapParams(CommonParams.QT, "/stream", "action", "list"));

      int workersComplete = 0;

      //Daemons should terminate after the topic is completed
      //Loop through all shards and wait for the daemons to be gone from the listing.
      for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
        INNER:
        while (true) {
          SolrStream solrStream = new SolrStream(jetty.getBaseUrl().toString() + "/collection1", sParams);
          solrStream.setStreamContext(streamContext);
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
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);
      assertEquals(5, tuples.size());

      Tuple tuple = tuples.get(0);
      assert (tuple.getLong("id") == 0);
      assert (tuple.get("a_s").equals("hello"));
      assert (tuple.getLong("a_i") == 0);
      assert (tuple.getDouble("a_f") == 0.0);
      assertList(tuple.getStrings("s_multi"), "aaaa", "bbbb");
      assertList(tuple.getLongs("i_multi"), Long.parseLong("4"), Long.parseLong("7"));

      tuple = tuples.get(1);
      assert (tuple.getLong("id") == 1);
      assert (tuple.get("a_s").equals("hello"));
      assert (tuple.getLong("a_i") == 1);
      assert (tuple.getDouble("a_f") == 1.0);
      assertList(tuple.getStrings("s_multi"), "aaaa4", "bbbb4");
      assertList(tuple.getLongs("i_multi"), Long.parseLong("44444"), Long.parseLong("77777"));

      tuple = tuples.get(2);
      assert (tuple.getLong("id") == 2);
      assert (tuple.get("a_s").equals("hello"));
      assert (tuple.getLong("a_i") == 2);
      assert (tuple.getDouble("a_f") == 0.0);
      assertList(tuple.getStrings("s_multi"), "aaaa1", "bbbb1");
      assertList(tuple.getLongs("i_multi"), Long.parseLong("44"), Long.parseLong("77"));

      tuple = tuples.get(3);
      assert (tuple.getLong("id") == 3);
      assert (tuple.get("a_s").equals("hello"));
      assert (tuple.getLong("a_i") == 3);
      assert (tuple.getDouble("a_f") == 3.0);
      assertList(tuple.getStrings("s_multi"), "aaaa2", "bbbb2");
      assertList(tuple.getLongs("i_multi"), Long.parseLong("444"), Long.parseLong("777"));

      tuple = tuples.get(4);
      assert (tuple.getLong("id") == 4);
      assert (tuple.get("a_s").equals("hello"));
      assert (tuple.getLong("a_i") == 4);
      assert (tuple.getDouble("a_f") == 4.0);
      assertList(tuple.getStrings("s_multi"), "aaaa3", "bbbb3");
      assertList(tuple.getLongs("i_multi"), Long.parseLong("4444"), Long.parseLong("7777"));
    } finally {
      CollectionAdminRequest.deleteCollection("parallelDestinationCollection1").process(cluster.getSolrClient());
      solrClientCache.close();
    }
  }



  ////////////////////////////////////////////
  @Test
  public void testCommitStream() throws Exception {

    CollectionAdminRequest.createCollection("destinationCollection", "conf", 2, 1).process(cluster.getSolrClient());
    AbstractDistribZkTestBase.waitForRecoveriesToFinish("destinationCollection", cluster.getSolrClient().getZkStateReader(),
        false, true, TIMEOUT);

    new UpdateRequest()
        .add(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "0", "s_multi", "aaaa", "s_multi", "bbbb", "i_multi", "4", "i_multi", "7")
        .add(id, "2", "a_s", "hello2", "a_i", "2", "a_f", "0", "s_multi", "aaaa1", "s_multi", "bbbb1", "i_multi", "44", "i_multi", "77")
        .add(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3", "s_multi", "aaaa2", "s_multi", "bbbb2", "i_multi", "444", "i_multi", "777")
        .add(id, "4", "a_s", "hello4", "a_i", "4", "a_f", "4", "s_multi", "aaaa3", "s_multi", "bbbb3", "i_multi", "4444", "i_multi", "7777")
        .add(id, "1", "a_s", "hello1", "a_i", "1", "a_f", "1", "s_multi", "aaaa4", "s_multi", "bbbb4", "i_multi", "44444", "i_multi", "77777")
        .commit(cluster.getSolrClient(), "collection1");
    
    StreamExpression expression;
    TupleStream stream;
    Tuple t;
    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);
    
    StreamFactory factory = new StreamFactory()
      .withCollectionZkHost("collection1", cluster.getZkServer().getZkAddress())
      .withCollectionZkHost("destinationCollection", cluster.getZkServer().getZkAddress())
      .withFunctionName("search", CloudSolrStream.class)
      .withFunctionName("update", UpdateStream.class)
      .withFunctionName("commit", CommitStream.class);

    try {
      //Copy all docs to destinationCollection
      expression = StreamExpressionParser.parse("commit(destinationCollection, batchSize=2, update(destinationCollection, batchSize=5, search(collection1, q=*:*, fl=\"id,a_s,a_i,a_f,s_multi,i_multi\", sort=\"a_f asc, a_i asc\")))");
      stream = factory.constructStream(expression);
      stream.setStreamContext(streamContext);
      List<Tuple> tuples = getTuples(stream);

      //Ensure that all CommitStream tuples indicate the correct number of copied/indexed docs
      assert (tuples.size() == 1);
      t = tuples.get(0);
      assert (t.EOF == false);
      assertEquals(5, t.get("batchIndexed"));

      //Ensure that destinationCollection actually has the new docs.
      expression = StreamExpressionParser.parse("search(destinationCollection, q=*:*, fl=\"id,a_s,a_i,a_f,s_multi,i_multi\", sort=\"a_i asc\")");
      stream = new CloudSolrStream(expression, factory);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);
      assertEquals(5, tuples.size());

      Tuple tuple = tuples.get(0);
      assert (tuple.getLong("id") == 0);
      assert (tuple.get("a_s").equals("hello0"));
      assert (tuple.getLong("a_i") == 0);
      assert (tuple.getDouble("a_f") == 0.0);
      assertList(tuple.getStrings("s_multi"), "aaaa", "bbbb");
      assertList(tuple.getLongs("i_multi"), Long.parseLong("4"), Long.parseLong("7"));

      tuple = tuples.get(1);
      assert (tuple.getLong("id") == 1);
      assert (tuple.get("a_s").equals("hello1"));
      assert (tuple.getLong("a_i") == 1);
      assert (tuple.getDouble("a_f") == 1.0);
      assertList(tuple.getStrings("s_multi"), "aaaa4", "bbbb4");
      assertList(tuple.getLongs("i_multi"), Long.parseLong("44444"), Long.parseLong("77777"));

      tuple = tuples.get(2);
      assert (tuple.getLong("id") == 2);
      assert (tuple.get("a_s").equals("hello2"));
      assert (tuple.getLong("a_i") == 2);
      assert (tuple.getDouble("a_f") == 0.0);
      assertList(tuple.getStrings("s_multi"), "aaaa1", "bbbb1");
      assertList(tuple.getLongs("i_multi"), Long.parseLong("44"), Long.parseLong("77"));

      tuple = tuples.get(3);
      assert (tuple.getLong("id") == 3);
      assert (tuple.get("a_s").equals("hello3"));
      assert (tuple.getLong("a_i") == 3);
      assert (tuple.getDouble("a_f") == 3.0);
      assertList(tuple.getStrings("s_multi"), "aaaa2", "bbbb2");
      assertList(tuple.getLongs("i_multi"), Long.parseLong("444"), Long.parseLong("777"));

      tuple = tuples.get(4);
      assert (tuple.getLong("id") == 4);
      assert (tuple.get("a_s").equals("hello4"));
      assert (tuple.getLong("a_i") == 4);
      assert (tuple.getDouble("a_f") == 4.0);
      assertList(tuple.getStrings("s_multi"), "aaaa3", "bbbb3");
      assertList(tuple.getLongs("i_multi"), Long.parseLong("4444"), Long.parseLong("7777"));
    } finally {
      CollectionAdminRequest.deleteCollection("destinationCollection").process(cluster.getSolrClient());
      solrClientCache.close();
    }
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
    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);
    
    String zkHost = cluster.getZkServer().getZkAddress();
    StreamFactory factory = new StreamFactory()
      .withCollectionZkHost("collection1", cluster.getZkServer().getZkAddress())
      .withCollectionZkHost("parallelDestinationCollection", cluster.getZkServer().getZkAddress())
      .withFunctionName("search", CloudSolrStream.class)
      .withFunctionName("update", UpdateStream.class)
      .withFunctionName("commit", CommitStream.class)
      .withFunctionName("parallel", ParallelStream.class);

    try {
      //Copy all docs to destinationCollection
      String updateExpression = "commit(parallelDestinationCollection, batchSize=0, zkHost=\"" + cluster.getZkServer().getZkAddress() + "\", update(parallelDestinationCollection, batchSize=2, search(collection1, q=*:*, fl=\"id,a_s,a_i,a_f,s_multi,i_multi\", sort=\"a_f asc, a_i asc\", partitionKeys=\"a_f\")))";
      TupleStream parallelUpdateStream = factory.constructStream("parallel(collection1, " + updateExpression + ", workers=\"2\", zkHost=\"" + zkHost + "\", sort=\"batchNumber asc\")");
      parallelUpdateStream.setStreamContext(streamContext);
      List<Tuple> tuples = getTuples(parallelUpdateStream);

      //Ensure that all UpdateStream tuples indicate the correct number of copied/indexed docs
      long count = 0;

      for (Tuple tuple : tuples) {
        count += tuple.getLong("batchIndexed");
      }

      assert (count == 5);


      //Ensure that destinationCollection actually has the new docs.
      expression = StreamExpressionParser.parse("search(parallelDestinationCollection, q=*:*, fl=\"id,a_s,a_i,a_f,s_multi,i_multi\", sort=\"a_i asc\")");
      stream = new CloudSolrStream(expression, factory);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);
      assertEquals(5, tuples.size());

      Tuple tuple = tuples.get(0);
      assert (tuple.getLong("id") == 0);
      assert (tuple.get("a_s").equals("hello0"));
      assert (tuple.getLong("a_i") == 0);
      assert (tuple.getDouble("a_f") == 0.0);
      assertList(tuple.getStrings("s_multi"), "aaaa", "bbbb");
      assertList(tuple.getLongs("i_multi"), Long.parseLong("4"), Long.parseLong("7"));

      tuple = tuples.get(1);
      assert (tuple.getLong("id") == 1);
      assert (tuple.get("a_s").equals("hello1"));
      assert (tuple.getLong("a_i") == 1);
      assert (tuple.getDouble("a_f") == 1.0);
      assertList(tuple.getStrings("s_multi"), "aaaa4", "bbbb4");
      assertList(tuple.getLongs("i_multi"), Long.parseLong("44444"), Long.parseLong("77777"));

      tuple = tuples.get(2);
      assert (tuple.getLong("id") == 2);
      assert (tuple.get("a_s").equals("hello2"));
      assert (tuple.getLong("a_i") == 2);
      assert (tuple.getDouble("a_f") == 0.0);
      assertList(tuple.getStrings("s_multi"), "aaaa1", "bbbb1");
      assertList(tuple.getLongs("i_multi"), Long.parseLong("44"), Long.parseLong("77"));

      tuple = tuples.get(3);
      assert (tuple.getLong("id") == 3);
      assert (tuple.get("a_s").equals("hello3"));
      assert (tuple.getLong("a_i") == 3);
      assert (tuple.getDouble("a_f") == 3.0);
      assertList(tuple.getStrings("s_multi"), "aaaa2", "bbbb2");
      assertList(tuple.getLongs("i_multi"), Long.parseLong("444"), Long.parseLong("777"));

      tuple = tuples.get(4);
      assert (tuple.getLong("id") == 4);
      assert (tuple.get("a_s").equals("hello4"));
      assert (tuple.getLong("a_i") == 4);
      assert (tuple.getDouble("a_f") == 4.0);
      assertList(tuple.getStrings("s_multi"), "aaaa3", "bbbb3");
      assertList(tuple.getLongs("i_multi"), Long.parseLong("4444"), Long.parseLong("7777"));
    } finally {
      CollectionAdminRequest.deleteCollection("parallelDestinationCollection").process(cluster.getSolrClient());
      solrClientCache.close();
    }
  }

  @Test
  public void testParallelDaemonCommitStream() throws Exception {

    CollectionAdminRequest.createCollection("parallelDestinationCollection1", "conf", 2, 1).process(cluster.getSolrClient());
    AbstractDistribZkTestBase.waitForRecoveriesToFinish("parallelDestinationCollection1", cluster.getSolrClient().getZkStateReader(),
        false, true, TIMEOUT);

    new UpdateRequest()
        .add(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "0", "s_multi", "aaaa", "s_multi", "bbbb", "i_multi", "4", "i_multi", "7")
        .add(id, "2", "a_s", "hello2", "a_i", "2", "a_f", "0", "s_multi", "aaaa1", "s_multi", "bbbb1", "i_multi", "44", "i_multi", "77")
        .add(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3", "s_multi", "aaaa2", "s_multi", "bbbb2", "i_multi", "444", "i_multi", "777")
        .add(id, "4", "a_s", "hello4", "a_i", "4", "a_f", "4", "s_multi", "aaaa3", "s_multi", "bbbb3", "i_multi", "4444", "i_multi", "7777")
        .add(id, "1", "a_s", "hello1", "a_i", "1", "a_f", "1", "s_multi", "aaaa4", "s_multi", "bbbb4", "i_multi", "44444", "i_multi", "77777")
        .commit(cluster.getSolrClient(), "collection1");

    StreamExpression expression;
    TupleStream stream;
    Tuple t;
    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);

    String zkHost = cluster.getZkServer().getZkAddress();
    StreamFactory factory = new StreamFactory()
        .withCollectionZkHost("collection1", cluster.getZkServer().getZkAddress())
        .withCollectionZkHost("parallelDestinationCollection1", cluster.getZkServer().getZkAddress())
        .withFunctionName("search", CloudSolrStream.class)
        .withFunctionName("update", UpdateStream.class)
        .withFunctionName("commit", CommitStream.class)
        .withFunctionName("parallel", ParallelStream.class)
        .withFunctionName("daemon", DaemonStream.class);

    try {
      //Copy all docs to destinationCollection
      String updateExpression = "daemon(commit(parallelDestinationCollection1, batchSize=0, zkHost=\"" + cluster.getZkServer().getZkAddress() + "\", update(parallelDestinationCollection1, batchSize=2, search(collection1, q=*:*, fl=\"id,a_s,a_i,a_f,s_multi,i_multi\", sort=\"a_f asc, a_i asc\", partitionKeys=\"a_f\"))), runInterval=\"1000\", id=\"test\")";
      TupleStream parallelUpdateStream = factory.constructStream("parallel(collection1, " + updateExpression + ", workers=\"2\", zkHost=\"" + zkHost + "\", sort=\"batchNumber asc\")");
      parallelUpdateStream.setStreamContext(streamContext);
      List<Tuple> tuples = getTuples(parallelUpdateStream);
      assert (tuples.size() == 2);

      //Lets sleep long enough for daemon updates to run.
      //Lets stop the daemons
      ModifiableSolrParams sParams = new ModifiableSolrParams(StreamingTest.mapParams(CommonParams.QT, "/stream", "action", "list"));

      int workersComplete = 0;
      for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
        int iterations = 0;
        INNER:
        while (iterations == 0) {
          SolrStream solrStream = new SolrStream(jetty.getBaseUrl().toString() + "/collection1", sParams);
          solrStream.setStreamContext(streamContext);
          solrStream.open();
          Tuple tupleResponse = solrStream.read();
          if (tupleResponse.EOF) {
            solrStream.close();
            break INNER;
          } else {
            long l = tupleResponse.getLong("iterations");
            if (l > 0) {
              ++workersComplete;
            } else {
              try {
                Thread.sleep(1000);
              } catch (Exception e) {
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
        solrStream.setStreamContext(streamContext);
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
        while (stopTime == 0) {
          SolrStream solrStream = new SolrStream(jetty.getBaseUrl() + "/collection1", sParams);
          solrStream.setStreamContext(streamContext);
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
              } catch (Exception e) {

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
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);
      assertEquals(5, tuples.size());

      Tuple tuple = tuples.get(0);
      assert (tuple.getLong("id") == 0);
      assert (tuple.get("a_s").equals("hello0"));
      assert (tuple.getLong("a_i") == 0);
      assert (tuple.getDouble("a_f") == 0.0);
      assertList(tuple.getStrings("s_multi"), "aaaa", "bbbb");
      assertList(tuple.getLongs("i_multi"), Long.parseLong("4"), Long.parseLong("7"));

      tuple = tuples.get(1);
      assert (tuple.getLong("id") == 1);
      assert (tuple.get("a_s").equals("hello1"));
      assert (tuple.getLong("a_i") == 1);
      assert (tuple.getDouble("a_f") == 1.0);
      assertList(tuple.getStrings("s_multi"), "aaaa4", "bbbb4");
      assertList(tuple.getLongs("i_multi"), Long.parseLong("44444"), Long.parseLong("77777"));

      tuple = tuples.get(2);
      assert (tuple.getLong("id") == 2);
      assert (tuple.get("a_s").equals("hello2"));
      assert (tuple.getLong("a_i") == 2);
      assert (tuple.getDouble("a_f") == 0.0);
      assertList(tuple.getStrings("s_multi"), "aaaa1", "bbbb1");
      assertList(tuple.getLongs("i_multi"), Long.parseLong("44"), Long.parseLong("77"));

      tuple = tuples.get(3);
      assert (tuple.getLong("id") == 3);
      assert (tuple.get("a_s").equals("hello3"));
      assert (tuple.getLong("a_i") == 3);
      assert (tuple.getDouble("a_f") == 3.0);
      assertList(tuple.getStrings("s_multi"), "aaaa2", "bbbb2");
      assertList(tuple.getLongs("i_multi"), Long.parseLong("444"), Long.parseLong("777"));

      tuple = tuples.get(4);
      assert (tuple.getLong("id") == 4);
      assert (tuple.get("a_s").equals("hello4"));
      assert (tuple.getLong("a_i") == 4);
      assert (tuple.getDouble("a_f") == 4.0);
      assertList(tuple.getStrings("s_multi"), "aaaa3", "bbbb3");
      assertList(tuple.getLongs("i_multi"), Long.parseLong("4444"), Long.parseLong("7777"));
    } finally {
      CollectionAdminRequest.deleteCollection("parallelDestinationCollection1").process(cluster.getSolrClient());
      solrClientCache.close();
    }
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
    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);

    StreamFactory factory = new StreamFactory()
      .withCollectionZkHost("collection1", cluster.getZkServer().getZkAddress())
      .withFunctionName("search", CloudSolrStream.class)
      .withFunctionName("intersect", IntersectStream.class);

    try {
      // basic
      expression = StreamExpressionParser.parse("intersect("
          + "search(collection1, q=a_s:(setA || setAB), fl=\"id,a_s,a_i\", sort=\"a_i asc, a_s asc\"),"
          + "search(collection1, q=a_s:(setB || setAB), fl=\"id,a_s,a_i\", sort=\"a_i asc\"),"
          + "on=\"a_i\")");
      stream = new IntersectStream(expression, factory);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);

      assert (tuples.size() == 5);
      assertOrder(tuples, 0, 7, 3, 4, 8);
    } finally {
      solrClientCache.close();
    }
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
  public void testCalculatorStream() throws Exception {
    String expr = "select(calc(), add(1, 1) as result)";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", expr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    SolrStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Tuple t = tuples.get(0);
    assertTrue(t.getLong("result").equals(2L));
  }

    @Test
  public void testAnalyzeEvaluator() throws Exception {

    UpdateRequest updateRequest = new UpdateRequest();
    updateRequest.add(id, "1", "test_t", "l b c d c");
    updateRequest.commit(cluster.getSolrClient(), COLLECTIONORALIAS);


    SolrClientCache cache = new SolrClientCache();
    try {

      String expr = "cartesianProduct(search("+COLLECTIONORALIAS+", q=\"*:*\", fl=\"id, test_t\", sort=\"id desc\"), analyze(test_t, test_t) as test_t)";
      ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
      paramsLoc.set("expr", expr);
      paramsLoc.set("qt", "/stream");
      String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;

      SolrStream solrStream = new SolrStream(url, paramsLoc);

      StreamContext context = new StreamContext();
      solrStream.setStreamContext(context);
      List<Tuple> tuples = getTuples(solrStream);
      assertTrue(tuples.size() == 5);

      Tuple t = tuples.get(0);
      assertTrue(t.getString("test_t").equals("l"));
      assertTrue(t.getString("id").equals("1"));

      t = tuples.get(1);
      assertTrue(t.getString("test_t").equals("b"));
      assertTrue(t.getString("id").equals("1"));


      t = tuples.get(2);
      assertTrue(t.getString("test_t").equals("c"));
      assertTrue(t.getString("id").equals("1"));


      t = tuples.get(3);
      assertTrue(t.getString("test_t").equals("d"));
      assertTrue(t.getString("id").equals("1"));

      t = tuples.get(4);
      assertTrue(t.getString("test_t").equals("c"));
      assertTrue(t.getString("id").equals("1"));


      expr = "analyze(\"hello world\", test_t)";
      paramsLoc = new ModifiableSolrParams();
      paramsLoc.set("expr", expr);
      paramsLoc.set("qt", "/stream");

      solrStream = new SolrStream(url, paramsLoc);
      context = new StreamContext();
      solrStream.setStreamContext(context);
      tuples = getTuples(solrStream);
      assertEquals(tuples.size(), 1);
      List terms = (List)tuples.get(0).get("return-value");
      assertTrue(terms.get(0).equals("hello"));
      assertTrue(terms.get(1).equals("world"));

      //Try with single param
      expr = "cartesianProduct(search("+COLLECTIONORALIAS+", q=\"*:*\", fl=\"id, test_t\", sort=\"id desc\"), analyze(test_t) as test_t)";
      paramsLoc = new ModifiableSolrParams();
      paramsLoc.set("expr", expr);
      paramsLoc.set("qt", "/stream");

      solrStream = new SolrStream(url, paramsLoc);

      context = new StreamContext();
      solrStream.setStreamContext(context);
      tuples = getTuples(solrStream);
      assertTrue(tuples.size() == 5);

      t = tuples.get(0);
      assertTrue(t.getString("test_t").equals("l"));
      assertTrue(t.getString("id").equals("1"));

      t = tuples.get(1);
      assertTrue(t.getString("test_t").equals("b"));
      assertTrue(t.getString("id").equals("1"));


      t = tuples.get(2);
      assertTrue(t.getString("test_t").equals("c"));
      assertTrue(t.getString("id").equals("1"));


      t = tuples.get(3);
      assertTrue(t.getString("test_t").equals("d"));
      assertTrue(t.getString("id").equals("1"));

      t = tuples.get(4);
      assertTrue(t.getString("test_t").equals("c"));
      assertTrue(t.getString("id").equals("1"));

      //Try with null in the test_t field
      expr = "cartesianProduct(search("+COLLECTIONORALIAS+", q=\"*:*\", fl=\"id\", sort=\"id desc\"), analyze(test_t, test_t) as test_t)";
      paramsLoc = new ModifiableSolrParams();
      paramsLoc.set("expr", expr);
      paramsLoc.set("qt", "/stream");

      solrStream = new SolrStream(url, paramsLoc);

      context = new StreamContext();
      solrStream.setStreamContext(context);
      tuples = getTuples(solrStream);
      assertTrue(tuples.size() == 1);

      //Test annotating tuple
      expr = "select(search("+COLLECTIONORALIAS+", q=\"*:*\", fl=\"id, test_t\", sort=\"id desc\"), analyze(test_t, test_t) as test1_t)";
      paramsLoc = new ModifiableSolrParams();
      paramsLoc.set("expr", expr);
      paramsLoc.set("qt", "/stream");

      solrStream = new SolrStream(url, paramsLoc);

      context = new StreamContext();
      solrStream.setStreamContext(context);
      tuples = getTuples(solrStream);
      assertTrue(tuples.size() == 1);
      List l = (List)tuples.get(0).get("test1_t");
      assertTrue(l.get(0).equals("l"));
      assertTrue(l.get(1).equals("b"));
      assertTrue(l.get(2).equals("c"));
      assertTrue(l.get(3).equals("d"));
      assertTrue(l.get(4).equals("c"));
    } finally {
      cache.close();
    }
  }

  @Test
  public void testEchoStream() throws Exception {
    String expr = "echo(hello world)";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", expr);
    paramsLoc.set("qt", "/stream");

    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    String s = (String)tuples.get(0).get("echo");
    assertTrue(s.equals("hello world"));

    expr = "echo(\"hello world\")";
    paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", expr);
    paramsLoc.set("qt", "/stream");

    solrStream = new SolrStream(url, paramsLoc);

    solrStream.setStreamContext(context);
    tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    s = (String)tuples.get(0).get("echo");
    assertTrue(s.equals("hello world"));

    expr = "echo(\"hello, world\")";
    paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", expr);
    paramsLoc.set("qt", "/stream");

    solrStream = new SolrStream(url, paramsLoc);

    solrStream.setStreamContext(context);
    tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    s = (String)tuples.get(0).get("echo");
    assertTrue(s.equals("hello, world"));

    expr = "echo(\"hello, \\\"t\\\" world\")";
    paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", expr);
    paramsLoc.set("qt", "/stream");

    solrStream = new SolrStream(url, paramsLoc);

    solrStream.setStreamContext(context);
    tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    s = (String)tuples.get(0).get("echo");

    assertTrue(s.equals("hello, \"t\" world"));

    expr = "parallel("+COLLECTIONORALIAS+", workers=2, sort=\"echo asc\", echo(\"hello, \\\"t\\\" world\"))";
    paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", expr);
    paramsLoc.set("qt", "/stream");

    solrStream = new SolrStream(url, paramsLoc);

    solrStream.setStreamContext(context);
    tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 2);
    s = (String)tuples.get(0).get("echo");
    assertTrue(s.equals("hello, \"t\" world"));
    s = (String)tuples.get(1).get("echo");
    assertTrue(s.equals("hello, \"t\" world"));

    expr = "echo(\"tuytuy iuyiuyi iuyiuyiu iuyiuyiuyiu iuyi iuyiyiuy iuyiuyiu iyiuyiu iyiuyiuyyiyiu yiuyiuyi" +
        " yiuyiuyi yiuyiuuyiu yiyiuyiyiu iyiuyiuyiuiuyiu yiuyiuyi yiuyiy yiuiyiuiuy\")";
    paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", expr);
    paramsLoc.set("qt", "/stream");

    solrStream = new SolrStream(url, paramsLoc);

    solrStream.setStreamContext(context);
    tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    s = (String)tuples.get(0).get("echo");

    assertTrue(s.equals("tuytuy iuyiuyi iuyiuyiu iuyiuyiuyiu iuyi iuyiyiuy iuyiuyiu iyiuyiu iyiuyiuyyiyiu yiuyiuyi yiuyiuyi " +
        "yiuyiuuyiu yiyiuyiyiu iyiuyiuyiuiuyiu yiuyiuyi yiuyiy yiuiyiuiuy"));



  }


  @Test
  public void testEvaluatorOnly() throws Exception {
    String expr = "sequence(20, 0, 1)";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", expr);
    paramsLoc.set("qt", "/stream");

    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Number> sequence = (List<Number>)tuples.get(0).get("return-value");
    assertTrue(sequence.size() == 20);
    for(int i=0; i<sequence.size(); i++) {
      assertTrue(sequence.get(i).intValue() == i);
    }
  }

  @Test
  public void testHist() throws Exception {
    String expr = "hist(sequence(100, 0, 1), 10)";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", expr);
    paramsLoc.set("qt", "/stream");

    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Map> hist = (List<Map>)tuples.get(0).get("return-value");
    assertTrue(hist.size() == 10);
    for(int i=0; i<hist.size(); i++) {
      Map stats = hist.get(i);
      assertTrue(((Number)stats.get("N")).intValue() == 10);
      assertTrue(((Number)stats.get("min")).intValue() == 10*i);
      assertTrue(((Number)stats.get("var")).doubleValue() == 9.166666666666666);
      assertTrue(((Number)stats.get("stdev")).doubleValue() == 3.0276503540974917);
    }

    expr = "hist(sequence(100, 0, 1), 5)";
    paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", expr);
    paramsLoc.set("qt", "/stream");

    solrStream = new SolrStream(url, paramsLoc);
    solrStream.setStreamContext(context);
    tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    hist = (List<Map>)tuples.get(0).get("return-value");
    assertTrue(hist.size() == 5);
    for(int i=0; i<hist.size(); i++) {
      Map stats = hist.get(i);
      assertTrue(((Number)stats.get("N")).intValue() == 20);
      assertTrue(((Number)stats.get("min")).intValue() == 20*i);
      assertTrue(((Number)stats.get("var")).doubleValue() == 35);
      assertTrue(((Number)stats.get("stdev")).doubleValue() == 5.916079783099616);
    }
  }


  @Test
  public void testCumulativeProbability() throws Exception {
    String expr = "cumulativeProbability(normalDistribution(500, 40), 500)";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", expr);
    paramsLoc.set("qt", "/stream");

    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Number number = (Number)tuples.get(0).get("return-value");
    assertTrue(number.doubleValue() == .5D);
  }




  @Test
  public void testEvalStream() throws Exception {
    UpdateRequest updateRequest = new UpdateRequest();
    updateRequest.add(id, "hello", "test_t", "l b c d c");
    updateRequest.commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    String expr = "eval(select(echo(\"search("+COLLECTIONORALIAS+", q=\\\"*:*\\\", fl=id, sort=\\\"id desc\\\")\"), echo as expr_s))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", expr);
    paramsLoc.set("qt", "/stream");

    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    String s = (String)tuples.get(0).get("id");
    assertTrue(s.equals("hello"));

  }

  private String getDateString(String year, String month, String day) {
    return year+"-"+month+"-"+day+"T00:00:00Z";

  }

  @Test
  public void testTimeSeriesStream() throws Exception {
    UpdateRequest updateRequest = new UpdateRequest();

    int i=0;
    while(i<50) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2016", "5", "1"), "price_f", "400.00");
    }

    while(i<100) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2015", "5", "1"), "price_f", "300.0");
    }

    while(i<150) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2014", "5", "1"), "price_f", "500.0");
    }

    while(i<250) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2013", "5", "1"), "price_f", "100.00");
    }

    updateRequest.commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    String expr = "timeseries("+COLLECTIONORALIAS+", q=\"*:*\", start=\"2013-01-01T01:00:00.000Z\", " +
        "end=\"2016-12-01T01:00:00.000Z\", " +
        "gap=\"+1YEAR\", " +
        "field=\"test_dt\", " +
        "count(*), sum(price_f), max(price_f), min(price_f))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", expr);
    paramsLoc.set("qt", "/stream");

    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 4);

    assertTrue(tuples.get(0).get("test_dt").equals("2013-01-01T01:00:00Z"));
    assertTrue(tuples.get(0).getLong("count(*)").equals(100L));
    assertTrue(tuples.get(0).getDouble("sum(price_f)").equals(10000D));
    assertTrue(tuples.get(0).getDouble("max(price_f)").equals(100D));
    assertTrue(tuples.get(0).getDouble("min(price_f)").equals(100D));

    assertTrue(tuples.get(1).get("test_dt").equals("2014-01-01T01:00:00Z"));
    assertTrue(tuples.get(1).getLong("count(*)").equals(50L));
    assertTrue(tuples.get(1).getDouble("sum(price_f)").equals(25000D));
    assertTrue(tuples.get(1).getDouble("max(price_f)").equals(500D));
    assertTrue(tuples.get(1).getDouble("min(price_f)").equals(500D));

    assertTrue(tuples.get(2).get("test_dt").equals("2015-01-01T01:00:00Z"));
    assertTrue(tuples.get(2).getLong("count(*)").equals(50L));
    assertTrue(tuples.get(2).getDouble("sum(price_f)").equals(15000D));
    assertTrue(tuples.get(2).getDouble("max(price_f)").equals(300D));
    assertTrue(tuples.get(2).getDouble("min(price_f)").equals(300D));

    assertTrue(tuples.get(3).get("test_dt").equals("2016-01-01T01:00:00Z"));
    assertTrue(tuples.get(3).getLong("count(*)").equals(50L));
    assertTrue(tuples.get(3).getDouble("sum(price_f)").equals(20000D));
    assertTrue(tuples.get(3).getDouble("max(price_f)").equals(400D));
    assertTrue(tuples.get(3).getDouble("min(price_f)").equals(400D));


    expr = "timeseries("+COLLECTIONORALIAS+", q=\"*:*\", start=\"2013-01-01T01:00:00.000Z\", " +
        "end=\"2016-12-01T01:00:00.000Z\", " +
        "gap=\"+1YEAR\", " +
        "field=\"test_dt\", " +
        "format=\"yyyy\", " +
        "count(*), sum(price_f), max(price_f), min(price_f))";
    paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", expr);
    paramsLoc.set("qt", "/stream");

    solrStream = new SolrStream(url, paramsLoc);

    solrStream.setStreamContext(context);
    tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 4);

    assertTrue(tuples.get(0).get("test_dt").equals("2013"));
    assertTrue(tuples.get(0).getLong("count(*)").equals(100L));
    assertTrue(tuples.get(0).getDouble("sum(price_f)").equals(10000D));
    assertTrue(tuples.get(0).getDouble("max(price_f)").equals(100D));
    assertTrue(tuples.get(0).getDouble("min(price_f)").equals(100D));

    assertTrue(tuples.get(1).get("test_dt").equals("2014"));
    assertTrue(tuples.get(1).getLong("count(*)").equals(50L));
    assertTrue(tuples.get(1).getDouble("sum(price_f)").equals(25000D));
    assertTrue(tuples.get(1).getDouble("max(price_f)").equals(500D));
    assertTrue(tuples.get(1).getDouble("min(price_f)").equals(500D));

    assertTrue(tuples.get(2).get("test_dt").equals("2015"));
    assertTrue(tuples.get(2).getLong("count(*)").equals(50L));
    assertTrue(tuples.get(2).getDouble("sum(price_f)").equals(15000D));
    assertTrue(tuples.get(2).getDouble("max(price_f)").equals(300D));
    assertTrue(tuples.get(2).getDouble("min(price_f)").equals(300D));

    assertTrue(tuples.get(3).get("test_dt").equals("2016"));
    assertTrue(tuples.get(3).getLong("count(*)").equals(50L));
    assertTrue(tuples.get(3).getDouble("sum(price_f)").equals(20000D));
    assertTrue(tuples.get(3).getDouble("max(price_f)").equals(400D));
    assertTrue(tuples.get(3).getDouble("min(price_f)").equals(400D));

    expr = "timeseries("+COLLECTIONORALIAS+", q=\"*:*\", start=\"2013-01-01T01:00:00.000Z\", " +
        "end=\"2016-12-01T01:00:00.000Z\", " +
        "gap=\"+1YEAR\", " +
        "field=\"test_dt\", " +
        "format=\"yyyy-MM\", " +
        "count(*), sum(price_f), max(price_f), min(price_f))";
    paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", expr);
    paramsLoc.set("qt", "/stream");

    solrStream = new SolrStream(url, paramsLoc);

    solrStream.setStreamContext(context);
    tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 4);

    assertTrue(tuples.get(0).get("test_dt").equals("2013-01"));
    assertTrue(tuples.get(0).getLong("count(*)").equals(100L));
    assertTrue(tuples.get(0).getDouble("sum(price_f)").equals(10000D));
    assertTrue(tuples.get(0).getDouble("max(price_f)").equals(100D));
    assertTrue(tuples.get(0).getDouble("min(price_f)").equals(100D));

    assertTrue(tuples.get(1).get("test_dt").equals("2014-01"));
    assertTrue(tuples.get(1).getLong("count(*)").equals(50L));
    assertTrue(tuples.get(1).getDouble("sum(price_f)").equals(25000D));
    assertTrue(tuples.get(1).getDouble("max(price_f)").equals(500D));
    assertTrue(tuples.get(1).getDouble("min(price_f)").equals(500D));

    assertTrue(tuples.get(2).get("test_dt").equals("2015-01"));
    assertTrue(tuples.get(2).getLong("count(*)").equals(50L));
    assertTrue(tuples.get(2).getDouble("sum(price_f)").equals(15000D));
    assertTrue(tuples.get(2).getDouble("max(price_f)").equals(300D));
    assertTrue(tuples.get(2).getDouble("min(price_f)").equals(300D));

    assertTrue(tuples.get(3).get("test_dt").equals("2016-01"));
    assertTrue(tuples.get(3).getLong("count(*)").equals(50L));
    assertTrue(tuples.get(3).getDouble("sum(price_f)").equals(20000D));
    assertTrue(tuples.get(3).getDouble("max(price_f)").equals(400D));
    assertTrue(tuples.get(3).getDouble("min(price_f)").equals(400D));


    expr = "timeseries("+COLLECTIONORALIAS+", q=\"*:*\", start=\"2012-01-01T01:00:00.000Z\", " +
        "end=\"2016-12-01T01:00:00.000Z\", " +
        "gap=\"+1YEAR\", " +
        "field=\"test_dt\", " +
        "format=\"yyyy-MM\", " +
        "count(*), sum(price_f), max(price_f), min(price_f))";
    paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", expr);
    paramsLoc.set("qt", "/stream");

    solrStream = new SolrStream(url, paramsLoc);

    solrStream.setStreamContext(context);
    tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 5);
    assertTrue(tuples.get(0).get("test_dt").equals("2012-01"));
    assertTrue(tuples.get(0).getLong("count(*)").equals(0L));
    assertTrue(tuples.get(0).getDouble("sum(price_f)") == null);
    assertTrue(tuples.get(0).getDouble("max(price_f)") == null);
    assertTrue(tuples.get(0).getDouble("min(price_f)") == null);

    assertTrue(tuples.get(1).get("test_dt").equals("2013-01"));
    assertTrue(tuples.get(1).getLong("count(*)").equals(100L));
    assertTrue(tuples.get(1).getDouble("sum(price_f)").equals(10000D));
    assertTrue(tuples.get(1).getDouble("max(price_f)").equals(100D));
    assertTrue(tuples.get(1).getDouble("min(price_f)").equals(100D));

    assertTrue(tuples.get(2).get("test_dt").equals("2014-01"));
    assertTrue(tuples.get(2).getLong("count(*)").equals(50L));
    assertTrue(tuples.get(2).getDouble("sum(price_f)").equals(25000D));
    assertTrue(tuples.get(2).getDouble("max(price_f)").equals(500D));
    assertTrue(tuples.get(2).getDouble("min(price_f)").equals(500D));

    assertTrue(tuples.get(3).get("test_dt").equals("2015-01"));
    assertTrue(tuples.get(3).getLong("count(*)").equals(50L));
    assertTrue(tuples.get(3).getDouble("sum(price_f)").equals(15000D));
    assertTrue(tuples.get(3).getDouble("max(price_f)").equals(300D));
    assertTrue(tuples.get(3).getDouble("min(price_f)").equals(300D));

    assertTrue(tuples.get(4).get("test_dt").equals("2016-01"));
    assertTrue(tuples.get(4).getLong("count(*)").equals(50L));
    assertTrue(tuples.get(4).getDouble("sum(price_f)").equals(20000D));
    assertTrue(tuples.get(4).getDouble("max(price_f)").equals(400D));
    assertTrue(tuples.get(4).getDouble("min(price_f)").equals(400D));
  }

  @Test
  public void testCorrelationStream() throws Exception {
    UpdateRequest updateRequest = new UpdateRequest();

    int i=0;
    while(i<50) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2016", "5", "1"), "price_f", "400.00");
    }

    while(i<100) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2015", "5", "1"), "price_f", "300.0");
    }

    while(i<150) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2014", "5", "1"), "price_f", "500.0");
    }

    while(i<250) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2013", "5", "1"), "price_f", "100.00");
    }

    updateRequest.commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    String expr = "timeseries("+COLLECTIONORALIAS+", q=\"*:*\", start=\"2013-01-01T01:00:00.000Z\", " +
        "end=\"2016-12-01T01:00:00.000Z\", " +
        "gap=\"+1YEAR\", " +
        "field=\"test_dt\", " +
        "count(*), sum(price_f), max(price_f), min(price_f))";

    String cexpr = "let(a="+expr+", b=select("+expr+",mult(-1, count(*)) as nvalue), c=col(a, count(*)), d=col(b, nvalue), " +
                       "tuple(corr=corr(c,d), scorr=corr(array(500, 50, 50, 50),d, type=spearmans), kcorr=corr(array(500, 50, 50, 50),d, type=kendalls), d=d))";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");

    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    assertTrue(tuples.get(0).getDouble("corr").equals(-1.0D));
    assertTrue(tuples.get(0).getDouble("scorr").equals(-1.0D));
    assertTrue(tuples.get(0).getDouble("kcorr").equals(-1.0D));
  }




  @Test
  public void testCovariance() throws Exception {
    UpdateRequest updateRequest = new UpdateRequest();

    int i=0;
    while(i<50) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2016", "5", "1"), "price_f", "400.00");
    }

    while(i<100) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2015", "5", "1"), "price_f", "300.0");
    }

    while(i<150) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2014", "5", "1"), "price_f", "500.0");
    }

    while(i<250) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2013", "5", "1"), "price_f", "100.00");
    }

    updateRequest.commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    String expr = "timeseries("+COLLECTIONORALIAS+", q=\"*:*\", start=\"2013-01-01T01:00:00.000Z\", " +
        "end=\"2016-12-01T01:00:00.000Z\", " +
        "gap=\"+1YEAR\", " +
        "field=\"test_dt\", " +
        "count(*), sum(price_f), max(price_f), min(price_f))";

    String cexpr = "let(a="+expr+", b=select("+expr+",mult(-1, count(*)) as nvalue), c=col(a, count(*)), d=col(b, nvalue), tuple(colc=c, cold=d, cov=cov(c,d)))";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");

    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    assertTrue(tuples.get(0).getDouble("cov").equals(-625.0D));
  }

  @Test
  public void testDistance() throws Exception {
    String cexpr = "let(echo=true, " +
                       "a=array(1,2,3,4)," +
                       "b=array(2,3,4,5), " +
                       "c=array(3,4,5,6), " +
                       "d=distance(a, b), " +
                       "e=distance(a, c)," +
                       "f=distance(b, c)," +
                       "g=transpose(matrix(a, b, c))," +
                       "h=distance(g)," +
                       "i=distance(a, b, type=manhattan), " +
                       "j=distance(a, c, type=manhattan)," +
                       "k=distance(b, c, type=manhattan)," +
                       "l=transpose(matrix(a, b, c))," +
                       "m=distance(l, type=manhattan)," +
                       "n=distance(a, b, type=canberra), " +
                       "o=distance(a, c, type=canberra)," +
                       "p=distance(b, c, type=canberra)," +
                       "q=transpose(matrix(a, b, c))," +
                       "r=distance(q, type=canberra)," +
                       "s=distance(a, b, type=earthMovers), " +
                       "t=distance(a, c, type=earthMovers)," +
                       "u=distance(b, c, type=earthMovers)," +
                       "w=transpose(matrix(a, b, c))," +
                       "x=distance(w, type=earthMovers)," +
                       ")";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Number d = (Number)tuples.get(0).get("d");
    assertEquals(d.doubleValue(), 2.0, 0.0);
    Number e = (Number)tuples.get(0).get("e");
    assertEquals(e.doubleValue(), 4.0, 0.0);
    Number f = (Number)tuples.get(0).get("f");
    assertEquals(f.doubleValue(), 2.0, 0.0);

    List<List<Number>> h = (List<List<Number>>)tuples.get(0).get("h");
    assertEquals(h.size(), 3);
    assertEquals(h.get(0).size(), 3);
    List<Number> row0 = h.get(0);
    assertEquals(row0.get(0).doubleValue(), 0, 0);
    assertEquals(row0.get(1).doubleValue(), 2, 0);
    assertEquals(row0.get(2).doubleValue(), 4, 0);

    List<Number> row1 = h.get(1);
    assertEquals(row1.get(0).doubleValue(), 2, 0);
    assertEquals(row1.get(1).doubleValue(), 0, 0);
    assertEquals(row1.get(2).doubleValue(), 2, 0);

    List<Number> row2 = h.get(2);
    assertEquals(row2.get(0).doubleValue(), 4, 0);
    assertEquals(row2.get(1).doubleValue(), 2, 0);
    assertEquals(row2.get(2).doubleValue(), 0, 0);

    Number i = (Number)tuples.get(0).get("i");
    assertEquals(i.doubleValue(), 4.0, 0.0);
    Number j = (Number)tuples.get(0).get("j");
    assertEquals(j.doubleValue(), 8.0, 0.0);
    Number k = (Number)tuples.get(0).get("k");
    assertEquals(k.doubleValue(), 4.0, 0.0);

    List<List<Number>> m = (List<List<Number>>)tuples.get(0).get("m");
    assertEquals(m.size(), 3);
    assertEquals(m.get(0).size(), 3);
    row0 = m.get(0);
    assertEquals(row0.get(0).doubleValue(), 0, 0);
    assertEquals(row0.get(1).doubleValue(), 4, 0);
    assertEquals(row0.get(2).doubleValue(), 8, 0);

    row1 = m.get(1);
    assertEquals(row1.get(0).doubleValue(), 4, 0);
    assertEquals(row1.get(1).doubleValue(), 0, 0);
    assertEquals(row1.get(2).doubleValue(), 4, 0);

    row2 = m.get(2);
    assertEquals(row2.get(0).doubleValue(), 8, 0);
    assertEquals(row2.get(1).doubleValue(), 4, 0);
    assertEquals(row2.get(2).doubleValue(), 0, 0);

    Number n = (Number)tuples.get(0).get("n");
    assertEquals(n.doubleValue(), 0.787302, 0.0001);
    Number o = (Number)tuples.get(0).get("o");
    assertEquals(o.doubleValue(), 1.283333, 0.0001);
    Number p = (Number)tuples.get(0).get("p");
    assertEquals(p.doubleValue(), 0.544877, 0.0001);

    List<List<Number>> r = (List<List<Number>>)tuples.get(0).get("r");
    assertEquals(r.size(), 3);
    assertEquals(r.get(0).size(), 3);
    row0 = r.get(0);
    assertEquals(row0.get(0).doubleValue(), 0, 0);
    assertEquals(row0.get(1).doubleValue(), 0.787302, .0001);
    assertEquals(row0.get(2).doubleValue(), 1.283333, .0001);

    row1 = r.get(1);
    assertEquals(row1.get(0).doubleValue(), 0.787302, .0001);
    assertEquals(row1.get(1).doubleValue(), 0, 0);
    assertEquals(row1.get(2).doubleValue(), 0.544877, .0001);

    row2 = r.get(2);
    assertEquals(row2.get(0).doubleValue(), 1.283333, .0001);
    assertEquals(row2.get(1).doubleValue(), 0.544877, .0001);
    assertEquals(row2.get(2).doubleValue(), 0, 0);


    Number s = (Number)tuples.get(0).get("s");
    assertEquals(s.doubleValue(), 10.0, 0);
    Number t = (Number)tuples.get(0).get("t");
    assertEquals(t.doubleValue(), 20.0, 0);
    Number u = (Number)tuples.get(0).get("u");
    assertEquals(u.doubleValue(), 10.0, 0);

    List<List<Number>> x = (List<List<Number>>)tuples.get(0).get("x");
    assertEquals(x.size(), 3);
    assertEquals(x.get(0).size(), 3);
    row0 = x.get(0);
    assertEquals(row0.get(0).doubleValue(), 0, 0);
    assertEquals(row0.get(1).doubleValue(), 10.0, 0);
    assertEquals(row0.get(2).doubleValue(), 20, 0);

    row1 = x.get(1);
    assertEquals(row1.get(0).doubleValue(), 10, 0);
    assertEquals(row1.get(1).doubleValue(), 0, 0);
    assertEquals(row1.get(2).doubleValue(), 10, 0);

    row2 = x.get(2);
    assertEquals(row2.get(0).doubleValue(), 20, 0);
    assertEquals(row2.get(1).doubleValue(), 10, 0);
    assertEquals(row2.get(2).doubleValue(), 0, 0);


  }



  @Test
  public void testReverse() throws Exception {
    UpdateRequest updateRequest = new UpdateRequest();

    int i=0;
    while(i<50) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2016", "5", "1"), "price_f", "400.00");
    }

    while(i<100) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2015", "5", "1"), "price_f", "300.0");
    }

    while(i<150) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2014", "5", "1"), "price_f", "500.0");
    }

    while(i<250) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2013", "5", "1"), "price_f", "100.00");
    }

    updateRequest.commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    String expr = "timeseries("+COLLECTIONORALIAS+", q=\"*:*\", start=\"2013-01-01T01:00:00.000Z\", " +
        "end=\"2016-12-01T01:00:00.000Z\", " +
        "gap=\"+1YEAR\", " +
        "field=\"test_dt\", " +
        "count(*), sum(price_f), max(price_f), min(price_f))";

    String cexpr = "let(a="+expr+", c=col(a, max(price_f)), tuple(reverse=rev(c)))";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");

    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Number> reverse = (List<Number>)tuples.get(0).get("reverse");
    assertTrue(reverse.size() == 4);
    assertTrue(reverse.get(0).doubleValue() == 400D);
    assertTrue(reverse.get(1).doubleValue() == 300D);
    assertTrue(reverse.get(2).doubleValue() == 500D);
    assertTrue(reverse.get(3).doubleValue() == 100D);
  }


  @Test
  public void testCopyOf() throws Exception {
    UpdateRequest updateRequest = new UpdateRequest();

    int i=0;
    while(i<50) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2016", "5", "1"), "price_f", "400.00");
    }

    while(i<100) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2015", "5", "1"), "price_f", "300.0");
    }

    while(i<150) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2014", "5", "1"), "price_f", "500.0");
    }

    while(i<250) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2013", "5", "1"), "price_f", "100.00");
    }

    updateRequest.commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    String expr = "timeseries("+COLLECTIONORALIAS+", q=\"*:*\", start=\"2013-01-01T01:00:00.000Z\", " +
        "end=\"2016-12-01T01:00:00.000Z\", " +
        "gap=\"+1YEAR\", " +
        "field=\"test_dt\", " +
        "count(*), sum(price_f), max(price_f), min(price_f))";

    String cexpr = "let(a="+expr+", c=col(a, max(price_f)), tuple(copy1=copyOf(c, 10), copy2=copyOf(c), copy3=copyOf(c, 2) ))";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");

    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Number> copy1 = (List<Number>)tuples.get(0).get("copy1");
    assertTrue(copy1.size() == 4);
    assertTrue(copy1.get(0).doubleValue() == 100D);
    assertTrue(copy1.get(1).doubleValue() == 500D);
    assertTrue(copy1.get(2).doubleValue() == 300D);
    assertTrue(copy1.get(3).doubleValue() == 400D);

    List<Number> copy2 = (List<Number>)tuples.get(0).get("copy2");
    assertTrue(copy2.size() == 4);
    assertTrue(copy2.get(0).doubleValue() == 100D);
    assertTrue(copy2.get(1).doubleValue() == 500D);
    assertTrue(copy2.get(2).doubleValue() == 300D);
    assertTrue(copy2.get(3).doubleValue() == 400D);

    List<Number> copy3 = (List<Number>)tuples.get(0).get("copy3");
    assertTrue(copy3.size() == 2);
    assertTrue(copy3.get(0).doubleValue() == 100D);
    assertTrue(copy3.get(1).doubleValue() == 500D);
  }


  public void testCopyOfRange() throws Exception {
    UpdateRequest updateRequest = new UpdateRequest();

    int i=0;
    while(i<50) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2016", "5", "1"), "price_f", "400.00");
    }

    while(i<100) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2015", "5", "1"), "price_f", "300.0");
    }

    while(i<150) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2014", "5", "1"), "price_f", "500.0");
    }

    while(i<250) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2013", "5", "1"), "price_f", "100.00");
    }

    updateRequest.commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    String expr = "timeseries("+COLLECTIONORALIAS+", q=\"*:*\", start=\"2013-01-01T01:00:00.000Z\", " +
        "end=\"2016-12-01T01:00:00.000Z\", " +
        "gap=\"+1YEAR\", " +
        "field=\"test_dt\", " +
        "count(*), sum(price_f), max(price_f), min(price_f))";

    String cexpr = "let(a="+expr+", c=col(a, max(price_f)), tuple(copy=copyOfRange(c, 1, 3), copy2=copyOfRange(c, 2, 4), l=length(c)))";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");

    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Number> copy1 = (List<Number>)tuples.get(0).get("copy");
    assertTrue(copy1.size() == 2);
    assertTrue(copy1.get(0).doubleValue() == 500D);
    assertTrue(copy1.get(1).doubleValue() == 300D);

    List<Number> copy2 = (List<Number>)tuples.get(0).get("copy2");
    assertTrue(copy2.size() == 2);
    assertTrue(copy2.get(0).doubleValue() == 300D);
    assertTrue(copy2.get(1).doubleValue() == 400D);

    long l = tuples.get(0).getLong("l");
    assertTrue(l == 4);

  }



  @Test
  public void testPercentile() throws Exception {
    String cexpr = "percentile(array(1,2,3,4,5,6,7,8,9,10,11), 50)";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");

    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Tuple tuple = tuples.get(0);
    double p = tuple.getDouble("return-value");
    assertEquals(p, 6, 0.0);


    cexpr = "percentile(array(11,10,3,4,5,6,7,8,9,2,1), 50)";
    paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");

    solrStream = new SolrStream(url, paramsLoc);

    context = new StreamContext();
    solrStream.setStreamContext(context);
    tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    tuple = tuples.get(0);
    p = tuple.getDouble("return-value");
    assertEquals(p, 6, 0.0);

    cexpr = "percentile(array(11,10,3,4,5,6,7,8,9,2,1), 20)";
    paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");

    solrStream = new SolrStream(url, paramsLoc);

    context = new StreamContext();
    solrStream.setStreamContext(context);
    tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    tuple = tuples.get(0);
    p = tuple.getDouble("return-value");
    assertEquals(p, 2.4, 0.001);
  }


  @Test
  public void testPrimes() throws Exception {
    String cexpr = "primes(10, 0)";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");

    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Tuple tuple = tuples.get(0);
    List<Number> asort = (List<Number>)tuple.get("return-value");
    assertEquals(asort.size(), 10);
    assertEquals(asort.get(0).intValue(), 2);
    assertEquals(asort.get(1).intValue(), 3);
    assertEquals(asort.get(2).intValue(), 5);
    assertEquals(asort.get(3).intValue(), 7);
    assertEquals(asort.get(4).intValue(), 11);
    assertEquals(asort.get(5).intValue(), 13);
    assertEquals(asort.get(6).intValue(), 17);
    assertEquals(asort.get(7).intValue(), 19);
    assertEquals(asort.get(8).intValue(), 23);
    assertEquals(asort.get(9).intValue(), 29);
  }

  @Test
  public void testBinomialCoefficient() throws Exception {
    String cexpr = "binomialCoefficient(8,3)";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Tuple tuple = tuples.get(0);
    long binomialCoefficient = (long) tuple.get("return-value");
    assertEquals(binomialCoefficient, 56);
  }

  @Test
  public void testAscend() throws Exception {
    String cexpr = "asc(array(11.5, 12.3, 4, 3, 1, 0))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");

    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Tuple tuple = tuples.get(0);
    List<Number> asort = (List<Number>)tuple.get("return-value");
    assertEquals(asort.size(), 6);
    assertEquals(asort.get(0).doubleValue(), 0, 0.0);
    assertEquals(asort.get(1).doubleValue(), 1, 0.0);
    assertEquals(asort.get(2).doubleValue(), 3, 0.0);
    assertEquals(asort.get(3).doubleValue(), 4, 0.0);
    assertEquals(asort.get(4).doubleValue(), 11.5, 0.0);
    assertEquals(asort.get(5).doubleValue(), 12.3, 0.0);
  }



  @Test
  public void testRankTransform() throws Exception {
    UpdateRequest updateRequest = new UpdateRequest();

    int i=0;
    while(i<50) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2016", "5", "1"), "price_f", "400.00");
    }

    while(i<100) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2015", "5", "1"), "price_f", "300.0");
    }

    while(i<150) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2014", "5", "1"), "price_f", "500.0");
    }

    while(i<250) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2013", "5", "1"), "price_f", "100.00");
    }

    updateRequest.commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    String expr = "timeseries("+COLLECTIONORALIAS+", q=\"*:*\", start=\"2013-01-01T01:00:00.000Z\", " +
        "end=\"2016-12-01T01:00:00.000Z\", " +
        "gap=\"+1YEAR\", " +
        "field=\"test_dt\", " +
        "count(*), sum(price_f), max(price_f), min(price_f))";

    String cexpr = "let(a="+expr+", c=col(a, max(price_f)), tuple(reverse=rev(c), ranked=rank(c)))";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");

    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Number> reverse = (List<Number>)tuples.get(0).get("reverse");
    assertTrue(reverse.size() == 4);
    assertTrue(reverse.get(0).doubleValue() == 400D);
    assertTrue(reverse.get(1).doubleValue() == 300D);
    assertTrue(reverse.get(2).doubleValue() == 500D);
    assertTrue(reverse.get(3).doubleValue() == 100D);

    List<Number> ranked = (List<Number>)tuples.get(0).get("ranked");
    assertTrue(ranked.size() == 4);
    assertTrue(ranked.get(0).doubleValue() == 1D);
    assertTrue(ranked.get(1).doubleValue() == 4D);
    assertTrue(ranked.get(2).doubleValue() == 2D);
    assertTrue(ranked.get(3).doubleValue() == 3D);
  }


  @Test
  public void testArray() throws Exception {
    String cexpr = "array(1, 2, 3, 300, 2, 500)";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Number> out = (List<Number>)tuples.get(0).get("return-value");
    assertTrue(out.size() == 6);
    assertTrue(out.get(0).intValue() == 1);
    assertTrue(out.get(1).intValue() == 2);
    assertTrue(out.get(2).intValue() == 3);
    assertTrue(out.get(3).intValue() == 300);
    assertTrue(out.get(4).intValue() == 2);
    assertTrue(out.get(5).intValue() == 500);

    cexpr = "array(1.122, 2.222, 3.333, 300.1, 2.13, 500.23)";
    paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    solrStream = new SolrStream(url, paramsLoc);
    solrStream.setStreamContext(context);
    tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    out = (List<Number>)tuples.get(0).get("return-value");
    assertTrue(out.size() == 6);
    assertTrue(out.get(0).doubleValue() == 1.122D);
    assertTrue(out.get(1).doubleValue() == 2.222D);
    assertTrue(out.get(2).doubleValue() == 3.333D);
    assertTrue(out.get(3).doubleValue() == 300.1D);
    assertTrue(out.get(4).doubleValue() == 2.13D);
    assertTrue(out.get(5).doubleValue() == 500.23D);
  }


  @Test
  public void testMatrix() throws Exception {
    String cexpr = "matrix(array(1, 2, 3), rev(array(4,5,6)))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<List<Number>> out = (List<List<Number>>)tuples.get(0).get("return-value");

    List<Number> array1 = out.get(0);
    assertEquals(array1.size(), 3);
    assertEquals(array1.get(0).doubleValue(), 1.0, 0.0);
    assertEquals(array1.get(1).doubleValue(), 2.0, 0.0);
    assertEquals(array1.get(2).doubleValue(), 3.0, 0.0);

    List<Number> array2 = out.get(1);
    assertEquals(array2.size(), 3);
    assertEquals(array2.get(0).doubleValue(), 6.0, 0.0);
    assertEquals(array2.get(1).doubleValue(), 5.0, 0.0);
    assertEquals(array2.get(2).doubleValue(), 4.0, 0.0);
  }


  @Test
  public void testMatrixMath() throws Exception {
    String cexpr = "let(echo=true, a=matrix(array(1.5, 2.5, 3.5), array(4.5,5.5,6.5)), " +
                                  "b=grandSum(a), " +
                                  "c=sumRows(a), " +
                                  "d=sumColumns(a), " +
                                  "e=scalarAdd(1, a)," +
                                  "f=scalarSubtract(1, a)," +
                                  "g=scalarMultiply(1.5, a)," +
                                  "h=scalarDivide(1.5, a)," +
                                  "i=scalarAdd(1.5, array(1.5, 2.5, 3.5))," +
                                  "j=scalarSubtract(1.5, array(1.5, 2.5, 3.5))," +
                                  "k=scalarMultiply(1.5, array(1.5, 2.5, 3.5))," +
                                  "l=scalarDivide(1.5, array(1.5, 2.5, 3.5)))";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);

    double grandSum = tuples.get(0).getDouble("b");
    assertEquals(grandSum, 24, 0.0);

    List<Number> sumRows = (List<Number>)tuples.get(0).get("c");
    assertEquals(sumRows.size(), 2);
    assertEquals(sumRows.get(0).doubleValue(), 7.5, 0.0);
    assertEquals(sumRows.get(1).doubleValue(), 16.5, 0.0);

    List<Number> sumCols = (List<Number>)tuples.get(0).get("d");
    assertEquals(sumCols.size(), 3);
    assertEquals(sumCols.get(0).doubleValue(), 6.0, 0.0);
    assertEquals(sumCols.get(1).doubleValue(), 8.0, 0.0);
    assertEquals(sumCols.get(2).doubleValue(), 10, 0.0);

    List<List<Number>> scalarAdd = (List<List<Number>>)tuples.get(0).get("e");
    List<Number> row1 = scalarAdd.get(0);
    assertEquals(row1.size(), 3);
    assertEquals(row1.get(0).doubleValue(), 2.5, 0.0);
    assertEquals(row1.get(1).doubleValue(), 3.5, 0.0);
    assertEquals(row1.get(2).doubleValue(), 4.5, 0.0);

    List<Number> row2 = scalarAdd.get(1);
    assertEquals(row2.get(0).doubleValue(), 5.5, 0.0);
    assertEquals(row2.get(1).doubleValue(), 6.5, 0.0);
    assertEquals(row2.get(2).doubleValue(), 7.5, 0.0);

    List<List<Number>> scalarSubtract = (List<List<Number>>)tuples.get(0).get("f");
    row1 = scalarSubtract.get(0);
    assertEquals(row1.size(), 3);
    assertEquals(row1.get(0).doubleValue(), 0.5, 0.0);
    assertEquals(row1.get(1).doubleValue(), 1.5, 0.0);
    assertEquals(row1.get(2).doubleValue(), 2.5, 0.0);

    row2 = scalarSubtract.get(1);
    assertEquals(row2.get(0).doubleValue(), 3.5, 0.0);
    assertEquals(row2.get(1).doubleValue(), 4.5, 0.0);
    assertEquals(row2.get(2).doubleValue(), 5.5, 0.0);

    List<List<Number>> scalarMultiply = (List<List<Number>>)tuples.get(0).get("g");
    row1 = scalarMultiply.get(0);
    assertEquals(row1.size(), 3);
    assertEquals(row1.get(0).doubleValue(), 2.25, 0.0);
    assertEquals(row1.get(1).doubleValue(), 3.75, 0.0);
    assertEquals(row1.get(2).doubleValue(), 5.25, 0.0);

    row2 = scalarMultiply.get(1);
    assertEquals(row2.get(0).doubleValue(), 6.75, 0.0);
    assertEquals(row2.get(1).doubleValue(), 8.25, 0.0);
    assertEquals(row2.get(2).doubleValue(), 9.75, 0.0);

    List<List<Number>> scalarDivide = (List<List<Number>>)tuples.get(0).get("h");
    row1 = scalarDivide.get(0);
    assertEquals(row1.size(), 3);
    assertEquals(row1.get(0).doubleValue(), 1.0, 0.0);
    assertEquals(row1.get(1).doubleValue(), 1.66666666666667, 0.001);
    assertEquals(row1.get(2).doubleValue(), 2.33333333333333, 0.001);

    row2 = scalarDivide.get(1);
    assertEquals(row2.get(0).doubleValue(), 3, 0.0);
    assertEquals(row2.get(1).doubleValue(), 3.66666666666667, 0.001);
    assertEquals(row2.get(2).doubleValue(), 4.33333333333333, 0.001);

    List<Number> rowA = (List<Number>)tuples.get(0).get("i");
    assertEquals(rowA.size(), 3);
    assertEquals(rowA.get(0).doubleValue(), 3.0, 0.0);
    assertEquals(rowA.get(1).doubleValue(), 4.0, 0.0);
    assertEquals(rowA.get(2).doubleValue(), 5.0, 0.0);

    rowA = (List<Number>)tuples.get(0).get("j");
    assertEquals(rowA.size(), 3);
    assertEquals(rowA.get(0).doubleValue(), 0, 0.0);
    assertEquals(rowA.get(1).doubleValue(), 1.0, 0.0);
    assertEquals(rowA.get(2).doubleValue(), 2.0, 0.0);

    rowA = (List<Number>)tuples.get(0).get("k");
    assertEquals(rowA.size(), 3);
    assertEquals(rowA.get(0).doubleValue(), 2.25, 0.0);
    assertEquals(rowA.get(1).doubleValue(), 3.75, 0.0);
    assertEquals(rowA.get(2).doubleValue(), 5.25, 0.0);

    rowA = (List<Number>)tuples.get(0).get("l");
    assertEquals(rowA.size(), 3);
    assertEquals(rowA.get(0).doubleValue(), 1.0, 0.0);
    assertEquals(rowA.get(1).doubleValue(), 1.66666666666667, 0.001);
    assertEquals(rowA.get(2).doubleValue(), 2.33333333333333, 0.001);
  }

  @Test
  public void testTranspose() throws Exception {
    String cexpr = "let(a=matrix(array(1,2,3), array(4,5,6)), b=transpose(a))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<List<Number>> out = (List<List<Number>>)tuples.get(0).get("b");
    assertEquals(out.size(), 3);
    List<Number> array1 = out.get(0);
    assertEquals(array1.size(), 2);
    assertEquals(array1.get(0).doubleValue(), 1.0, 0.0);
    assertEquals(array1.get(1).doubleValue(), 4.0, 0.0);

    List<Number> array2 = out.get(1);
    assertEquals(array2.size(), 2);
    assertEquals(array2.get(0).doubleValue(), 2.0, 0.0);
    assertEquals(array2.get(1).doubleValue(), 5.0, 0.0);

    List<Number> array3 = out.get(2);
    assertEquals(array3.size(), 2);
    assertEquals(array3.get(0).doubleValue(), 3.0, 0.0);
    assertEquals(array3.get(1).doubleValue(), 6.0, 0.0);
  }

  @Test
  public void testUnitize() throws Exception {
    String cexpr = "let(echo=true, a=unitize(matrix(array(1,2,3), array(4,5,6))), b=unitize(array(4,5,6)))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<List<Number>> out = (List<List<Number>>)tuples.get(0).get("a");
    assertEquals(out.size(), 2);
    List<Number> array1 = out.get(0);
    assertEquals(array1.size(), 3);
    assertEquals(array1.get(0).doubleValue(), 0.2672612419124244, 0.0);
    assertEquals(array1.get(1).doubleValue(), 0.5345224838248488, 0.0);
    assertEquals(array1.get(2).doubleValue(), 0.8017837257372732, 0.0);

    List<Number> array2 = out.get(1);
    assertEquals(array2.size(), 3);
    assertEquals(array2.get(0).doubleValue(), 0.4558423058385518, 0.0);
    assertEquals(array2.get(1).doubleValue(), 0.5698028822981898, 0.0);
    assertEquals(array2.get(2).doubleValue(), 0.6837634587578276, 0.0);

    List<Number> array3 = (List<Number>)tuples.get(0).get("b");
    assertEquals(array3.size(), 3);
    assertEquals(array3.get(0).doubleValue(), 0.4558423058385518, 0.0);
    assertEquals(array3.get(1).doubleValue(), 0.5698028822981898, 0.0);
    assertEquals(array3.get(2).doubleValue(), 0.6837634587578276, 0.0);
  }


  @Test
  public void testNormalizeSum() throws Exception {
    String cexpr = "let(echo=true, " +
                       "a=normalizeSum(matrix(array(1,2,3), array(4,5,6))), " +
                       "b=normalizeSum(array(1,2,3))," +
                       "c=normalizeSum(array(1,2,3), 100))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<List<Number>> out = (List<List<Number>>)tuples.get(0).get("a");
    assertEquals(out.size(), 2);
    List<Number> array1 = out.get(0);
    assertEquals(array1.size(), 3);
    assertEquals(array1.get(0).doubleValue(), 0.16666666666666666, 0.0001);
    assertEquals(array1.get(1).doubleValue(), 0.3333333333333333, 0.00001);
    assertEquals(array1.get(2).doubleValue(), 0.5, 0.0001);

    List<Number> array2 = out.get(1);
    assertEquals(array2.size(), 3);
    assertEquals(array2.get(0).doubleValue(), 0.26666666666666666, 0.0001);
    assertEquals(array2.get(1).doubleValue(), 0.3333333333333333, 0.0001);
    assertEquals(array2.get(2).doubleValue(), 0.4, 0.0001);

    List<Number> array3 = (List<Number>)tuples.get(0).get("b");
    assertEquals(array3.size(), 3);
    assertEquals(array3.get(0).doubleValue(), 0.16666666666666666, 0.0001);
    assertEquals(array3.get(1).doubleValue(), 0.3333333333333333, 0.0001);
    assertEquals(array3.get(2).doubleValue(), 0.5, 0.0001);

    List<Number> array4 = (List<Number>)tuples.get(0).get("c");
    assertEquals(array4.size(), 3);
    assertEquals(array4.get(0).doubleValue(), 16.666666666666666, 0.0001);
    assertEquals(array4.get(1).doubleValue(), 33.33333333333333, 0.00001);
    assertEquals(array4.get(2).doubleValue(), 50, 0.0001);
  }


  @Test
  public void testStandardize() throws Exception {
    String cexpr = "let(echo=true, a=standardize(matrix(array(1,2,3), array(4,5,6))), b=standardize(array(4,5,6)))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<List<Number>> out = (List<List<Number>>)tuples.get(0).get("a");
    assertEquals(out.size(), 2);
    List<Number> array1 = out.get(0);
    assertEquals(array1.size(), 3);
    assertEquals(array1.get(0).doubleValue(), -1, 0.0);
    assertEquals(array1.get(1).doubleValue(), 0, 0.0);
    assertEquals(array1.get(2).doubleValue(), 1, 0.0);

    List<Number> array2 = out.get(1);
    assertEquals(array2.size(), 3);
    assertEquals(array2.get(0).doubleValue(), -1, 0.0);
    assertEquals(array2.get(1).doubleValue(), 0, 0.0);
    assertEquals(array2.get(2).doubleValue(), 1, 0.0);

    List<Number> array3 = (List<Number>)tuples.get(0).get("b");
    assertEquals(array3.size(), 3);
    assertEquals(array2.get(0).doubleValue(), -1, 0.0);
    assertEquals(array2.get(1).doubleValue(), 0, 0.0);
    assertEquals(array2.get(2).doubleValue(), 1, 0.0);
  }

  @Test
  public void testMarkovChain() throws Exception {
    String cexpr = "let(state0=array(.5,.5),\n" +
                   "    state1=array(.5,.5),\n" +
                   "    states=matrix(state0, state1),\n" +
                   "    m=markovChain(states, 0),\n" +
                   "    s=sample(m, 50000),\n" +
                   "    f=freqTable(s))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Map<String, Number>> out = (List<Map<String, Number>>)tuples.get(0).get("f");
    assertEquals(out.size(), 2);
    Map<String, Number> bin0 = out.get(0);
    double state0Pct = bin0.get("pct").doubleValue();
    assertEquals(state0Pct, .5, .015);
    Map<String, Number> bin1 = out.get(1);
    double state1Pct = bin1.get("pct").doubleValue();
    assertEquals(state1Pct, .5, .015);
  }



  @Test
  public void testAddAll() throws Exception {
    String cexpr = "addAll(array(1, 2, 3), array(4.5, 5.5, 6.5), array(7,8,9))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Number> out = (List<Number>)tuples.get(0).get("return-value");
    assertTrue(out.size() == 9);
    assertTrue(out.get(0).intValue() == 1);
    assertTrue(out.get(1).intValue() == 2);
    assertTrue(out.get(2).intValue() == 3);
    assertTrue(out.get(3).doubleValue() == 4.5D);
    assertTrue(out.get(4).doubleValue() == 5.5D);
    assertTrue(out.get(5).doubleValue() == 6.5D);
    assertTrue(out.get(6).intValue() == 7);
    assertTrue(out.get(7).intValue() == 8);
    assertTrue(out.get(8).intValue() == 9);
  }

  @Test
  public void fakeTest(){
    NormalDistribution a = new NormalDistribution(10, 2);
    NormalDistribution c = new NormalDistribution(100, 6);
    double[] d = c.sample(250);
    
    KolmogorovSmirnovTest ks = new KolmogorovSmirnovTest();
    double pv = ks.kolmogorovSmirnovStatistic(a, d);
    
    String s = "";
  }

  @Test
  public void testProbabilityRange() throws Exception {
    String cexpr = "let(a=normalDistribution(500, 20), " +
                       "b=probability(a, 520, 530))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Number prob = (Number)tuples.get(0).get("b");
    assertEquals(prob.doubleValue(),  0.09184805266259899, 0.0);
  }

      @Test
  public void testDistributions() throws Exception {
    String cexpr = "let(a=normalDistribution(10, 2), " +
                       "b=sample(a, 250), " +
                       "c=normalDistribution(100, 6), " +
                       "d=sample(c, 250), " +
                       "u=uniformDistribution(1, 6),"+
                       "t=sample(u, 250),"+
                       "e=empiricalDistribution(d),"+
                       "f=sample(e, 250),"+
                       "tuple(sample=b, ks=ks(a,b), ks2=ks(a, d), ks3=ks(u, t)))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    try {
      TupleStream solrStream = new SolrStream(url, paramsLoc);
      StreamContext context = new StreamContext();
      solrStream.setStreamContext(context);
      List<Tuple> tuples = getTuples(solrStream);
      assertTrue(tuples.size() == 1);
      List<Number> out = (List<Number>) tuples.get(0).get("sample");

      Map ks = (Map) tuples.get(0).get("ks");
      Map ks2 = (Map) tuples.get(0).get("ks2");
      Map ks3 = (Map) tuples.get(0).get("ks3");

      assertTrue(out.size() == 250);
      Number pvalue = (Number) ks.get("p-value");
      Number pvalue2 = (Number) ks2.get("p-value");
      Number pvalue3 = (Number) ks3.get("p-value");

      assertTrue(pvalue.doubleValue() > .05D);
      assertTrue(pvalue2.doubleValue() == 0);
      assertTrue(pvalue3.doubleValue() > .05D);

    } catch(AssertionError e) {

      //This test will have random failures do to the random sampling. So if it fails try it again.
      //If it fails twice in a row, we probably broke some code.

      TupleStream solrStream = new SolrStream(url, paramsLoc);
      StreamContext context = new StreamContext();
      solrStream.setStreamContext(context);
      List<Tuple> tuples = getTuples(solrStream);
      assertTrue(tuples.size() == 1);
      List<Number> out = (List<Number>) tuples.get(0).get("sample");

      Map ks = (Map) tuples.get(0).get("ks");
      Map ks2 = (Map) tuples.get(0).get("ks2");
      Map ks3 = (Map) tuples.get(0).get("ks3");

      assertTrue(out.size() == 250);
      Number pvalue = (Number) ks.get("p-value");
      Number pvalue2 = (Number) ks2.get("p-value");
      Number pvalue3 = (Number) ks3.get("p-value");

      assertTrue(pvalue.doubleValue() > .05D);
      assertTrue(pvalue2.doubleValue() == 0);
      assertTrue(pvalue3.doubleValue() > .05D);
    }
  }


  @Test
  public void testSumDifference() throws Exception {
    String cexpr = "sumDifference(array(2,4,6,8,10,12),array(1,2,3,4,5,6))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    double sd = tuples.get(0).getDouble("return-value");
    assertEquals(sd, 21.0D, 0.0);
  }

  @Test
  public void testMeanDifference() throws Exception {
    String cexpr = "meanDifference(array(2,4,6,8,10,12),array(1,2,3,4,5,6))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    double sd = tuples.get(0).getDouble("return-value");
    assertEquals(sd, 3.5, 0.0);
  }


  @Test
  public void testEBESubtract() throws Exception {
    String cexpr = "ebeSubtract(array(2,4,6,8,10,12),array(1,2,3,4,5,6))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Number> out = (List<Number>)tuples.get(0).get("return-value");
    assertTrue(out.size() == 6);
    assertTrue(out.get(0).intValue() == 1);
    assertTrue(out.get(1).intValue() == 2);
    assertTrue(out.get(2).intValue() == 3);
    assertTrue(out.get(3).intValue() == 4);
    assertTrue(out.get(4).intValue() == 5);
    assertTrue(out.get(5).intValue() == 6);
  }


  @Test
  public void testEBEMultiply() throws Exception {
    String cexpr = "ebeMultiply(array(2,4,6,8,10,12),array(1,2,3,4,5,6))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Number> out = (List<Number>)tuples.get(0).get("return-value");
    assertTrue(out.size() == 6);
    assertTrue(out.get(0).intValue() == 2);
    assertTrue(out.get(1).intValue() == 8);
    assertTrue(out.get(2).intValue() == 18);
    assertTrue(out.get(3).intValue() == 32);
    assertTrue(out.get(4).intValue() == 50);
    assertTrue(out.get(5).intValue() == 72);
  }


  @Test
  public void testEBEAdd() throws Exception {
    String cexpr = "ebeAdd(array(2,4,6,8,10,12),array(1,2,3,4,5,6))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Number> out = (List<Number>)tuples.get(0).get("return-value");
    assertTrue(out.size() == 6);
    assertTrue(out.get(0).intValue() == 3);
    assertTrue(out.get(1).intValue() == 6);
    assertTrue(out.get(2).intValue() == 9);
    assertTrue(out.get(3).intValue() == 12);
    assertTrue(out.get(4).intValue() == 15);
    assertTrue(out.get(5).intValue() == 18);
  }


  @Test
  public void testEBEDivide() throws Exception {
    String cexpr = "ebeDivide(array(2,4,6,8,10,12),array(1,2,3,4,5,6))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Number> out = (List<Number>)tuples.get(0).get("return-value");
    assertTrue(out.size() == 6);
    assertTrue(out.get(0).intValue() == 2);
    assertTrue(out.get(1).intValue() == 2);
    assertTrue(out.get(2).intValue() == 2);
    assertTrue(out.get(3).intValue() == 2);
    assertTrue(out.get(4).intValue() == 2);
    assertTrue(out.get(5).intValue() == 2);
  }


  @Test
  public void testFreqTable() throws Exception {
    String cexpr = "freqTable(array(2,4,6,8,10,12,12,4,8,8,8,2))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Map<String,Number>> out = (List<Map<String, Number>>)tuples.get(0).get("return-value");
    assertTrue(out.size() == 6);
    Map<String, Number> bucket = out.get(0);
    assertEquals(bucket.get("value").longValue(), 2);
    assertEquals(bucket.get("count").longValue(), 2);

    bucket = out.get(1);
    assertEquals(bucket.get("value").longValue(), 4);
    assertEquals(bucket.get("count").longValue(), 2);

    bucket = out.get(2);
    assertEquals(bucket.get("value").longValue(), 6);
    assertEquals(bucket.get("count").longValue(), 1);

    bucket = out.get(3);
    assertEquals(bucket.get("value").longValue(), 8);
    assertEquals(bucket.get("count").longValue(), 4);

    bucket = out.get(4);
    assertEquals(bucket.get("value").longValue(), 10);
    assertEquals(bucket.get("count").longValue(), 1);

    bucket = out.get(5);
    assertEquals(bucket.get("value").longValue(), 12);
    assertEquals(bucket.get("count").longValue(), 2);
  }



  @Test
  public void testCosineSimilarity() throws Exception {
    String cexpr = "cosineSimilarity(array(2,4,6,8),array(1,1,3,4))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Number cs = (Number)tuples.get(0).get("return-value");
    assertTrue(cs.doubleValue() == 0.9838197164968291);
  }

  @Test
  public void testPoissonDistribution() throws Exception {
    String cexpr = "let(a=poissonDistribution(100)," +
        "               b=sample(a, 10000)," +
        "               tuple(d=describe(b), " +
        "                     p=probability(a, 100), " +
        "                     c=cumulativeProbability(a, 100)))";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Map map = (Map)tuples.get(0).get("d");
    Number mean = (Number)map.get("mean");
    Number var = (Number)map.get("var");
    //The mean and variance should be almost the same for poisson distribution
    assertEquals(mean.doubleValue(), var.doubleValue(), 7.0);
    Number prob = (Number)tuples.get(0).get("p");
    assertEquals(prob.doubleValue(), 0.03986099680914713, 0.0);
    Number cprob = (Number)tuples.get(0).get("c");
    assertEquals(cprob.doubleValue(), 0.5265621985303708, 0.0);
  }

  @Test
  public void testGeometricDistribution() throws Exception {
    String cexpr = "let(a=geometricDistribution(.2)," +
        "               b=geometricDistribution(.5)," +
        "               c=geometricDistribution(.8)," +
        "               d=sample(a, 10000)," +
        "               e=sample(b, 10000)," +
        "               f=sample(c, 10000)," +
        "               g=freqTable(d)," +
        "               h=freqTable(e)," +
        "               i=freqTable(f)," +
        "               tuple(g=g, h=h, i=i))";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);

    List<Map> listg = (List<Map>)tuples.get(0).get("g");
    Map mapg = listg.get(0);
    double pctg = (double) mapg.get("pct");
    assertEquals(pctg, .2, .02);

    List<Map> listh = (List<Map>)tuples.get(0).get("h");
    Map maph = listh.get(0);
    double pcth = (double)maph.get("pct");
    assertEquals(pcth, .5, .02);

    List<Map> listi = (List<Map>)tuples.get(0).get("i");
    Map mapi = listi.get(0);
    double pcti = (double)mapi.get("pct");
    assertEquals(pcti, .8, .02);
  }

  @Test
  public void testBinomialDistribution() throws Exception {
    String cexpr = "let(a=binomialDistribution(100, .50)," +
        "               b=sample(a, 10000)," +
        "               tuple(d=describe(b), " +
        "                     p=probability(a, 50), " +
        "                     c=cumulativeProbability(a, 50)))";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Number prob = (Number)tuples.get(0).get("p");
    assertEquals(prob.doubleValue(),0.07958923738717877, 0.0);
    Number cprob = (Number)tuples.get(0).get("c");
    assertEquals(cprob.doubleValue(), 0.5397946186935851, 0.0);
  }

  @Test
  public void testUniformIntegerDistribution() throws Exception {
    String cexpr = "let(a=uniformIntegerDistribution(1, 10)," +
        "               b=sample(a, 10000)," +
        "               tuple(d=describe(b), " +
        "                     p=probability(a, 5), " +
        "                     c=cumulativeProbability(a, 5)))";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Map map = (Map)tuples.get(0).get("d");
    Number N = (Number)map.get("N");
    assertEquals(N.intValue(), 10000);
    Number prob = (Number)tuples.get(0).get("p");
    assertEquals(prob.doubleValue(), 0.1, 0.0);
    Number cprob = (Number)tuples.get(0).get("c");
    assertEquals(cprob.doubleValue(), 0.5, 0.0);
  }

  @Test
  public void testZipFDistribution() throws Exception {
    String cexpr = "let(a=sample(zipFDistribution(10, 1), 50000), b=freqTable(a), c=col(b, count))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Number> counts = (List<Number>)tuples.get(0).get("c");

    assertTrue(counts.size() == 10);

    int lastCount = Integer.MAX_VALUE;
    for(Number number : counts) {
      int current = number.intValue();
      if(current > lastCount) {
        throw new Exception("Zipf distribution not descending!!!");
      } else {
        lastCount = current;
      }
    }
  }


  @Test
  public void testBetaDistribution() throws Exception {
    String cexpr = "let(a=sample(betaDistribution(1, 5), 50000), b=hist(a, 11), c=col(b, N))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Number> counts = (List<Number>)tuples.get(0).get("c");

    int lastCount = Integer.MAX_VALUE;
    for(Number number : counts) {
      int current = number.intValue();
      if(current > lastCount) {
        throw new Exception("This beta distribution should be descending");
      } else {
        lastCount = current;
      }
    }

    cexpr = "let(a=sample(betaDistribution(5, 1), 50000), b=hist(a, 11), c=col(b, N))";
    paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    solrStream = new SolrStream(url, paramsLoc);
    context = new StreamContext();
    solrStream.setStreamContext(context);
    tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    counts = (List<Number>)tuples.get(0).get("c");

    lastCount = Integer.MIN_VALUE;
    for(Number number : counts) {
      int current = number.intValue();
      if(current < lastCount) {
        throw new Exception("This beta distribution should be ascending");
      } else {
        lastCount = current;
      }
    }
  }

  @Test
  public void testEnumeratedDistribution() throws Exception {
    String cexpr = "let(a=uniformIntegerDistribution(1, 10)," +
        "               b=sample(a, 10000)," +
        "               c=enumeratedDistribution(b),"+
        "               tuple(d=describe(b), " +
        "                     p=probability(c, 5), " +
        "                     c=cumulativeProbability(c, 5)))";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Map map = (Map)tuples.get(0).get("d");
    Number N = (Number)map.get("N");
    assertEquals(N.intValue(), 10000);
    Number prob = (Number)tuples.get(0).get("p");
    assertEquals(prob.doubleValue(), 0.1, 0.07);
    Number cprob = (Number)tuples.get(0).get("c");
    assertEquals(cprob.doubleValue(), 0.5, 0.07);


    cexpr = "let(a=sample(enumeratedDistribution(array(1,2,3,4), array(40, 30, 20, 10)), 50000),"+
                "b=freqTable(a),"+
                "y=col(b, pct))";

    paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    solrStream = new SolrStream(url, paramsLoc);
    context = new StreamContext();
    solrStream.setStreamContext(context);
    tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Number> freqs = (List<Number>)tuples.get(0).get("y");
    assertEquals(freqs.get(0).doubleValue(), .40, .03);
    assertEquals(freqs.get(1).doubleValue(), .30, .03);
    assertEquals(freqs.get(2).doubleValue(), .20, .03);
    assertEquals(freqs.get(3).doubleValue(), .10, .03);
  }

  @Test
  public void testDotProduct() throws Exception {
    String cexpr = "dotProduct(array(2,4,6,8,10,12),array(1,2,3,4,5,6))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Number dotProduct = (Number)tuples.get(0).get("return-value");
    assertTrue(dotProduct.doubleValue() == 182);
  }

  @Test
  public void testExponentialMovingAverage() throws Exception {
    String cexpr = "expMovingAvg(array(22.27, 22.19, 22.08, 22.17, 22.18, 22.13, 22.23, 22.43, 22.24, 22.29, " +
                   "22.15, 22.39, 22.38, 22.61, 23.36, 24.05, 23.75, 23.83, 23.95, 23.63, 23.82, 23.87, 23.65, 23.19,"+
                   "23.10, 23.33, 22.68, 23.10, 22.40, 22.17), 10)";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Number> out = (List<Number>)tuples.get(0).get("return-value");
    assertTrue(out.size() == 21);
    assertEquals((double)out.get(0), 22.22, 0.009);
    assertEquals((double)out.get(1), 22.21, 0.009);
    assertEquals((double)out.get(2), 22.24, 0.009);
    assertEquals((double)out.get(3), 22.27, 0.009);
    assertEquals((double)out.get(4), 22.33, 0.009);
    assertEquals((double)out.get(5), 22.52, 0.009);
    assertEquals((double)out.get(6), 22.80, 0.009);
    assertEquals((double)out.get(7), 22.97, 0.009);
    assertEquals((double)out.get(8), 23.13, 0.009);
    assertEquals((double)out.get(9), 23.28, 0.009);
    assertEquals((double)out.get(10), 23.34, 0.009);
    assertEquals((double)out.get(11), 23.43, 0.009);
    assertEquals((double)out.get(12), 23.51, 0.009);
    assertEquals((double)out.get(13), 23.54, 0.009);
    assertEquals((double)out.get(14), 23.47, 0.009);
    assertEquals((double)out.get(15), 23.40, 0.009);
    assertEquals((double)out.get(16), 23.39, 0.009);
    assertEquals((double)out.get(17), 23.26, 0.009);
    assertEquals((double)out.get(18), 23.23, 0.009);
    assertEquals((double)out.get(19), 23.08, 0.009);
    assertEquals((double)out.get(20), 22.92, 0.009);
  }

  @Test
  public void testTimeDifferencingDefaultLag() throws Exception {
    String cexpr = "diff(array(1709.0, 1621.0, 1973.0, 1812.0, 1975.0, 1862.0, 1940.0, 2013.0, 1596.0, 1725.0, 1676.0, 1814.0, 1615.0, 1557.0, 1891.0, 1956.0, 1885.0, 1623.0, 1903.0, 1997.0))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Number> out = (List<Number>)tuples.get(0).get("return-value");
    assertTrue(out.size() == 19);
    assertEquals(out.get(0).doubleValue(),-88.0, 0.01);
    assertEquals(out.get(1).doubleValue(),352.0, 0.01);
    assertEquals(out.get(2).doubleValue(),-161.0, 0.01);
    assertEquals(out.get(3).doubleValue(),163.0, 0.01);
    assertEquals(out.get(4).doubleValue(),-113.0, 0.01);
    assertEquals(out.get(5).doubleValue(),78.0, 0.01);
    assertEquals(out.get(6).doubleValue(),73.0, 0.01);
    assertEquals(out.get(7).doubleValue(),-417.0, 0.01);
    assertEquals(out.get(8).doubleValue(),129.0, 0.01);
    assertEquals(out.get(9).doubleValue(),-49.0, 0.01);
    assertEquals(out.get(10).doubleValue(),138.0, 0.01);
    assertEquals(out.get(11).doubleValue(),-199.0, 0.01);
    assertEquals(out.get(12).doubleValue(),-58.0, 0.01);
    assertEquals(out.get(13).doubleValue(),334.0, 0.01);
    assertEquals(out.get(14).doubleValue(),65.0, 0.01);
    assertEquals(out.get(15).doubleValue(),-71.0, 0.01);
    assertEquals(out.get(16).doubleValue(),-262.0, 0.01);
    assertEquals(out.get(17).doubleValue(),280.0, 0.01);
    assertEquals(out.get(18).doubleValue(),94.0, 0.01);
  }

  @Test
  public void testTimeDifferencingDefinedLag() throws Exception {
    String cexpr = "diff(array(1709.0, 1621.0, 1973.0, 1812.0, 1975.0, 1862.0, 1940.0, 2013.0, 1596.0, 1725.0, 1676.0, 1814.0, 1615.0, 1557.0, 1891.0, 1956.0, 1885.0, 1623.0, 1903.0, 1997.0), 12)";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Number> out = (List<Number>)tuples.get(0).get("return-value");
    assertTrue(out.size() == 8);
    assertEquals(out.get(0).doubleValue(),-94.0, 0.01);
    assertEquals(out.get(1).doubleValue(),-64.0, 0.01);
    assertEquals(out.get(2).doubleValue(),-82.0, 0.01);
    assertEquals(out.get(3).doubleValue(),144.0, 0.01);
    assertEquals(out.get(4).doubleValue(),-90.0, 0.01);
    assertEquals(out.get(5).doubleValue(),-239.0, 0.01);
    assertEquals(out.get(6).doubleValue(),-37.0, 0.01);
    assertEquals(out.get(7).doubleValue(),-16.0, 0.01);

  }

  @Test
  public void testNestedDoubleTimeDifference() throws Exception {
    String cexpr = "diff(diff(array(1709.0, 1621.0, 1973.0, 1812.0, 1975.0, 1862.0, 1940.0, 2013.0, 1596.0, 1725.0, 1676.0, 1814.0, 1615.0, 1557.0, 1891.0, 1956.0, 1885.0, 1623.0, 1903.0, 1997.0)), 12)";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Number> out = (List<Number>)tuples.get(0).get("return-value");
    assertTrue(out.size() == 7);
    assertEquals(out.get(0).doubleValue(),30.0,0.01);
    assertEquals(out.get(1).doubleValue(),-18.0,0.01);
    assertEquals(out.get(2).doubleValue(),226.0,0.01);
    assertEquals(out.get(3).doubleValue(),-234.0,0.01);
    assertEquals(out.get(4).doubleValue(),-149.0,0.01);
    assertEquals(out.get(5).doubleValue(),202.0,0.01);
    assertEquals(out.get(6).doubleValue(),21.0,0.01);
  }


  @Test
  public void testPolyfit() throws Exception {
    String cexpr = "let(echo=true," +
                   "    a=array(0,1,2,3,4,5,6,7)," +
                   "    fit=polyfit(a, 1))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Number> out = (List<Number>)tuples.get(0).get("fit");
    assertTrue(out.size() == 8);
    assertTrue(out.get(0).intValue() == 0);
    assertTrue(out.get(1).intValue() == 1);
    assertTrue(out.get(2).intValue() == 2);
    assertTrue(out.get(3).intValue() == 3);
    assertTrue(out.get(4).intValue() == 4);
    assertTrue(out.get(5).intValue() == 5);
    assertTrue(out.get(6).intValue() == 6);
    assertTrue(out.get(7).intValue() == 7);
  }


  @Test
  public void testTtest() throws Exception {
    String cexpr = "let(echo=true," +
                       "a=array(0,1,2,3,4,5,6,7,9,10,11,12), " +
                       "b=array(0,1,2,3,4,5,6,7,1,1,1,1), " +
                       "ttest=ttest(a, b)," +
                       "sample2Mean=mean(b),"+
                       "onesamplettest=ttest(sample2Mean, b)," +
                       "pairedttest=pairedTtest(a,b))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Map testResult = (Map)tuples.get(0).get("ttest");
    Number tstat = (Number)testResult.get("t-statistic");
    Number pval = (Number)testResult.get("p-value");
    assertEquals(tstat.doubleValue(), 2.3666107120397575, .0001);
    assertEquals(pval.doubleValue(), 0.029680704317867967, .0001);

    Map testResult2 = (Map)tuples.get(0).get("onesamplettest");
    Number tstat2 = (Number)testResult2.get("t-statistic");
    Number pval2 = (Number)testResult2.get("p-value");
    assertEquals(tstat2.doubleValue(), 0, .0001);
    assertEquals(pval2.doubleValue(), 1, .0001);

    Map testResult3 = (Map)tuples.get(0).get("pairedttest");
    Number tstat3 = (Number)testResult3.get("t-statistic");
    Number pval3 = (Number)testResult3.get("p-value");
    assertEquals(tstat3.doubleValue(), 2.321219442769799, .0001);
    assertEquals(pval3.doubleValue(), 0.0404907407662755, .0001);
  }

  @Test
  public void testMultiVariateNormalDistribution() throws Exception {
    String cexpr = "let(echo=true," +
        "     a=array(1,2,3,4,5,6,7)," +
        "     b=array(100, 110, 120, 130,140,150,180)," +
        "     c=transpose(matrix(a, b))," +
        "     d=array(mean(a), mean(b))," +
        "     e=cov(c)," +
        "     f=multiVariateNormalDistribution(d, e)," +
        "     g=sample(f, 10000)," +
        "     h=cov(g)," +
        "     i=sample(f)," +
        "     j=density(f, array(4.016093243274465, 138.7283428008585)))";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<List<Number>> cov = (List<List<Number>>)tuples.get(0).get("h");
    assertEquals(cov.size(), 2);
    List<Number> row1 = cov.get(0);
    assertEquals(row1.size(), 2);

    double a = row1.get(0).doubleValue();
    double b = row1.get(1).doubleValue();
    assertEquals(a, 4.666666666666667, 2.5);
    assertEquals(b, 56.66666666666667, 7);

    List<Number> row2 = cov.get(1);

    double c = row2.get(0).doubleValue();
    double d = row2.get(1).doubleValue();
    assertEquals(c, 56.66666666666667, 7);
    assertEquals(d, 723.8095238095239, 50);

    List<Number> sample = (List<Number>)tuples.get(0).get("i");
    assertEquals(sample.size(), 2);
    Number sample1 = sample.get(0);
    Number sample2 = sample.get(1);
    assertTrue(sample1.doubleValue() > -30 && sample1.doubleValue() < 30);
    assertTrue(sample2.doubleValue() > 50 && sample2.doubleValue() < 250);

    Number density = (Number)tuples.get(0).get("j");
    assertEquals(density.doubleValue(), 0.007852638121596995, .00001);
  }


  @Test
  public void testIntegrate() throws Exception {
    String cexpr = "let(echo=true, " +
                       "a=sequence(50, 1, 0), " +
                       "b=spline(a), " +
                       "c=integrate(b, 0, 49), " +
                       "d=integrate(b, 0, 20), " +
                       "e=integrate(b, 20, 49))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Number integral = (Number)tuples.get(0).get("c");
    assertEquals(integral.doubleValue(), 49, 0.0);
    integral = (Number)tuples.get(0).get("d");
    assertEquals(integral.doubleValue(), 20, 0.0);
    integral = (Number)tuples.get(0).get("e");
    assertEquals(integral.doubleValue(), 29, 0.0);
  }


  @Test
  public void testLoess() throws Exception {
    String cexpr = "let(echo=true," +
                   "    a=array(0,1,2,3,4,5,6,7)," +
                   "    fit=loess(a), " +
                   "    der=derivative(fit))";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Number> out = (List<Number>)tuples.get(0).get("fit");
    assertTrue(out.size() == 8);
    assertEquals(out.get(0).doubleValue(), 0.0, 0.0);
    assertEquals(out.get(1).doubleValue(), 1.0, 0.0);
    assertEquals(out.get(2).doubleValue(), 2.0, 0.0);
    assertEquals(out.get(3).doubleValue(), 3.0, 0.0);
    assertEquals(out.get(4).doubleValue(), 4.0, 0.0);
    assertEquals(out.get(5).doubleValue(), 5.0, 0.0);
    assertEquals(out.get(6).doubleValue(), 6.0, 0.0);
    assertEquals(out.get(7).doubleValue(), 7.0, 0.0);

    List<Number> out1 = (List<Number>)tuples.get(0).get("der");
    assertTrue(out1.size() == 8);
    assertEquals(out1.get(0).doubleValue(), 1.0, 0.0);
    assertEquals(out1.get(1).doubleValue(), 1.0, 0.0);
    assertEquals(out1.get(2).doubleValue(), 1.0, 0.0);
    assertEquals(out1.get(3).doubleValue(), 1.0, 0.0);
    assertEquals(out1.get(4).doubleValue(), 1.0, 0.0);
    assertEquals(out1.get(5).doubleValue(), 1.0, 0.0);
    assertEquals(out1.get(6).doubleValue(), 1.0, 0.0);
    assertEquals(out1.get(7).doubleValue(), 1.0, 0.0);

  }

  @Test
  public void testSpline() throws Exception {
    String cexpr = "let(echo=true," +
                   "    a=array(0,1,2,3,4,5,6,7), b=array(1,70,90,10,78, 100, 1, 9)," +
                   "    fit=spline(a, b), " +
                   "    der=derivative(fit))";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Number> out = (List<Number>)tuples.get(0).get("fit");
    assertTrue(out.size() == 8);
    assertEquals(out.get(0).doubleValue(), 1.0, 0.0001);
    assertEquals(out.get(1).doubleValue(), 70.0, 0.0001);
    assertEquals(out.get(2).doubleValue(), 90.0, 0.0001);
    assertEquals(out.get(3).doubleValue(), 10.0, 0.0001);
    assertEquals(out.get(4).doubleValue(), 78.0, 0.0001);
    assertEquals(out.get(5).doubleValue(), 100.0, 0.0001);
    assertEquals(out.get(6).doubleValue(), 1.0, 0.0001);
    assertEquals(out.get(7).doubleValue(), 9.0, 0.0001);

    List<Number> out1 = (List<Number>)tuples.get(0).get("der");

    assertTrue(out1.size() == 8);
    assertEquals(out1.get(0).doubleValue(), 72.06870491240123, 0.0001);
    assertEquals(out1.get(1).doubleValue(), 62.86259017519753, 0.0001);
    assertEquals(out1.get(2).doubleValue(),-56.519065613191344, 0.0001);
    assertEquals(out1.get(3).doubleValue(), -16.786327722432148, 0.0001);
    assertEquals(out1.get(4).doubleValue(), 87.66437650291996, 0.0001);
    assertEquals(out1.get(5).doubleValue(), -63.87117828924769, 0.0001);
    assertEquals(out1.get(6).doubleValue(), -63.17966334592923, 0.0001);
    assertEquals(out1.get(7).doubleValue(), 43.58983167296462, 0.0001);
  }


  @Test
  public void testHarmonicFit() throws Exception {
    String cexpr = "let(a=sin(sequence(100, 1, 6)), b=harmonicFit(a), s=ebeSubtract(a, b))";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Number> out = (List<Number>)tuples.get(0).get("s");
    assertTrue(out.size() == 100);
    for(Number n : out) {
      assertEquals(n.doubleValue(), 0.0, .01);
    }
  }

  @Test
  public void testAnova() throws Exception {
    String cexpr = "anova(array(1,2,3,5,4,6), array(5,2,3,5,4,6), array(1,2,7,5,4,6))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Map out = (Map)tuples.get(0).get("return-value");
    assertEquals((double) out.get("p-value"), 0.788298D, .0001);
    assertEquals((double) out.get("f-ratio"), 0.24169D, .0001);
  }

  @Test
  public void testOlsRegress() throws Exception {
    String cexpr = "let(echo=true, a=array(8.5, 12.89999962, 5.199999809, 10.69999981, 3.099999905, 3.5, 9.199999809, 9, 15.10000038, 10.19999981), " +
                       "b=array(5.099999905, 5.800000191, 2.099999905, 8.399998665, 2.900000095, 1.200000048, 3.700000048, 7.599999905, 7.699999809, 4.5)," +
                       "c=array(4.699999809, 8.800000191, 15.10000038, 12.19999981, 10.60000038, 3.5, 9.699999809, 5.900000095, 20.79999924, 7.900000095)," +
                       "d=array(85.09999847, 106.3000031, 50.20000076, 130.6000061, 54.79999924, 30.29999924, 79.40000153, 91, 135.3999939, 89.30000305)," +
                       "e=transpose(matrix(a, b, c))," +
                       "f=olsRegress(e, d)," +
                       "g=predict(f, e))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Map regression = (Map)tuples.get(0).get("f");

    Number rsquared = (Number)regression.get("RSquared");

    assertEquals(rsquared.doubleValue(), 0.9667887860584002, .000001);

    List<Number> regressionParameters = (List<Number>)regression.get("regressionParameters");

    assertEquals(regressionParameters.get(0).doubleValue(), 7.676028542255028, .0001);
    assertEquals(regressionParameters.get(1).doubleValue(), 3.661604009261836, .0001);
    assertEquals(regressionParameters.get(2).doubleValue(), 7.621051256504592, .0001);
    assertEquals(regressionParameters.get(3).doubleValue(), 0.8284680662898674, .0001);

    List<Number> predictions = (List<Number>)tuples.get(0).get("g");

    assertEquals(predictions.get(0).doubleValue(), 81.56082305847914, .0001);
    assertEquals(predictions.get(1).doubleValue(), 106.40333675525883, .0001);
    assertEquals(predictions.get(2).doubleValue(), 55.23044372150484, .0001);
    assertEquals(predictions.get(3).doubleValue(), 120.97932137751451, .0001);
    assertEquals(predictions.get(4).doubleValue(), 49.90981180846799, .0001);
    assertEquals(predictions.get(5).doubleValue(), 32.53654268030196, .0001);
    assertEquals(predictions.get(6).doubleValue(), 77.59681482774931, .0001);
    assertEquals(predictions.get(7).doubleValue(), 103.43841512086125, .0001);
    assertEquals(predictions.get(8).doubleValue(), 138.88047884217636, .0001);
    assertEquals(predictions.get(9).doubleValue(), 85.86401719768607, .0001);
  }

  @Test
  public void testPlot() throws Exception {
    String cexpr = "let(a=array(3,2,3), plot(type=scatter, x=a, y=array(5,6,3)))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    String plot = tuples.get(0).getString("plot");
    assertTrue(plot.equals("scatter"));
    List<List<Number>> data = (List<List<Number>>)tuples.get(0).get("data");
    assertTrue(data.size() == 3);
    List<Number> pair1 = data.get(0);
    assertTrue(pair1.get(0).intValue() == 3);
    assertTrue(pair1.get(1).intValue() == 5);
    List<Number> pair2 = data.get(1);
    assertTrue(pair2.get(0).intValue() == 2);
    assertTrue(pair2.get(1).intValue() == 6);
    List<Number> pair3 = data.get(2);
    assertTrue(pair3.get(0).intValue() == 3);
    assertTrue(pair3.get(1).intValue() == 3);
  }

  @Test
  public void testMovingAverage() throws Exception {
    String cexpr = "movingAvg(array(1,2,3,4,5,6,7), 4)";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Number> out = (List<Number>)tuples.get(0).get("return-value");
    assertTrue(out.size()==4);
    assertEquals((double) out.get(0), 2.5, .0);
    assertEquals((double) out.get(1), 3.5, .0);
    assertEquals((double) out.get(2), 4.5, .0);
    assertEquals((double) out.get(3), 5.5, .0);
  }

  @Test
  public void testMannWhitney() throws Exception {
    String cexpr = "mannWhitney(array(0.15,0.10,0.11,0.24,0.08,0.08,0.10,0.10,0.10,0.12,0.04,0.07), " +
                               "array(0.10,0.20,0.30,0.10,0.10,0.02,0.05,0.07))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Map out = (Map)tuples.get(0).get("return-value");
    assertEquals((double) out.get("u-statistic"), 52.5, .1);
    assertEquals((double) out.get("p-value"), 0.7284, .001);
  }

  @Test
  public void testMovingMedian() throws Exception {
    String cexpr = "movingMedian(array(1,2,6,9,10,12,15), 5)";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Number> out = (List<Number>)tuples.get(0).get("return-value");
    assertTrue(out.size() == 3);
    assertEquals(out.get(0).doubleValue(), 6.0, .0);
    assertEquals(out.get(1).doubleValue(), 9.0, .0);
    assertEquals(out.get(2).doubleValue(), 10.0, .0);
  }

  @Test
  public void testMonteCarlo() throws Exception {
    String cexpr = "let(a=constantDistribution(10), b=constantDistribution(20), c=monteCarlo(add(sample(a), sample(b)), 10))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Number> out = (List<Number>)tuples.get(0).get("c");
    assertTrue(out.size()==10);
    assertEquals(out.get(0).doubleValue(), 30.0, .0);
    assertEquals(out.get(1).doubleValue(), 30.0, .0);
    assertEquals(out.get(2).doubleValue(), 30.0, .0);
    assertEquals(out.get(3).doubleValue(), 30.0, .0);
    assertEquals(out.get(4).doubleValue(), 30.0, .0);
    assertEquals(out.get(5).doubleValue(), 30.0, .0);
    assertEquals(out.get(6).doubleValue(), 30.0, .0);
    assertEquals(out.get(7).doubleValue(), 30.0, .0);
    assertEquals(out.get(8).doubleValue(), 30.0, .0);
    assertEquals(out.get(9).doubleValue(), 30.0, .0);
  }


  @Test
  public void testWeibullDistribution() throws Exception {
    String cexpr = "let(echo=true, " +
                       "a=describe(sample(weibullDistribution(.1, 10),10000)), " +
                       "b=describe(sample(weibullDistribution(.5, 10),10000)), " +
                       "c=describe(sample(weibullDistribution(1, 10),10000))," +
                       "d=describe(sample(weibullDistribution(6, 10),10000))," +
                       "e=mean(sample(weibullDistribution(1, 10),10000))," +
                       "f=mean(sample(weibullDistribution(1, 20),10000))," +
                       "g=mean(sample(weibullDistribution(1, 30),10000)))";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Map a = (Map)tuples.get(0).get("a");
    Map b = (Map)tuples.get(0).get("b");
    Map c = (Map)tuples.get(0).get("c");
    Map d = (Map)tuples.get(0).get("d");

    Number sa = (Number)a.get("skewness");
    Number sb = (Number)b.get("skewness");
    Number sc = (Number)c.get("skewness");
    Number sd = (Number)d.get("skewness");

    //Test shape change
    assertTrue(sa.doubleValue() > sb.doubleValue());
    assertTrue(sb.doubleValue() > sc.doubleValue());
    assertTrue(sc.doubleValue() > sd.doubleValue());
    assertTrue(sd.doubleValue() < 0.0);

    //Test scale change

    Number e = (Number)tuples.get(0).get("e");
    Number f = (Number)tuples.get(0).get("f");
    Number g = (Number)tuples.get(0).get("g");

    assertTrue(e.doubleValue() < f.doubleValue());
    assertTrue(f.doubleValue() < g.doubleValue());
  }


  @Test
  public void testGammaDistribution() throws Exception {
    String cexpr = "#comment\nlet(echo=true, " +
        "a=describe(sample(gammaDistribution(1, 10),10000)), " +
        "\n# commment\n"+
        "b=describe(sample(gammaDistribution(3, 10),10000)), " +
        "c=describe(sample(gammaDistribution(5, 10),10000))," +
        "d=describe(sample(gammaDistribution(7, 10),10000))," +
        "e=mean(sample(gammaDistribution(1, 10),10000))," +
        "f=mean(sample(gammaDistribution(1, 20),10000))," +
        "g=mean(sample(gammaDistribution(1, 30),10000)))";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Map a = (Map)tuples.get(0).get("a");
    Map b = (Map)tuples.get(0).get("b");
    Map c = (Map)tuples.get(0).get("c");
    Map d = (Map)tuples.get(0).get("d");

    Number sa = (Number)a.get("skewness");
    Number sb = (Number)b.get("skewness");
    Number sc = (Number)c.get("skewness");
    Number sd = (Number)d.get("skewness");

    //Test shape change
    assertTrue(sa.doubleValue() > sb.doubleValue());
    assertTrue(sb.doubleValue() > sc.doubleValue());
    assertTrue(sc.doubleValue() > sd.doubleValue());

    //Test scale change

    Number e = (Number)tuples.get(0).get("e");
    Number f = (Number)tuples.get(0).get("f");
    Number g = (Number)tuples.get(0).get("g");

    assertTrue(e.doubleValue() < f.doubleValue());
    assertTrue(f.doubleValue() < g.doubleValue());
  }




  @Test
  public void testLogNormalDistribution() throws Exception {
    String cexpr = "let(echo=true, " +
        "a=describe(sample(logNormalDistribution(.1, 0),10000)), " +
        "b=describe(sample(logNormalDistribution(.3, 0),10000)), " +
        "c=describe(sample(logNormalDistribution(.6, 0),10000))," +
        "d=mean(sample(logNormalDistribution(.3, 0),10000)), " +
        "e=mean(sample(logNormalDistribution(.3, 2),10000)), " +
        ")";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Map a = (Map)tuples.get(0).get("a");
    Map b = (Map)tuples.get(0).get("b");
    Map c = (Map)tuples.get(0).get("c");

    Number sa = (Number)a.get("skewness");
    Number sb = (Number)b.get("skewness");
    Number sc = (Number)c.get("skewness");

    assertTrue(sa.doubleValue() < sb.doubleValue());
    assertTrue(sb.doubleValue() < sc.doubleValue());

    Number d = (Number)tuples.get(0).get("d");
    Number e = (Number)tuples.get(0).get("e");

    assertTrue(d.doubleValue() < e.doubleValue());

  }

  @Test
  public void testTriangularDistribution() throws Exception {
    String cexpr = "let(echo=true, " +
        "a=describe(sample(triangularDistribution(10, 15, 30),10000)), " +
        "b=describe(sample(triangularDistribution(10, 25, 30),10000)), " +
        ")";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Map a = (Map)tuples.get(0).get("a");
    Map b = (Map)tuples.get(0).get("b");

    Number sa = (Number)a.get("skewness");
    Number sb = (Number)b.get("skewness");

    Number mina = (Number)a.get("min");
    Number maxa = (Number)a.get("max");

    assertTrue(sa.doubleValue() > 0);
    assertTrue(sb.doubleValue() < 0);
    assertEquals(mina.doubleValue(), 10, .5);
    assertEquals(maxa.doubleValue(), 30, .5);
  }

  @Test
  public void testCovMatrix() throws Exception {
    String cexpr = "let(a=array(1,2,3), b=array(2,4,6), c=array(4, 8, 12), d=transpose(matrix(a, b, c)), f=cov(d))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<List<Number>> cm = (List<List<Number>>)tuples.get(0).get("f");
    assertEquals(cm.size(), 3);
    List<Number> row1 = cm.get(0);
    assertEquals(row1.size(), 3);
    assertEquals(row1.get(0).longValue(), 1);
    assertEquals(row1.get(1).longValue(), 2);
    assertEquals(row1.get(2).longValue(), 4);

    List<Number> row2 = cm.get(1);
    assertEquals(row2.size(), 3);
    assertEquals(row2.get(0).longValue(), 2);
    assertEquals(row2.get(1).longValue(), 4);
    assertEquals(row2.get(2).longValue(), 8);

    List<Number> row3 = cm.get(2);
    assertEquals(row3.size(), 3);
    assertEquals(row3.get(0).longValue(), 4);
    assertEquals(row3.get(1).longValue(), 8);
    assertEquals(row3.get(2).longValue(), 16);
  }

  @Test
  public void testCorrMatrix() throws Exception {
    String cexpr = "let(echo=true," +
                       "a=array(1,2,3), " +
                       "b=array(2,4,6), " +
                       "c=array(4, 8, 52), " +
                       "d=transpose(matrix(a, b, c)), " +
                       "f=corr(d), " +
                       "g=corr(d, type=kendalls), " +
                       "h=corr(d, type=spearmans)," +
                       "i=corrPValues(f))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<List<Number>> cm = (List<List<Number>>)tuples.get(0).get("f");
    assertEquals(cm.size(), 3);
    List<Number> row1 = cm.get(0);
    assertEquals(row1.size(), 3);
    assertEquals(row1.get(0).doubleValue(), 1, 0);
    assertEquals(row1.get(1).doubleValue(), 1, 0);
    assertEquals(row1.get(2).doubleValue(), 0.901127113779166, 0);

    List<Number> row2 = cm.get(1);
    assertEquals(row2.size(), 3);
    assertEquals(row2.get(0).doubleValue(), 1, 0);
    assertEquals(row2.get(1).doubleValue(), 1, 0);
    assertEquals(row2.get(2).doubleValue(), 0.901127113779166, 0);

    List<Number> row3 = cm.get(2);
    assertEquals(row3.size(), 3);
    assertEquals(row3.get(0).doubleValue(), 0.901127113779166, 0);
    assertEquals(row3.get(1).doubleValue(), 0.901127113779166, 0);
    assertEquals(row3.get(2).doubleValue(), 1, 0);

    cm = (List<List<Number>>)tuples.get(0).get("g");
    assertEquals(cm.size(), 3);
    row1 = cm.get(0);
    assertEquals(row1.size(), 3);
    assertEquals(row1.get(0).doubleValue(), 1, 0);
    assertEquals(row1.get(1).doubleValue(), 1, 0);
    assertEquals(row1.get(2).doubleValue(), 1, 0);

    row2 = cm.get(1);
    assertEquals(row2.size(), 3);
    assertEquals(row2.get(0).doubleValue(), 1, 0);
    assertEquals(row2.get(1).doubleValue(), 1, 0);
    assertEquals(row2.get(2).doubleValue(), 1, 0);

    row3 = cm.get(2);
    assertEquals(row3.size(), 3);
    assertEquals(row3.get(0).doubleValue(), 1, 0);
    assertEquals(row3.get(1).doubleValue(), 1, 0);
    assertEquals(row3.get(2).doubleValue(), 1, 0);

    cm = (List<List<Number>>)tuples.get(0).get("h");
    assertEquals(cm.size(), 3);
    row1 = cm.get(0);
    assertEquals(row1.size(), 3);
    assertEquals(row1.get(0).doubleValue(), 1, 0);
    assertEquals(row1.get(1).doubleValue(), 1, 0);
    assertEquals(row1.get(2).doubleValue(), 1, 0);

    row2 = cm.get(1);
    assertEquals(row2.size(), 3);
    assertEquals(row2.get(0).doubleValue(), 1, 0);
    assertEquals(row2.get(1).doubleValue(), 1, 0);
    assertEquals(row2.get(2).doubleValue(), 1, 0);

    row3 = cm.get(2);
    assertEquals(row3.size(), 3);
    assertEquals(row3.get(0).doubleValue(), 1, 0);
    assertEquals(row3.get(1).doubleValue(), 1, 0);
    assertEquals(row3.get(2).doubleValue(), 1, 0);

    cm = (List<List<Number>>)tuples.get(0).get("i");
    assertEquals(cm.size(), 3);
    row1 = cm.get(0);
    assertEquals(row1.size(), 3);
    assertEquals(row1.get(0).doubleValue(), 0, 0);
    assertEquals(row1.get(1).doubleValue(), 0, 0);
    assertEquals(row1.get(2).doubleValue(), 0.28548201004998375, 0);

    row2 = cm.get(1);
    assertEquals(row2.size(), 3);
    assertEquals(row2.get(0).doubleValue(), 0, 0);
    assertEquals(row2.get(1).doubleValue(), 0, 0);
    assertEquals(row2.get(2).doubleValue(), 0.28548201004998375, 0);

    row3 = cm.get(2);
    assertEquals(row3.size(), 3);
    assertEquals(row3.get(0).doubleValue(), 0.28548201004998375, 0);
    assertEquals(row3.get(1).doubleValue(), 0.28548201004998375, 0);
    assertEquals(row3.get(2).doubleValue(), 0, 0);
  }


  @Test
  public void testPrecision() throws Exception {
    String cexpr = "let(echo=true, a=precision(array(1.44445, 1, 2.00006), 4), b=precision(1.44445, 4))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);

    List<Number> nums = (List<Number>)tuples.get(0).get("a");
    assertTrue(nums.size() == 3);
    assertEquals(nums.get(0).doubleValue(), 1.4445, 0.0);
    assertEquals(nums.get(1).doubleValue(), 1, 0.0);
    assertEquals(nums.get(2).doubleValue(), 2.0001, 0.0);

    double num = tuples.get(0).getDouble("b");
    assertEquals(num, 1.4445, 0.0);
  }

  @Test
  public void testMinMaxScale() throws Exception {
    String cexpr = "let(echo=true, a=minMaxScale(matrix(array(1,2,3,4,5), array(10,20,30,40,50))), " +
                                  "b=minMaxScale(matrix(array(1,2,3,4,5), array(10,20,30,40,50)), 0, 100)," +
                                  "c=minMaxScale(array(1,2,3,4,5))," +
                                  "d=minMaxScale(array(1,2,3,4,5), 0, 100))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);

    List<List<Number>> matrix = (List<List<Number>>)tuples.get(0).get("a");
    List<Number> row1 = matrix.get(0);
    assertEquals(row1.get(0).doubleValue(), 0,0);
    assertEquals(row1.get(1).doubleValue(), .25,0);
    assertEquals(row1.get(2).doubleValue(), .5,0);
    assertEquals(row1.get(3).doubleValue(), .75, 0);
    assertEquals(row1.get(4).doubleValue(), 1, 0);

    List<Number> row2 = matrix.get(1);
    assertEquals(row2.get(0).doubleValue(), 0,0);
    assertEquals(row2.get(1).doubleValue(), .25,0);
    assertEquals(row2.get(2).doubleValue(), .5,0);
    assertEquals(row2.get(3).doubleValue(), .75,0);
    assertEquals(row2.get(4).doubleValue(), 1,0);

    matrix = (List<List<Number>>)tuples.get(0).get("b");
    row1 = matrix.get(0);
    assertEquals(row1.get(0).doubleValue(), 0,0);
    assertEquals(row1.get(1).doubleValue(), 25,0);
    assertEquals(row1.get(2).doubleValue(), 50,0);
    assertEquals(row1.get(3).doubleValue(), 75,0);
    assertEquals(row1.get(4).doubleValue(), 100,0);

    row2 = matrix.get(1);
    assertEquals(row2.get(0).doubleValue(), 0,0);
    assertEquals(row2.get(1).doubleValue(), 25,0);
    assertEquals(row2.get(2).doubleValue(), 50,0);
    assertEquals(row2.get(3).doubleValue(), 75,0);
    assertEquals(row2.get(4).doubleValue(), 100,0);

    List<Number> row3= (List<Number>)tuples.get(0).get("c");
    assertEquals(row3.get(0).doubleValue(), 0,0);
    assertEquals(row3.get(1).doubleValue(), .25,0);
    assertEquals(row3.get(2).doubleValue(), .5,0);
    assertEquals(row3.get(3).doubleValue(), .75,0);
    assertEquals(row3.get(4).doubleValue(), 1,0);

    List<Number> row4= (List<Number>)tuples.get(0).get("d");
    assertEquals(row4.get(0).doubleValue(), 0,0);
    assertEquals(row4.get(1).doubleValue(), 25,0);
    assertEquals(row4.get(2).doubleValue(), 50,0);
    assertEquals(row4.get(3).doubleValue(), 75,0);
    assertEquals(row4.get(4).doubleValue(), 100,0);
  }


  @Test
  public void testMean() throws Exception {
    String cexpr = "mean(array(1,2,3,4,5))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Number mean = (Number)tuples.get(0).get("return-value");
    assertEquals(mean.doubleValue(), 3.0D, 0.0D);
  }


  @Test
  public void testScale() throws Exception {
    UpdateRequest updateRequest = new UpdateRequest();

    int i=0;
    while(i<50) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2016", "5", "1"), "price_f", "400.00");
    }

    while(i<100) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2015", "5", "1"), "price_f", "300.0");
    }

    while(i<150) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2014", "5", "1"), "price_f", "500.0");
    }

    while(i<250) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2013", "5", "1"), "price_f", "100.00");
    }

    updateRequest.commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    String expr = "timeseries("+COLLECTIONORALIAS+", q=\"*:*\", start=\"2013-01-01T01:00:00.000Z\", " +
        "end=\"2016-12-01T01:00:00.000Z\", " +
        "gap=\"+1YEAR\", " +
        "field=\"test_dt\", " +
        "count(*), sum(price_f), max(price_f), min(price_f))";

    String cexpr = "let(a="+expr+", c=col(a, max(price_f)), tuple(reverse=rev(c), scaled=scale(2, c)))";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");

    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Number> reverse = (List<Number>)tuples.get(0).get("reverse");
    assertTrue(reverse.size() == 4);
    assertTrue(reverse.get(0).doubleValue() == 400D);
    assertTrue(reverse.get(1).doubleValue() == 300D);
    assertTrue(reverse.get(2).doubleValue() == 500D);
    assertTrue(reverse.get(3).doubleValue() == 100D);

    List<Number> ranked = (List<Number>)tuples.get(0).get("scaled");
    assertTrue(ranked.size() == 4);
    assertTrue(ranked.get(0).doubleValue() == 200D);
    assertTrue(ranked.get(1).doubleValue() == 1000D);
    assertTrue(ranked.get(2).doubleValue() == 600D);
    assertTrue(ranked.get(3).doubleValue() == 800D);
  }


  @Test
  public void testConvolution() throws Exception {
    UpdateRequest updateRequest = new UpdateRequest();

    int i=0;
    while(i<50) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2016", "5", "1"), "price_f", "400.00");
    }

    while(i<100) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2015", "5", "1"), "price_f", "300.0");
    }

    while(i<150) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2014", "5", "1"), "price_f", "500.0");
    }

    while(i<250) {
      updateRequest.add(id, "id_"+(++i),"test_dt", getDateString("2013", "5", "1"), "price_f", "100.00");
    }

    updateRequest.commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    String expr = "timeseries("+COLLECTIONORALIAS+", q=\"*:*\", start=\"2013-01-01T01:00:00.000Z\", " +
        "end=\"2016-12-01T01:00:00.000Z\", " +
        "gap=\"+1YEAR\", " +
        "field=\"test_dt\", " +
        "count(*), sum(price_f), max(price_f), min(price_f))";

    String cexpr = "let(a="+expr+", b=select("+expr+",mult(2, count(*)) as nvalue), c=col(a, count(*)), d=col(b, nvalue), tuple(colc=c, cold=d, conv=conv(c,d)))";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");

    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Number> convolution = (List<Number>)(tuples.get(0)).get("conv");
    assertTrue(convolution.size() == 7);
    assertTrue(convolution.get(0).equals(20000L));
    assertTrue(convolution.get(1).equals(20000L));
    assertTrue(convolution.get(2).equals(25000L));
    assertTrue(convolution.get(3).equals(30000L));
    assertTrue(convolution.get(4).equals(15000L));
    assertTrue(convolution.get(5).equals(10000L));
    assertTrue(convolution.get(6).equals(5000L));
  }


  @Test
  public void testRegressAndPredict() throws Exception {
    UpdateRequest updateRequest = new UpdateRequest();


    updateRequest.add(id, "1", "price_f", "100.0", "col_s", "a", "order_i", "1");
    updateRequest.add(id, "2", "price_f", "200.0", "col_s", "a", "order_i", "2");
    updateRequest.add(id, "3", "price_f", "300.0", "col_s", "a", "order_i", "3");
    updateRequest.add(id, "4", "price_f", "100.0", "col_s", "a", "order_i", "4");
    updateRequest.add(id, "5", "price_f", "200.0", "col_s", "a", "order_i", "5");
    updateRequest.add(id, "6", "price_f", "400.0", "col_s", "a", "order_i", "6");
    updateRequest.add(id, "7", "price_f", "600.0", "col_s", "a", "order_i", "7");

    updateRequest.add(id, "8", "price_f", "200.0", "col_s", "b", "order_i", "1");
    updateRequest.add(id, "9", "price_f", "400.0", "col_s", "b", "order_i", "2");
    updateRequest.add(id, "10", "price_f", "600.0", "col_s", "b", "order_i", "3");
    updateRequest.add(id, "11", "price_f", "200.0", "col_s", "b", "order_i", "4");
    updateRequest.add(id, "12", "price_f", "400.0", "col_s", "b", "order_i", "5");
    updateRequest.add(id, "13", "price_f", "800.0", "col_s", "b", "order_i", "6");
    updateRequest.add(id, "14", "price_f", "1200.0", "col_s", "b", "order_i", "7");
    updateRequest.commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    String expr1 = "search("+COLLECTIONORALIAS+", q=\"col_s:a\", fl=\"price_f, order_i\", sort=\"order_i asc\")";
    String expr2 = "search("+COLLECTIONORALIAS+", q=\"col_s:b\", fl=\"price_f, order_i\", sort=\"order_i asc\")";

    String cexpr = "let(a="+expr1+", b="+expr2+", c=col(a, price_f), d=col(b, price_f), e=regress(c, d), tuple(regress=e, p=predict(e, 300), pl=predict(e, c)))";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");

    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Tuple tuple = tuples.get(0);
    Map regression = (Map)tuple.get("regress");
    double slope = (double)regression.get("slope");
    double intercept= (double) regression.get("intercept");
    double rSquare= (double) regression.get("RSquared");
    assertTrue(slope == 2.0D);
    assertTrue(intercept == 0.0D);
    assertTrue(rSquare == 1.0D);
    double prediction = tuple.getDouble("p");
    assertTrue(prediction == 600.0D);
    List<Number> predictions = (List<Number>)tuple.get("pl");
    assertList(predictions, 200L, 400L, 600L, 200L, 400L, 800L, 1200L);
  }


  @Test
  public void testFinddelay() throws Exception {
    UpdateRequest updateRequest = new UpdateRequest();

    //Pad column 1 with three zeros.
    updateRequest.add(id, "10", "price_f", "0.0", "col_s", "a", "order_i", "0");
    updateRequest.add(id, "11", "price_f", "0.0", "col_s", "a", "order_i", "0");
    updateRequest.add(id, "12", "price_f", "0.0", "col_s", "a", "order_i", "0");
    updateRequest.add(id, "1", "price_f", "100.0", "col_s", "a", "order_i", "1");
    updateRequest.add(id, "2", "price_f", "200.0", "col_s", "a", "order_i", "2");
    updateRequest.add(id, "3", "price_f", "300.0", "col_s", "a", "order_i", "3");
    updateRequest.add(id, "4", "price_f", "100.0", "col_s", "a", "order_i", "4");
    updateRequest.add(id, "5", "price_f", "200.0", "col_s", "a", "order_i", "5");
    updateRequest.add(id, "6", "price_f", "400.0", "col_s", "a", "order_i", "6");
    updateRequest.add(id, "7", "price_f", "600.0", "col_s", "a", "order_i", "7");

    updateRequest.add(id, "100", "price_f", "200.0", "col_s", "b", "order_i", "1");
    updateRequest.add(id, "101", "price_f", "400.0", "col_s", "b", "order_i", "2");
    updateRequest.add(id, "102", "price_f", "600.0", "col_s", "b", "order_i", "3");
    updateRequest.add(id, "103", "price_f", "200.0", "col_s", "b", "order_i", "4");
    updateRequest.add(id, "104", "price_f", "400.0", "col_s", "b", "order_i", "5");
    updateRequest.add(id, "105", "price_f", "800.0", "col_s", "b", "order_i", "6");
    updateRequest.add(id, "106", "price_f", "1200.0", "col_s", "b", "order_i", "7");


    updateRequest.add(id, "200", "price_f", "-200.0", "col_s", "c", "order_i", "1");
    updateRequest.add(id, "301", "price_f", "-400.0", "col_s", "c", "order_i", "2");
    updateRequest.add(id, "402", "price_f", "-600.0", "col_s", "c", "order_i", "3");
    updateRequest.add(id, "503", "price_f", "-200.0", "col_s", "c", "order_i", "4");
    updateRequest.add(id, "604", "price_f", "-400.0", "col_s", "c", "order_i", "5");
    updateRequest.add(id, "705", "price_f", "-800.0", "col_s", "c", "order_i", "6");
    updateRequest.add(id, "806", "price_f", "-1200.0", "col_s", "c", "order_i", "7");
    updateRequest.commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    String expr1 = "search("+COLLECTIONORALIAS+", q=\"col_s:a\", fl=\"price_f, order_i\", sort=\"order_i asc\")";
    String expr2 = "search("+COLLECTIONORALIAS+", q=\"col_s:b\", fl=\"price_f, order_i\", sort=\"order_i asc\")";

    String cexpr = "let(a="+expr1+", b="+expr2+", c=col(a, price_f), d=col(b, price_f), tuple(delay=finddelay(c, d)))";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");

    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Tuple tuple = tuples.get(0);
    long delay = tuple.getLong("delay");
    assert(delay == 3);

    expr1 = "search("+COLLECTIONORALIAS+", q=\"col_s:a\", fq=\"id:(1 2 3 4 5 6 7)\", fl=\"price_f, order_i\", sort=\"order_i asc\")";
    expr2 = "search("+COLLECTIONORALIAS+", q=\"col_s:b\", fl=\"price_f, order_i\", sort=\"order_i asc\")";

    cexpr = "let(a="+expr1+", b="+expr2+", c=col(a, price_f), d=col(b, price_f), tuple(delay=finddelay(c, d)))";

    paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");

    solrStream = new SolrStream(url, paramsLoc);

    solrStream.setStreamContext(context);
    tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    tuple = tuples.get(0);
    delay = tuple.getLong("delay");
    assert(delay == 0);

    //Test negative correlation.
    expr1 = "search("+COLLECTIONORALIAS+", q=\"col_s:a\", fq=\"id:(1 2 3 4 5 6 7 11 12)\",fl=\"price_f, order_i\", sort=\"order_i asc\")";
    expr2 = "search("+COLLECTIONORALIAS+", q=\"col_s:c\", fl=\"price_f, order_i\", sort=\"order_i asc\")";

    cexpr = "let(a="+expr1+", b="+expr2+", c=col(a, price_f), d=col(b, price_f), tuple(delay=finddelay(c, d)))";

    paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");

    solrStream = new SolrStream(url, paramsLoc);

    solrStream.setStreamContext(context);
    tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    tuple = tuples.get(0);
    delay = tuple.getLong("delay");
    assert(delay == 2);
  }


  @Test
  public void testDescribe() throws Exception {
    UpdateRequest updateRequest = new UpdateRequest();


    updateRequest.add(id, "1", "price_f", "100.0", "col_s", "a", "order_i", "1");
    updateRequest.add(id, "2", "price_f", "200.0", "col_s", "a", "order_i", "2");
    updateRequest.add(id, "3", "price_f", "300.0", "col_s", "a", "order_i", "3");
    updateRequest.add(id, "4", "price_f", "100.0", "col_s", "a", "order_i", "4");
    updateRequest.add(id, "5", "price_f", "200.0", "col_s", "a", "order_i", "5");
    updateRequest.add(id, "6", "price_f", "400.0", "col_s", "a", "order_i", "6");
    updateRequest.add(id, "7", "price_f", "600.0", "col_s", "a", "order_i", "7");


    updateRequest.commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    String expr1 = "search("+COLLECTIONORALIAS+", q=\"col_s:a\", fl=\"price_f, order_i\", sort=\"order_i asc\")";

    String cexpr = "let(a="+expr1+", b=col(a, price_f),  tuple(stats=describe(b)))";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");

    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Tuple tuple = tuples.get(0);
    Map stats = (Map)tuple.get("stats");
    Number min = (Number)stats.get("min");
    Number max = (Number)stats.get("max");
    Number mean = (Number)stats.get("mean");
    Number stdev = (Number)stats.get("stdev");
    Number popVar = (Number)stats.get("popVar");
    Number skewness = (Number)stats.get("skewness");
    Number kurtosis = (Number)stats.get("kurtosis");
    Number var = (Number)stats.get("var");
    Number geometricMean = (Number)stats.get("geometricMean");
    Number N = (Number)stats.get("N");
    assertEquals(min.doubleValue(), 100.0D, 0.0);
    assertEquals(max.doubleValue(), 600.0D, 0.0);
    assertEquals(N.doubleValue(), 7.0D, 0.0);
    assertEquals(mean.doubleValue(), 271.42D, 0.5);
    assertEquals(popVar.doubleValue(), 27755.10, 0.5);
    assertEquals(kurtosis.doubleValue(), .70D, 0.5);
    assertEquals(skewness.doubleValue(), 1.07D, 0.5);
    assertEquals(var.doubleValue(), 32380.95D, 0.5);
    assertEquals(geometricMean .doubleValue(), 224.56D, 0.5);
    assertEquals(stdev .doubleValue(), 179.94D, 0.5);
  }


  @Test
  public void testLength() throws Exception {
    UpdateRequest updateRequest = new UpdateRequest();


    updateRequest.add(id, "1", "price_f", "100.0", "col_s", "a", "order_i", "1");
    updateRequest.add(id, "2", "price_f", "200.0", "col_s", "a", "order_i", "2");
    updateRequest.add(id, "3", "price_f", "300.0", "col_s", "a", "order_i", "3");
    updateRequest.add(id, "4", "price_f", "100.0", "col_s", "a", "order_i", "4");
    updateRequest.add(id, "5", "price_f", "200.0", "col_s", "a", "order_i", "5");
    updateRequest.add(id, "6", "price_f", "400.0", "col_s", "a", "order_i", "6");
    updateRequest.add(id, "7", "price_f", "600.0", "col_s", "a", "order_i", "7");

    updateRequest.add(id, "8", "price_f", "200.0", "col_s", "b", "order_i", "1");
    updateRequest.add(id, "9", "price_f", "400.0", "col_s", "b", "order_i", "2");
    updateRequest.add(id, "10", "price_f", "600.0", "col_s", "b", "order_i", "3");
    updateRequest.add(id, "11", "price_f", "200.0", "col_s", "b", "order_i", "4");
    updateRequest.add(id, "12", "price_f", "400.0", "col_s", "b", "order_i", "5");
    updateRequest.add(id, "13", "price_f", "800.0", "col_s", "b", "order_i", "6");
    updateRequest.add(id, "14", "price_f", "1200.0", "col_s", "b", "order_i", "7");
    updateRequest.commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    String expr1 = "search("+COLLECTIONORALIAS+", q=\"col_s:a\", fl=\"price_f, order_i\", sort=\"order_i asc\")";
    String expr2 = "search("+COLLECTIONORALIAS+", q=\"col_s:b\", fl=\"price_f, order_i\", sort=\"order_i asc\")";

    String cexpr = "let(a="+expr1+", b="+expr2+", c=col(a, price_f), d=col(b, price_f), e=regress(c, d), tuple(regress=e, p=predict(e, 300), l=length(d)))";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cexpr);
    paramsLoc.set("qt", "/stream");

    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Tuple tuple = tuples.get(0);
    Map regression = (Map)tuple.get("regress");
    double slope = (double)regression.get("slope");
    double intercept= (double) regression.get("intercept");
    double length = tuple.getDouble("l");

    assertTrue(slope == 2.0D);
    assertTrue(intercept == 0.0D);
    double prediction = tuple.getDouble("p");
    assertTrue(prediction == 600.0D);
    assertTrue(length == 7);
  }

  @Test
  public void testListStream() throws Exception {
    UpdateRequest updateRequest = new UpdateRequest();
    updateRequest.add(id, "hello", "test_t", "l b c d c");
    updateRequest.add(id, "hello1", "test_t", "l b c d c");
    updateRequest.add(id, "hello2", "test_t", "l b c d c");

    updateRequest.commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    String expr1 = "search("+COLLECTIONORALIAS+", q=\"id:hello\",  fl=id, sort=\"id desc\")";
    String expr2 = "search("+COLLECTIONORALIAS+", q=\"id:hello1\", fl=id, sort=\"id desc\")";
    String expr3 = "search("+COLLECTIONORALIAS+", q=\"id:hello2\", fl=id, sort=\"id desc\")";

    String cat = "list("+expr1+","+expr2+","+expr3+")";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cat);
    paramsLoc.set("qt", "/stream");

    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 3);
    String s = (String)tuples.get(0).get("id");
    assertTrue(s.equals("hello"));
    s = (String)tuples.get(1).get("id");
    assertTrue(s.equals("hello1"));
    s = (String)tuples.get(2).get("id");
    assertTrue(s.equals("hello2"));
  }

  @Test
  public void testCellStream() throws Exception {
    UpdateRequest updateRequest = new UpdateRequest();
    updateRequest.add(id, "hello", "test_t", "l b c d c e");
    updateRequest.add(id, "hello1", "test_t", "l b c d c");

    updateRequest.commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    String expr = "search("+COLLECTIONORALIAS+", q=\"*:*\", fl=\"id,test_t\", sort=\"id desc\")";
    String cat = "cell(results,"+expr+")";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cat);
    paramsLoc.set("qt", "/stream");

    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Map> results  = (List<Map>)tuples.get(0).get("results");
    assertTrue(results.get(0).get("id").equals("hello1"));
    assertTrue(results.get(0).get("test_t").equals("l b c d c"));
    assertTrue(results.get(1).get("id").equals("hello"));
    assertTrue(results.get(1).get("test_t").equals("l b c d c e"));

  }

  @Test
  public void testTupleStream() throws Exception {
    UpdateRequest updateRequest = new UpdateRequest();
    updateRequest.add(id, "hello", "test_t", "l b c d c e");
    updateRequest.add(id, "hello1", "test_t", "l b c d c");
    updateRequest.commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    String expr = "search("+COLLECTIONORALIAS+", q=\"*:*\", fl=\"id,test_t\", sort=\"id desc\")";

    //Add a Stream and an Evaluator to the Tuple.
    String cat = "tuple(results="+expr+", sum=add(1,1))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cat);
    paramsLoc.set("qt", "/stream");

    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    List<Map> results  = (List<Map>)tuples.get(0).get("results");
    assertTrue(results.get(0).get("id").equals("hello1"));
    assertTrue(results.get(0).get("test_t").equals("l b c d c"));
    assertTrue(results.get(1).get("id").equals("hello"));
    assertTrue(results.get(1).get("test_t").equals("l b c d c e"));

    assertTrue(tuples.get(0).getLong("sum").equals(2L));

  }

  @Test
  public void testLetStream() throws Exception {
    UpdateRequest updateRequest = new UpdateRequest();
    updateRequest.add(id, "hello", "test_t", "l b c d c e", "test_i", "5");
    updateRequest.add(id, "hello1", "test_t", "l b c d c", "test_i", "4");

    updateRequest.commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    String expr = "search("+COLLECTIONORALIAS+", q=\"*:*\", fl=\"id,test_t, test_i\", sort=\"id desc\")";
    String cat = "let(d ="+expr+", b = add(1,3), c=col(d, test_i), tuple(test = add(1,1), test1=b, results=d, test2=add(c)))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cat);
    paramsLoc.set("qt", "/stream");

    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Tuple tuple1 = tuples.get(0);
    List<Map> results = (List<Map>)tuple1.get("results");
    assertTrue(results.size() == 2);
    assertTrue(results.get(0).get("id").equals("hello1"));
    assertTrue(results.get(0).get("test_t").equals("l b c d c"));
    assertTrue(results.get(1).get("id").equals("hello"));
    assertTrue(results.get(1).get("test_t").equals("l b c d c e"));

    assertTrue(tuple1.getLong("test").equals(2L));
    assertTrue(tuple1.getLong("test1").equals(4L));
    assertTrue(tuple1.getLong("test2").equals(9L));


  }
  
  @Test
  public void testGetStreamForEOFTuple() throws Exception {
    UpdateRequest updateRequest = new UpdateRequest();
    updateRequest.add(id, "hello", "test_t", "l b c d c e", "test_i", "5");
    updateRequest.add(id, "hello1", "test_t", "l b c d c", "test_i", "4");

    updateRequest.commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    String expr = "search("+COLLECTIONORALIAS+", q=\"id:hello2\", fl=\"id,test_t, test_i\", sort=\"id desc\")";
    String cat = "let(a ="+expr+",get(a))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cat);
    paramsLoc.set("qt", "/stream");

    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 0);


  }

  @Test
  public void testConvertEvaluator() throws Exception {

    UpdateRequest updateRequest = new UpdateRequest();
    updateRequest.add(id, "1", "miles_i", "50");
    updateRequest.add(id, "2", "miles_i", "70");

    updateRequest.commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    //Test annotating tuple
    String expr = "select(calc(), convert(miles, kilometers, 10) as kilometers)";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", expr);
    paramsLoc.set("qt", "/stream");

    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    double d = (double)tuples.get(0).get("kilometers");
    assertTrue(d == (double)(10*1.61));


    expr = "select(search("+COLLECTIONORALIAS+", q=\"*:*\", sort=\"miles_i asc\", fl=\"miles_i\"), convert(miles, kilometers, miles_i) as kilometers)";
    paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", expr);
    paramsLoc.set("qt", "/stream");

    solrStream = new SolrStream(url, paramsLoc);
    context = new StreamContext();
    solrStream.setStreamContext(context);
    tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 2);
    d = (double)tuples.get(0).get("kilometers");
    assertTrue(d == (double)(50*1.61));
    d = (double)tuples.get(1).get("kilometers");
    assertTrue(d == (double)(70*1.61));

    expr = "parallel("+COLLECTIONORALIAS+", workers=2, sort=\"miles_i asc\", select(search("+COLLECTIONORALIAS+", q=\"*:*\", partitionKeys=miles_i, sort=\"miles_i asc\", fl=\"miles_i\"), convert(miles, kilometers, miles_i) as kilometers))";
    paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", expr);
    paramsLoc.set("qt", "/stream");
    solrStream = new SolrStream(url, paramsLoc);
    context = new StreamContext();
    solrStream.setStreamContext(context);
    tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 2);
    d = (double)tuples.get(0).get("kilometers");
    assertTrue(d == (double)(50*1.61));
    d = (double)tuples.get(1).get("kilometers");
    assertTrue(d == (double)(70*1.61));

    expr = "select(stats("+COLLECTIONORALIAS+", q=\"*:*\", sum(miles_i)), convert(miles, kilometers, sum(miles_i)) as kilometers)";
    paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", expr);
    paramsLoc.set("qt", "/stream");
    solrStream = new SolrStream(url, paramsLoc);
    context = new StreamContext();
    solrStream.setStreamContext(context);
    tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    d = (double)tuples.get(0).get("kilometers");
    assertTrue(d == (double)(120*1.61));
  }

  @Test
  public void testExecutorStream() throws Exception {
    CollectionAdminRequest.createCollection("workQueue", "conf", 2, 1).processAndWait(cluster.getSolrClient(), DEFAULT_TIMEOUT);
    AbstractDistribZkTestBase.waitForRecoveriesToFinish("workQueue", cluster.getSolrClient().getZkStateReader(),
        false, true, TIMEOUT);
    CollectionAdminRequest.createCollection("mainCorpus", "conf", 2, 1).processAndWait(cluster.getSolrClient(), DEFAULT_TIMEOUT);
    AbstractDistribZkTestBase.waitForRecoveriesToFinish("mainCorpus", cluster.getSolrClient().getZkStateReader(),
        false, true, TIMEOUT);
    CollectionAdminRequest.createCollection("destination", "conf", 2, 1).processAndWait(cluster.getSolrClient(), DEFAULT_TIMEOUT);
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
    CollectionAdminRequest.createCollection("workQueue1", "conf", 2, 1).processAndWait(cluster.getSolrClient(),DEFAULT_TIMEOUT);
    AbstractDistribZkTestBase.waitForRecoveriesToFinish("workQueue1", cluster.getSolrClient().getZkStateReader(),
        false, true, TIMEOUT);
    CollectionAdminRequest.createCollection("mainCorpus1", "conf", 2, 1).processAndWait(cluster.getSolrClient(), DEFAULT_TIMEOUT);
    AbstractDistribZkTestBase.waitForRecoveriesToFinish("mainCorpus1", cluster.getSolrClient().getZkStateReader(),
        false, true, TIMEOUT);
    CollectionAdminRequest.createCollection("destination1", "conf", 2, 1).processAndWait(cluster.getSolrClient(), DEFAULT_TIMEOUT);
    AbstractDistribZkTestBase.waitForRecoveriesToFinish("destination1", cluster.getSolrClient().getZkStateReader(),
        false, true, TIMEOUT);

    UpdateRequest workRequest = new UpdateRequest();
    UpdateRequest dataRequest = new UpdateRequest();


    for (int i = 0; i < 500; i++) {
      workRequest.add(id, String.valueOf(i), "expr_s", "update(destination1, batchSize=50, search(mainCorpus1, q=id:"+i+", rows=1, sort=\"id asc\", fl=\"id, body_t, field_i\"))");
      dataRequest.add(id, String.valueOf(i), "body_t", "hello world "+i, "field_i", Integer.toString(i));
    }

    workRequest.commit(cluster.getSolrClient(), "workQueue1");
    dataRequest.commit(cluster.getSolrClient(), "mainCorpus1");

    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString() + "/destination1";
    TupleStream executorStream;
    ModifiableSolrParams paramsLoc;

    StreamFactory factory = new StreamFactory()
        .withCollectionZkHost("workQueue1", cluster.getZkServer().getZkAddress())
        .withCollectionZkHost("mainCorpus1", cluster.getZkServer().getZkAddress())
        .withCollectionZkHost("destination1", cluster.getZkServer().getZkAddress())
        .withFunctionName("search", CloudSolrStream.class)
        .withFunctionName("executor", ExecutorStream.class)
        .withFunctionName("parallel", ParallelStream.class)
        .withFunctionName("update", UpdateStream.class);

    String executorExpression = "parallel(workQueue1, workers=2, sort=\"EOF asc\", executor(threads=3, queueSize=100, search(workQueue1, q=\"*:*\", fl=\"id, expr_s\", rows=1000, partitionKeys=id, sort=\"id desc\")))";
    executorStream = factory.constructStream(executorExpression);

    StreamContext context = new StreamContext();
    SolrClientCache clientCache = new SolrClientCache();
    context.setSolrClientCache(clientCache);
    executorStream.setStreamContext(context);
    getTuples(executorStream);
    //Destination collection should now contain all the records in the main corpus.
    cluster.getSolrClient().commit("destination1");
    paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", "search(destination1, q=\"*:*\", fl=\"id, body_t, field_i\", rows=1000, sort=\"field_i asc\")");
    paramsLoc.set("qt", "/stream");

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
    CollectionAdminRequest.deleteCollection("workQueue1").process(cluster.getSolrClient());
    CollectionAdminRequest.deleteCollection("mainCorpus1").process(cluster.getSolrClient());
    CollectionAdminRequest.deleteCollection("destination1").process(cluster.getSolrClient());
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
    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);

    StreamFactory factory = new StreamFactory()
        .withCollectionZkHost("collection1", cluster.getZkServer().getZkAddress())
        .withCollectionZkHost("destinationCollection", cluster.getZkServer().getZkAddress())
        .withFunctionName("features", FeaturesSelectionStream.class)
        .withFunctionName("train", TextLogitStream.class)
        .withFunctionName("search", CloudSolrStream.class)
        .withFunctionName("update", UpdateStream.class);
    try {
      expression = StreamExpressionParser.parse("features(collection1, q=\"*:*\", featureSet=\"first\", field=\"tv_text\", outcome=\"out_i\", numTerms=4)");
      stream = new FeaturesSelectionStream(expression, factory);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);

      assert (tuples.size() == 4);
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
          "features(collection1, q=\"*:*\", featureSet=\"first\", field=\"tv_text\", outcome=\"out_i\", numTerms=4)," +
          "q=\"*:*\", " +
          "name=\"model\", " +
          "field=\"tv_text\", " +
          "outcome=\"out_i\", " +
          "maxIterations=100)";
      stream = factory.constructStream(textLogitExpression);
      stream.setStreamContext(streamContext);
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

      stream = factory.constructStream("update(destinationCollection, batchSize=5, " + textLogitExpression + ")");
      getTuples(stream);
      cluster.getSolrClient().commit("destinationCollection");

      stream = factory.constructStream("search(destinationCollection, " +
          "q=*:*, " +
          "fl=\"iteration_i,* \", " +
          "rows=100, " +
          "sort=\"iteration_i desc\")");
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);
      assertEquals(100, tuples.size());
      Tuple lastModel = tuples.get(0);
      ClassificationEvaluation evaluation = ClassificationEvaluation.create(lastModel.fields);
      assertTrue(evaluation.getF1() >= 1.0);
      assertEquals(Math.log(5000.0 / (2500 + 1)), lastModel.getDoubles("idfs_ds").get(0), 0.0001);
      // make sure the tuples is retrieved in correct order
      Tuple firstTuple = tuples.get(99);
      assertEquals(1L, (long) firstTuple.getLong("iteration_i"));
    } finally {
      CollectionAdminRequest.deleteCollection("destinationCollection").process(cluster.getSolrClient());
      solrClientCache.close();
    }
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

    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);

    try {
      String zkHost = cluster.getZkServer().getZkAddress();
      final TupleStream stream = streamFactory.constructStream("parallel("
          + "collection1, "
          + "intersect("
          + "search(collection1, q=a_s:(setA || setAB), fl=\"id,a_s,a_i\", sort=\"a_i asc, a_s asc\", partitionKeys=\"a_i\"),"
          + "search(collection1, q=a_s:(setB || setAB), fl=\"id,a_s,a_i\", sort=\"a_i asc\", partitionKeys=\"a_i\"),"
          + "on=\"a_i\"),"
          + "workers=\"2\", zkHost=\"" + zkHost + "\", sort=\"a_i asc\")");

      stream.setStreamContext(streamContext);

      final List<Tuple> tuples = getTuples(stream);

      assert (tuples.size() == 5);
      assertOrder(tuples, 0, 7, 3, 4, 8);
    } finally {
      solrClientCache.close();
    }
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
    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);

    StreamFactory factory = new StreamFactory()
        .withCollectionZkHost("collection1", cluster.getZkServer().getZkAddress())
        .withCollectionZkHost("destinationCollection", cluster.getZkServer().getZkAddress())
        .withFunctionName("featuresSelection", FeaturesSelectionStream.class)
        .withFunctionName("search", CloudSolrStream.class)
        .withFunctionName("update", UpdateStream.class);


    try {
      String featuresExpression = "featuresSelection(collection1, q=\"*:*\", featureSet=\"first\", field=\"whitetok\", outcome=\"out_i\", numTerms=4)";
      // basic
      expression = StreamExpressionParser.parse(featuresExpression);
      stream = new FeaturesSelectionStream(expression, factory);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);

      assert (tuples.size() == 4);

      assertTrue(tuples.get(0).get("term_s").equals("c"));
      assertTrue(tuples.get(1).get("term_s").equals("d"));
      assertTrue(tuples.get(2).get("term_s").equals("e"));
      assertTrue(tuples.get(3).get("term_s").equals("f"));

      // update
      expression = StreamExpressionParser.parse("update(destinationCollection, batchSize=5, " + featuresExpression + ")");
      stream = new UpdateStream(expression, factory);
      stream.setStreamContext(streamContext);
      getTuples(stream);
      cluster.getSolrClient().commit("destinationCollection");

      expression = StreamExpressionParser.parse("search(destinationCollection, q=featureSet_s:first, fl=\"index_i, term_s\", sort=\"index_i asc\")");
      stream = new CloudSolrStream(expression, factory);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);
      assertEquals(4, tuples.size());
      assertTrue(tuples.get(0).get("term_s").equals("c"));
      assertTrue(tuples.get(1).get("term_s").equals("d"));
      assertTrue(tuples.get(2).get("term_s").equals("e"));
      assertTrue(tuples.get(3).get("term_s").equals("f"));
    } finally {
      CollectionAdminRequest.deleteCollection("destinationCollection").process(cluster.getSolrClient());
      solrClientCache.close();
    }
  }


  @Test
  public void testSignificantTermsStream() throws Exception {

    UpdateRequest updateRequest = new UpdateRequest();
    for (int i = 0; i < 5000; i++) {
      updateRequest.add(id, "a"+i, "test_t", "a b c d m l");
    }

    for (int i = 0; i < 5000; i++) {
      updateRequest.add(id, "b"+i, "test_t", "a b e f");
    }

    for (int i = 0; i < 900; i++) {
      updateRequest.add(id, "c"+i, "test_t", "c");
    }

    for (int i = 0; i < 600; i++) {
      updateRequest.add(id, "d"+i, "test_t", "d");
    }

    for (int i = 0; i < 500; i++) {
      updateRequest.add(id, "e"+i, "test_t", "m");
    }

    updateRequest.commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    TupleStream stream;
    List<Tuple> tuples;

    StreamFactory factory = new StreamFactory()
        .withCollectionZkHost("collection1", cluster.getZkServer().getZkAddress())
        .withDefaultZkHost(cluster.getZkServer().getZkAddress())
        .withFunctionName("significantTerms", SignificantTermsStream.class);

    StreamContext streamContext = new StreamContext();
    SolrClientCache cache = new SolrClientCache();
    streamContext.setSolrClientCache(cache);
    try {

      String significantTerms = "significantTerms(collection1, q=\"id:a*\",  field=\"test_t\", limit=3, minTermLength=1, maxDocFreq=\".5\")";
      stream = factory.constructStream(significantTerms);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);

      assert (tuples.size() == 3);
      assertTrue(tuples.get(0).get("term").equals("l"));
      assertTrue(tuples.get(0).getLong("background") == 5000);
      assertTrue(tuples.get(0).getLong("foreground") == 5000);


      assertTrue(tuples.get(1).get("term").equals("m"));
      assertTrue(tuples.get(1).getLong("background") == 5500);
      assertTrue(tuples.get(1).getLong("foreground") == 5000);

      assertTrue(tuples.get(2).get("term").equals("d"));
      assertTrue(tuples.get(2).getLong("background") == 5600);
      assertTrue(tuples.get(2).getLong("foreground") == 5000);

      //Test maxDocFreq
      significantTerms = "significantTerms(collection1, q=\"id:a*\",  field=\"test_t\", limit=3, maxDocFreq=2650, minTermLength=1)";
      stream = factory.constructStream(significantTerms);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);

      assert (tuples.size() == 1);
      assertTrue(tuples.get(0).get("term").equals("l"));
      assertTrue(tuples.get(0).getLong("background") == 5000);
      assertTrue(tuples.get(0).getLong("foreground") == 5000);

      //Test maxDocFreq percentage

      significantTerms = "significantTerms(collection1, q=\"id:a*\",  field=\"test_t\", limit=3, maxDocFreq=\".45\", minTermLength=1)";
      stream = factory.constructStream(significantTerms);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);
      assert (tuples.size() == 1);
      assertTrue(tuples.get(0).get("term").equals("l"));
      assertTrue(tuples.get(0).getLong("background") == 5000);
      assertTrue(tuples.get(0).getLong("foreground") == 5000);


      //Test min doc freq
      significantTerms = "significantTerms(collection1, q=\"id:a*\",  field=\"test_t\", limit=3, minDocFreq=\"2700\", minTermLength=1, maxDocFreq=\".5\")";
      stream = factory.constructStream(significantTerms);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);

      assert (tuples.size() == 3);

      assertTrue(tuples.get(0).get("term").equals("m"));
      assertTrue(tuples.get(0).getLong("background") == 5500);
      assertTrue(tuples.get(0).getLong("foreground") == 5000);

      assertTrue(tuples.get(1).get("term").equals("d"));
      assertTrue(tuples.get(1).getLong("background") == 5600);
      assertTrue(tuples.get(1).getLong("foreground") == 5000);

      assertTrue(tuples.get(2).get("term").equals("c"));
      assertTrue(tuples.get(2).getLong("background") == 5900);
      assertTrue(tuples.get(2).getLong("foreground") == 5000);


      //Test min doc freq percent
      significantTerms = "significantTerms(collection1, q=\"id:a*\",  field=\"test_t\", limit=3, minDocFreq=\".478\", minTermLength=1, maxDocFreq=\".5\")";
      stream = factory.constructStream(significantTerms);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);

      assert (tuples.size() == 1);

      assertTrue(tuples.get(0).get("term").equals("c"));
      assertTrue(tuples.get(0).getLong("background") == 5900);
      assertTrue(tuples.get(0).getLong("foreground") == 5000);

      //Test limit

      significantTerms = "significantTerms(collection1, q=\"id:a*\",  field=\"test_t\", limit=2, minDocFreq=\"2700\", minTermLength=1, maxDocFreq=\".5\")";
      stream = factory.constructStream(significantTerms);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);

      assert (tuples.size() == 2);

      assertTrue(tuples.get(0).get("term").equals("m"));
      assertTrue(tuples.get(0).getLong("background") == 5500);
      assertTrue(tuples.get(0).getLong("foreground") == 5000);

      assertTrue(tuples.get(1).get("term").equals("d"));
      assertTrue(tuples.get(1).getLong("background") == 5600);
      assertTrue(tuples.get(1).getLong("foreground") == 5000);

      //Test term length

      significantTerms = "significantTerms(collection1, q=\"id:a*\",  field=\"test_t\", limit=2, minDocFreq=\"2700\", minTermLength=2)";
      stream = factory.constructStream(significantTerms);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);
      assert (tuples.size() == 0);


      //Test with shards parameter
      List<String> shardUrls = TupleStream.getShards(cluster.getZkServer().getZkAddress(), COLLECTIONORALIAS, streamContext);

      Map<String, List<String>> shardsMap = new HashMap();
      shardsMap.put("myCollection", shardUrls);
      StreamContext context = new StreamContext();
      context.put("shards", shardsMap);
      context.setSolrClientCache(cache);
      significantTerms = "significantTerms(myCollection, q=\"id:a*\",  field=\"test_t\", limit=2, minDocFreq=\"2700\", minTermLength=1, maxDocFreq=\".5\")";
      stream = factory.constructStream(significantTerms);
      stream.setStreamContext(context);
      tuples = getTuples(stream);

      assert (tuples.size() == 2);

      assertTrue(tuples.get(0).get("term").equals("m"));
      assertTrue(tuples.get(0).getLong("background") == 5500);
      assertTrue(tuples.get(0).getLong("foreground") == 5000);

      assertTrue(tuples.get(1).get("term").equals("d"));
      assertTrue(tuples.get(1).getLong("background") == 5600);
      assertTrue(tuples.get(1).getLong("foreground") == 5000);

      //Execersise the /stream hander

      //Add the shards http parameter for the myCollection
      StringBuilder buf = new StringBuilder();
      for (String shardUrl : shardUrls) {
        if (buf.length() > 0) {
          buf.append(",");
        }
        buf.append(shardUrl);
      }

      ModifiableSolrParams solrParams = new ModifiableSolrParams();
      solrParams.add("qt", "/stream");
      solrParams.add("expr", significantTerms);
      solrParams.add("myCollection.shards", buf.toString());
      SolrStream solrStream = new SolrStream(shardUrls.get(0), solrParams);
      tuples = getTuples(solrStream);
      assert (tuples.size() == 2);

      assertTrue(tuples.get(0).get("term").equals("m"));
      assertTrue(tuples.get(0).getLong("background") == 5500);
      assertTrue(tuples.get(0).getLong("foreground") == 5000);

      assertTrue(tuples.get(1).get("term").equals("d"));
      assertTrue(tuples.get(1).getLong("background") == 5600);
      assertTrue(tuples.get(1).getLong("foreground") == 5000);

      //Add a negative test to prove that it cannot find slices if shards parameter is removed

      try {
        ModifiableSolrParams solrParamsBad = new ModifiableSolrParams();
        solrParamsBad.add("qt", "/stream");
        solrParamsBad.add("expr", significantTerms);
        solrStream = new SolrStream(shardUrls.get(0), solrParamsBad);
        tuples = getTuples(solrStream);
        throw new Exception("Exception should have been thrown above");
      } catch (IOException e) {
        assertTrue(e.getMessage().contains("Slices not found for myCollection"));
      }
    } finally {
      cache.close();
    }




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
    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);

    StreamFactory factory = new StreamFactory()
      .withCollectionZkHost("collection1", cluster.getZkServer().getZkAddress())
      .withFunctionName("search", CloudSolrStream.class)
      .withFunctionName("complement", ComplementStream.class);

    try {
      // basic
      expression = StreamExpressionParser.parse("complement("
          + "search(collection1, q=a_s:(setA || setAB), fl=\"id,a_s,a_i\", sort=\"a_i asc, a_s asc\"),"
          + "search(collection1, q=a_s:(setB || setAB), fl=\"id,a_s,a_i\", sort=\"a_i asc\"),"
          + "on=\"a_i\")");
      stream = new ComplementStream(expression, factory);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);

      assert (tuples.size() == 1);
      assertOrder(tuples, 2);
    } finally {
      solrClientCache.close();
    }
  }

  @Test
  public void testCartesianProductStream() throws Exception {

    new UpdateRequest()
        .add(id, "0", "a_ss", "a", "a_ss", "b", "a_ss", "c", "a_ss", "d", "a_ss", "e", "b_ls", "1", "b_ls", "2", "b_ls", "3")
        .add(id, "1", "a_ss", "a", "a_ss", "b", "a_ss", "c", "a_ss", "d", "a_ss", "e")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);
    
    StreamExpression expression;
    TupleStream stream;
    List<Tuple> tuples;
    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);
    
    StreamFactory factory = new StreamFactory()
      .withCollectionZkHost("collection1", cluster.getZkServer().getZkAddress())
      .withFunctionName("search", CloudSolrStream.class)
      .withFunctionName("cartesian", CartesianProductStream.class);
      
    // single selection, no sort
    try {
      stream = factory.constructStream("cartesian("
          + "search(collection1, q=*:*, fl=\"id,a_ss\", sort=\"id asc\"),"
          + "a_ss"
          + ")");
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);

      assertEquals(10, tuples.size());
      assertOrder(tuples, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1);
      assertEquals("a", tuples.get(0).get("a_ss"));
      assertEquals("c", tuples.get(2).get("a_ss"));
      assertEquals("a", tuples.get(5).get("a_ss"));
      assertEquals("c", tuples.get(7).get("a_ss"));

      // single selection, sort
      stream = factory.constructStream("cartesian("
          + "search(collection1, q=*:*, fl=\"id,a_ss\", sort=\"id asc\"),"
          + "a_ss,"
          + "productSort=\"a_ss DESC\""
          + ")");
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);

      assertEquals(10, tuples.size());
      assertOrder(tuples, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1);
      assertEquals("e", tuples.get(0).get("a_ss"));
      assertEquals("c", tuples.get(2).get("a_ss"));
      assertEquals("e", tuples.get(5).get("a_ss"));
      assertEquals("c", tuples.get(7).get("a_ss"));

      // multi selection, sort
      stream = factory.constructStream("cartesian("
          + "search(collection1, q=*:*, fl=\"id,a_ss,b_ls\", sort=\"id asc\"),"
          + "a_ss,"
          + "b_ls,"
          + "productSort=\"a_ss ASC\""
          + ")");
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);

      assertEquals(20, tuples.size()); // (5 * 3) + 5
      assertOrder(tuples, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1);
      assertEquals("a", tuples.get(0).get("a_ss"));
      assertEquals(1L, tuples.get(0).get("b_ls"));
      assertEquals("a", tuples.get(1).get("a_ss"));
      assertEquals(2L, tuples.get(1).get("b_ls"));
      assertEquals("a", tuples.get(2).get("a_ss"));
      assertEquals(3L, tuples.get(2).get("b_ls"));

      assertEquals("b", tuples.get(3).get("a_ss"));
      assertEquals(1L, tuples.get(3).get("b_ls"));
      assertEquals("b", tuples.get(4).get("a_ss"));
      assertEquals(2L, tuples.get(4).get("b_ls"));
      assertEquals("b", tuples.get(5).get("a_ss"));
      assertEquals(3L, tuples.get(5).get("b_ls"));

      // multi selection, sort
      stream = factory.constructStream("cartesian("
          + "search(collection1, q=*:*, fl=\"id,a_ss,b_ls\", sort=\"id asc\"),"
          + "a_ss,"
          + "b_ls,"
          + "productSort=\"a_ss ASC, b_ls DESC\""
          + ")");
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);

      assertEquals(20, tuples.size()); // (5 * 3) + 5
      assertOrder(tuples, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1);
      assertEquals("a", tuples.get(0).get("a_ss"));
      assertEquals(3L, tuples.get(0).get("b_ls"));
      assertEquals("a", tuples.get(1).get("a_ss"));
      assertEquals(2L, tuples.get(1).get("b_ls"));
      assertEquals("a", tuples.get(2).get("a_ss"));
      assertEquals(1L, tuples.get(2).get("b_ls"));

      assertEquals("b", tuples.get(3).get("a_ss"));
      assertEquals(3L, tuples.get(3).get("b_ls"));
      assertEquals("b", tuples.get(4).get("a_ss"));
      assertEquals(2L, tuples.get(4).get("b_ls"));
      assertEquals("b", tuples.get(5).get("a_ss"));
      assertEquals(1L, tuples.get(5).get("b_ls"));

      // multi selection, sort
      stream = factory.constructStream("cartesian("
          + "search(collection1, q=*:*, fl=\"id,a_ss,b_ls\", sort=\"id asc\"),"
          + "a_ss,"
          + "b_ls,"
          + "productSort=\"b_ls DESC\""
          + ")");
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);

      assertEquals(20, tuples.size()); // (5 * 3) + 5
      assertOrder(tuples, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1);
      assertEquals("a", tuples.get(0).get("a_ss"));
      assertEquals(3L, tuples.get(0).get("b_ls"));
      assertEquals("b", tuples.get(1).get("a_ss"));
      assertEquals(3L, tuples.get(1).get("b_ls"));
      assertEquals("c", tuples.get(2).get("a_ss"));
      assertEquals(3L, tuples.get(2).get("b_ls"));
      assertEquals("d", tuples.get(3).get("a_ss"));
      assertEquals(3L, tuples.get(3).get("b_ls"));
      assertEquals("e", tuples.get(4).get("a_ss"));
      assertEquals(3L, tuples.get(4).get("b_ls"));

      assertEquals("a", tuples.get(5).get("a_ss"));
      assertEquals(2L, tuples.get(5).get("b_ls"));
      assertEquals("b", tuples.get(6).get("a_ss"));
      assertEquals(2L, tuples.get(6).get("b_ls"));
      assertEquals("c", tuples.get(7).get("a_ss"));
      assertEquals(2L, tuples.get(7).get("b_ls"));
      assertEquals("d", tuples.get(8).get("a_ss"));
      assertEquals(2L, tuples.get(8).get("b_ls"));
      assertEquals("e", tuples.get(9).get("a_ss"));
      assertEquals(2L, tuples.get(9).get("b_ls"));
    } finally {
      solrClientCache.close();
    }
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

    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);

    try {
      final String zkHost = cluster.getZkServer().getZkAddress();
      final TupleStream stream = streamFactory.constructStream("parallel("
          + "collection1, "
          + "complement("
          + "search(collection1, q=a_s:(setA || setAB), fl=\"id,a_s,a_i\", sort=\"a_i asc, a_s asc\", partitionKeys=\"a_i\"),"
          + "search(collection1, q=a_s:(setB || setAB), fl=\"id,a_s,a_i\", sort=\"a_i asc\", partitionKeys=\"a_i\"),"
          + "on=\"a_i\"),"
          + "workers=\"2\", zkHost=\"" + zkHost + "\", sort=\"a_i asc\")");

      stream.setStreamContext(streamContext);
      final List<Tuple> tuples = getTuples(stream);
      assert (tuples.size() == 1);
      assertOrder(tuples, 2);
    } finally {
      solrClientCache.close();
    }
  }

  protected List<Tuple> getTuples(TupleStream tupleStream) throws IOException {
    List<Tuple> tuples = new ArrayList<Tuple>();

    try {
      tupleStream.open();
      for (Tuple t = tupleStream.read(); !t.EOF; t = tupleStream.read()) {
        tuples.add(t);
      }
    } finally {
      tupleStream.close();
    }
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
