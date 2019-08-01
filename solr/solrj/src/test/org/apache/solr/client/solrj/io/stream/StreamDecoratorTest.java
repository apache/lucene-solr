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
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
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

@Slow
@SolrTestCaseJ4.SuppressSSL
@LuceneTestCase.SuppressCodecs({"Lucene3x", "Lucene40","Lucene41","Lucene42","Lucene45"})
public class StreamDecoratorTest extends SolrCloudTestCase {

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
    
    cluster.waitForActiveCollection(collection, 2, 2);
    
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
      stream = factory.constructStream("parallel(" + COLLECTIONORALIAS + ", workers=2, sort=\"nullCount desc\", null(search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc\", partitionKeys=id, qt=\"/export\"), by=\"a_i asc\"))");
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

    stream = factory.constructStream("parallel(" + COLLECTIONORALIAS + ", workers=2, sort=\"a_f asc\", having(search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc\", partitionKeys=id, qt=\"/export\"), eq(a_i, 9)))");
    StreamContext context = new StreamContext();
    context.setSolrClientCache(solrClientCache);
    stream.setStreamContext(context);
    tuples = getTuples(stream);

    assert(tuples.size() == 1);
    Tuple t = tuples.get(0);
    assertTrue(t.getString("id").equals("9"));

    stream = factory.constructStream("parallel(" + COLLECTIONORALIAS + ", workers=2, sort=\"a_f asc\", having(search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc\", partitionKeys=id, qt=\"/export\"), and(eq(a_i, 9),lt(a_i, 10))))");
    context = new StreamContext();
    context.setSolrClientCache(solrClientCache);
    stream.setStreamContext(context);
    tuples = getTuples(stream);

    assert(tuples.size() == 1);
    t = tuples.get(0);
    assertTrue(t.getString("id").equals("9"));

    stream = factory.constructStream("parallel(" + COLLECTIONORALIAS + ", workers=2, sort=\"a_f asc\",having(search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc\", partitionKeys=id, qt=\"/export\"), or(eq(a_i, 9),eq(a_i, 8))))");
    context = new StreamContext();
    context.setSolrClientCache(solrClientCache);
    stream.setStreamContext(context);
    tuples = getTuples(stream);

    assert(tuples.size() == 2);
    t = tuples.get(0);
    assertTrue(t.getString("id").equals("8"));

    t = tuples.get(1);
    assertTrue(t.getString("id").equals("9"));


    stream = factory.constructStream("parallel(" + COLLECTIONORALIAS + ", workers=2, sort=\"a_f asc\", having(search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc\", partitionKeys=id, qt=\"/export\"), and(eq(a_i, 9),not(eq(a_i, 9)))))");
    context = new StreamContext();
    context.setSolrClientCache(solrClientCache);
    stream.setStreamContext(context);
    tuples = getTuples(stream);

    assert(tuples.size() == 0);


    stream = factory.constructStream("parallel(" + COLLECTIONORALIAS + ", workers=2, sort=\"a_f asc\",having(search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc\", partitionKeys=id, qt=\"/export\"), and(lteq(a_i, 9), gteq(a_i, 8))))");
    context = new StreamContext();
    context.setSolrClientCache(solrClientCache);
    stream.setStreamContext(context);
    tuples = getTuples(stream);

    assert(tuples.size() == 2);

    t = tuples.get(0);
    assertTrue(t.getString("id").equals("8"));

    t = tuples.get(1);
    assertTrue(t.getString("id").equals("9"));

    stream = factory.constructStream("parallel("+COLLECTIONORALIAS+", workers=2, sort=\"a_f asc\", having(rollup(over=a_f, sum(a_i), search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc\", partitionKeys=a_f, qt=\"/export\")), and(eq(sum(a_i), 9),eq(sum(a_i),9))))");
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

      stream = factory.constructStream("parallel(" + COLLECTIONORALIAS + ", workers=2, sort=\"a_f asc\", fetch(" + COLLECTIONORALIAS + ",  search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc\", partitionKeys=\"id\", qt=\"/export\"), on=\"id=a_i\", batchSize=\"2\", fl=\"subject\"))");
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


      stream = factory.constructStream("parallel(" + COLLECTIONORALIAS + ", workers=2, sort=\"a_f asc\", fetch(" + COLLECTIONORALIAS + ",  search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc\", partitionKeys=\"id\", qt=\"/export\"), on=\"id=a_i\", batchSize=\"3\", fl=\"subject\"))");
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

        assertTrue(bucket.equals("hello0"));
        assertTrue(sumi.doubleValue() == 18.0D);

        tuple = daemonStream.read();
        bucket = tuple.getString("a_s");
        sumi = tuple.getDouble("sum(a_i)");

        assertTrue(bucket.equals("hello3"));
        assertTrue(sumi.doubleValue() == 38.0D);

        tuple = daemonStream.read();
        bucket = tuple.getString("a_s");
        sumi = tuple.getDouble("sum(a_i)");
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
  public void testHashRollupStream() throws Exception {

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
        .withFunctionName("hashRollup", HashRollupStream.class)
        .withFunctionName("sum", SumMetric.class)
        .withFunctionName("min", MinMetric.class)
        .withFunctionName("max", MaxMetric.class)
        .withFunctionName("avg", MeanMetric.class)
        .withFunctionName("count", CountMetric.class)
        .withFunctionName("sort", SortStream.class);

    StreamExpression expression;
    TupleStream stream;
    List<Tuple> tuples;
    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);
    try {
      expression = StreamExpressionParser.parse("sort(hashRollup("
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
          + "), by=\"avg(a_f) asc\")");
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


      System.out.println("################:bucket"+bucket);


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



    } finally {
      solrClientCache.close();
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

      ParallelStream pstream = (ParallelStream) streamFactory.constructStream("parallel(" + COLLECTIONORALIAS + ", unique(search(collection1, q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\", partitionKeys=\"a_f\", qt=\"/export\"), over=\"a_f\"), workers=\"2\", zkHost=\"" + zkHost + "\", sort=\"a_f asc\")");
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
          "search(" + COLLECTIONORALIAS + ", q=\"*:*\", fl=\"id,a_s,a_i,a_f\", sort=\"a_s asc,a_f asc\", partitionKeys=\"a_s\", qt=\"/export\"), " +
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
          "search(" + COLLECTIONORALIAS + ", q=\"*:*\", fl=\"id,a_s,a_i,a_f\", sort=\"a_s desc,a_f asc\", partitionKeys=\"a_s\", qt=\"/export\"), " +
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
          + "search(" + COLLECTIONORALIAS + ", q=\"*:*\", fl=\"id,a_s,a_i\", sort=\"a_i asc\", partitionKeys=\"a_i\", qt=\"/export\"), "
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
      ParallelStream pstream = (ParallelStream) streamFactory.constructStream("parallel(" + COLLECTIONORALIAS + ", merge(search(" + COLLECTIONORALIAS + ", q=\"id:(4 1 8 7 9)\", fl=\"id,a_s,a_i\", sort=\"a_i asc\", partitionKeys=\"a_i\", qt=\"/export\"), search(" + COLLECTIONORALIAS + ", q=\"id:(0 2 3 6)\", fl=\"id,a_s,a_i\", sort=\"a_i asc\", partitionKeys=\"a_i\", qt=\"/export\"), on=\"a_i asc\"), workers=\"2\", zkHost=\"" + zkHost + "\", sort=\"a_i asc\")");
      pstream.setStreamContext(streamContext);
      List<Tuple> tuples = getTuples(pstream);

      assert (tuples.size() == 9);
      assertOrder(tuples, 0, 1, 2, 3, 4, 7, 6, 8, 9);

      //Test descending

      pstream = (ParallelStream) streamFactory.constructStream("parallel(" + COLLECTIONORALIAS + ", merge(search(" + COLLECTIONORALIAS + ", q=\"id:(4 1 8 9)\", fl=\"id,a_s,a_i\", sort=\"a_i desc\", partitionKeys=\"a_i\", qt=\"/export\"), search(" + COLLECTIONORALIAS + ", q=\"id:(0 2 3 6)\", fl=\"id,a_s,a_i\", sort=\"a_i desc\", partitionKeys=\"a_i\", qt=\"/export\"), on=\"a_i desc\"), workers=\"2\", zkHost=\"" + zkHost + "\", sort=\"a_i desc\")");
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
              + "search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"a_s,a_i,a_f\", sort=\"a_s asc\", partitionKeys=\"a_s\", qt=\"/export\"),"
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
  public void testParallelHashRollupStream() throws Exception {

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
        .withFunctionName("hashRollup", HashRollupStream.class)
        .withFunctionName("sum", SumMetric.class)
        .withFunctionName("min", MinMetric.class)
        .withFunctionName("max", MaxMetric.class)
        .withFunctionName("avg", MeanMetric.class)
        .withFunctionName("count", CountMetric.class)
        .withFunctionName("sort", SortStream.class);


    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);

    StreamExpression expression;
    TupleStream stream;
    List<Tuple> tuples;

    try {
      expression = StreamExpressionParser.parse("sort(parallel(" + COLLECTIONORALIAS + ","
          + "hashRollup("
          + "search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"a_s,a_i,a_f\", sort=\"a_s asc\", partitionKeys=\"a_s\", qt=\"/export\"),"
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
          + "workers=\"2\", zkHost=\"" + cluster.getZkServer().getZkAddress() + "\", sort=\"a_s asc\"), by=\"avg(a_f) asc\")"
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
  public void testHashJoinStreamWithKnownConflict() throws Exception {

    new UpdateRequest()
        .add(id, "1", "type_s","left", "bbid_s", "MG!!00TNH1", "ykey_s", "Mtge")
        .add(id, "2", "type_s","right", "bbid_s", "MG!!00TNGP", "ykey_s", "Mtge")
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
          + "  search(collection1, q=*:*, fl=\"bbid_s,ykey_s\", fq=\"type_s:left\", sort=\"bbid_s asc, ykey_s asc\"),"
          + "  hashed=search(collection1, q=*:*, fl=\"bbid_s,ykey_s\", fq=\"type_s:right\", sort=\"bbid_s asc, ykey_s asc\"),"
          + "  on=\"bbid_s,ykey_s\""
          + ")");
      stream = new HashJoinStream(expression, factory);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);
      
      assertEquals(0, tuples.size());
    
    
    } finally {
      solrClientCache.close();
    }
  }

  @Test
  public void testOuterHashJoinStreamWithKnownConflict() throws Exception {

    new UpdateRequest()
        .add(id, "1", "type_s","left", "bbid_s", "MG!!00TNH1", "ykey_s", "Mtge")
        .add(id, "2", "type_s","right", "bbid_s", "MG!!00TNGP", "ykey_s", "Mtge", "extra_s", "foo")
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
      .withFunctionName("outerHashJoin", OuterHashJoinStream.class);
    try {
      // Basic test
      expression = StreamExpressionParser.parse("outerHashJoin("
          + "  search(collection1, q=*:*, fl=\"bbid_s,ykey_s\", fq=\"type_s:left\", sort=\"bbid_s asc, ykey_s asc\"),"
          + "  hashed=search(collection1, q=*:*, fl=\"bbid_s,ykey_s,extra_s\", fq=\"type_s:right\", sort=\"bbid_s asc, ykey_s asc\"),"
          + "  on=\"bbid_s,ykey_s\""
          + ")");
      stream = new OuterHashJoinStream(expression, factory);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);
      
      assertEquals(1, tuples.size());
      assertFalse(tuples.get(0).fields.containsKey("extra_s"));
    
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
  public void testUpdateStream() throws Exception {

    CollectionAdminRequest.createCollection("destinationCollection", "conf", 2, 1).process(cluster.getSolrClient());
    cluster.waitForActiveCollection("destinationCollection", 2, 2);

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
    cluster.waitForActiveCollection("parallelDestinationCollection", 2, 2);

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
      String updateExpression = "update(parallelDestinationCollection, batchSize=2, search(collection1, q=*:*, fl=\"id,a_s,a_i,a_f,s_multi,i_multi\", sort=\"a_f asc, a_i asc\", partitionKeys=\"a_f\", qt=\"/export\"))";
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
    cluster.waitForActiveCollection("parallelDestinationCollection1", 2, 2);

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
      String updateExpression = "daemon(update(parallelDestinationCollection1, batchSize=2, search(collection1, q=*:*, fl=\"id,a_s,a_i,a_f,s_multi,i_multi\", sort=\"a_f asc, a_i asc\", partitionKeys=\"a_f\", qt=\"/export\")), runInterval=\"1000\", id=\"test\")";
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
    cluster.waitForActiveCollection("parallelDestinationCollection1", 2, 2);

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


  @Test
  public void testParseCSV() throws Exception {
    String expr = "parseCSV(list(tuple(file=\"file1\", line=\"a,b,c\"), " +
        "                        tuple(file=\"file1\", line=\"1,2,3\")," +
        "                        tuple(file=\"file1\", line=\"\\\"hello, world\\\",9000,20\")," +
        "                        tuple(file=\"file2\", line=\"field_1,field_2,field_3\"), "+
        "                        tuple(file=\"file2\", line=\"8,9,\")))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", expr);
    paramsLoc.set("qt", "/stream");

    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString() + "/" + COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertEquals(tuples.size(),  3);
    assertEquals(tuples.get(0).getString("a"), "1");
    assertEquals(tuples.get(0).getString("b"), "2");
    assertEquals(tuples.get(0).getString("c"), "3");

    assertEquals(tuples.get(1).getString("a"), "hello, world");
    assertEquals(tuples.get(1).getString("b"), "9000");
    assertEquals(tuples.get(1).getString("c"), "20");

    assertEquals(tuples.get(2).getString("field_1"), "8");
    assertEquals(tuples.get(2).getString("field_2"), "9");
    assertNull(tuples.get(2).get("field_3"));
  }


  @Test
  public void testParseTSV() throws Exception {
    String expr = "parseTSV(list(tuple(file=\"file1\", line=\"a\tb\tc\"), " +
        "                        tuple(file=\"file1\", line=\"1\t2\t3\")," +
        "                        tuple(file=\"file1\", line=\"hello, world\t9000\t20\")," +
        "                        tuple(file=\"file2\", line=\"field_1\tfield_2\tfield_3\"), "+
        "                        tuple(file=\"file2\", line=\"8\t\t9\")))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", expr);
    paramsLoc.set("qt", "/stream");

    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString() + "/" + COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertEquals(tuples.size(),  3);
    assertEquals(tuples.get(0).getString("a"), "1");
    assertEquals(tuples.get(0).getString("b"), "2");
    assertEquals(tuples.get(0).getString("c"), "3");

    assertEquals(tuples.get(1).getString("a"), "hello, world");
    assertEquals(tuples.get(1).getString("b"), "9000");
    assertEquals(tuples.get(1).getString("c"), "20");

    assertEquals(tuples.get(2).getString("field_1"), "8");
    assertNull(tuples.get(2).get("field_2"));
    assertEquals(tuples.get(2).getString("field_3"), "9");

  }



  @Test
  public void testCommitStream() throws Exception {

    CollectionAdminRequest.createCollection("destinationCollection", "conf", 2, 1).process(cluster.getSolrClient());
    cluster.waitForActiveCollection("destinationCollection", 2, 2);

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
    cluster.waitForActiveCollection("parallelDestinationCollection", 2, 2);

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
      String updateExpression = "commit(parallelDestinationCollection, batchSize=0, zkHost=\"" + cluster.getZkServer().getZkAddress() + "\", update(parallelDestinationCollection, batchSize=2, search(collection1, q=*:*, fl=\"id,a_s,a_i,a_f,s_multi,i_multi\", sort=\"a_f asc, a_i asc\", partitionKeys=\"a_f\", qt=\"/export\")))";
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
    cluster.waitForActiveCollection("parallelDestinationCollection1", 2, 2);

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
      String updateExpression = "daemon(commit(parallelDestinationCollection1, batchSize=0, zkHost=\"" + cluster.getZkServer().getZkAddress() + "\", update(parallelDestinationCollection1, batchSize=2, search(collection1, q=*:*, fl=\"id,a_s,a_i,a_f,s_multi,i_multi\", sort=\"a_f asc, a_i asc\", partitionKeys=\"a_f\", qt=\"/export\"))), runInterval=\"1000\", id=\"test\")";
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
    cluster.waitForActiveCollection("modelCollection", 2, 2);
    CollectionAdminRequest.createCollection("uknownCollection", "ml", 2, 1).process(cluster.getSolrClient());
    cluster.waitForActiveCollection("uknownCollection", 2, 2);
    CollectionAdminRequest.createCollection("checkpointCollection", "ml", 2, 1).process(cluster.getSolrClient());
    cluster.waitForActiveCollection("checkpointCollection", 2, 2);

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
      // use cacheMillis=0 to prevent cached results. it doesn't matter on the first run,
      // but we want to ensure that when we re-use this expression later after
      // training another model, we'll still get accurate results.
      "model(modelCollection, id=\"model\", cacheMillis=0)," +
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
  public void testStream() throws Exception {
    UpdateRequest updateRequest = new UpdateRequest();
    updateRequest.add(id, "hello", "test_t", "l b c d c e", "test_i", "5");
    updateRequest.add(id, "hello1", "test_t", "l b c d c", "test_i", "4");

    updateRequest.commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    String expr = "search("+COLLECTIONORALIAS+", q=\"id:hello1\", fl=\"id,test_t, test_i\", sort=\"id desc\")";
    String cat = "let(a ="+expr+",stream(a))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", cat);
    paramsLoc.set("qt", "/stream");

    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);


  }

  @Test
  public void testExecutorStream() throws Exception {
    CollectionAdminRequest.createCollection("workQueue", "conf", 2, 1).processAndWait(cluster.getSolrClient(), DEFAULT_TIMEOUT);
    cluster.waitForActiveCollection("workQueue", 2, 2);
    CollectionAdminRequest.createCollection("mainCorpus", "conf", 2, 1).processAndWait(cluster.getSolrClient(), DEFAULT_TIMEOUT);
    cluster.waitForActiveCollection("mainCorpus", 2, 2);
    CollectionAdminRequest.createCollection("destination", "conf", 2, 1).processAndWait(cluster.getSolrClient(), DEFAULT_TIMEOUT);
    cluster.waitForActiveCollection("destination", 2, 2);

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

    CollectionAdminRequest.createCollection("mainCorpus1", "conf", 2, 1).processAndWait(cluster.getSolrClient(), DEFAULT_TIMEOUT);

    CollectionAdminRequest.createCollection("destination1", "conf", 2, 1).processAndWait(cluster.getSolrClient(), DEFAULT_TIMEOUT);

    cluster.waitForActiveCollection("workQueue1", 2, 2);
    cluster.waitForActiveCollection("mainCorpus1", 2, 2);
    cluster.waitForActiveCollection("destination1", 2, 2);

    UpdateRequest workRequest = new UpdateRequest();
    UpdateRequest dataRequest = new UpdateRequest();

    int cnt = TEST_NIGHTLY ? 500 : 100;
    for (int i = 0; i < cnt; i++) {
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

    String executorExpression = "parallel(workQueue1, workers=2, sort=\"EOF asc\", executor(threads=3, queueSize=100, search(workQueue1, q=\"*:*\", fl=\"id, expr_s\", rows=1000, partitionKeys=id, sort=\"id desc\", qt=\"/export\")))";
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
    assertTrue(tuples.size() == cnt);
    for(int i=0; i<cnt; i++) {
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
          + "search(collection1, q=a_s:(setA || setAB), fl=\"id,a_s,a_i\", sort=\"a_i asc, a_s asc\", partitionKeys=\"a_i\", qt=\"/export\"),"
          + "search(collection1, q=a_s:(setB || setAB), fl=\"id,a_s,a_i\", sort=\"a_i asc\", partitionKeys=\"a_i\", qt=\"/export\"),"
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
          + "search(collection1, q=a_s:(setA || setAB), fl=\"id,a_s,a_i\", sort=\"a_i asc, a_s asc\", partitionKeys=\"a_i\", qt=\"/export\"),"
          + "search(collection1, q=a_s:(setB || setAB), fl=\"id,a_s,a_i\", sort=\"a_i asc\", partitionKeys=\"a_i\", qt=\"/export\"),"
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
}
