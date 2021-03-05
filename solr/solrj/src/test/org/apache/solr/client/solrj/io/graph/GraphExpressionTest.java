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

package org.apache.solr.client.solrj.io.graph;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.InputStreamResponseParser;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.ComparatorOrder;
import org.apache.solr.client.solrj.io.comp.FieldComparator;
import org.apache.solr.client.solrj.io.stream.*;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.client.solrj.io.stream.metrics.CountMetric;
import org.apache.solr.client.solrj.io.stream.metrics.MaxMetric;
import org.apache.solr.client.solrj.io.stream.metrics.MeanMetric;
import org.apache.solr.client.solrj.io.stream.metrics.MinMetric;
import org.apache.solr.client.solrj.io.stream.metrics.SumMetric;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.AbstractDistribZkTestBase;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.BaseTestHarness;
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
public class GraphExpressionTest extends SolrCloudTestCase {

  private static final String COLLECTION = "collection1";

  private static final String id = "id";

  private static final int TIMEOUT = 30;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2)
        .addConfig("conf", getFile("solrj").toPath().resolve("solr").resolve("configsets").resolve("streaming").resolve("conf"))
        .configure();

    CollectionAdminRequest.createCollection(COLLECTION, "conf", 2, 1).process(cluster.getSolrClient());
    AbstractDistribZkTestBase.waitForRecoveriesToFinish(COLLECTION, cluster.getSolrClient().getZkStateReader(),
        false, true, TIMEOUT);
  }

  @Before
  public void cleanIndex() throws Exception {
    new UpdateRequest()
        .deleteByQuery("*:*")
        .commit(cluster.getSolrClient(), COLLECTION);
  }

  @Test
  // commented 4-Sep-2018  @LuceneTestCase.BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 2-Aug-2018
  @SuppressWarnings({"unchecked"})
  public void testShortestPathStream() throws Exception {

    new UpdateRequest()
        .add(id, "0", "from_s", "jim", "to_s", "mike", "predicate_s", "knows")
        .add(id, "1", "from_s", "jim", "to_s", "dave", "predicate_s", "knows")
        .add(id, "2", "from_s", "jim", "to_s", "stan", "predicate_s", "knows")
        .add(id, "3", "from_s", "dave", "to_s", "stan", "predicate_s", "knows")
        .add(id, "4", "from_s", "dave", "to_s", "bill", "predicate_s", "knows")
        .add(id, "5", "from_s", "dave", "to_s", "mike", "predicate_s", "knows")
        .add(id, "20", "from_s", "dave", "to_s", "alex", "predicate_s", "knows")
        .add(id, "21", "from_s", "alex", "to_s", "steve", "predicate_s", "knows")
        .add(id, "6", "from_s", "stan", "to_s", "alice", "predicate_s", "knows")
        .add(id, "7", "from_s", "stan", "to_s", "mary", "predicate_s", "knows")
        .add(id, "8", "from_s", "stan", "to_s", "dave", "predicate_s", "knows")
        .add(id, "10", "from_s", "mary", "to_s", "mike", "predicate_s", "knows")
        .add(id, "11", "from_s", "mary", "to_s", "max", "predicate_s", "knows")
        .add(id, "12", "from_s", "mary", "to_s", "jim", "predicate_s", "knows")
        .add(id, "13", "from_s", "mary", "to_s", "steve", "predicate_s", "knows")
        .commit(cluster.getSolrClient(), COLLECTION);

    List<Tuple> tuples = null;
    Set<String> paths = null;
    ShortestPathStream stream = null;
    StreamContext context = new StreamContext();
    SolrClientCache cache = new SolrClientCache();
    context.setSolrClientCache(cache);

    StreamFactory factory = new StreamFactory()
        .withCollectionZkHost("collection1", cluster.getZkServer().getZkAddress())
        .withFunctionName("shortestPath", ShortestPathStream.class);

    @SuppressWarnings({"rawtypes"})
    Map params = new HashMap();
    params.put("fq", "predicate_s:knows");

    stream = (ShortestPathStream)factory.constructStream("shortestPath(collection1, " +
        "from=\"jim\", " +
        "to=\"steve\"," +
        "edge=\"from_s=to_s\"," +
        "fq=\"predicate_s:knows\","+
        "threads=\"3\","+
        "partitionSize=\"3\","+
        "maxDepth=\"6\")");

    stream.setStreamContext(context);
    paths = new HashSet<>();
    tuples = getTuples(stream);

    assertTrue(tuples.size() == 2);

    for(Tuple tuple : tuples) {
      paths.add(tuple.getStrings("path").toString());
    }

    assertTrue(paths.contains("[jim, dave, alex, steve]"));
    assertTrue(paths.contains("[jim, stan, mary, steve]"));

    //Test with batch size of 1

    params.put("fq", "predicate_s:knows");

    stream = (ShortestPathStream)factory.constructStream("shortestPath(collection1, " +
        "from=\"jim\", " +
        "to=\"steve\"," +
        "edge=\"from_s=to_s\"," +
        "fq=\"predicate_s:knows\","+
        "threads=\"3\","+
        "partitionSize=\"1\","+
        "maxDepth=\"6\")");

    stream.setStreamContext(context);
    paths = new HashSet<>();
    tuples = getTuples(stream);

    assertTrue(tuples.size() == 2);

    for(Tuple tuple : tuples) {
      paths.add(tuple.getStrings("path").toString());
    }

    assertTrue(paths.contains("[jim, dave, alex, steve]"));
    assertTrue(paths.contains("[jim, stan, mary, steve]"));

    //Test with bad predicate


    stream = (ShortestPathStream)factory.constructStream("shortestPath(collection1, " +
        "from=\"jim\", " +
        "to=\"steve\"," +
        "edge=\"from_s=to_s\"," +
        "fq=\"predicate_s:crap\","+
        "threads=\"3\","+
        "partitionSize=\"3\","+
        "maxDepth=\"6\")");

    stream.setStreamContext(context);
    tuples = getTuples(stream);

    assertTrue(tuples.size() == 0);

    //Test with depth 2

    stream = (ShortestPathStream)factory.constructStream("shortestPath(collection1, " +
        "from=\"jim\", " +
        "to=\"steve\"," +
        "edge=\"from_s=to_s\"," +
        "fq=\"predicate_s:knows\","+
        "threads=\"3\","+
        "partitionSize=\"3\","+
        "maxDepth=\"2\")");


    stream.setStreamContext(context);
    tuples = getTuples(stream);

    assertTrue(tuples.size() == 0);

    //Take out alex
    params.put("fq", "predicate_s:knows NOT to_s:alex");

    stream = (ShortestPathStream)factory.constructStream("shortestPath(collection1, " +
        "from=\"jim\", " +
        "to=\"steve\"," +
        "edge=\"from_s=to_s\"," +
        "fq=\" predicate_s:knows NOT to_s:alex\","+
        "threads=\"3\","+
        "partitionSize=\"3\","+
        "maxDepth=\"6\")");


    stream.setStreamContext(context);
    paths = new HashSet<>();
    tuples = getTuples(stream);
    assertTrue(tuples.size() == 1);

    for(Tuple tuple : tuples) {
      paths.add(tuple.getStrings("path").toString());
    }

    assertTrue(paths.contains("[jim, stan, mary, steve]"));

    cache.close();

  }

  @Test
  public void testGatherNodesStream() throws Exception {

    new UpdateRequest()
        .add(id, "0", "basket_s", "basket1", "product_s", "product1", "price_f", "20", "time_ten_seconds_s", "2020-09-24T18:23:50Z")
        .add(id, "1", "basket_s", "basket1", "product_s", "product3", "price_f", "30", "time_ten_seconds_s", "2020-09-24T18:23:40Z")
        .add(id, "2", "basket_s", "basket1", "product_s", "product5", "price_f", "1", "time_ten_seconds_s", "2020-09-24T18:23:30Z")
        .add(id, "3", "basket_s", "basket2", "product_s", "product1", "price_f", "2", "time_ten_seconds_s", "2020-09-24T18:23:20Z")
        .add(id, "4", "basket_s", "basket2", "product_s", "product6", "price_f", "5", "time_ten_seconds_s", "2020-09-24T18:23:10Z")
        .add(id, "5", "basket_s", "basket2", "product_s", "product7", "price_f", "10", "time_ten_seconds_s", "2020-09-24T18:23:00Z")
        .add(id, "6", "basket_s", "basket3", "product_s", "product4", "price_f", "20", "time_ten_seconds_s", "2020-09-24T18:22:50Z")
        .add(id, "7", "basket_s", "basket3", "product_s", "product3", "price_f", "10", "time_ten_seconds_s", "2020-09-24T18:22:40Z")
        .add(id, "8", "basket_s", "basket3", "product_s", "product1", "price_f", "10", "time_ten_seconds_s", "2020-09-24T18:22:30Z")
        .add(id, "9", "basket_s", "basket4", "product_s", "product4", "price_f", "40", "time_ten_seconds_s", "2020-09-24T18:22:20Z")
        .add(id, "10", "basket_s", "basket4", "product_s", "product3", "price_f", "10", "time_ten_seconds_s", "2020-09-24T18:22:10Z")
        .add(id, "11", "basket_s", "basket4", "product_s", "product1", "price_f", "10", "time_ten_seconds_s", "2020-09-24T18:22:00Z")
        .commit(cluster.getSolrClient(), COLLECTION);

    List<Tuple> tuples = null;
    Set<String> paths = null;
    GatherNodesStream stream = null;
    StreamContext context = new StreamContext();
    SolrClientCache cache = new SolrClientCache();
    context.setSolrClientCache(cache);

    StreamFactory factory = new StreamFactory()
        .withCollectionZkHost("collection1", cluster.getZkServer().getZkAddress())
        .withFunctionName("gatherNodes", GatherNodesStream.class)
        .withFunctionName("nodes", GatherNodesStream.class)
        .withFunctionName("search", CloudSolrStream.class)
        .withFunctionName("random", RandomStream.class)
        .withFunctionName("count", CountMetric.class)
        .withFunctionName("avg", MeanMetric.class)
        .withFunctionName("sum", SumMetric.class)
        .withFunctionName("min", MinMetric.class)
        .withFunctionName("sort", SortStream.class)
        .withFunctionName("max", MaxMetric.class);

    String expr = "nodes(collection1, " +
        "walk=\"product1->product_s\"," +
        "gather=\"basket_s\")";

    stream = (GatherNodesStream)factory.constructStream(expr);
    stream.setStreamContext(context);

    tuples = getTuples(stream);

    Collections.sort(tuples, new FieldComparator("node", ComparatorOrder.ASCENDING));
    assertTrue(tuples.size() == 4);

    assertTrue(tuples.get(0).getString("node").equals("basket1"));
    assertTrue(tuples.get(1).getString("node").equals("basket2"));
    assertTrue(tuples.get(2).getString("node").equals("basket3"));
    assertTrue(tuples.get(3).getString("node").equals("basket4"));


    //Test maxDocFreq param
    String docFreqExpr = "gatherNodes(collection1, " +
        "walk=\"product1, product7->product_s\"," +
        "maxDocFreq=\"2\","+
        "gather=\"basket_s\")";

    stream = (GatherNodesStream)factory.constructStream(docFreqExpr);
    stream.setStreamContext(context);

    tuples = getTuples(stream);
    Collections.sort(tuples, new FieldComparator("node", ComparatorOrder.ASCENDING));
    assertTrue(tuples.size() == 1);
    assertTrue(tuples.get(0).getString("node").equals("basket2"));



    String expr2 = "gatherNodes(collection1, " +
        expr+","+
        "walk=\"node->basket_s\"," +
        "gather=\"product_s\", count(*), avg(price_f), sum(price_f), min(price_f), max(price_f))";

    stream = (GatherNodesStream)factory.constructStream(expr2);

    context = new StreamContext();
    context.setSolrClientCache(cache);

    stream.setStreamContext(context);


    tuples = getTuples(stream);

    Collections.sort(tuples, new FieldComparator("node", ComparatorOrder.ASCENDING));


    assertTrue(tuples.size() == 5);


    assertTrue(tuples.get(0).getString("node").equals("product3"));
    assertTrue(tuples.get(0).getDouble("count(*)").equals(3.0D));

    assertTrue(tuples.get(1).getString("node").equals("product4"));
    assertTrue(tuples.get(1).getDouble("count(*)").equals(2.0D));
    assertTrue(tuples.get(1).getDouble("avg(price_f)").equals(30.0D));
    assertTrue(tuples.get(1).getDouble("sum(price_f)").equals(60.0D));
    assertTrue(tuples.get(1).getDouble("min(price_f)").equals(20.0D));
    assertTrue(tuples.get(1).getDouble("max(price_f)").equals(40.0D));

    assertTrue(tuples.get(2).getString("node").equals("product5"));
    assertTrue(tuples.get(2).getDouble("count(*)").equals(1.0D));
    assertTrue(tuples.get(3).getString("node").equals("product6"));
    assertTrue(tuples.get(3).getDouble("count(*)").equals(1.0D));
    assertTrue(tuples.get(4).getString("node").equals("product7"));
    assertTrue(tuples.get(4).getDouble("count(*)").equals(1.0D));

    //Test list of root nodes
    expr = "gatherNodes(collection1, " +
        "walk=\"product4, product7->product_s\"," +
        "gather=\"basket_s\")";

    stream = (GatherNodesStream)factory.constructStream(expr);

    context = new StreamContext();
    context.setSolrClientCache(cache);
    stream.setStreamContext(context);
    tuples = getTuples(stream);
    Collections.sort(tuples, new FieldComparator("node", ComparatorOrder.ASCENDING));
    assertTrue(tuples.size() == 3);
    assertTrue(tuples.get(0).getString("node").equals("basket2"));
    assertTrue(tuples.get(1).getString("node").equals("basket3"));
    assertTrue(tuples.get(2).getString("node").equals("basket4"));

    //Test with negative filter query

    expr = "gatherNodes(collection1, " +
        "walk=\"product4, product7->product_s\"," +
        "gather=\"basket_s\", fq=\"-basket_s:basket4\")";

    stream = (GatherNodesStream)factory.constructStream(expr);

    context = new StreamContext();
    context.setSolrClientCache(cache);
    stream.setStreamContext(context);
    tuples = getTuples(stream);

    Collections.sort(tuples, new FieldComparator("node", ComparatorOrder.ASCENDING));
    assertTrue(tuples.size() == 2);
    assertTrue(tuples.get(0).getString("node").equals("basket2"));
    assertTrue(tuples.get(1).getString("node").equals("basket3"));


    //Test the window without lag

    expr = "nodes(collection1, random(collection1, q=\"id:(1 2)\", fl=\"time_ten_seconds_s\"), walk=\"time_ten_seconds_s->time_ten_seconds_s\", gather=\"id\", window=\"3\")";

    stream = (GatherNodesStream)factory.constructStream(expr);

    context = new StreamContext();
    context.setSolrClientCache(cache);
    stream.setStreamContext(context);
    tuples = getTuples(stream);

    Collections.sort(tuples, new FieldComparator("node", ComparatorOrder.ASCENDING));
    assertTrue(tuples.size() == 5);
    assertTrue(tuples.get(0).getString("node").equals("1"));
    assertTrue(tuples.get(1).getString("node").equals("2"));
    assertTrue(tuples.get(2).getString("node").equals("3"));
    assertTrue(tuples.get(3).getString("node").equals("4"));
    assertTrue(tuples.get(4).getString("node").equals("5"));


    //Test window with lag

    expr = "nodes(collection1, random(collection1, q=\"id:(1)\", fl=\"time_ten_seconds_s\"), walk=\"time_ten_seconds_s->time_ten_seconds_s\", gather=\"id\", window=\"2\", lag=\"2\")";

    stream = (GatherNodesStream)factory.constructStream(expr);

    context = new StreamContext();
    context.setSolrClientCache(cache);
    stream.setStreamContext(context);
    tuples = getTuples(stream);

    Collections.sort(tuples, new FieldComparator("node", ComparatorOrder.ASCENDING));
    assertTrue(tuples.size() == 2);
    assertTrue(tuples.get(0).getString("node").equals("3"));
    assertTrue(tuples.get(1).getString("node").equals("4"));
    cache.close();
  }


  @Test
  public void testScoreNodesStream() throws Exception {


    new UpdateRequest()
        .add(id, "0", "basket_s", "basket1", "product_s", "product1", "price_f", "1")
        .add(id, "1", "basket_s", "basket1", "product_s", "product3", "price_f", "1")
        .add(id, "2", "basket_s", "basket1", "product_s", "product5", "price_f", "100")
        .add(id, "3", "basket_s", "basket2", "product_s", "product1", "price_f", "1")
        .add(id, "4", "basket_s", "basket2", "product_s", "product6", "price_f", "1")
        .add(id, "5", "basket_s", "basket2", "product_s", "product7", "price_f", "1")
        .add(id, "6", "basket_s", "basket3", "product_s", "product4", "price_f", "1")
        .add(id, "7", "basket_s", "basket3", "product_s", "product3", "price_f", "1")
        .add(id, "8", "basket_s", "basket3", "product_s", "product1", "price_f", "1")
        .add(id, "9", "basket_s", "basket4", "product_s", "product4", "price_f", "1")
        .add(id, "10", "basket_s", "basket4", "product_s", "product3", "price_f", "1")
        .add(id, "11", "basket_s", "basket4", "product_s", "product1", "price_f", "1")
        .add(id, "12", "basket_s", "basket5", "product_s", "product1", "price_f", "1")
        .add(id, "13", "basket_s", "basket6", "product_s", "product1", "price_f", "1")
        .add(id, "14", "basket_s", "basket7", "product_s", "product1", "price_f", "1")
        .add(id, "15", "basket_s", "basket4", "product_s", "product1", "price_f", "1")
        .commit(cluster.getSolrClient(), COLLECTION);

    List<Tuple> tuples = null;
    TupleStream stream = null;
    StreamContext context = new StreamContext();
    SolrClientCache cache = new SolrClientCache();
    context.setSolrClientCache(cache);

    StreamFactory factory = new StreamFactory()
        .withCollectionZkHost("collection1", cluster.getZkServer().getZkAddress())
        .withDefaultZkHost(cluster.getZkServer().getZkAddress())
        .withFunctionName("gatherNodes", GatherNodesStream.class)
        .withFunctionName("scoreNodes", ScoreNodesStream.class)
        .withFunctionName("search", CloudSolrStream.class)
        .withFunctionName("sort", SortStream.class)
        .withFunctionName("count", CountMetric.class)
        .withFunctionName("avg", MeanMetric.class)
        .withFunctionName("sum", SumMetric.class)
        .withFunctionName("min", MinMetric.class)
        .withFunctionName("max", MaxMetric.class);

    String expr = "gatherNodes(collection1, " +
                               "walk=\"product3->product_s\"," +
                               "gather=\"basket_s\")";


    String expr2 = "sort(by=\"nodeScore desc\", " +
                         "scoreNodes(gatherNodes(collection1, " +
                                                 expr+","+
                                                 "walk=\"node->basket_s\"," +
                                                 "gather=\"product_s\", " +
                                                 "count(*), " +
                                                 "avg(price_f), " +
                                                 "sum(price_f), " +
                                                 "min(price_f), " +
                                                 "max(price_f))))";

    stream = factory.constructStream(expr2);

    context = new StreamContext();
    context.setSolrClientCache(cache);

    stream.setStreamContext(context);

    tuples = getTuples(stream);

    Tuple tuple0 = tuples.get(0);
    assert(tuple0.getString("node").equals("product4"));
    assert(tuple0.getLong("docFreq") == 2);
    assert(tuple0.getLong("count(*)") == 2);

    Tuple tuple1 = tuples.get(1);
    assert(tuple1.getString("node").equals("product1"));
    assert(tuple1.getLong("docFreq") == 8);
    assert(tuple1.getLong("count(*)") == 3);

    Tuple tuple2 = tuples.get(2);
    assert(tuple2.getString("node").equals("product5"));
    assert(tuple2.getLong("docFreq") == 1);
    assert(tuple2.getLong("count(*)") == 1);


    //Test using a different termFreq field then the default count(*)
    expr2 = "sort(by=\"nodeScore desc\", " +
                 "scoreNodes(termFreq=\"avg(price_f)\",gatherNodes(collection1, " +
                                                                   expr+","+
                                                                   "walk=\"node->basket_s\"," +
                                                                   "gather=\"product_s\", " +
                                                                   "count(*), " +
                                                                   "avg(price_f), " +
                                                                   "sum(price_f), " +
                                                                   "min(price_f), " +
                                                                   "max(price_f))))";

    stream = factory.constructStream(expr2);

    context = new StreamContext();
    context.setSolrClientCache(cache);

    stream.setStreamContext(context);

    tuples = getTuples(stream);

    tuple0 = tuples.get(0);
    assert(tuple0.getString("node").equals("product5"));
    assert(tuple0.getLong("docFreq") == 1);
    assert(tuple0.getDouble("avg(price_f)") == 100);

    tuple1 = tuples.get(1);
    assert(tuple1.getString("node").equals("product4"));
    assert(tuple1.getLong("docFreq") == 2);
    assert(tuple1.getDouble("avg(price_f)") == 1);

    tuple2 = tuples.get(2);
    assert(tuple2.getString("node").equals("product1"));
    assert(tuple2.getLong("docFreq") == 8);
    assert(tuple2.getDouble("avg(price_f)") == 1);

    cache.close();
  }


  @Test
  public void testScoreNodesFacetStream() throws Exception {


    new UpdateRequest()
        .add(id, "0", "basket_s", "basket1", "product_ss", "product1", "product_ss", "product3", "product_ss", "product5", "price_f", "1")
        .add(id, "3", "basket_s", "basket2", "product_ss", "product1", "product_ss", "product6", "product_ss", "product7", "price_f", "1")
        .add(id, "6", "basket_s", "basket3", "product_ss", "product4",  "product_ss","product3", "product_ss","product1", "price_f", "1")
        .add(id, "9", "basket_s", "basket4", "product_ss", "product4", "product_ss", "product3", "product_ss", "product1","price_f", "1")
        //.add(id, "12", "basket_s", "basket5", "product_ss", "product1", "price_f", "1")
        //.add(id, "13", "basket_s", "basket6", "product_ss", "product1", "price_f", "1")
        //.add(id, "14", "basket_s", "basket7", "product_ss", "product1", "price_f", "1")
        //.add(id, "15", "basket_s", "basket4", "product_ss", "product1", "price_f", "1")
        .commit(cluster.getSolrClient(), COLLECTION);

    List<Tuple> tuples = null;
    TupleStream stream = null;
    StreamContext context = new StreamContext();
    SolrClientCache cache = new SolrClientCache();
    context.setSolrClientCache(cache);

    StreamFactory factory = new StreamFactory()
        .withCollectionZkHost("collection1", cluster.getZkServer().getZkAddress())
        .withDefaultZkHost(cluster.getZkServer().getZkAddress())
        .withFunctionName("gatherNodes", GatherNodesStream.class)
        .withFunctionName("scoreNodes", ScoreNodesStream.class)
        .withFunctionName("search", CloudSolrStream.class)
        .withFunctionName("facet", FacetStream.class)
        .withFunctionName("sort", SortStream.class)
        .withFunctionName("count", CountMetric.class)
        .withFunctionName("avg", MeanMetric.class)
        .withFunctionName("sum", SumMetric.class)
        .withFunctionName("min", MinMetric.class)
        .withFunctionName("max", MaxMetric.class);

    String expr = "sort(by=\"nodeScore desc\",scoreNodes(facet(collection1, q=\"product_ss:product3\", buckets=\"product_ss\", bucketSorts=\"count(*) desc\", bucketSizeLimit=100, count(*))))";

    stream = factory.constructStream(expr);

    context = new StreamContext();
    context.setSolrClientCache(cache);

    stream.setStreamContext(context);
    tuples = getTuples(stream);

    Tuple tuple = tuples.get(0);
    assert(tuple.getString("node").equals("product3"));
    assert(tuple.getLong("docFreq") == 3);
    assert(tuple.getLong("count(*)") == 3);

    Tuple tuple0 = tuples.get(1);
    assert(tuple0.getString("node").equals("product4"));
    assert(tuple0.getLong("docFreq") == 2);
    assert(tuple0.getLong("count(*)") == 2);

    Tuple tuple1 = tuples.get(2);
    assert(tuple1.getString("node").equals("product1"));
    assert(tuple1.getLong("docFreq") == 4);
    assert(tuple1.getLong("count(*)") == 3);

    Tuple tuple2 = tuples.get(3);
    assert(tuple2.getString("node").equals("product5"));
    assert(tuple2.getLong("docFreq") == 1);
    assert(tuple2.getLong("count(*)") == 1);


    cache.close();
  }





  @Test
  public void testGatherNodesFriendsStream() throws Exception {

    new UpdateRequest()
        .add(id, "0", "from_s", "bill", "to_s", "jim", "message_t", "Hello jim")
        .add(id, "1", "from_s", "bill", "to_s", "sam", "message_t", "Hello sam")
        .add(id, "2", "from_s", "bill", "to_s", "max", "message_t", "Hello max")
        .add(id, "3", "from_s", "max",  "to_s", "kip", "message_t", "Hello kip")
        .add(id, "4", "from_s", "sam",  "to_s", "steve", "message_t", "Hello steve")
        .add(id, "5", "from_s", "jim",  "to_s", "ann", "message_t", "Hello steve")
        .commit(cluster.getSolrClient(), COLLECTION);

    List<Tuple> tuples = null;
    GatherNodesStream stream = null;
    StreamContext context = new StreamContext();
    SolrClientCache cache = new SolrClientCache();
    context.setSolrClientCache(cache);

    StreamFactory factory = new StreamFactory()
        .withCollectionZkHost("collection1", cluster.getZkServer().getZkAddress())
        .withFunctionName("gatherNodes", GatherNodesStream.class)
        .withFunctionName("search", CloudSolrStream.class)
        .withFunctionName("count", CountMetric.class)
        .withFunctionName("hashJoin", HashJoinStream.class)
        .withFunctionName("avg", MeanMetric.class)
        .withFunctionName("sum", SumMetric.class)
        .withFunctionName("min", MinMetric.class)
        .withFunctionName("max", MaxMetric.class);

    String expr = "gatherNodes(collection1, " +
        "walk=\"bill->from_s\"," +
        "gather=\"to_s\")";

    stream = (GatherNodesStream)factory.constructStream(expr);
    stream.setStreamContext(context);

    tuples = getTuples(stream);

    Collections.sort(tuples, new FieldComparator("node", ComparatorOrder.ASCENDING));
    assertTrue(tuples.size() == 3);
    assertTrue(tuples.get(0).getString("node").equals("jim"));
    assertTrue(tuples.get(1).getString("node").equals("max"));
    assertTrue(tuples.get(2).getString("node").equals("sam"));

    //Test scatter branches, leaves and trackTraversal

    expr = "gatherNodes(collection1, " +
        "walk=\"bill->from_s\"," +
        "gather=\"to_s\","+
        "scatter=\"branches, leaves\", trackTraversal=\"true\")";

    stream = (GatherNodesStream)factory.constructStream(expr);
    context = new StreamContext();
    context.setSolrClientCache(cache);
    stream.setStreamContext(context);

    tuples = getTuples(stream);

    Collections.sort(tuples, new FieldComparator("node", ComparatorOrder.ASCENDING));
    assertTrue(tuples.size() == 4);
    assertTrue(tuples.get(0).getString("node").equals("bill"));
    assertTrue(tuples.get(0).getLong("level").equals(0L));
    assertTrue(tuples.get(0).getStrings("ancestors").size() == 0);
    assertTrue(tuples.get(1).getString("node").equals("jim"));
    assertTrue(tuples.get(1).getLong("level").equals(1L));
    List<String> ancestors = tuples.get(1).getStrings("ancestors");
    System.out.println("##################### Ancestors:"+ancestors);
    assert(ancestors.size() == 1);
    assert(ancestors.get(0).equals("bill"));

    assertTrue(tuples.get(2).getString("node").equals("max"));
    assertTrue(tuples.get(2).getLong("level").equals(1L));
    ancestors = tuples.get(2).getStrings("ancestors");
    assert(ancestors.size() == 1);
    assert(ancestors.get(0).equals("bill"));

    assertTrue(tuples.get(3).getString("node").equals("sam"));
    assertTrue(tuples.get(3).getLong("level").equals(1L));
    ancestors = tuples.get(3).getStrings("ancestors");
    assert(ancestors.size() == 1);
    assert(ancestors.get(0).equals("bill"));

    // Test query root

    expr = "gatherNodes(collection1, " +
        "search(collection1, q=\"message_t:jim\", fl=\"from_s\", sort=\"from_s asc\"),"+
        "walk=\"from_s->from_s\"," +
        "gather=\"to_s\")";

    stream = (GatherNodesStream)factory.constructStream(expr);
    context = new StreamContext();
    context.setSolrClientCache(cache);
    stream.setStreamContext(context);

    tuples = getTuples(stream);

    Collections.sort(tuples, new FieldComparator("node", ComparatorOrder.ASCENDING));
    assertTrue(tuples.size() == 3);
    assertTrue(tuples.get(0).getString("node").equals("jim"));
    assertTrue(tuples.get(1).getString("node").equals("max"));
    assertTrue(tuples.get(2).getString("node").equals("sam"));


    // Test query root scatter branches

    expr = "gatherNodes(collection1, " +
        "search(collection1, q=\"message_t:jim\", fl=\"from_s\", sort=\"from_s asc\"),"+
        "walk=\"from_s->from_s\"," +
        "gather=\"to_s\", scatter=\"branches, leaves\")";

    stream = (GatherNodesStream)factory.constructStream(expr);
    context = new StreamContext();
    context.setSolrClientCache(cache);
    stream.setStreamContext(context);

    tuples = getTuples(stream);

    Collections.sort(tuples, new FieldComparator("node", ComparatorOrder.ASCENDING));
    assertTrue(tuples.size() == 4);
    assertTrue(tuples.get(0).getString("node").equals("bill"));
    assertTrue(tuples.get(0).getLong("level").equals(0L));
    assertTrue(tuples.get(1).getString("node").equals("jim"));
    assertTrue(tuples.get(1).getLong("level").equals(1L));
    assertTrue(tuples.get(2).getString("node").equals("max"));
    assertTrue(tuples.get(2).getLong("level").equals(1L));
    assertTrue(tuples.get(3).getString("node").equals("sam"));
    assertTrue(tuples.get(3).getLong("level").equals(1L));

    expr = "gatherNodes(collection1, " +
        "search(collection1, q=\"message_t:jim\", fl=\"from_s\", sort=\"from_s asc\"),"+
        "walk=\"from_s->from_s\"," +
        "gather=\"to_s\")";

    String expr2 = "gatherNodes(collection1, " +
        expr+","+
        "walk=\"node->from_s\"," +
        "gather=\"to_s\")";

    stream = (GatherNodesStream)factory.constructStream(expr2);
    context = new StreamContext();
    context.setSolrClientCache(cache);
    stream.setStreamContext(context);

    tuples = getTuples(stream);
    Collections.sort(tuples, new FieldComparator("node", ComparatorOrder.ASCENDING));

    assertTrue(tuples.size() == 3);
    assertTrue(tuples.get(0).getString("node").equals("ann"));
    assertTrue(tuples.get(1).getString("node").equals("kip"));
    assertTrue(tuples.get(2).getString("node").equals("steve"));


    //Test two traversals in the same expression
    String expr3 = "hashJoin("+expr2+", hashed="+expr2+", on=\"node\")";

    HashJoinStream hstream = (HashJoinStream)factory.constructStream(expr3);
    context = new StreamContext();
    context.setSolrClientCache(cache);
    hstream.setStreamContext(context);

    tuples = getTuples(hstream);
    Collections.sort(tuples, new FieldComparator("node", ComparatorOrder.ASCENDING));

    assertTrue(tuples.size() == 3);
    assertTrue(tuples.get(0).getString("node").equals("ann"));
    assertTrue(tuples.get(1).getString("node").equals("kip"));
    assertTrue(tuples.get(2).getString("node").equals("steve"));

    //=================================


    expr = "gatherNodes(collection1, " +
        "search(collection1, q=\"message_t:jim\", fl=\"from_s\", sort=\"from_s asc\"),"+
        "walk=\"from_s->from_s\"," +
        "gather=\"to_s\")";

    expr2 = "gatherNodes(collection1, " +
        expr+","+
        "walk=\"node->from_s\"," +
        "gather=\"to_s\", scatter=\"branches, leaves\")";

    stream = (GatherNodesStream)factory.constructStream(expr2);
    context = new StreamContext();
    context.setSolrClientCache(cache);
    stream.setStreamContext(context);

    tuples = getTuples(stream);
    Collections.sort(tuples, new FieldComparator("node", ComparatorOrder.ASCENDING));


    assertTrue(tuples.size() == 7);
    assertTrue(tuples.get(0).getString("node").equals("ann"));
    assertTrue(tuples.get(0).getLong("level").equals(2L));
    assertTrue(tuples.get(1).getString("node").equals("bill"));
    assertTrue(tuples.get(1).getLong("level").equals(0L));
    assertTrue(tuples.get(2).getString("node").equals("jim"));
    assertTrue(tuples.get(2).getLong("level").equals(1L));
    assertTrue(tuples.get(3).getString("node").equals("kip"));
    assertTrue(tuples.get(3).getLong("level").equals(2L));
    assertTrue(tuples.get(4).getString("node").equals("max"));
    assertTrue(tuples.get(4).getLong("level").equals(1L));
    assertTrue(tuples.get(5).getString("node").equals("sam"));
    assertTrue(tuples.get(5).getLong("level").equals(1L));
    assertTrue(tuples.get(6).getString("node").equals("steve"));
    assertTrue(tuples.get(6).getLong("level").equals(2L));

    //Add a cycle from jim to bill
    new UpdateRequest()
        .add(id, "6", "from_s", "jim", "to_s", "bill", "message_t", "Hello steve")
        .add(id, "7", "from_s", "sam", "to_s", "bill", "message_t", "Hello steve")
        .commit(cluster.getSolrClient(), COLLECTION);

    expr = "gatherNodes(collection1, " +
        "search(collection1, q=\"message_t:jim\", fl=\"from_s\", sort=\"from_s asc\"),"+
        "walk=\"from_s->from_s\"," +
        "gather=\"to_s\", trackTraversal=\"true\")";

    expr2 = "gatherNodes(collection1, " +
        expr+","+
        "walk=\"node->from_s\"," +
        "gather=\"to_s\", scatter=\"branches, leaves\", trackTraversal=\"true\")";

    stream = (GatherNodesStream)factory.constructStream(expr2);
    context = new StreamContext();
    context.setSolrClientCache(cache);
    stream.setStreamContext(context);

    tuples = getTuples(stream);
    Collections.sort(tuples, new FieldComparator("node", ComparatorOrder.ASCENDING));

    assertTrue(tuples.size() == 7);
    assertTrue(tuples.get(0).getString("node").equals("ann"));
    assertTrue(tuples.get(0).getLong("level").equals(2L));
    //Bill should now have one ancestor
    assertTrue(tuples.get(1).getString("node").equals("bill"));
    assertTrue(tuples.get(1).getLong("level").equals(0L));
    assertTrue(tuples.get(1).getStrings("ancestors").size() == 2);
    List<String> anc = tuples.get(1).getStrings("ancestors");

    Collections.sort(anc);
    assertTrue(anc.get(0).equals("jim"));
    assertTrue(anc.get(1).equals("sam"));

    assertTrue(tuples.get(2).getString("node").equals("jim"));
    assertTrue(tuples.get(2).getLong("level").equals(1L));
    assertTrue(tuples.get(3).getString("node").equals("kip"));
    assertTrue(tuples.get(3).getLong("level").equals(2L));
    assertTrue(tuples.get(4).getString("node").equals("max"));
    assertTrue(tuples.get(4).getLong("level").equals(1L));
    assertTrue(tuples.get(5).getString("node").equals("sam"));
    assertTrue(tuples.get(5).getLong("level").equals(1L));
    assertTrue(tuples.get(6).getString("node").equals("steve"));
    assertTrue(tuples.get(6).getLong("level").equals(2L));

    cache.close();

  }

  @Test
  public void testGraphHandler() throws Exception {


    new UpdateRequest()
        .add(id, "0", "from_s", "bill", "to_s", "jim", "message_t", "Hello jim")
        .add(id, "1", "from_s", "bill", "to_s", "sam", "message_t", "Hello sam")
        .add(id, "2", "from_s", "bill", "to_s", "max", "message_t", "Hello max")
        .add(id, "3", "from_s", "max",  "to_s", "kip", "message_t", "Hello kip")
        .add(id, "4", "from_s", "sam",  "to_s", "steve", "message_t", "Hello steve")
        .add(id, "5", "from_s", "jim",  "to_s", "ann", "message_t", "Hello steve")
        .commit(cluster.getSolrClient(), COLLECTION);

    commit();

    List<JettySolrRunner> runners = cluster.getJettySolrRunners();
    JettySolrRunner runner = runners.get(0);
    String url = runner.getBaseUrl().toString();

    HttpSolrClient client = getHttpSolrClient(url);
    ModifiableSolrParams params = new ModifiableSolrParams();


    String expr = "sort(by=\"node asc\", gatherNodes(collection1, " +
        "walk=\"bill->from_s\"," +
        "trackTraversal=\"true\"," +
        "gather=\"to_s\"))";

    params.add("expr", expr);
    QueryRequest query = new QueryRequest(params);
    query.setPath("/collection1/graph");

    query.setResponseParser(new InputStreamResponseParser("xml"));
    query.setMethod(SolrRequest.METHOD.POST);

    NamedList<Object> genericResponse = client.request(query);


    InputStream stream = (InputStream)genericResponse.get("stream");
    InputStreamReader reader = new InputStreamReader(stream, StandardCharsets.UTF_8);
    String xml = readString(reader);
    //Validate the nodes
    String error = BaseTestHarness.validateXPath(xml,
        "//graph/node[1][@id ='jim']",
        "//graph/node[2][@id ='max']",
        "//graph/node[3][@id ='sam']");
    if(error != null) {
      throw new Exception(error);
    }
    //Validate the edges
    error = BaseTestHarness.validateXPath(xml,
        "//graph/edge[1][@source ='bill']",
        "//graph/edge[1][@target ='jim']",
        "//graph/edge[2][@source ='bill']",
        "//graph/edge[2][@target ='max']",
        "//graph/edge[3][@source ='bill']",
        "//graph/edge[3][@target ='sam']");

    if(error != null) {
      throw new Exception(error);
    }

    client.close();
  }






  private String readString(InputStreamReader reader) throws Exception{
    StringBuilder builder = new StringBuilder();
    int c = 0;
    while((c = reader.read()) != -1) {
      builder.append(((char)c));
    }

    return builder.toString();
  }




  protected List<Tuple> getTuples(TupleStream tupleStream) throws IOException {
    tupleStream.open();
    List<Tuple> tuples = new ArrayList<>();
    for(Tuple t = tupleStream.read(); !t.EOF; t = tupleStream.read()) {
      tuples.add(t);
    }
    tupleStream.close();
    return tuples;
  }

  protected boolean assertOrderOf(List<Tuple> tuples, String fieldName, int... ids) throws Exception {
    int i = 0;
    for(int val : ids) {
      Tuple t = tuples.get(i);
      Long tip = (Long)t.get(fieldName);
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

}
