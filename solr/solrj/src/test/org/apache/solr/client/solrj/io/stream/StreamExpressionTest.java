package org.apache.solr.client.solrj.io.stream;

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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.ops.ReplaceOperation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParser;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.client.solrj.io.stream.metrics.CountMetric;
import org.apache.solr.client.solrj.io.stream.metrics.MaxMetric;
import org.apache.solr.client.solrj.io.stream.metrics.MeanMetric;
import org.apache.solr.client.solrj.io.stream.metrics.MinMetric;
import org.apache.solr.client.solrj.io.stream.metrics.SumMetric;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.cloud.AbstractZkTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.junit.After;
import org.junit.AfterClass;
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
public class StreamExpressionTest extends AbstractFullDistribZkTestBase {

  private static final String SOLR_HOME = getFile("solrj" + File.separator + "solr").getAbsolutePath();

  static {
    schemaString = "schema-streaming.xml";
  }

  @BeforeClass
  public static void beforeSuperClass() {
    AbstractZkTestCase.SOLRHOME = new File(SOLR_HOME());
  }

  @AfterClass
  public static void afterSuperClass() {

  }

  protected String getCloudSolrConfig() {
    return "solrconfig-streaming.xml";
  }


  @Override
  public String getSolrHome() {
    return SOLR_HOME;
  }

  public static String SOLR_HOME() {
    return SOLR_HOME;
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    // we expect this time of exception as shards go up and down...
    //ignoreException(".*");

    System.setProperty("numShards", Integer.toString(sliceCount));
  }

  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
    resetExceptionIgnores();
  }

  public StreamExpressionTest() {
    super();
    sliceCount = 2;
  }

  @Test
  public void testAll() throws Exception{
    assertNotNull(cloudClient);

    handle.clear();
    handle.put("timestamp", SKIPVAL);

    waitForRecoveriesToFinish(false);

    del("*:*");
    commit();
    
    testCloudSolrStream();
    testCloudSolrStreamWithZkHost();
    testMergeStream();
    testRankStream();
    testReducerStream();
    testUniqueStream();
    testRollupStream();
    testStatsStream();
    testParallelUniqueStream();
    testParallelReducerStream();
    testParallelRankStream();
    testParallelMergeStream();
    testParallelRollupStream();
    testInnerJoinStream();
    testLeftOuterJoinStream();
    testHashJoinStream();
    testOuterHashJoinStream();
    testSelectStream();
  }

  private void testCloudSolrStream() throws Exception {

    indexr(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "0");
    indexr(id, "2", "a_s", "hello2", "a_i", "2", "a_f", "0");
    indexr(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3");
    indexr(id, "4", "a_s", "hello4", "a_i", "4", "a_f", "4");
    indexr(id, "1", "a_s", "hello1", "a_i", "1", "a_f", "1");
    commit();

    StreamFactory factory = new StreamFactory().withCollectionZkHost("collection1", zkServer.getZkAddress());
    StreamExpression expression;
    CloudSolrStream stream;
    List<Tuple> tuples;
    
    // Basic test
    expression = StreamExpressionParser.parse("search(collection1, q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\")");
    stream = new CloudSolrStream(expression, factory);
    tuples = getTuples(stream);

    assert(tuples.size() == 5);
    assertOrder(tuples, 0,2,1,3,4);
    assertLong(tuples.get(0),"a_i", 0);

    // Basic w/aliases
    expression = StreamExpressionParser.parse("search(collection1, q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\", aliases=\"a_i=alias.a_i, a_s=name\")");
    stream = new CloudSolrStream(expression, factory);
    tuples = getTuples(stream);

    assert(tuples.size() == 5);
    assertOrder(tuples, 0,2,1,3,4);
    assertLong(tuples.get(0),"alias.a_i", 0);
    assertString(tuples.get(0),"name", "hello0");    

    // Basic filtered test
    expression = StreamExpressionParser.parse("search(collection1, q=\"id:(0 3 4)\", fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\")");
    stream = new CloudSolrStream(expression, factory);
    tuples = getTuples(stream);

    assert(tuples.size() == 3);
    assertOrder(tuples, 0,3,4);
    assertLong(tuples.get(1),"a_i", 3);
    
    del("*:*");
    commit();
  }

  private void testCloudSolrStreamWithZkHost() throws Exception {

    indexr(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "0");
    indexr(id, "2", "a_s", "hello2", "a_i", "2", "a_f", "0");
    indexr(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3");
    indexr(id, "4", "a_s", "hello4", "a_i", "4", "a_f", "4");
    indexr(id, "1", "a_s", "hello1", "a_i", "1", "a_f", "1");
    commit();

    StreamFactory factory = new StreamFactory();
    StreamExpression expression;
    CloudSolrStream stream;
    List<Tuple> tuples;
    
    // Basic test
    expression = StreamExpressionParser.parse("search(collection1, zkHost=" + zkServer.getZkAddress() + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\")");
    stream = new CloudSolrStream(expression, factory);
    tuples = getTuples(stream);

    assert(tuples.size() == 5);
    assertOrder(tuples, 0,2,1,3,4);
    assertLong(tuples.get(0),"a_i", 0);

    // Basic w/aliases
    expression = StreamExpressionParser.parse("search(collection1, q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\", aliases=\"a_i=alias.a_i, a_s=name\", zkHost=" + zkServer.getZkAddress() + ")");
    stream = new CloudSolrStream(expression, factory);
    tuples = getTuples(stream);

    assert(tuples.size() == 5);
    assertOrder(tuples, 0,2,1,3,4);
    assertLong(tuples.get(0),"alias.a_i", 0);
    assertString(tuples.get(0),"name", "hello0");    

    // Basic filtered test
    expression = StreamExpressionParser.parse("search(collection1, q=\"id:(0 3 4)\", fl=\"id,a_s,a_i,a_f\", zkHost=" + zkServer.getZkAddress() + ", sort=\"a_f asc, a_i asc\")");
    stream = new CloudSolrStream(expression, factory);
    tuples = getTuples(stream);

    assert(tuples.size() == 3);
    assertOrder(tuples, 0,3,4);
    assertLong(tuples.get(1),"a_i", 3);
    
    del("*:*");
    commit();
  }

  
  private void testUniqueStream() throws Exception {

    indexr(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "0");
    indexr(id, "2", "a_s", "hello2", "a_i", "2", "a_f", "0");
    indexr(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3");
    indexr(id, "4", "a_s", "hello4", "a_i", "4", "a_f", "4");
    indexr(id, "1", "a_s", "hello1", "a_i", "1", "a_f", "1");
    commit();

    StreamExpression expression;
    TupleStream stream;
    List<Tuple> tuples;
    
    StreamFactory factory = new StreamFactory()
      .withCollectionZkHost("collection1", zkServer.getZkAddress())
      .withFunctionName("search", CloudSolrStream.class)
      .withFunctionName("unique", UniqueStream.class);
    
    // Basic test
    expression = StreamExpressionParser.parse("unique(search(collection1, q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\"), over=\"a_f\")");
    stream = new UniqueStream(expression, factory);
    tuples = getTuples(stream);
    
    assert(tuples.size() == 4);
    assertOrder(tuples, 0, 1, 3, 4);

    // Basic test desc
    expression = StreamExpressionParser.parse("unique(search(collection1, q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f desc, a_i desc\"), over=\"a_f\")");
    stream = new UniqueStream(expression, factory);
    tuples = getTuples(stream);
    
    assert(tuples.size() == 4);
    assertOrder(tuples, 4,3,1,2);
    
    // Basic w/multi comp
    expression = StreamExpressionParser.parse("unique(search(collection1, q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\"), over=\"a_f, a_i\")");
    stream = new UniqueStream(expression, factory);
    tuples = getTuples(stream);
    
    assert(tuples.size() == 5);
    assertOrder(tuples, 0,2,1,3,4);
    
    // full factory w/multi comp
    stream = factory.constructStream("unique(search(collection1, q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\"), over=\"a_f, a_i\")");
    tuples = getTuples(stream);
    
    assert(tuples.size() == 5);
    assertOrder(tuples, 0, 2, 1, 3, 4);
    
    del("*:*");
    commit();
  }
  
  private void testMergeStream() throws Exception {

    indexr(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "0");
    indexr(id, "2", "a_s", "hello2", "a_i", "2", "a_f", "0");
    indexr(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3");
    indexr(id, "4", "a_s", "hello4", "a_i", "4", "a_f", "4");
    indexr(id, "1", "a_s", "hello1", "a_i", "1", "a_f", "1");
    commit();

    StreamExpression expression;
    TupleStream stream;
    List<Tuple> tuples;
    
    StreamFactory factory = new StreamFactory()
      .withCollectionZkHost("collection1", zkServer.getZkAddress())
      .withFunctionName("search", CloudSolrStream.class)
      .withFunctionName("unique", UniqueStream.class)
      .withFunctionName("merge", MergeStream.class);
    
    // Basic test
    expression = StreamExpressionParser.parse("merge("
                                                + "search(collection1, q=\"id:(0 3 4)\", fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc\"),"
                                                + "search(collection1, q=\"id:(1)\", fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc\"),"
                                                + "on=\"a_f asc\")");
    stream = new MergeStream(expression, factory);
    tuples = getTuples(stream);
    
    assert(tuples.size() == 4);
    assertOrder(tuples, 0,1,3,4);

    // Basic test desc
    expression = StreamExpressionParser.parse("merge("
                                              + "search(collection1, q=\"id:(0 3 4)\", fl=\"id,a_s,a_i,a_f\", sort=\"a_f desc\"),"
                                              + "search(collection1, q=\"id:(1)\", fl=\"id,a_s,a_i,a_f\", sort=\"a_f desc\"),"
                                              + "on=\"a_f desc\")");
    stream = new MergeStream(expression, factory);
    tuples = getTuples(stream);
    
    assert(tuples.size() == 4);
    assertOrder(tuples, 4,3,1,0);
    
    // Basic w/multi comp
    expression = StreamExpressionParser.parse("merge("
                                              + "search(collection1, q=\"id:(0 3 4)\", fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_s asc\"),"
                                              + "search(collection1, q=\"id:(1 2)\", fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_s asc\"),"
                                              + "on=\"a_f asc, a_s asc\")");
    stream = new MergeStream(expression, factory);
    tuples = getTuples(stream);
    
    assert(tuples.size() == 5);
    assertOrder(tuples, 0,2,1,3,4);
    
    // full factory w/multi comp
    stream = factory.constructStream("merge("
                                    + "search(collection1, q=\"id:(0 3 4)\", fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_s asc\"),"
                                    + "search(collection1, q=\"id:(1 2)\", fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_s asc\"),"
                                    + "on=\"a_f asc, a_s asc\")");
    tuples = getTuples(stream);
    
    assert(tuples.size() == 5);
    assertOrder(tuples, 0,2,1,3,4);
    
    // full factory w/multi streams
    stream = factory.constructStream("merge("
                                    + "search(collection1, q=\"id:(0 4)\", fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_s asc\"),"
                                    + "search(collection1, q=\"id:(1)\", fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_s asc\"),"
                                    + "search(collection1, q=\"id:(2)\", fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_s asc\"),"
                                    + "on=\"a_f asc\")");
    tuples = getTuples(stream);
    
    assert(tuples.size() == 4);
    assertOrder(tuples, 0,2,1,4);
    
    del("*:*");
    commit();
  }
  
  private void testRankStream() throws Exception {

    indexr(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "0");
    indexr(id, "2", "a_s", "hello2", "a_i", "2", "a_f", "0");
    indexr(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3");
    indexr(id, "4", "a_s", "hello4", "a_i", "4", "a_f", "4");
    indexr(id, "1", "a_s", "hello1", "a_i", "1", "a_f", "1");
    commit();

    StreamExpression expression;
    TupleStream stream;
    List<Tuple> tuples;
    
    StreamFactory factory = new StreamFactory()
      .withCollectionZkHost("collection1", zkServer.getZkAddress())
      .withFunctionName("search", CloudSolrStream.class)
      .withFunctionName("unique", UniqueStream.class)
      .withFunctionName("top", RankStream.class);
    
    // Basic test
    expression = StreamExpressionParser.parse("top("
                                              + "n=3,"
                                              + "search(collection1, q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\"),"
                                              + "sort=\"a_f asc, a_i asc\")");
    stream = new RankStream(expression, factory);
    tuples = getTuples(stream);
    
    assert(tuples.size() == 3);
    assertOrder(tuples, 0,2,1);

    // Basic test desc
    expression = StreamExpressionParser.parse("top("
                                              + "n=2,"
                                              + "unique("
                                              +   "search(collection1, q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f desc\"),"
                                              +   "over=\"a_f\"),"
                                              + "sort=\"a_f desc\")");
    stream = new RankStream(expression, factory);
    tuples = getTuples(stream);
    
    assert(tuples.size() == 2);
    assertOrder(tuples, 4,3);
    
    // full factory
    stream = factory.constructStream("top("
                                    + "n=4,"
                                    + "unique("
                                    +   "search(collection1, q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\"),"
                                    +   "over=\"a_f\"),"
                                    + "sort=\"a_f asc\")");
    tuples = getTuples(stream);
    
    assert(tuples.size() == 4);
    assertOrder(tuples, 0,1,3,4);

    // full factory, switch order
    stream = factory.constructStream("top("
            + "n=4,"
            + "unique("
            +   "search(collection1, q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f desc, a_i desc\"),"
            +   "over=\"a_f\"),"
            + "sort=\"a_f asc\")");
    tuples = getTuples(stream);
    
    assert(tuples.size() == 4);
    assertOrder(tuples, 2,1,3,4);
    
    del("*:*");
    commit();
  }
  
  private void testReducerStream() throws Exception{
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
    
    StreamExpression expression;
    TupleStream stream;
    List<Tuple> tuples;
    Tuple t0, t1, t2;
    List<Map> maps0, maps1, maps2;
    
    StreamFactory factory = new StreamFactory()
      .withCollectionZkHost("collection1", zkServer.getZkAddress())
      .withFunctionName("search", CloudSolrStream.class)
      .withFunctionName("unique", UniqueStream.class)
      .withFunctionName("top", RankStream.class)
      .withFunctionName("group", ReducerStream.class);

    // basic
    expression = StreamExpressionParser.parse("group("
                                              + "search(collection1, q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_s asc, a_f asc\"),"
                                              + "by=\"a_s\")");
    stream = new ReducerStream(expression, factory);
    tuples = getTuples(stream);

    assert(tuples.size() == 3);
    assertOrder(tuples, 0,3,4);

    t0 = tuples.get(0);
    maps0 = t0.getMaps();
    assertMaps(maps0, 0, 2,1, 9);

    t1 = tuples.get(1);
    maps1 = t1.getMaps();
    assertMaps(maps1, 3, 5, 7, 8);

    t2 = tuples.get(2);
    maps2 = t2.getMaps();
    assertMaps(maps2, 4, 6);
    
    // basic w/spaces
    expression = StreamExpressionParser.parse("group("
                                              + "search(collection1, q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_s asc, a_f       asc\"),"
                                              + "by=\"a_s\")");
    stream = new ReducerStream(expression, factory);
    tuples = getTuples(stream);

    assert(tuples.size() == 3);
    assertOrder(tuples, 0,3,4);

    t0 = tuples.get(0);
    maps0 = t0.getMaps();
    assertMaps(maps0, 0, 2,1, 9);

    t1 = tuples.get(1);
    maps1 = t1.getMaps();
    assertMaps(maps1, 3, 5, 7, 8);

    t2 = tuples.get(2);
    maps2 = t2.getMaps();
    assertMaps(maps2, 4, 6);

    del("*:*");
    commit();
  }

  private void testRollupStream() throws Exception {

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

    StreamFactory factory = new StreamFactory()
      .withCollectionZkHost("collection1", zkServer.getZkAddress())
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
                                              + "search(collection1, q=*:*, fl=\"a_s,a_i,a_f\", sort=\"a_s asc\"),"
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

    del("*:*");
    commit();
  }
  
  private void testStatsStream() throws Exception {

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

    StreamFactory factory = new StreamFactory()
    .withCollectionZkHost("collection1", zkServer.getZkAddress())
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

    del("*:*");
    commit();
  }
  
  private void testParallelUniqueStream() throws Exception {

    indexr(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "0");
    indexr(id, "2", "a_s", "hello2", "a_i", "2", "a_f", "0");
    indexr(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3");
    indexr(id, "4", "a_s", "hello4", "a_i", "4", "a_f", "4");
    indexr(id, "1", "a_s", "hello1", "a_i", "1", "a_f", "1");
    indexr(id, "5", "a_s", "hello1", "a_i", "10", "a_f", "1");
    indexr(id, "6", "a_s", "hello1", "a_i", "11", "a_f", "5");
    indexr(id, "7", "a_s", "hello1", "a_i", "12", "a_f", "5");
    indexr(id, "8", "a_s", "hello1", "a_i", "13", "a_f", "4");

    commit();

    String zkHost = zkServer.getZkAddress();
    StreamFactory streamFactory = new StreamFactory().withCollectionZkHost("collection1", zkServer.getZkAddress())
        .withFunctionName("search", CloudSolrStream.class)
        .withFunctionName("unique", UniqueStream.class)
        .withFunctionName("top", RankStream.class)
        .withFunctionName("group", ReducerStream.class)
        .withFunctionName("parallel", ParallelStream.class);

    ParallelStream pstream = (ParallelStream)streamFactory.constructStream("parallel(collection1, unique(search(collection1, q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\", partitionKeys=\"a_f\"), over=\"a_f\"), workers=\"2\", zkHost=\""+zkHost+"\", sort=\"a_f asc\")");

    List<Tuple> tuples = getTuples(pstream);
    assert(tuples.size() == 5);
    assertOrder(tuples, 0,1,3,4,6);

    //Test the eofTuples

    Map<String,Tuple> eofTuples = pstream.getEofTuples();
    assert(eofTuples.size() == 2); //There should be an EOF tuple for each worker.

    del("*:*");
    commit();

  }

  private void testParallelReducerStream() throws Exception {

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

    String zkHost = zkServer.getZkAddress();
    StreamFactory streamFactory = new StreamFactory().withCollectionZkHost("collection1", zkServer.getZkAddress())
        .withFunctionName("search", CloudSolrStream.class)
        .withFunctionName("unique", UniqueStream.class)
        .withFunctionName("top", RankStream.class)
        .withFunctionName("group", ReducerStream.class)
        .withFunctionName("parallel", ParallelStream.class);

    ParallelStream pstream = (ParallelStream)streamFactory.constructStream("parallel(collection1, group(search(collection1, q=\"*:*\", fl=\"id,a_s,a_i,a_f\", sort=\"a_s asc,a_f asc\", partitionKeys=\"a_s\"), by=\"a_s\"), workers=\"2\", zkHost=\""+zkHost+"\", sort=\"a_s asc\")");

    List<Tuple> tuples = getTuples(pstream);

    assert(tuples.size() == 3);
    assertOrder(tuples, 0,3,4);

    Tuple t0 = tuples.get(0);
    List<Map> maps0 = t0.getMaps();
    assertMaps(maps0, 0, 2, 1, 9);

    Tuple t1 = tuples.get(1);
    List<Map> maps1 = t1.getMaps();
    assertMaps(maps1, 3, 5, 7, 8);

    Tuple t2 = tuples.get(2);
    List<Map> maps2 = t2.getMaps();
    assertMaps(maps2, 4, 6);

    //Test Descending with Ascending subsort

    pstream = (ParallelStream)streamFactory.constructStream("parallel(collection1, group(search(collection1, q=\"*:*\", fl=\"id,a_s,a_i,a_f\", sort=\"a_s desc,a_f asc\", partitionKeys=\"a_s\"), by=\"a_s\"), workers=\"2\", zkHost=\""+zkHost+"\", sort=\"a_s desc\")");

    tuples = getTuples(pstream);

    assert(tuples.size() == 3);
    assertOrder(tuples, 4,3,0);

    t0 = tuples.get(0);
    maps0 = t0.getMaps();
    assertMaps(maps0, 4, 6);


    t1 = tuples.get(1);
    maps1 = t1.getMaps();
    assertMaps(maps1, 3, 5, 7, 8);


    t2 = tuples.get(2);
    maps2 = t2.getMaps();
    assertMaps(maps2, 0, 2, 1, 9);



    del("*:*");
    commit();
  }

  private void testParallelRankStream() throws Exception {

    indexr(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "0");
    indexr(id, "2", "a_s", "hello2", "a_i", "2", "a_f", "0");
    indexr(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3");
    indexr(id, "4", "a_s", "hello4", "a_i", "4", "a_f", "4");
    indexr(id, "5", "a_s", "hello1", "a_i", "5", "a_f", "1");
    indexr(id, "6", "a_s", "hello1", "a_i", "6", "a_f", "1");
    indexr(id, "7", "a_s", "hello1", "a_i", "7", "a_f", "1");
    indexr(id, "8", "a_s", "hello1", "a_i", "8", "a_f", "1");
    indexr(id, "9", "a_s", "hello1", "a_i", "9", "a_f", "1");
    indexr(id, "10", "a_s", "hello1", "a_i", "10", "a_f", "1");

    commit();

    String zkHost = zkServer.getZkAddress();
    StreamFactory streamFactory = new StreamFactory().withCollectionZkHost("collection1", zkServer.getZkAddress())
        .withFunctionName("search", CloudSolrStream.class)
        .withFunctionName("unique", UniqueStream.class)
        .withFunctionName("top", RankStream.class)
        .withFunctionName("group", ReducerStream.class)
        .withFunctionName("parallel", ParallelStream.class);

    ParallelStream pstream = (ParallelStream)streamFactory.constructStream("parallel("
        + "collection1, "
        + "top("
          + "search(collection1, q=\"*:*\", fl=\"id,a_s,a_i\", sort=\"a_i asc\", partitionKeys=\"a_i\"), "
          + "n=\"11\", "
          + "sort=\"a_i desc\"), workers=\"2\", zkHost=\""+zkHost+"\", sort=\"a_i desc\")");

    List<Tuple> tuples = getTuples(pstream);

    assert(tuples.size() == 10);
    assertOrder(tuples, 10,9,8,7,6,5,4,3,2,0);

    del("*:*");
    commit();
  }

  private void testParallelMergeStream() throws Exception {

    indexr(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "0");
    indexr(id, "2", "a_s", "hello2", "a_i", "2", "a_f", "0");
    indexr(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3");
    indexr(id, "4", "a_s", "hello4", "a_i", "4", "a_f", "4");
    indexr(id, "1", "a_s", "hello1", "a_i", "1", "a_f", "1");
    indexr(id, "5", "a_s", "hello0", "a_i", "10", "a_f", "0");
    indexr(id, "6", "a_s", "hello2", "a_i", "8", "a_f", "0");
    indexr(id, "7", "a_s", "hello3", "a_i", "7", "a_f", "3");
    indexr(id, "8", "a_s", "hello4", "a_i", "11", "a_f", "4");
    indexr(id, "9", "a_s", "hello1", "a_i", "100", "a_f", "1");

    commit();

    String zkHost = zkServer.getZkAddress();
    StreamFactory streamFactory = new StreamFactory().withCollectionZkHost("collection1", zkServer.getZkAddress())
        .withFunctionName("search", CloudSolrStream.class)
        .withFunctionName("unique", UniqueStream.class)
        .withFunctionName("top", RankStream.class)
        .withFunctionName("group", ReducerStream.class)
        .withFunctionName("merge", MergeStream.class)
        .withFunctionName("parallel", ParallelStream.class);

    //Test ascending
    ParallelStream pstream = (ParallelStream)streamFactory.constructStream("parallel(collection1, merge(search(collection1, q=\"id:(4 1 8 7 9)\", fl=\"id,a_s,a_i\", sort=\"a_i asc\", partitionKeys=\"a_i\"), search(collection1, q=\"id:(0 2 3 6)\", fl=\"id,a_s,a_i\", sort=\"a_i asc\", partitionKeys=\"a_i\"), on=\"a_i asc\"), workers=\"2\", zkHost=\""+zkHost+"\", sort=\"a_i asc\")");

    List<Tuple> tuples = getTuples(pstream);



    assert(tuples.size() == 9);
    assertOrder(tuples, 0,1,2,3,4,7,6,8,9);

    //Test descending

    pstream = (ParallelStream)streamFactory.constructStream("parallel(collection1, merge(search(collection1, q=\"id:(4 1 8 9)\", fl=\"id,a_s,a_i\", sort=\"a_i desc\", partitionKeys=\"a_i\"), search(collection1, q=\"id:(0 2 3 6)\", fl=\"id,a_s,a_i\", sort=\"a_i desc\", partitionKeys=\"a_i\"), on=\"a_i desc\"), workers=\"2\", zkHost=\""+zkHost+"\", sort=\"a_i desc\")");

    tuples = getTuples(pstream);

    assert(tuples.size() == 8);
    assertOrder(tuples, 9,8,6,4,3,2,1,0);

    del("*:*");
    commit();
  }
  
  private void testParallelRollupStream() throws Exception {

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

    StreamFactory factory = new StreamFactory()
      .withCollectionZkHost("collection1", zkServer.getZkAddress())
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

    expression = StreamExpressionParser.parse("parallel(collection1,"
                                              + "rollup("
                                                + "search(collection1, q=*:*, fl=\"a_s,a_i,a_f\", sort=\"a_s asc\", partitionKeys=\"a_s\"),"
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
                                              + "workers=\"2\", zkHost=\""+zkServer.getZkAddress()+"\", sort=\"a_s asc\")"
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

    del("*:*");
    commit();
  }

  private void testInnerJoinStream() throws Exception {

    indexr(id, "1", "side_s", "left", "join1_i", "0", "join2_s", "a", "ident_s", "left_1"); // 8, 9
    indexr(id, "15", "side_s", "left", "join1_i", "0", "join2_s", "a", "ident_s", "left_1"); // 8, 9
    indexr(id, "2", "side_s", "left", "join1_i", "0", "join2_s", "b", "ident_s", "left_2");
    indexr(id, "3", "side_s", "left", "join1_i", "1", "join2_s", "a", "ident_s", "left_3"); // 10
    indexr(id, "4", "side_s", "left", "join1_i", "1", "join2_s", "b", "ident_s", "left_4"); // 11
    indexr(id, "5", "side_s", "left", "join1_i", "1", "join2_s", "c", "ident_s", "left_5"); // 12
    indexr(id, "6", "side_s", "left", "join1_i", "2", "join2_s", "d", "ident_s", "left_6");
    indexr(id, "7", "side_s", "left", "join1_i", "3", "join2_s", "e", "ident_s", "left_7"); // 14

    indexr(id, "8", "side_s", "right", "join1_i", "0", "join2_s", "a", "ident_s", "right_1", "join3_i", "0"); // 1,15
    indexr(id, "9", "side_s", "right", "join1_i", "0", "join2_s", "a", "ident_s", "right_2", "join3_i", "0"); // 1,15
    indexr(id, "10", "side_s", "right", "join1_i", "1", "join2_s", "a", "ident_s", "right_3", "join3_i", "1"); // 3
    indexr(id, "11", "side_s", "right", "join1_i", "1", "join2_s", "b", "ident_s", "right_4", "join3_i", "1"); // 4
    indexr(id, "12", "side_s", "right", "join1_i", "1", "join2_s", "c", "ident_s", "right_5", "join3_i", "1"); // 5
    indexr(id, "13", "side_s", "right", "join1_i", "2", "join2_s", "dad", "ident_s", "right_6", "join3_i", "2"); 
    indexr(id, "14", "side_s", "right", "join1_i", "3", "join2_s", "e", "ident_s", "right_7", "join3_i", "3"); // 7
    commit();

    StreamExpression expression;
    TupleStream stream;
    List<Tuple> tuples;
    
    StreamFactory factory = new StreamFactory()
      .withCollectionZkHost("collection1", zkServer.getZkAddress())
      .withFunctionName("search", CloudSolrStream.class)
      .withFunctionName("innerJoin", InnerJoinStream.class);
    
    // Basic test
    expression = StreamExpressionParser.parse("innerJoin("
                                                + "search(collection1, q=\"side_s:left\", fl=\"id,join1_i,join2_s,ident_s\", sort=\"join1_i asc, join2_s asc, id asc\"),"
                                                + "search(collection1, q=\"side_s:right\", fl=\"join1_i,join2_s,ident_s\", sort=\"join1_i asc, join2_s asc\"),"
                                                + "on=\"join1_i=join1_i, join2_s=join2_s\")");
    stream = new InnerJoinStream(expression, factory);
    tuples = getTuples(stream);    
    assert(tuples.size() == 8);
    assertOrder(tuples, 1,1,15,15,3,4,5,7);

    // Basic desc
    expression = StreamExpressionParser.parse("innerJoin("
                                                + "search(collection1, q=\"side_s:left\", fl=\"id,join1_i,join2_s,ident_s\", sort=\"join1_i desc, join2_s asc\"),"
                                                + "search(collection1, q=\"side_s:right\", fl=\"join1_i,join2_s,ident_s\", sort=\"join1_i desc, join2_s asc\"),"
                                                + "on=\"join1_i=join1_i, join2_s=join2_s\")");
    stream = new InnerJoinStream(expression, factory);
    tuples = getTuples(stream);    
    assert(tuples.size() == 8);
    assertOrder(tuples, 7,3,4,5,1,1,15,15);
    
    // Results in both searches, no join matches
    expression = StreamExpressionParser.parse("innerJoin("
                                                + "search(collection1, q=\"side_s:left\", fl=\"id,join1_i,join2_s,ident_s\", sort=\"ident_s asc\"),"
                                                + "search(collection1, q=\"side_s:right\", fl=\"id,join1_i,join2_s,ident_s\", sort=\"ident_s asc\", aliases=\"id=right.id, join1_i=right.join1_i, join2_s=right.join2_s, ident_s=right.ident_s\"),"
                                                + "on=\"ident_s=right.ident_s\")");
    stream = new InnerJoinStream(expression, factory);
    tuples = getTuples(stream);    
    assert(tuples.size() == 0);
    
    // Differing field names
    expression = StreamExpressionParser.parse("innerJoin("
                                                + "search(collection1, q=\"side_s:left\", fl=\"id,join1_i,join2_s,ident_s\", sort=\"join1_i asc, join2_s asc, id asc\"),"
                                                + "search(collection1, q=\"side_s:right\", fl=\"join3_i,join2_s,ident_s\", sort=\"join3_i asc, join2_s asc\", aliases=\"join3_i=aliasesField\"),"
                                                + "on=\"join1_i=aliasesField, join2_s=join2_s\")");
    stream = new InnerJoinStream(expression, factory);
    tuples = getTuples(stream);
    
    assert(tuples.size() == 8);
    assertOrder(tuples, 1,1,15,15,3,4,5,7);
    
    del("*:*");
    commit();
  }
  
  private void testLeftOuterJoinStream() throws Exception {

    indexr(id, "1", "side_s", "left", "join1_i", "0", "join2_s", "a", "ident_s", "left_1"); // 8, 9
    indexr(id, "15", "side_s", "left", "join1_i", "0", "join2_s", "a", "ident_s", "left_1"); // 8, 9
    indexr(id, "2", "side_s", "left", "join1_i", "0", "join2_s", "b", "ident_s", "left_2");
    indexr(id, "3", "side_s", "left", "join1_i", "1", "join2_s", "a", "ident_s", "left_3"); // 10
    indexr(id, "4", "side_s", "left", "join1_i", "1", "join2_s", "b", "ident_s", "left_4"); // 11
    indexr(id, "5", "side_s", "left", "join1_i", "1", "join2_s", "c", "ident_s", "left_5"); // 12
    indexr(id, "6", "side_s", "left", "join1_i", "2", "join2_s", "d", "ident_s", "left_6");
    indexr(id, "7", "side_s", "left", "join1_i", "3", "join2_s", "e", "ident_s", "left_7"); // 14

    indexr(id, "8", "side_s", "right", "join1_i", "0", "join2_s", "a", "ident_s", "right_1", "join3_i", "0"); // 1,15
    indexr(id, "9", "side_s", "right", "join1_i", "0", "join2_s", "a", "ident_s", "right_2", "join3_i", "0"); // 1,15
    indexr(id, "10", "side_s", "right", "join1_i", "1", "join2_s", "a", "ident_s", "right_3", "join3_i", "1"); // 3
    indexr(id, "11", "side_s", "right", "join1_i", "1", "join2_s", "b", "ident_s", "right_4", "join3_i", "1"); // 4
    indexr(id, "12", "side_s", "right", "join1_i", "1", "join2_s", "c", "ident_s", "right_5", "join3_i", "1"); // 5
    indexr(id, "13", "side_s", "right", "join1_i", "2", "join2_s", "dad", "ident_s", "right_6", "join3_i", "2"); 
    indexr(id, "14", "side_s", "right", "join1_i", "3", "join2_s", "e", "ident_s", "right_7", "join3_i", "3"); // 7
    commit();

    StreamExpression expression;
    TupleStream stream;
    List<Tuple> tuples;
    
    StreamFactory factory = new StreamFactory()
      .withCollectionZkHost("collection1", zkServer.getZkAddress())
      .withFunctionName("search", CloudSolrStream.class)
      .withFunctionName("leftOuterJoin", LeftOuterJoinStream.class);
    
    // Basic test
    expression = StreamExpressionParser.parse("leftOuterJoin("
                                                + "search(collection1, q=\"side_s:left\", fl=\"id,join1_i,join2_s,ident_s\", sort=\"join1_i asc, join2_s asc, id asc\"),"
                                                + "search(collection1, q=\"side_s:right\", fl=\"join1_i,join2_s,ident_s\", sort=\"join1_i asc, join2_s asc\"),"
                                                + "on=\"join1_i=join1_i, join2_s=join2_s\")");
    stream = new LeftOuterJoinStream(expression, factory);
    tuples = getTuples(stream);    
    assert(tuples.size() == 10);
    assertOrder(tuples, 1,1,15,15,2,3,4,5,6,7);

    // Basic desc
    expression = StreamExpressionParser.parse("leftOuterJoin("
                                                + "search(collection1, q=\"side_s:left\", fl=\"id,join1_i,join2_s,ident_s\", sort=\"join1_i desc, join2_s asc\"),"
                                                + "search(collection1, q=\"side_s:right\", fl=\"join1_i,join2_s,ident_s\", sort=\"join1_i desc, join2_s asc\"),"
                                                + "on=\"join1_i=join1_i, join2_s=join2_s\")");
    stream = new LeftOuterJoinStream(expression, factory);
    tuples = getTuples(stream);    
    assert(tuples.size() == 10);
    assertOrder(tuples, 7,6,3,4,5,1,1,15,15,2);
    
    // Results in both searches, no join matches
    expression = StreamExpressionParser.parse("leftOuterJoin("
                                                + "search(collection1, q=\"side_s:left\", fl=\"id,join1_i,join2_s,ident_s\", sort=\"ident_s asc\"),"
                                                + "search(collection1, q=\"side_s:right\", fl=\"id,join1_i,join2_s,ident_s\", sort=\"ident_s asc\", aliases=\"id=right.id, join1_i=right.join1_i, join2_s=right.join2_s, ident_s=right.ident_s\"),"
                                                + "on=\"ident_s=right.ident_s\")");
    stream = new LeftOuterJoinStream(expression, factory);
    tuples = getTuples(stream);    
    assert(tuples.size() == 8);
    assertOrder(tuples, 1,15,2,3,4,5,6,7);
    
    // Differing field names
    expression = StreamExpressionParser.parse("leftOuterJoin("
                                                + "search(collection1, q=\"side_s:left\", fl=\"id,join1_i,join2_s,ident_s\", sort=\"join1_i asc, join2_s asc, id asc\"),"
                                                + "search(collection1, q=\"side_s:right\", fl=\"join3_i,join2_s,ident_s\", sort=\"join3_i asc, join2_s asc\", aliases=\"join3_i=aliasesField\"),"
                                                + "on=\"join1_i=aliasesField, join2_s=join2_s\")");
    stream = new LeftOuterJoinStream(expression, factory);
    tuples = getTuples(stream);
    assert(tuples.size() == 10);
    assertOrder(tuples, 1,1,15,15,2,3,4,5,6,7);
    
    del("*:*");
    commit();
  }

  private void testHashJoinStream() throws Exception {

    indexr(id, "1", "side_s", "left", "join1_i", "0", "join2_s", "a", "ident_s", "left_1"); // 8, 9
    indexr(id, "15", "side_s", "left", "join1_i", "0", "join2_s", "a", "ident_s", "left_1"); // 8, 9
    indexr(id, "2", "side_s", "left", "join1_i", "0", "join2_s", "b", "ident_s", "left_2");
    indexr(id, "3", "side_s", "left", "join1_i", "1", "join2_s", "a", "ident_s", "left_3"); // 10
    indexr(id, "4", "side_s", "left", "join1_i", "1", "join2_s", "b", "ident_s", "left_4"); // 11
    indexr(id, "5", "side_s", "left", "join1_i", "1", "join2_s", "c", "ident_s", "left_5"); // 12
    indexr(id, "6", "side_s", "left", "join1_i", "2", "join2_s", "d", "ident_s", "left_6");
    indexr(id, "7", "side_s", "left", "join1_i", "3", "join2_s", "e", "ident_s", "left_7"); // 14

    indexr(id, "8", "side_s", "right", "join1_i", "0", "join2_s", "a", "ident_s", "right_1", "join3_i", "0"); // 1,15
    indexr(id, "9", "side_s", "right", "join1_i", "0", "join2_s", "a", "ident_s", "right_2", "join3_i", "0"); // 1,15
    indexr(id, "10", "side_s", "right", "join1_i", "1", "join2_s", "a", "ident_s", "right_3", "join3_i", "1"); // 3
    indexr(id, "11", "side_s", "right", "join1_i", "1", "join2_s", "b", "ident_s", "right_4", "join3_i", "1"); // 4
    indexr(id, "12", "side_s", "right", "join1_i", "1", "join2_s", "c", "ident_s", "right_5", "join3_i", "1"); // 5
    indexr(id, "13", "side_s", "right", "join1_i", "2", "join2_s", "dad", "ident_s", "right_6", "join3_i", "2"); 
    indexr(id, "14", "side_s", "right", "join1_i", "3", "join2_s", "e", "ident_s", "right_7", "join3_i", "3"); // 7
    commit();

    StreamExpression expression;
    TupleStream stream;
    List<Tuple> tuples;
    
    StreamFactory factory = new StreamFactory()
      .withCollectionZkHost("collection1", zkServer.getZkAddress())
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
    
    del("*:*");
    commit();
  }
  
  private void testOuterHashJoinStream() throws Exception {

    indexr(id, "1", "side_s", "left", "join1_i", "0", "join2_s", "a", "ident_s", "left_1"); // 8, 9
    indexr(id, "15", "side_s", "left", "join1_i", "0", "join2_s", "a", "ident_s", "left_1"); // 8, 9
    indexr(id, "2", "side_s", "left", "join1_i", "0", "join2_s", "b", "ident_s", "left_2");
    indexr(id, "3", "side_s", "left", "join1_i", "1", "join2_s", "a", "ident_s", "left_3"); // 10
    indexr(id, "4", "side_s", "left", "join1_i", "1", "join2_s", "b", "ident_s", "left_4"); // 11
    indexr(id, "5", "side_s", "left", "join1_i", "1", "join2_s", "c", "ident_s", "left_5"); // 12
    indexr(id, "6", "side_s", "left", "join1_i", "2", "join2_s", "d", "ident_s", "left_6");
    indexr(id, "7", "side_s", "left", "join1_i", "3", "join2_s", "e", "ident_s", "left_7"); // 14

    indexr(id, "8", "side_s", "right", "join1_i", "0", "join2_s", "a", "ident_s", "right_1", "join3_i", "0"); // 1,15
    indexr(id, "9", "side_s", "right", "join1_i", "0", "join2_s", "a", "ident_s", "right_2", "join3_i", "0"); // 1,15
    indexr(id, "10", "side_s", "right", "join1_i", "1", "join2_s", "a", "ident_s", "right_3", "join3_i", "1"); // 3
    indexr(id, "11", "side_s", "right", "join1_i", "1", "join2_s", "b", "ident_s", "right_4", "join3_i", "1"); // 4
    indexr(id, "12", "side_s", "right", "join1_i", "1", "join2_s", "c", "ident_s", "right_5", "join3_i", "1"); // 5
    indexr(id, "13", "side_s", "right", "join1_i", "2", "join2_s", "dad", "ident_s", "right_6", "join3_i", "2"); 
    indexr(id, "14", "side_s", "right", "join1_i", "3", "join2_s", "e", "ident_s", "right_7", "join3_i", "3"); // 7
    commit();

    StreamExpression expression;
    TupleStream stream;
    List<Tuple> tuples;
    
    StreamFactory factory = new StreamFactory()
      .withCollectionZkHost("collection1", zkServer.getZkAddress())
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
    assertOrder(tuples, 1,1,15,15,2,3,4,5,6,7);

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
        
    del("*:*");
    commit();
  }
  
  private void testSelectStream() throws Exception {

    indexr(id, "1", "side_s", "left", "join1_i", "0", "join2_s", "a", "ident_s", "left_1"); // 8, 9
    indexr(id, "15", "side_s", "left", "join1_i", "0", "join2_s", "a", "ident_s", "left_1"); // 8, 9
    indexr(id, "2", "side_s", "left", "join1_i", "0", "join2_s", "b", "ident_s", "left_2");
    indexr(id, "3", "side_s", "left", "join1_i", "1", "join2_s", "a", "ident_s", "left_3"); // 10
    indexr(id, "4", "side_s", "left", "join1_i", "1", "join2_s", "b", "ident_s", "left_4"); // 11
    indexr(id, "5", "side_s", "left", "join1_i", "1", "join2_s", "c", "ident_s", "left_5"); // 12
    indexr(id, "6", "side_s", "left", "join1_i", "2", "join2_s", "d", "ident_s", "left_6");
    indexr(id, "7", "side_s", "left", "join1_i", "3", "join2_s", "e", "ident_s", "left_7"); // 14

    indexr(id, "8", "side_s", "right", "join1_i", "0", "join2_s", "a", "ident_s", "right_1", "join3_i", "0"); // 1,15
    indexr(id, "9", "side_s", "right", "join1_i", "0", "join2_s", "a", "ident_s", "right_2", "join3_i", "0"); // 1,15
    indexr(id, "10", "side_s", "right", "join1_i", "1", "join2_s", "a", "ident_s", "right_3", "join3_i", "1"); // 3
    indexr(id, "11", "side_s", "right", "join1_i", "1", "join2_s", "b", "ident_s", "right_4", "join3_i", "1"); // 4
    indexr(id, "12", "side_s", "right", "join1_i", "1", "join2_s", "c", "ident_s", "right_5", "join3_i", "1"); // 5
    indexr(id, "13", "side_s", "right", "join1_i", "2", "join2_s", "dad", "ident_s", "right_6", "join3_i", "2"); 
    indexr(id, "14", "side_s", "right", "join1_i", "3", "join2_s", "e", "ident_s", "right_7", "join3_i", "3"); // 7
    commit();

    String clause;
    TupleStream stream;
    List<Tuple> tuples;
    
    StreamFactory factory = new StreamFactory()
      .withCollectionZkHost("collection1", zkServer.getZkAddress())
      .withFunctionName("search", CloudSolrStream.class)
      .withFunctionName("innerJoin", InnerJoinStream.class)
      .withFunctionName("select", SelectStream.class)
      .withFunctionName("replace", ReplaceOperation.class);
    
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
    
    del("*:*");
    commit();
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
      Long tip = (Long)t.get(fieldName);
      if(tip.intValue() != val) {
        throw new Exception("Found value:"+tip.intValue()+" expecting:"+val);
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
      Long tip = (Long)t.get("id");
      if(tip.intValue() != val) {
        throw new Exception("Found value:"+tip.intValue()+" expecting:"+val);
      }
      ++i;
    }
    return true;
  }

  @Override
  protected void indexr(Object... fields) throws Exception {
    SolrInputDocument doc = getDoc(fields);
    indexDoc(doc);
  }
}
