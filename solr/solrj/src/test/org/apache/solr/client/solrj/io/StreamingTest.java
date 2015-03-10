package org.apache.solr.client.solrj.io;

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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.cloud.AbstractZkTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.ArrayList;

/**
 *  All base tests will be done with CloudSolrStream. Under the covers CloudSolrStream uses SolrStream so
 *  SolrStream will get fully exercised through these tests.
 *
 **/

@Slow
@LuceneTestCase.SuppressCodecs({"Lucene3x", "Lucene40","Lucene41","Lucene42","Lucene45"})
public class StreamingTest extends AbstractFullDistribZkTestBase {

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

  public StreamingTest() {
    super();
    sliceCount = 2;
  }

  private void testUniqueStream() throws Exception {

    //Test CloudSolrStream and UniqueStream

    indexr(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "0");
    indexr(id, "2", "a_s", "hello2", "a_i", "2", "a_f", "0");
    indexr(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3");
    indexr(id, "4", "a_s", "hello4", "a_i", "4", "a_f", "4");
    indexr(id, "1", "a_s", "hello1", "a_i", "1", "a_f", "1");

    commit();


    String zkHost = zkServer.getZkAddress();

    Map params = mapParams("q","*:*","fl","id,a_s,a_i,a_f","sort", "a_f asc,a_i asc");
    CloudSolrStream stream = new CloudSolrStream(zkHost, "collection1", params);
    UniqueStream ustream = new UniqueStream(stream, new AscFieldComp("a_f"));
    List<Tuple> tuples = getTuples(ustream);
    assert(tuples.size() == 4);
    assertOrder(tuples, 0,1,3,4);

    del("*:*");
    commit();

  }

  private void testHashJoinStream() throws Exception {

    indexr(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "0");
    indexr(id, "2", "a_s", "hello2", "a_i", "2", "a_f", "0", "join_i", "1000");
    indexr(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3");
    indexr(id, "4", "a_s", "hello4", "a_i", "4", "a_f", "4");
    indexr(id, "1", "a_s", "hello1", "a_i", "1", "a_f", "1");
    indexr(id, "6", "a_s", "hello1", "a_i", "1", "a_f", "0", "join_i", "2000");
    indexr(id, "7", "a_s", "hello7", "a_i", "1", "a_f", "0");

    commit();

    String zkHost = zkServer.getZkAddress();

    //Test one-to-one
    Map paramsA = mapParams("q","id:(0 1 3 4) ","fl","id,a_s,a_f", "sort", "a_s desc");
    CloudSolrStream streamA = new CloudSolrStream(zkHost, "collection1", paramsA);

    Map fieldMappings = new HashMap();
    fieldMappings.put("id","streamB.id");

    Map paramsB = mapParams("q","id:(2)","fl","id,a_s,a_f,join_i", "sort", "a_s desc");
    CloudSolrStream streamB = new CloudSolrStream(zkHost, "collection1", paramsB);
    streamB.setFieldMappings(fieldMappings);

    String[] keys = {"a_f"};

    HashJoinStream fstream = new HashJoinStream(streamA, streamB, keys);
    List<Tuple> tuples = getTuples(fstream);

    assert(tuples.size() == 1);
    assertOrder(tuples, 0);
    assertLong(tuples.get(0), "join_i", 1000);


    //Test one-to-many

    paramsA = mapParams("q","id:(0 1 3 4) ","fl","id,a_s,a_f", "sort", "a_s desc");
    streamA = new CloudSolrStream(zkHost, "collection1", paramsA);

    fieldMappings = new HashMap();
    fieldMappings.put("id","streamB.id");

    paramsB = mapParams("q","id:(2 6)","fl","id,a_s,a_f,join_i", "sort", "a_s desc");
    streamB = new CloudSolrStream(zkHost, "collection1", paramsB);
    streamB.setFieldMappings(fieldMappings);


    fstream = new HashJoinStream(streamA, streamB, keys);
    tuples = getTuples(fstream);

    assert(tuples.size() == 2);
    assertOrder(tuples, 0,0);
    assertLong(tuples.get(0), "join_i", 1000);
    assertLong(tuples.get(1), "join_i", 2000);

    //Test many-to-one

    paramsA = mapParams("q","id:(0 2 1 3 4) ","fl","id,a_s,a_f", "sort", "a_s desc");
    streamA = new CloudSolrStream(zkHost, "collection1", paramsA);

    fieldMappings = new HashMap();
    fieldMappings.put("id","streamB.id");

    paramsB = mapParams("q","id:(6)","fl","id,a_s,a_f,join_i", "sort", "a_s desc");
    streamB = new CloudSolrStream(zkHost, "collection1", paramsB);
    streamB.setFieldMappings(fieldMappings);


    fstream = new HashJoinStream(streamA, streamB, keys);
    tuples = getTuples(fstream);

    assert(tuples.size() == 2);
    assertOrder(tuples, 2,0);
    assertLong(tuples.get(0), "join_i", 2000);
    assertLong(tuples.get(1), "join_i", 2000);

    //Test many-to-many

    paramsA = mapParams("q","id:(0 7 1 3 4) ","fl","id,a_s,a_f", "sort", "a_s desc");
    streamA = new CloudSolrStream(zkHost, "collection1", paramsA);

    fieldMappings = new HashMap();
    fieldMappings.put("id","streamB.id");

    paramsB = mapParams("q","id:(6 2)","fl","id,a_s,a_f,join_i", "sort", "a_s desc");
    streamB = new CloudSolrStream(zkHost, "collection1", paramsB);
    streamB.setFieldMappings(fieldMappings);


    fstream = new HashJoinStream(streamA, streamB, keys);
    tuples = getTuples(fstream);

    assert(tuples.size() == 4);
    assertOrder(tuples, 7,7,0,0);
    assertLong(tuples.get(0), "join_i", 1000);
    assertLong(tuples.get(1), "join_i", 2000);
    assertLong(tuples.get(2), "join_i", 1000);
    assertLong(tuples.get(3), "join_i", 2000);

    del("*:*");
    commit();

  }

  private void testMergeJoinStream() throws Exception {

    indexr(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "0");
    indexr(id, "2", "a_s", "hello2", "a_i", "2", "a_f", "0", "join_i", "1000");
    indexr(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3");
    indexr(id, "4", "a_s", "hello4", "a_i", "4", "a_f", "4");
    indexr(id, "1", "a_s", "hello1", "a_i", "1", "a_f", "1");
    indexr(id, "6", "a_s", "hello1", "a_i", "1", "a_f", "0", "join_i", "2000");
    indexr(id, "7", "a_s", "hello7", "a_i", "1", "a_f", "0");

    commit();

    String zkHost = zkServer.getZkAddress();

    //Test one-to-one
    Map paramsA = mapParams("q","id:(0 1 3 4) ","fl","id,a_s,a_f", "sort", "a_f desc");
    CloudSolrStream streamA = new CloudSolrStream(zkHost, "collection1", paramsA);

    Map fieldMappings = new HashMap();
    fieldMappings.put("id","streamB.id");

    Map paramsB = mapParams("q","id:(2)","fl","id,a_s,a_f,join_i", "sort", "a_f desc");
    CloudSolrStream streamB = new CloudSolrStream(zkHost, "collection1", paramsB);
    streamB.setFieldMappings(fieldMappings);

    String[] keys = {"a_f"};

    MergeJoinStream fstream = new MergeJoinStream(streamA, streamB, new DescFieldComp("a_f"));
    List<Tuple> tuples = getTuples(fstream);

    assert(tuples.size() == 1);
    assertOrder(tuples, 0);
    assertLong(tuples.get(0), "join_i", 1000);


    //Test one-to-many

    paramsA = mapParams("q","id:(0 1 3 4) ","fl","id,a_s,a_f", "sort", "a_f desc");
    streamA = new CloudSolrStream(zkHost, "collection1", paramsA);

    fieldMappings = new HashMap();
    fieldMappings.put("id","streamB.id");

    paramsB = mapParams("q","id:(2 6)","fl","id,a_s,a_f,join_i", "sort", "a_f desc");
    streamB = new CloudSolrStream(zkHost, "collection1", paramsB);
    streamB.setFieldMappings(fieldMappings);


    fstream = new MergeJoinStream(streamA, streamB, new DescFieldComp("a_f"));
    tuples = getTuples(fstream);

    assert(tuples.size() == 2);
    assertOrder(tuples, 0,0);
    assertLong(tuples.get(0), "join_i", 1000);
    assertLong(tuples.get(1), "join_i", 2000);

    //Test many-to-one

    paramsA = mapParams("q","id:(0 2 1 3 4) ","fl","id,a_s,a_f", "sort", "a_f desc");
    streamA = new CloudSolrStream(zkHost, "collection1", paramsA);

    fieldMappings = new HashMap();
    fieldMappings.put("id","streamB.id");

    paramsB = mapParams("q","id:(6)","fl","id,a_s,a_f,join_i", "sort", "a_f desc");
    streamB = new CloudSolrStream(zkHost, "collection1", paramsB);
    streamB.setFieldMappings(fieldMappings);


    fstream = new MergeJoinStream(streamA, streamB, new DescFieldComp("a_f"));
    tuples = getTuples(fstream);

    assert(tuples.size() == 2);
    assertOrder(tuples, 2,0);
    assertLong(tuples.get(0), "join_i", 2000);
    assertLong(tuples.get(1), "join_i", 2000);

    //Test many-to-many

    paramsA = mapParams("q","id:(0 7 1 3 4) ","fl","id,a_s,a_f", "sort", "a_f desc");
    streamA = new CloudSolrStream(zkHost, "collection1", paramsA);

    fieldMappings = new HashMap();
    fieldMappings.put("id","streamB.id");

    paramsB = mapParams("q","id:(6 2)","fl","id,a_s,a_f,join_i", "sort", "a_f desc");
    streamB = new CloudSolrStream(zkHost, "collection1", paramsB);
    streamB.setFieldMappings(fieldMappings);


    fstream = new MergeJoinStream(streamA, streamB, new DescFieldComp("a_f"));
    tuples = getTuples(fstream);

    assert(tuples.size() == 4);
    assertOrder(tuples, 7,7,0,0);
    assertLong(tuples.get(0), "join_i", 1000);
    assertLong(tuples.get(1), "join_i", 2000);
    assertLong(tuples.get(2), "join_i", 1000);
    assertLong(tuples.get(3), "join_i", 2000);

    del("*:*");
    commit();

  }

  private void testParallelMergeJoinStream() throws Exception {

    indexr(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "0");
    indexr(id, "2", "a_s", "hello2", "a_i", "2", "a_f", "0", "join_i", "1000");
    indexr(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3");
    indexr(id, "4", "a_s", "hello4", "a_i", "4", "a_f", "4");
    indexr(id, "1", "a_s", "hello1", "a_i", "1", "a_f", "1");
    indexr(id, "6", "a_s", "hello1", "a_i", "1", "a_f", "0", "join_i", "2000");
    indexr(id, "7", "a_s", "hello7", "a_i", "1", "a_f", "0");

    commit();

    String zkHost = zkServer.getZkAddress();

    //Test one-to-one
    Map paramsA = mapParams("q","id:(0 1 3 4) ","fl","id,a_s,a_f", "sort", "a_f desc", "partitionKeys","a_f");
    CloudSolrStream streamA = new CloudSolrStream(zkHost, "collection1", paramsA);

    Map fieldMappings = new HashMap();
    fieldMappings.put("id","streamB.id");

    Map paramsB = mapParams("q","id:(2)","fl","id,a_s,a_f,join_i", "sort", "a_f desc", "partitionKeys","a_f");
    CloudSolrStream streamB = new CloudSolrStream(zkHost, "collection1", paramsB);
    streamB.setFieldMappings(fieldMappings);

    String[] keys = {"a_f"};

    MergeJoinStream mstream = new MergeJoinStream(streamA, streamB, new DescFieldComp("a_f"));
    ParallelStream fstream = new ParallelStream(zkHost,"collection1", mstream, 2, new DescFieldComp("a_f"));

    List<Tuple> tuples = getTuples(fstream);

    assert(tuples.size() == 1);
    assertOrder(tuples, 0);
    assertLong(tuples.get(0), "join_i", 1000);


    //Test one-to-many

    paramsA = mapParams("q","id:(0 1 3 4) ","fl","id,a_s,a_f", "sort", "a_f desc", "partitionKeys","a_f");
    streamA = new CloudSolrStream(zkHost, "collection1", paramsA);

    fieldMappings = new HashMap();
    fieldMappings.put("id","streamB.id");

    paramsB = mapParams("q","id:(2 6)","fl","id,a_s,a_f,join_i", "sort", "a_f desc", "partitionKeys","a_f");
    streamB = new CloudSolrStream(zkHost, "collection1", paramsB);
    streamB.setFieldMappings(fieldMappings);


    mstream = new MergeJoinStream(streamA, streamB, new DescFieldComp("a_f"));
    fstream = new ParallelStream(zkHost,"collection1", mstream, 2, new DescFieldComp("a_f"));

    tuples = getTuples(fstream);

    assert(tuples.size() == 2);
    assertOrder(tuples, 0,0);
    assertLong(tuples.get(0), "join_i", 1000);
    assertLong(tuples.get(1), "join_i", 2000);

    //Test many-to-one

    paramsA = mapParams("q","id:(0 2 1 3 4) ","fl","id,a_s,a_f", "sort", "a_f desc", "partitionKeys","a_f");
    streamA = new CloudSolrStream(zkHost, "collection1", paramsA);

    fieldMappings = new HashMap();
    fieldMappings.put("id","streamB.id");

    paramsB = mapParams("q","id:(6)","fl","id,a_s,a_f,join_i", "sort", "a_f desc", "partitionKeys","a_f");
    streamB = new CloudSolrStream(zkHost, "collection1", paramsB);
    streamB.setFieldMappings(fieldMappings);


    mstream = new MergeJoinStream(streamA, streamB, new DescFieldComp("a_f"));
    fstream = new ParallelStream(zkHost,"collection1", mstream, 2, new DescFieldComp("a_f"));

    tuples = getTuples(fstream);

    assert(tuples.size() == 2);
    assertOrder(tuples, 2,0);
    assertLong(tuples.get(0), "join_i", 2000);
    assertLong(tuples.get(1), "join_i", 2000);

    //Test many-to-many

    paramsA = mapParams("q","id:(0 7 1 3 4) ","fl","id,a_s,a_f", "sort", "a_f desc", "partitionKeys","a_f");
    streamA = new CloudSolrStream(zkHost, "collection1", paramsA);

    fieldMappings = new HashMap();
    fieldMappings.put("id","streamB.id");

    paramsB = mapParams("q","id:(6 2)","fl","id,a_s,a_f,join_i", "sort", "a_f desc", "partitionKeys","a_f");
    streamB = new CloudSolrStream(zkHost, "collection1", paramsB);
    streamB.setFieldMappings(fieldMappings);


    mstream = new MergeJoinStream(streamA, streamB, new DescFieldComp("a_f"));
    fstream = new ParallelStream(zkHost,"collection1", mstream, 2, new DescFieldComp("a_f"));

    tuples = getTuples(fstream);

    assert(tuples.size() == 4);
    assertOrder(tuples, 7,7,0,0);
    assertLong(tuples.get(0), "join_i", 1000);
    assertLong(tuples.get(1), "join_i", 2000);
    assertLong(tuples.get(2), "join_i", 1000);
    assertLong(tuples.get(3), "join_i", 2000);

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

    String zkHost = zkServer.getZkAddress();

    Map params = mapParams("q","*:*","fl","id,a_s,a_i","sort", "a_i asc");
    CloudSolrStream stream = new CloudSolrStream(zkHost, "collection1", params);
    RankStream rstream = new RankStream(stream, 3, new DescFieldComp("a_i"));
    List<Tuple> tuples = getTuples(rstream);


    assert(tuples.size() == 3);
    assertOrder(tuples, 4,3,2);

    del("*:*");
    commit();
  }

  private void testRollupStream() throws Exception {
    indexr(id, "0", "a_s", "hello0", "a_i", "100", "a_f", "0");
    indexr(id, "2", "a_s", "hello0", "a_i", "2", "a_f", "0");
    indexr(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3");
    indexr(id, "4", "a_s", "hello3", "a_i", "4", "a_f", "4");
    indexr(id, "1", "a_s", "hello1", "a_i", "1", "a_f", "1");
    indexr(id, "6", "a_s", "hello1", "a_i", "1", "a_f", "1");
    indexr(id, "7", "a_s", "hello1", "a_i", "1", "a_f", "1");

    commit();

    String zkHost = zkServer.getZkAddress();

    Bucket[] buckets = {new Bucket("a_s")};
    Metric[] metrics = {new SumMetric("a_i", false),
        new MeanMetric("a_i", false),
        new CountMetric(),
        new MinMetric("a_i", false),
        new MaxMetric("a_i", false)};

    Map params = mapParams("q","*:*","fl","id,a_s,a_i","sort", "a_s asc");
    CloudSolrStream stream = new CloudSolrStream(zkHost, "collection1", params);
    RollupStream rstream = new RollupStream(stream, buckets, metrics);
    rstream.open();
    Tuple tuple = rstream.read();
    String b = (String)tuple.get("buckets");
    List<Double> values = (List<Double>)tuple.get("metricValues");
    assert(b.equals("hello0"));
    assert(values.get(0) == 102.0d);
    assert(values.get(1) == 51.0d);
    assert(values.get(2) == 2.0d);
    assert(values.get(3) == 2.0d);
    assert(values.get(4) == 100.0d);

    tuple = rstream.read();
    b = (String)tuple.get("buckets");
    values = (List<Double>)tuple.get("metricValues");
    assert(b.equals("hello1"));
    assert(values.get(0) == 3.0d);
    assert(values.get(1) == 1.0d);
    assert(values.get(2) == 3.0d);
    assert(values.get(3) == 1.0d);
    assert(values.get(4) == 1.0d);


    tuple = rstream.read();
    b = (String)tuple.get("buckets");
    values = (List<Double>)tuple.get("metricValues");
    assert(b.equals("hello3"));
    assert(values.get(0) == 7.0d);
    assert(values.get(1) == 3.5d);
    assert(values.get(2) == 2.0d);
    assert(values.get(3) == 3.0d);
    assert(values.get(4) == 4.0d);

    tuple = rstream.read();
    assert(tuple.EOF);

    rstream.close();
    del("*:*");
    commit();
  }

  private void testParallelRollupStream() throws Exception {
    indexr(id, "0", "a_s", "hello0", "a_i", "100", "a_f", "0");
    indexr(id, "2", "a_s", "hello0", "a_i", "2", "a_f", "0");
    indexr(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3");
    indexr(id, "4", "a_s", "hello3", "a_i", "4", "a_f", "4");
    indexr(id, "1", "a_s", "hello1", "a_i", "1", "a_f", "1");
    indexr(id, "6", "a_s", "hello1", "a_i", "1", "a_f", "1");
    indexr(id, "7", "a_s", "hello1", "a_i", "1", "a_f", "1");

    commit();

    String zkHost = zkServer.getZkAddress();

    Bucket[] buckets = {new Bucket("a_s")};
    Metric[] metrics = {new SumMetric("a_i", false),
        new MeanMetric("a_i", false),
        new CountMetric(),
        new MinMetric("a_i", false),
        new MaxMetric("a_i", false)};

    Map params = mapParams("q","*:*","fl","id,a_s,a_i","sort", "a_s asc","partitionKeys","a_s");
    CloudSolrStream stream = new CloudSolrStream(zkHost, "collection1", params);
    RollupStream rostream = new RollupStream(stream, buckets, metrics);
    ParallelStream rstream = new ParallelStream(zkHost,"collection1", rostream, 2, new AscFieldComp("buckets"));

    rstream.open();
    Tuple tuple = rstream.read();
    String b = (String)tuple.get("buckets");
    List<Double> values = (List<Double>)tuple.get("metricValues");
    assert(b.equals("hello0"));
    assert(values.get(0) == 102.0d);
    assert(values.get(1) == 51.0d);
    assert(values.get(2) == 2.0d);
    assert(values.get(3) == 2.0d);
    assert(values.get(4) == 100.0d);

    tuple = rstream.read();
    b = (String)tuple.get("buckets");
    values = (List<Double>)tuple.get("metricValues");
    assert(b.equals("hello1"));
    assert(values.get(0) == 3.0d);
    assert(values.get(1) == 1.0d);
    assert(values.get(2) == 3.0d);
    assert(values.get(3) == 1.0d);
    assert(values.get(4) == 1.0d);


    tuple = rstream.read();
    b = (String)tuple.get("buckets");
    values = (List<Double>)tuple.get("metricValues");
    assert(b.equals("hello3"));
    assert(values.get(0) == 7.0d);
    assert(values.get(1) == 3.5d);
    assert(values.get(2) == 2.0d);
    assert(values.get(3) == 3.0d);
    assert(values.get(4) == 4.0d);

    tuple = rstream.read();
    assert(tuple.EOF);

    rstream.close();
    del("*:*");
    commit();
  }



  private void testMetricStream() throws Exception {

    indexr(id, "0", "a_s", "hello0", "a_i", "100", "a_f", "0");
    indexr(id, "2", "a_s", "hello0", "a_i", "2", "a_f", "0");
    indexr(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3");
    indexr(id, "4", "a_s", "hello3", "a_i", "4", "a_f", "4");
    indexr(id, "1", "a_s", "hello1", "a_i", "1", "a_f", "1");
    indexr(id, "6", "a_s", "hello1", "a_i", "1", "a_f", "1");
    indexr(id, "7", "a_s", "hello1", "a_i", "1", "a_f", "1");

    commit();

    String zkHost = zkServer.getZkAddress();

    Bucket[] buckets = {new Bucket("a_s")};
    Metric[] metrics = {new SumMetric("a_i", false),
                        new MeanMetric("a_i", false),
                        new CountMetric(),
                        new MinMetric("a_i", false),
                        new MaxMetric("a_i", false)};

    Map params = mapParams("q","*:*","fl","id,a_s,a_i","sort", "a_i asc");
    CloudSolrStream stream = new CloudSolrStream(zkHost, "collection1", params);
    MetricStream mstream = new MetricStream(stream, buckets, metrics, "metric1", new DescBucketComp(0),5);
    getTuples(mstream);

    BucketMetrics[] bucketMetrics = mstream.getBucketMetrics();
    assert(bucketMetrics.length == 3);

    //Bucket should be is descending order based on Metric 0, which is the SumMetric.

    assert(bucketMetrics[0].getKey().toString().equals("hello0"));
    assert(bucketMetrics[1].getKey().toString().equals("hello3"));
    assert(bucketMetrics[2].getKey().toString().equals("hello1"));

    assertMetric(bucketMetrics[0].getMetrics()[0], 102.0d); //Test the first Metric of the first BucketMetrics
    assertMetric(bucketMetrics[0].getMetrics()[1], 51.0d); //Test the second Metric of the first BucketMetrics
    assertMetric(bucketMetrics[0].getMetrics()[2], 2.0d); //Test the third Metric of the first BucketMetrics
    assertMetric(bucketMetrics[0].getMetrics()[3], 2.0d); //Test the fourth Metric of the first BucketMetrics
    assertMetric(bucketMetrics[0].getMetrics()[4], 100.0d); //Test the fifth Metric of the first BucketMetrics


    assertMetric(bucketMetrics[1].getMetrics()[0], 7.0d);
    assertMetric(bucketMetrics[2].getMetrics()[0], 3.0d);


    params = mapParams("q","*:*","fl","id,a_s,a_i","sort", "a_i asc");
    stream = new CloudSolrStream(zkHost, "collection1", params);
    mstream = new MetricStream(stream, buckets, metrics, "metric1", new AscBucketComp(0),5);
    getTuples(mstream);

    bucketMetrics = mstream.getBucketMetrics();

    assertMetric(bucketMetrics[0].getMetrics()[0], 3.0d); //Test the first Metric of the first BucketMetrics
    assertMetric(bucketMetrics[0].getMetrics()[1], 1.0d); //Test the second Metric of the first BucketMetrics
    assertMetric(bucketMetrics[0].getMetrics()[2], 3.0d); //Test the third Metric of the first BucketMetrics
    assertMetric(bucketMetrics[0].getMetrics()[3], 1.0d); //Test the fourth Metric of the first BucketMetrics
    assertMetric(bucketMetrics[0].getMetrics()[4], 1.0d); //Test the fifth Metric of the first BucketMetrics

    assertMetric(bucketMetrics[1].getMetrics()[0], 7.0d);
    assertMetric(bucketMetrics[2].getMetrics()[0], 102.0d);

    indexr(id, "8", "a_s", "hello4", "a_i", "1000", "a_f", "1"); //Add a fourth record.
    commit();

    //Test desc comp with more buckets then priority queue can hold.
    params = mapParams("q","*:*","fl","id,a_s,a_i","sort", "a_i asc");
    stream = new CloudSolrStream(zkHost, "collection1", params);
    mstream = new MetricStream(stream, buckets, metrics, "metric1", new DescBucketComp(0),3);
    getTuples(mstream);

    bucketMetrics = mstream.getBucketMetrics();
    assert(bucketMetrics.length == 3);
    assert(bucketMetrics[0].getKey().toString().equals("hello4"));
    assert(bucketMetrics[1].getKey().toString().equals("hello0"));
    assert(bucketMetrics[2].getKey().toString().equals("hello3"));

    //Test asc comp with more buckets then priority queue can hold.
    params = mapParams("q","*:*","fl","id,a_s,a_i","sort", "a_i asc");
    stream = new CloudSolrStream(zkHost, "collection1", params);
    mstream = new MetricStream(stream, buckets, metrics, "metric1", new AscBucketComp(0),3);
    getTuples(mstream);

    bucketMetrics = mstream.getBucketMetrics();
    assert(bucketMetrics.length == 3);
    assert(bucketMetrics[0].getKey().toString().equals("hello1"));
    assert(bucketMetrics[1].getKey().toString().equals("hello3"));
    assert(bucketMetrics[2].getKey().toString().equals("hello0"));


    //Test with no buckets
    params = mapParams("q","*:*","fl","id,a_s,a_i","sort", "a_i asc");
    stream = new CloudSolrStream(zkHost, "collection1", params);
    mstream = new MetricStream(stream, metrics, "metric1");
    getTuples(mstream);

    bucketMetrics = mstream.getBucketMetrics();
    assert(bucketMetrics.length == 1);
    assert(bucketMetrics[0].getKey().toString().equals("metrics"));
    assertMetric(bucketMetrics[0].getMetrics()[0], 1112.0d); //Test the first Metric of the first BucketMetrics

    del("*:*");
    commit();
  }


  private void testParallelMetricStream() throws Exception {

    indexr(id, "0", "a_s", "hello0", "a_i", "100", "a_f", "0");
    indexr(id, "2", "a_s", "hello0", "a_i", "2", "a_f", "0");
    indexr(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3");
    indexr(id, "4", "a_s", "hello3", "a_i", "4", "a_f", "4");
    indexr(id, "1", "a_s", "hello1", "a_i", "1", "a_f", "1");
    indexr(id, "6", "a_s", "hello1", "a_i", "1", "a_f", "1");
    indexr(id, "7", "a_s", "hello1", "a_i", "1", "a_f", "1");

    commit();

    String zkHost = zkServer.getZkAddress();

    Bucket[] buckets = {new Bucket("a_s")};
    Metric[] metrics = {new SumMetric("a_i", false),
        new MeanMetric("a_i", false),
        new CountMetric(),
        new MinMetric("a_i", false),
        new MaxMetric("a_i", false)};

    Map params = mapParams("q","*:*","fl","id,a_s,a_i","sort", "a_i asc", "partitionKeys", "a_i");
    CloudSolrStream stream = new CloudSolrStream(zkHost, "collection1", params);
    MetricStream mstream = new MetricStream(stream, buckets, metrics, "metric1", new DescBucketComp(0),5);
    ParallelStream pstream = new ParallelStream(zkHost,"collection1",mstream,2,new AscFieldComp("a_i"));
    getTuples(pstream);

    BucketMetrics[] bucketMetrics = mstream.getBucketMetrics();
    assert(bucketMetrics.length == 3);

    //Bucket should be is descending order based on Metric 0, which is the SumMetric.

    assert(bucketMetrics[0].getKey().toString().equals("hello0"));
    assert(bucketMetrics[1].getKey().toString().equals("hello3"));
    assert(bucketMetrics[2].getKey().toString().equals("hello1"));

    assertMetric(bucketMetrics[0].getMetrics()[0], 102.0d); //Test the first Metric of the first BucketMetrics
    assertMetric(bucketMetrics[0].getMetrics()[1], 51.0d); //Test the second Metric of the first BucketMetrics
    assertMetric(bucketMetrics[0].getMetrics()[2], 2.0d); //Test the third Metric of the first BucketMetrics
    assertMetric(bucketMetrics[0].getMetrics()[3], 2.0d); //Test the fourth Metric of the first BucketMetrics
    assertMetric(bucketMetrics[0].getMetrics()[4], 100.0d); //Test the fifth Metric of the first BucketMetrics


    assertMetric(bucketMetrics[1].getMetrics()[0], 7.0d);
    assertMetric(bucketMetrics[2].getMetrics()[0], 3.0d);


    params = mapParams("q","*:*","fl","id,a_s,a_i","sort", "a_i asc");
    stream = new CloudSolrStream(zkHost, "collection1", params);
    mstream = new MetricStream(stream, buckets, metrics, "metric1", new AscBucketComp(0),5);
    getTuples(mstream);

    bucketMetrics = mstream.getBucketMetrics();

    assertMetric(bucketMetrics[0].getMetrics()[0], 3.0d); //Test the first Metric of the first BucketMetrics
    assertMetric(bucketMetrics[0].getMetrics()[1], 1.0d); //Test the second Metric of the first BucketMetrics
    assertMetric(bucketMetrics[0].getMetrics()[2], 3.0d); //Test the third Metric of the first BucketMetrics
    assertMetric(bucketMetrics[0].getMetrics()[3], 1.0d); //Test the fourth Metric of the first BucketMetrics
    assertMetric(bucketMetrics[0].getMetrics()[4], 1.0d); //Test the fifth Metric of the first BucketMetrics

    assertMetric(bucketMetrics[1].getMetrics()[0], 7.0d);
    assertMetric(bucketMetrics[2].getMetrics()[0], 102.0d);

    indexr(id, "8", "a_s", "hello4", "a_i", "1000", "a_f", "1"); //Add a fourth record.
    commit();

    //Test desc comp with more buckets then priority queue can hold.
    params = mapParams("q","*:*","fl","id,a_s,a_i","sort", "a_i asc");
    stream = new CloudSolrStream(zkHost, "collection1", params);
    mstream = new MetricStream(stream, buckets, metrics, "metric1", new DescBucketComp(0),3);
    getTuples(mstream);

    bucketMetrics = mstream.getBucketMetrics();
    assert(bucketMetrics.length == 3);
    assert(bucketMetrics[0].getKey().toString().equals("hello4"));
    assert(bucketMetrics[1].getKey().toString().equals("hello0"));
    assert(bucketMetrics[2].getKey().toString().equals("hello3"));

    //Test asc comp with more buckets then priority queue can hold.
    params = mapParams("q","*:*","fl","id,a_s,a_i","sort", "a_i asc");
    stream = new CloudSolrStream(zkHost, "collection1", params);
    mstream = new MetricStream(stream, buckets, metrics, "metric1", new AscBucketComp(0),3);
    getTuples(mstream);

    bucketMetrics = mstream.getBucketMetrics();
    assert(bucketMetrics.length == 3);
    assert(bucketMetrics[0].getKey().toString().equals("hello1"));
    assert(bucketMetrics[1].getKey().toString().equals("hello3"));
    assert(bucketMetrics[2].getKey().toString().equals("hello0"));


    //Test with no buckets
    params = mapParams("q","*:*","fl","id,a_s,a_i","sort", "a_i asc");
    stream = new CloudSolrStream(zkHost, "collection1", params);
    mstream = new MetricStream(stream, metrics, "metric1");
    getTuples(mstream);

    bucketMetrics = mstream.getBucketMetrics();
    assert(bucketMetrics.length == 1);
    assert(bucketMetrics[0].getKey().toString().equals("metrics"));
    assertMetric(bucketMetrics[0].getMetrics()[0], 1112.0d); //Test the first Metric of the first BucketMetrics

    del("*:*");
    commit();
  }

  private void testGroupByStream() throws Exception {

    indexr(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "0");
    indexr(id, "2", "a_s", "hello0", "a_i", "2", "a_f", "0");
    indexr(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3");
    indexr(id, "4", "a_s", "hello4", "a_i", "4", "a_f", "4");
    indexr(id, "1", "a_s", "hello0", "a_i", "1", "a_f", "1");

    commit();

    //Test CloudSolrStream and SumStream over an int field
    String zkHost = zkServer.getZkAddress();

    Map paramsA = mapParams("q","*:*","fl","id,a_s,a_i","sort", "a_s asc");
    CloudSolrStream stream = new CloudSolrStream(zkHost, "collection1", paramsA);
    GroupByStream gstream = new GroupByStream(stream, new AscFieldComp("a_s"), new DescFieldComp("a_i"), 5);

    List<Tuple> tuples = getTuples(gstream);

    assert(tuples.size() == 3);
    assertOrder(tuples, 2,3,4);
    assertGroupOrder(tuples.get(0), 1, 0);

    del("*:*");
    commit();
  }


  private void testFilterStream() throws Exception {

    indexr(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "0");
    indexr(id, "2", "a_s", "hello2", "a_i", "2", "a_f", "0");
    indexr(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3");
    indexr(id, "4", "a_s", "hello4", "a_i", "4", "a_f", "4");
    indexr(id, "1", "a_s", "hello1", "a_i", "1", "a_f", "1");

    commit();

    //Test CloudSolrStream and SumStream over an int field
    String zkHost = zkServer.getZkAddress();

    Map paramsA = mapParams("q","*:*","fl","id,a_s,a_i","sort", "a_s asc");
    CloudSolrStream streamA = new CloudSolrStream(zkHost, "collection1", paramsA);

    Map paramsB = mapParams("q","id:(0 2)","fl","a_s","sort", "a_s asc");
    CloudSolrStream streamB = new CloudSolrStream(zkHost, "collection1", paramsB);


    FilterStream fstream = new FilterStream(streamA, streamB, new AscFieldComp("a_s"));
    List<Tuple> tuples = getTuples(fstream);

    assert(tuples.size() == 2);
    assertOrder(tuples, 0,2);

    del("*:*");
    commit();
  }

  private void testParallelStream() throws Exception {

    indexr(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "0");
    indexr(id, "2", "a_s", "hello2", "a_i", "2", "a_f", "0");
    indexr(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3");
    indexr(id, "4", "a_s", "hello4", "a_i", "4", "a_f", "4");
    indexr(id, "1", "a_s", "hello1", "a_i", "1", "a_f", "1");

    commit();

    String zkHost = zkServer.getZkAddress();

    Map paramsA = mapParams("q","*:*","fl","id,a_s,a_i","sort", "a_s asc", "partitionKeys","a_s");
    CloudSolrStream streamA = new CloudSolrStream(zkHost, "collection1", paramsA);

    Map paramsB = mapParams("q","id:(0 2)","fl","a_s","sort", "a_s asc", "partitionKeys","a_s");
    CloudSolrStream streamB = new CloudSolrStream(zkHost, "collection1", paramsB);

    FilterStream fstream = new FilterStream(streamA, streamB, new AscFieldComp("a_s"));
    ParallelStream pstream = new ParallelStream(zkHost,"collection1", fstream, 2, new AscFieldComp("a_s"));
    List<Tuple> tuples = getTuples(pstream);

    assert(tuples.size() == 2);
    assertOrder(tuples, 0,2);

    del("*:*");
    commit();
  }

  private void testParallelStreamSingleWorker() throws Exception {

    indexr(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "0");
    indexr(id, "2", "a_s", "hello2", "a_i", "2", "a_f", "0");
    indexr(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3");
    indexr(id, "4", "a_s", "hello4", "a_i", "4", "a_f", "4");
    indexr(id, "1", "a_s", "hello1", "a_i", "1", "a_f", "1");

    commit();

    String zkHost = zkServer.getZkAddress();

    Map paramsA = mapParams("q","*:*","fl","id,a_s,a_i","sort", "a_s asc", "partitionKeys","a_s");
    CloudSolrStream streamA = new CloudSolrStream(zkHost, "collection1", paramsA);

    Map paramsB = mapParams("q","id:(0 2)","fl","a_s","sort", "a_s asc", "partitionKeys","a_s");
    CloudSolrStream streamB = new CloudSolrStream(zkHost, "collection1", paramsB);

    FilterStream fstream = new FilterStream(streamA, streamB, new AscFieldComp("a_s"));
    ParallelStream pstream = new ParallelStream(zkHost,"collection1", fstream, 1, new AscFieldComp("a_s"));
    List<Tuple> tuples = getTuples(pstream);

    assert(tuples.size() == 2);
    assertOrder(tuples, 0,2);

    del("*:*");
    commit();
  }


  private void testParallelHashJoinStream() {

  }

  private void testParallelGroupByStream() throws Exception {

    indexr(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "0");
    indexr(id, "2", "a_s", "hello2", "a_i", "2", "a_f", "0");
    indexr(id, "3", "a_s", "hello0", "a_i", "3", "a_f", "3");
    indexr(id, "4", "a_s", "hello0", "a_i", "4", "a_f", "4");
    indexr(id, "1", "a_s", "hello1", "a_i", "1", "a_f", "1");

    commit();

    String zkHost = zkServer.getZkAddress();

    Map params = mapParams("q","*:*","fl","id,a_s,a_i","sort", "a_s asc", "partitionKeys","a_s");
    CloudSolrStream stream = new CloudSolrStream(zkHost, "collection1", params);
    GroupByStream gstream  = new GroupByStream(stream, new AscFieldComp("a_s"), new AscFieldComp("a_i"),5);
    ParallelStream pstream = new ParallelStream(zkHost,"collection1", gstream, 2, new AscFieldComp("a_s"));
    List<Tuple> tuples = getTuples(pstream);

    assert(tuples.size() == 3);
    assertOrder(tuples, 0,1,2);
    assertGroupOrder(tuples.get(0),3,4);
    del("*:*");
    commit();
  }


  private void testTuple() throws Exception {

    indexr(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "5.1", "s_multi", "a", "s_multi", "b", "i_multi", "1", "i_multi", "2", "f_multi", "1.2", "f_multi", "1.3");

    commit();

    String zkHost = zkServer.getZkAddress();

    Map params = mapParams("q","*:*","fl","id,a_s,a_i,a_f,s_multi,i_multi,f_multi","sort", "a_s asc", "partitionKeys","a_s");
    CloudSolrStream stream = new CloudSolrStream(zkHost, "collection1", params);
    List<Tuple> tuples = getTuples(stream);
    Tuple tuple = tuples.get(0);

    String s = tuple.getString("a_s");
    assert(s.equals("hello0")) ;

    long l = tuple.getLong("a_i");
    assert(l == 0);

    double d = tuple.getDouble("a_f");
    assert(d == 5.1);


    List<String> stringList = tuple.getStrings("s_multi");
    assert(stringList.get(0).equals("a"));
    assert(stringList.get(1).equals("b"));

    List<Long> longList = tuple.getLongs("i_multi");
    assert(longList.get(0).longValue() == 1);
    assert(longList.get(1).longValue() == 2);

    List<Double> doubleList = tuple.getDoubles("f_multi");
    assert(doubleList.get(0).doubleValue() == 1.2);
    assert(doubleList.get(1).doubleValue() == 1.3);

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

    String zkHost = zkServer.getZkAddress();

    //Test ascending
    Map paramsA = mapParams("q","id:(4 1)","fl","id,a_s,a_i","sort", "a_i asc");
    CloudSolrStream streamA = new CloudSolrStream(zkHost, "collection1", paramsA);

    Map paramsB = mapParams("q","id:(0 2 3)","fl","id,a_s,a_i","sort", "a_i asc");
    CloudSolrStream streamB = new CloudSolrStream(zkHost, "collection1", paramsB);

    MergeStream mstream = new MergeStream(streamA, streamB, new AscFieldComp("a_i"));
    List<Tuple> tuples = getTuples(mstream);

    assert(tuples.size() == 5);
    assertOrder(tuples, 0,1,2,3,4);

    //Test descending
    paramsA = mapParams("q","id:(4 1)","fl","id,a_s,a_i","sort", "a_i desc");
    streamA = new CloudSolrStream(zkHost, "collection1", paramsA);

    paramsB = mapParams("q","id:(0 2 3)","fl","id,a_s,a_i","sort", "a_i desc");
    streamB = new CloudSolrStream(zkHost, "collection1", paramsB);

    mstream = new MergeStream(streamA, streamB, new DescFieldComp("a_i"));
    tuples = getTuples(mstream);

    assert(tuples.size() == 5);
    assertOrder(tuples, 4,3,2,1,0);

    //Test compound sort

    paramsA = mapParams("q","id:(2 4 1)","fl","id,a_s,a_i,a_f","sort", "a_f asc,a_i asc");
    streamA = new CloudSolrStream(zkHost, "collection1", paramsA);

    paramsB = mapParams("q","id:(0 3)","fl","id,a_s,a_i,a_f","sort", "a_f asc,a_i asc");
    streamB = new CloudSolrStream(zkHost, "collection1", paramsB);

    mstream = new MergeStream(streamA, streamB, new MultiComp(new AscFieldComp("a_f"),new AscFieldComp("a_i")));
    tuples = getTuples(mstream);

    assert(tuples.size() == 5);
    assertOrder(tuples, 0,2,1,3,4);

    paramsA = mapParams("q","id:(2 4 1)","fl","id,a_s,a_i,a_f","sort", "a_f asc,a_i desc");
    streamA = new CloudSolrStream(zkHost, "collection1", paramsA);

    paramsB = mapParams("q","id:(0 3)","fl","id,a_s,a_i,a_f","sort", "a_f asc,a_i desc");
    streamB = new CloudSolrStream(zkHost, "collection1", paramsB);

    mstream = new MergeStream(streamA, streamB, new MultiComp(new AscFieldComp("a_f"),new DescFieldComp("a_i")));
    tuples = getTuples(mstream);

    assert(tuples.size() == 5);
    assertOrder(tuples, 2,0,1,3,4);

    del("*:*");
    commit();
  }


  @Test
  public void streamTests() throws Exception {
    assertNotNull(cloudClient);

    handle.clear();
    handle.put("timestamp", SKIPVAL);

    waitForThingsToLevelOut(30);

    del("*:*");

    commit();

    indexr(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "0");
    indexr(id, "2", "a_s", "hello2", "a_i", "2", "a_f", "0");
    indexr(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3");
    indexr(id, "4", "a_s", "hello4", "a_i", "4", "a_f", "4");
    indexr(id, "1", "a_s", "hello1", "a_i", "1", "a_f", "1");

    commit();

    String zkHost = zkServer.getZkAddress();
    Map params = null;

    //Basic CloudSolrStream Test with Ascending Sort

    params = mapParams("q","*:*","fl","id,a_s,a_i","sort", "a_i desc");
    CloudSolrStream stream = new CloudSolrStream(zkHost, "collection1", params);
    List<Tuple> tuples = getTuples(stream);

    assert(tuples.size() == 5);
    assertOrder(tuples, 4, 3, 2, 1, 0);

    //With Descending Sort
    params = mapParams("q","*:*","fl","id,a_s,a_i","sort", "a_i asc");
    stream = new CloudSolrStream(zkHost, "collection1", params);
    tuples = getTuples(stream);

    assert(tuples.size() == 5);
    assertOrder(tuples, 0,1,2,3,4);


    //Test compound sort
    params = mapParams("q","*:*","fl","id,a_s,a_i,a_f","sort", "a_f asc,a_i desc");
    stream = new CloudSolrStream(zkHost, "collection1", params);
    tuples = getTuples(stream);

    assert(tuples.size() == 5);
    assertOrder(tuples, 2,0,1,3,4);


    params = mapParams("q","*:*","fl","id,a_s,a_i,a_f","sort", "a_f asc,a_i asc");
    stream = new CloudSolrStream(zkHost, "collection1", params);
    tuples = getTuples(stream);

    assert(tuples.size() == 5);
    assertOrder(tuples, 0,2,1,3,4);

    del("*:*");
    commit();

    testTuple();
    testUniqueStream();
    testMetricStream();
    testRollupStream();
    testRankStream();
    testFilterStream();
    testGroupByStream();
    testHashJoinStream();
    testMergeJoinStream();
    testMergeStream();
    testParallelStreamSingleWorker();
    testParallelStream();
    testParallelRollupStream();
    testParallelMetricStream();
    testParallelGroupByStream();
    testParallelHashJoinStream();
    testParallelMergeJoinStream();
  }

  protected Map mapParams(String... vals) {
    Map params = new HashMap();
    String k = null;
    for(String val : vals) {
      if(k == null) {
        k = val;
      } else {
        params.put(k, val);
        k = null;
      }
    }

    return params;
  }

  protected List<Tuple> getTuples(TupleStream tupleStream) throws IOException {
    tupleStream.open();
    List<Tuple> tuples = new ArrayList();
    for(Tuple t = tupleStream.read(); !t.EOF; t = tupleStream.read()) {
      tuples.add(t);
    }
    tupleStream.close();
    return tuples;
  }

  protected boolean assertOrder(List<Tuple> tuples, int... ids) throws Exception {
    int i = 0;
    for(int val : ids) {
      Tuple t = tuples.get(i);
      Long tip = (Long)t.get("id");
      if(tip.intValue() != val) {
        throw new Exception("Found value:"+tip.intValue()+" expecting:"+val);
      }
      ++i;
    }
    return true;
  }

  protected boolean assertGroupOrder(Tuple tuple, int... ids) throws Exception {
    List group = (List)tuple.get("tuples");
    int i=0;
    for(int val : ids) {
      Map t = (Map)group.get(i);
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

  public boolean assertMetric(Metric metric, double value) throws Exception {
    Double d = metric.getValue();
    if(d.doubleValue() != value) {
      throw new Exception("Unexpected Metric "+d+"!="+value);
    }

    return true;
  }

  @Override
  protected void indexr(Object... fields) throws Exception {
    SolrInputDocument doc = getDoc(fields);
    indexDoc(doc);
  }
}
