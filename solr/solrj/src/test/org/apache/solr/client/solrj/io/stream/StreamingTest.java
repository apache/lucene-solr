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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.ComparatorOrder;
import org.apache.solr.client.solrj.io.comp.MultipleFieldComparator;
import org.apache.solr.client.solrj.io.comp.FieldComparator;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.eq.FieldEqualitor;
import org.apache.solr.client.solrj.io.ops.GroupOperation;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.client.solrj.io.stream.metrics.Bucket;
import org.apache.solr.client.solrj.io.stream.metrics.CountMetric;
import org.apache.solr.client.solrj.io.stream.metrics.MaxMetric;
import org.apache.solr.client.solrj.io.stream.metrics.MeanMetric;
import org.apache.solr.client.solrj.io.stream.metrics.Metric;
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
public class StreamingTest extends AbstractFullDistribZkTestBase {

  private static final String SOLR_HOME = getFile("solrj" + File.separator + "solr").getAbsolutePath();
  private StreamFactory streamFactory;

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
    //System.setProperty("export.test", "true");
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

    streamFactory = new StreamFactory()
                    .withFunctionName("search", CloudSolrStream.class)
                    .withFunctionName("merge", MergeStream.class)
                    .withFunctionName("unique", UniqueStream.class)
                    .withFunctionName("top", RankStream.class)
                    .withFunctionName("reduce", ReducerStream.class)
                    .withFunctionName("group", GroupOperation.class)
                    .withFunctionName("rollup", RollupStream.class)
                    .withFunctionName("parallel", ParallelStream.class);
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
    streamFactory.withCollectionZkHost("collection1", zkHost);

    Map params = mapParams("q","*:*","fl","id,a_s,a_i,a_f","sort", "a_f asc,a_i asc");
    CloudSolrStream stream = new CloudSolrStream(zkHost, "collection1", params);
    UniqueStream ustream = new UniqueStream(stream, new FieldEqualitor("a_f"));
    List<Tuple> tuples = getTuples(ustream);
    assert(tuples.size() == 4);
    assertOrder(tuples, 0,1,3,4);

    del("*:*");
    commit();

  }


  private void testSpacesInParams() throws Exception {

    String zkHost = zkServer.getZkAddress();
    streamFactory.withCollectionZkHost("collection1", zkHost);

    Map params = mapParams("q", "*:*", "fl", "id , a_s , a_i , a_f", "sort", "a_f  asc , a_i  asc");

    //CloudSolrStream compares the values of the sort with the fl field.
    //The constructor will throw an exception if the sort fields do not the
    //a value in the field list.

    CloudSolrStream stream = new CloudSolrStream(zkHost, "collection1", params);

    del("*:*");
    commit();

  }

  private void testNonePartitionKeys() throws Exception {

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
    streamFactory.withCollectionZkHost("collection1", zkHost);

    Map paramsA = mapParams("q", "*:*", "fl", "id,a_s,a_i,a_f", "sort", "a_s asc,a_f asc", "partitionKeys", "none");
    CloudSolrStream stream = new CloudSolrStream(zkHost, "collection1", paramsA);
    ParallelStream pstream = new ParallelStream(zkHost, "collection1", stream, 2, new FieldComparator("a_s",ComparatorOrder.ASCENDING));

    attachStreamFactory(pstream);
    List<Tuple> tuples = getTuples(pstream);

    assert(tuples.size() == 20); // Each tuple will be double counted.

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
    streamFactory.withCollectionZkHost("collection1", zkHost);

    Map params = mapParams("q","*:*","fl","id,a_s,a_i,a_f","sort", "a_f asc,a_i asc", "partitionKeys", "a_f");
    CloudSolrStream stream = new CloudSolrStream(zkHost, "collection1", params);
    UniqueStream ustream = new UniqueStream(stream, new FieldEqualitor("a_f"));
    ParallelStream pstream = new ParallelStream(zkHost, "collection1", ustream, 2, new FieldComparator("a_f",ComparatorOrder.ASCENDING));
    attachStreamFactory(pstream);
    List<Tuple> tuples = getTuples(pstream);
    assert(tuples.size() == 5);
    assertOrder(tuples, 0,1,3,4,6);

    //Test the eofTuples

    Map<String,Tuple> eofTuples = pstream.getEofTuples();
    assert(eofTuples.size() == 2); //There should be an EOF tuple for each worker.

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
    streamFactory.withCollectionZkHost("collection1", zkHost);

    Map params = mapParams("q", "*:*", "fl", "id,a_s,a_i", "sort", "a_i asc");
    CloudSolrStream stream = new CloudSolrStream(zkHost, "collection1", params);
    RankStream rstream = new RankStream(stream, 3, new FieldComparator("a_i",ComparatorOrder.DESCENDING));
    List<Tuple> tuples = getTuples(rstream);


    assert(tuples.size() == 3);
    assertOrder(tuples, 4,3,2);

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
    streamFactory.withCollectionZkHost("collection1", zkHost);

    Map params = mapParams("q", "*:*", "fl", "id,a_s,a_i", "sort", "a_i asc", "partitionKeys", "a_i");
    CloudSolrStream stream = new CloudSolrStream(zkHost, "collection1", params);
    RankStream rstream = new RankStream(stream, 11, new FieldComparator("a_i",ComparatorOrder.DESCENDING));
    ParallelStream pstream = new ParallelStream(zkHost, "collection1", rstream, 2, new FieldComparator("a_i",ComparatorOrder.DESCENDING));
    attachStreamFactory(pstream);
    List<Tuple> tuples = getTuples(pstream);

    assert(tuples.size() == 10);
    assertOrder(tuples, 10,9,8,7,6,5,4,3,2,0);

    del("*:*");
    commit();
  }

  private void testTrace() throws Exception {

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
    streamFactory.withCollectionZkHost("collection1", zkHost);

    //Test with spaces in the parameter lists.
    Map paramsA = mapParams("q","*:*","fl","id,a_s, a_i,  a_f","sort", "a_s asc  ,  a_f   asc");
    CloudSolrStream stream = new CloudSolrStream(zkHost, "collection1", paramsA);
    stream.setTrace(true);
    List<Tuple> tuples = getTuples(stream);
    assert(tuples.get(0).get("_COLLECTION_").equals("collection1"));
    assert(tuples.get(1).get("_COLLECTION_").equals("collection1"));
    assert(tuples.get(2).get("_COLLECTION_").equals("collection1"));
    assert(tuples.get(3).get("_COLLECTION_").equals("collection1"));

    del("*:*");
    commit();
  }




  private void testReducerStream() throws Exception {

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
    streamFactory.withCollectionZkHost("collection1", zkHost);

    //Test with spaces in the parameter lists.
    Map paramsA = mapParams("q","*:*","fl","id,a_s, a_i,  a_f","sort", "a_s asc  ,  a_f   asc");
    CloudSolrStream stream = new CloudSolrStream(zkHost, "collection1", paramsA);
    ReducerStream rstream  = new ReducerStream(stream,
                                               new FieldEqualitor("a_s"),
                                               new GroupOperation(new FieldComparator("a_f", ComparatorOrder.ASCENDING), 5));

    List<Tuple> tuples = getTuples(rstream);

    assert(tuples.size() == 3);

    Tuple t0 = tuples.get(0);
    List<Map> maps0 = t0.getMaps("group");
    assertMaps(maps0, 0, 2, 1, 9);

    Tuple t1 = tuples.get(1);
    List<Map> maps1 = t1.getMaps("group");
    assertMaps(maps1, 3, 5, 7, 8);

    Tuple t2 = tuples.get(2);
    List<Map> maps2 = t2.getMaps("group");
    assertMaps(maps2, 4, 6);

    //Test with spaces in the parameter lists using a comparator
    paramsA = mapParams("q","*:*","fl","id,a_s, a_i,  a_f","sort", "a_s asc  ,  a_f   asc");
    stream  = new CloudSolrStream(zkHost, "collection1", paramsA);
    rstream = new ReducerStream(stream,
                                new FieldComparator("a_s", ComparatorOrder.ASCENDING),
                                new GroupOperation(new FieldComparator("a_f", ComparatorOrder.DESCENDING), 5));

    tuples = getTuples(rstream);

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

    del("*:*");
    commit();
  }

  private void testZeroReducerStream() throws Exception {

    //Gracefully handle zero results
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
    streamFactory.withCollectionZkHost("collection1", zkHost);

    //Test with spaces in the parameter lists.
    Map paramsA = mapParams("q", "blah", "fl", "id,a_s, a_i,  a_f", "sort", "a_s asc  ,  a_f   asc");
    CloudSolrStream stream = new CloudSolrStream(zkHost, "collection1", paramsA);
    ReducerStream rstream = new ReducerStream(stream,
                                              new FieldEqualitor("a_s"),
                                              new GroupOperation(new FieldComparator("a_f", ComparatorOrder.ASCENDING), 5));

    List<Tuple> tuples = getTuples(rstream);

    assert(tuples.size() == 0);

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
    streamFactory.withCollectionZkHost("collection1", zkHost);

    Map paramsA = mapParams("q","*:*","fl","id,a_s,a_i,a_f","sort", "a_s asc,a_f asc", "partitionKeys", "a_s");
    CloudSolrStream stream = new CloudSolrStream(zkHost, "collection1", paramsA);

    ReducerStream rstream = new ReducerStream(stream,
                                              new FieldEqualitor("a_s"),
                                              new GroupOperation(new FieldComparator("a_f", ComparatorOrder.DESCENDING), 5));

    ParallelStream pstream = new ParallelStream(zkHost, "collection1", rstream, 2, new FieldComparator("a_s",ComparatorOrder.ASCENDING));

    attachStreamFactory(pstream);
    List<Tuple> tuples = getTuples(pstream);

    assert(tuples.size() == 3);

    Tuple t0 = tuples.get(0);
    List<Map> maps0 = t0.getMaps("group");
    assertMaps(maps0, 9, 1, 2, 0);

    Tuple t1 = tuples.get(1);
    List<Map> maps1 = t1.getMaps("group");
    assertMaps(maps1, 8, 7, 5, 3);

    Tuple t2 = tuples.get(2);
    List<Map> maps2 = t2.getMaps("group");
    assertMaps(maps2, 6, 4);

    //Test Descending with Ascending subsort

    paramsA = mapParams("q","*:*","fl","id,a_s,a_i,a_f","sort", "a_s desc,a_f asc", "partitionKeys", "a_s");
    stream = new CloudSolrStream(zkHost, "collection1", paramsA);

    rstream = new ReducerStream(stream,
                                new FieldEqualitor("a_s"),
                                new GroupOperation(new FieldComparator("a_f", ComparatorOrder.ASCENDING), 3));

    pstream = new ParallelStream(zkHost, "collection1", rstream, 2, new FieldComparator("a_s",ComparatorOrder.DESCENDING));

    attachStreamFactory(pstream);
    tuples = getTuples(pstream);

    assert(tuples.size() == 3);

    t0 = tuples.get(0);
    maps0 = t0.getMaps("group");
    assertMaps(maps0, 4, 6);

    t1 = tuples.get(1);
    maps1 = t1.getMaps("group");
    assertMaps(maps1, 3, 5, 7);

    t2 = tuples.get(2);
    maps2 = t2.getMaps("group");
    assertMaps(maps2, 0, 2, 1);



    del("*:*");
    commit();
  }


  private void testExceptionStream() throws Exception {

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


    //Test an error that comes originates from the /select handler
    Map paramsA = mapParams("q", "*:*", "fl", "a_s,a_i,a_f,blah", "sort", "blah asc");
    CloudSolrStream stream = new CloudSolrStream(zkHost, "collection1", paramsA);
    ExceptionStream estream = new ExceptionStream(stream);
    Tuple t = getTuple(estream);
    assert(t.EOF);
    assert(t.EXCEPTION);
    assert(t.getException().contains("sort param field can't be found: blah"));

    //Test an error that comes originates from the /export handler
    paramsA = mapParams("q", "*:*", "fl", "a_s,a_i,a_f,score", "sort", "a_s asc", "qt","/export");
    stream = new CloudSolrStream(zkHost, "collection1", paramsA);
    estream = new ExceptionStream(stream);
    t = getTuple(estream);
    assert(t.EOF);
    assert(t.EXCEPTION);
    //The /export handler will pass through a real exception.
    assert(t.getException().contains("undefined field:"));
  }

  private void testParallelExceptionStream() throws Exception {

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

    Map paramsA = mapParams("q", "*:*", "fl", "a_s,a_i,a_f,blah", "sort", "blah asc");
    CloudSolrStream stream = new CloudSolrStream(zkHost, "collection1", paramsA);
    ParallelStream pstream = new ParallelStream(zkHost,"collection1", stream, 2, new FieldComparator("blah", ComparatorOrder.ASCENDING));
    ExceptionStream estream = new ExceptionStream(pstream);
    Tuple t = getTuple(estream);
    assert(t.EOF);
    assert(t.EXCEPTION);
    //ParallelStream requires that partitionKeys be set.
    assert(t.getException().contains("When numWorkers > 1 partitionKeys must be set."));


    //Test an error that originates from the /select handler
    paramsA = mapParams("q", "*:*", "fl", "a_s,a_i,a_f,blah", "sort", "blah asc", "partitionKeys","a_s");
    stream = new CloudSolrStream(zkHost, "collection1", paramsA);
    pstream = new ParallelStream(zkHost,"collection1", stream, 2, new FieldComparator("blah", ComparatorOrder.ASCENDING));
    estream = new ExceptionStream(pstream);
    t = getTuple(estream);
    assert(t.EOF);
    assert(t.EXCEPTION);
    assert(t.getException().contains("sort param field can't be found: blah"));


    //Test an error that originates from the /export handler
    paramsA = mapParams("q", "*:*", "fl", "a_s,a_i,a_f,score", "sort", "a_s asc", "qt","/export", "partitionKeys","a_s");
    stream = new CloudSolrStream(zkHost, "collection1", paramsA);
    pstream = new ParallelStream(zkHost,"collection1", stream, 2, new FieldComparator("a_s", ComparatorOrder.ASCENDING));
    estream = new ExceptionStream(pstream);
    t = getTuple(estream);
    assert(t.EOF);
    assert(t.EXCEPTION);
    //The /export handler will pass through a real exception.
    assert(t.getException().contains("undefined field:"));
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

    String zkHost = zkServer.getZkAddress();

    Map paramsA = mapParams("q", "*:*");

    Metric[] metrics = {new SumMetric("a_i"),
                        new SumMetric("a_f"),
                        new MinMetric("a_i"),
                        new MinMetric("a_f"),
                        new MaxMetric("a_i"),
                        new MaxMetric("a_f"),
                        new MeanMetric("a_i"),
                        new MeanMetric("a_f"),
                        new CountMetric()};

    StatsStream statsStream = new StatsStream(zkHost,
                                              "collection1",
                                              paramsA,
                                              metrics);

    List<Tuple> tuples = getTuples(statsStream);

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

  private void testFacetStream() throws Exception {

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

    Map paramsA = mapParams("q", "*:*", "fl", "a_s,a_i,a_f", "sort", "a_s asc");

    Bucket[] buckets =  {new Bucket("a_s")};

    Metric[] metrics = {new SumMetric("a_i"),
                        new SumMetric("a_f"),
                        new MinMetric("a_i"),
                        new MinMetric("a_f"),
                        new MaxMetric("a_i"),
                        new MaxMetric("a_f"),
                        new MeanMetric("a_i"),
                        new MeanMetric("a_f"),
                        new CountMetric()};

    FieldComparator[] sorts = {new FieldComparator("sum(a_i)",
                                                   ComparatorOrder.ASCENDING)};

    FacetStream facetStream = new FacetStream(zkHost,
                                              "collection1",
                                              paramsA,
                                              buckets,
                                              metrics,
                                              sorts,
                                              100);

    List<Tuple> tuples = getTuples(facetStream);

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

    sorts[0] = new FieldComparator("sum(a_i)", ComparatorOrder.DESCENDING);

    facetStream = new FacetStream(zkHost,
                                  "collection1",
                                  paramsA,
                                  buckets,
                                  metrics,
                                  sorts,
                                  100);

    tuples = getTuples(facetStream);

    assert(tuples.size() == 3);

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

    sorts[0] = new FieldComparator("a_s", ComparatorOrder.DESCENDING);


    facetStream = new FacetStream(zkHost,
                                  "collection1",
                                  paramsA,
                                  buckets,
                                  metrics,
                                  sorts,
                                  100);

    tuples = getTuples(facetStream);

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

    sorts[0] = new FieldComparator("a_s", ComparatorOrder.ASCENDING);

    facetStream = new FacetStream(zkHost,
                                  "collection1",
                                  paramsA,
                                  buckets,
                                  metrics,
                                  sorts,
                                  100);

    tuples = getTuples(facetStream);

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

    del("*:*");
    commit();
  }


  private void testSubFacetStream() throws Exception {

    indexr(id, "0", "level1_s", "hello0", "level2_s", "a", "a_i", "0", "a_f", "1");
    indexr(id, "2", "level1_s", "hello0", "level2_s", "a", "a_i", "2", "a_f", "2");
    indexr(id, "3", "level1_s", "hello3", "level2_s", "a", "a_i", "3", "a_f", "3");
    indexr(id, "4", "level1_s", "hello4", "level2_s", "a", "a_i", "4", "a_f", "4");
    indexr(id, "1", "level1_s", "hello0", "level2_s", "b", "a_i", "1", "a_f", "5");
    indexr(id, "5", "level1_s", "hello3", "level2_s", "b", "a_i", "10", "a_f", "6");
    indexr(id, "6", "level1_s", "hello4", "level2_s", "b", "a_i", "11", "a_f", "7");
    indexr(id, "7", "level1_s", "hello3", "level2_s", "b", "a_i", "12", "a_f", "8");
    indexr(id, "8", "level1_s", "hello3", "level2_s", "b", "a_i", "13", "a_f", "9");
    indexr(id, "9", "level1_s", "hello0", "level2_s", "b", "a_i", "14", "a_f", "10");

    commit();

    String zkHost = zkServer.getZkAddress();

    Map paramsA = mapParams("q","*:*","fl","a_i,a_f");

    Bucket[] buckets =  {new Bucket("level1_s"), new Bucket("level2_s")};

    Metric[] metrics = {new SumMetric("a_i"),
                        new CountMetric()};

    FieldComparator[] sorts = {new FieldComparator("sum(a_i)", ComparatorOrder.DESCENDING), new FieldComparator("sum(a_i)", ComparatorOrder.DESCENDING)};


    FacetStream facetStream = new FacetStream(
        zkHost,
        "collection1",
        paramsA,
        buckets,
        metrics,
        sorts,
        100);

    List<Tuple> tuples = getTuples(facetStream);
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

    sorts[0] =  new FieldComparator("level1_s", ComparatorOrder.DESCENDING );
    sorts[1] =  new FieldComparator("level2_s", ComparatorOrder.DESCENDING );
    facetStream = new FacetStream(
        zkHost,
        "collection1",
        paramsA,
        buckets,
        metrics,
        sorts,
        100);

    tuples = getTuples(facetStream);
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

    String zkHost = zkServer.getZkAddress();

    Map paramsA = mapParams("q","*:*","fl","a_s,a_i,a_f","sort", "a_s asc");
    CloudSolrStream stream = new CloudSolrStream(zkHost, "collection1", paramsA);

    Bucket[] buckets =  {new Bucket("a_s")};

    Metric[] metrics = {new SumMetric("a_i"),
                        new SumMetric("a_f"),
                        new MinMetric("a_i"),
                        new MinMetric("a_f"),
                        new MaxMetric("a_i"),
                        new MaxMetric("a_f"),
                        new MeanMetric("a_i"),
                        new MeanMetric("a_f"),
                        new CountMetric()};

    RollupStream rollupStream = new RollupStream(stream, buckets, metrics);
    List<Tuple> tuples = getTuples(rollupStream);

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

    String zkHost = zkServer.getZkAddress();
    streamFactory.withCollectionZkHost("collection1", zkHost);

    Map paramsA = mapParams("q", "*:*", "fl", "a_s,a_i,a_f", "sort", "a_s asc", "partitionKeys", "a_s");
    CloudSolrStream stream = new CloudSolrStream(zkHost, "collection1", paramsA);

    Bucket[] buckets =  {new Bucket("a_s")};

    Metric[] metrics = {new SumMetric("a_i"),
                        new SumMetric("a_f"),
                        new MinMetric("a_i"),
                        new MinMetric("a_f"),
                        new MaxMetric("a_i"),
                        new MaxMetric("a_f"),
                        new MeanMetric("a_i"),
                        new MeanMetric("a_f"),
                        new CountMetric()};

    RollupStream rollupStream = new RollupStream(stream, buckets, metrics);
    ParallelStream parallelStream = new ParallelStream(zkHost, "collection1", rollupStream, 2, new FieldComparator("a_s", ComparatorOrder.ASCENDING));
    attachStreamFactory(parallelStream);
    List<Tuple> tuples = getTuples(parallelStream);

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

  private void testZeroParallelReducerStream() throws Exception {

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
    streamFactory.withCollectionZkHost("collection1", zkHost);

    Map paramsA = mapParams("q","blah","fl","id,a_s,a_i,a_f","sort", "a_s asc,a_f asc", "partitionKeys", "a_s");
    CloudSolrStream stream = new CloudSolrStream(zkHost, "collection1", paramsA);
    ReducerStream rstream = new ReducerStream(stream,
                                              new FieldEqualitor("a_s"),
                                              new GroupOperation(new FieldComparator("a_s", ComparatorOrder.ASCENDING), 2));

    ParallelStream pstream = new ParallelStream(zkHost, "collection1", rstream, 2, new FieldComparator("a_s", ComparatorOrder.ASCENDING));

    attachStreamFactory(pstream);
    List<Tuple> tuples = getTuples(pstream);
    assert(tuples.size() == 0);
    del("*:*");
    commit();
  }


  private void testTuple() throws Exception {

    indexr(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "5.1", "s_multi", "a", "s_multi", "b", "i_multi", "1", "i_multi", "2", "f_multi", "1.2", "f_multi", "1.3");

    commit();

    String zkHost = zkServer.getZkAddress();
    streamFactory.withCollectionZkHost("collection1", zkHost);

    Map params = mapParams("q","*:*","fl","id,a_s,a_i,a_f,s_multi,i_multi,f_multi","sort", "a_s asc");
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
    streamFactory.withCollectionZkHost("collection1", zkHost);

    //Test ascending
    Map paramsA = mapParams("q","id:(4 1)","fl","id,a_s,a_i","sort", "a_i asc");
    CloudSolrStream streamA = new CloudSolrStream(zkHost, "collection1", paramsA);

    Map paramsB = mapParams("q","id:(0 2 3)","fl","id,a_s,a_i","sort", "a_i asc");
    CloudSolrStream streamB = new CloudSolrStream(zkHost, "collection1", paramsB);

    MergeStream mstream = new MergeStream(streamA, streamB, new FieldComparator("a_i",ComparatorOrder.ASCENDING));
    List<Tuple> tuples = getTuples(mstream);

    assert(tuples.size() == 5);
    assertOrder(tuples, 0,1,2,3,4);

    //Test descending
    paramsA = mapParams("q","id:(4 1)","fl","id,a_s,a_i","sort", "a_i desc");
    streamA = new CloudSolrStream(zkHost, "collection1", paramsA);

    paramsB = mapParams("q","id:(0 2 3)","fl","id,a_s,a_i","sort", "a_i desc");
    streamB = new CloudSolrStream(zkHost, "collection1", paramsB);

    mstream = new MergeStream(streamA, streamB, new FieldComparator("a_i",ComparatorOrder.DESCENDING));
    tuples = getTuples(mstream);

    assert(tuples.size() == 5);
    assertOrder(tuples, 4,3,2,1,0);

    //Test compound sort

    paramsA = mapParams("q","id:(2 4 1)","fl","id,a_s,a_i,a_f","sort", "a_f asc,a_i asc");
    streamA = new CloudSolrStream(zkHost, "collection1", paramsA);

    paramsB = mapParams("q","id:(0 3)","fl","id,a_s,a_i,a_f","sort", "a_f asc,a_i asc");
    streamB = new CloudSolrStream(zkHost, "collection1", paramsB);

    mstream = new MergeStream(streamA, streamB, new MultipleFieldComparator(new FieldComparator("a_f",ComparatorOrder.ASCENDING),new FieldComparator("a_i",ComparatorOrder.ASCENDING)));
    tuples = getTuples(mstream);

    assert(tuples.size() == 5);
    assertOrder(tuples, 0,2,1,3,4);

    paramsA = mapParams("q","id:(2 4 1)","fl","id,a_s,a_i,a_f","sort", "a_f asc,a_i desc");
    streamA = new CloudSolrStream(zkHost, "collection1", paramsA);

    paramsB = mapParams("q","id:(0 3)","fl","id,a_s,a_i,a_f","sort", "a_f asc,a_i desc");
    streamB = new CloudSolrStream(zkHost, "collection1", paramsB);

    mstream = new MergeStream(streamA, streamB, new MultipleFieldComparator(new FieldComparator("a_f",ComparatorOrder.ASCENDING),new FieldComparator("a_i",ComparatorOrder.DESCENDING)));
    tuples = getTuples(mstream);

    assert(tuples.size() == 5);
    assertOrder(tuples, 2,0,1,3,4);

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
    streamFactory.withCollectionZkHost("collection1", zkHost);

    //Test ascending
    Map paramsA = mapParams("q","id:(4 1 8 7 9)","fl","id,a_s,a_i","sort", "a_i asc", "partitionKeys", "a_i");
    CloudSolrStream streamA = new CloudSolrStream(zkHost, "collection1", paramsA);

    Map paramsB = mapParams("q","id:(0 2 3 6)","fl","id,a_s,a_i","sort", "a_i asc", "partitionKeys", "a_i");
    CloudSolrStream streamB = new CloudSolrStream(zkHost, "collection1", paramsB);

    MergeStream mstream = new MergeStream(streamA, streamB, new FieldComparator("a_i",ComparatorOrder.ASCENDING));
    ParallelStream pstream = new ParallelStream(zkHost, "collection1", mstream, 2, new FieldComparator("a_i",ComparatorOrder.ASCENDING));
    attachStreamFactory(pstream);
    List<Tuple> tuples = getTuples(pstream);

    assert(tuples.size() == 9);
    assertOrder(tuples, 0,1,2,3,4,7,6,8,9);

    //Test descending
    paramsA = mapParams("q", "id:(4 1 8 9)", "fl", "id,a_s,a_i", "sort", "a_i desc", "partitionKeys", "a_i");
    streamA = new CloudSolrStream(zkHost, "collection1", paramsA);

    paramsB = mapParams("q","id:(0 2 3 6)","fl","id,a_s,a_i","sort", "a_i desc", "partitionKeys", "a_i");
    streamB = new CloudSolrStream(zkHost, "collection1", paramsB);

    mstream = new MergeStream(streamA, streamB, new FieldComparator("a_i",ComparatorOrder.DESCENDING));
    pstream = new ParallelStream(zkHost, "collection1", mstream, 2, new FieldComparator("a_i",ComparatorOrder.DESCENDING));
    attachStreamFactory(pstream);
    tuples = getTuples(pstream);

    assert(tuples.size() == 8);
    assertOrder(tuples, 9,8,6,4,3,2,1,0);

    del("*:*");
    commit();
  }

  private void testParallelEOF() throws Exception {

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
    streamFactory.withCollectionZkHost("collection1", zkHost);

    //Test ascending
    Map paramsA = mapParams("q","id:(4 1 8 7 9)","fl","id,a_s,a_i","sort", "a_i asc", "partitionKeys", "a_i");
    CloudSolrStream streamA = new CloudSolrStream(zkHost, "collection1", paramsA);

    Map paramsB = mapParams("q","id:(0 2 3 6)","fl","id,a_s,a_i","sort", "a_i asc", "partitionKeys", "a_i");
    CloudSolrStream streamB = new CloudSolrStream(zkHost, "collection1", paramsB);

    MergeStream mstream = new MergeStream(streamA, streamB, new FieldComparator("a_i",ComparatorOrder.ASCENDING));
    ParallelStream pstream = new ParallelStream(zkHost, "collection1", mstream, 2, new FieldComparator("a_i",ComparatorOrder.ASCENDING));
    
    attachStreamFactory(pstream);
    List<Tuple> tuples = getTuples(pstream);

    assert(tuples.size() == 9);
    Map<String, Tuple> eofTuples = pstream.getEofTuples();
    assert(eofTuples.size() == 2); // There should be an EOF Tuple for each worker.

    del("*:*");
    commit();
  }



  @Test
  public void streamTests() throws Exception {
    assertNotNull(cloudClient);

    handle.clear();
    handle.put("timestamp", SKIPVAL);

    waitForRecoveriesToFinish(false);

    del("*:*");

    commit();

    indexr(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "0");
    indexr(id, "2", "a_s", "hello2", "a_i", "2", "a_f", "0");
    indexr(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3");
    indexr(id, "4", "a_s", "hello4", "a_i", "4", "a_f", "4");
    indexr(id, "1", "a_s", "hello1", "a_i", "1", "a_f", "1");

    commit();

    String zkHost = zkServer.getZkAddress();
    streamFactory.withCollectionZkHost("collection1", zkHost);
    Map params = null;

    //Basic CloudSolrStream Test with Descending Sort

    params = mapParams("q","*:*","fl","id,a_s,a_i","sort", "a_i desc");
    CloudSolrStream stream = new CloudSolrStream(zkHost, "collection1", params);
    List<Tuple> tuples = getTuples(stream);

    assert(tuples.size() == 5);
    assertOrder(tuples, 4, 3, 2, 1, 0);

    //With Ascending Sort
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

    assert (tuples.size() == 5);
    assertOrder(tuples, 0, 2, 1, 3, 4);

    del("*:*");
    commit();

    testTuple();
    testSpacesInParams();
    testNonePartitionKeys();
    testTrace();
    testUniqueStream();
    testRankStream();
    testMergeStream();
    testReducerStream();
    testRollupStream();
    testZeroReducerStream();
    testFacetStream();
    testSubFacetStream();
    testStatsStream();
    //testExceptionStream();
    testParallelEOF();
    testParallelUniqueStream();
    testParallelRankStream();
    testParallelMergeStream();
    testParallelRollupStream();
    testParallelReducerStream();
    //testParallelExceptionStream();
    testZeroParallelReducerStream();
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
    for(;;) {
      Tuple t = tupleStream.read();
      if(t.EOF) {
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

  public boolean assertLong(Tuple tuple, String fieldName, long l) throws Exception {
    long lv = (long)tuple.get(fieldName);
    if(lv != l) {
      throw new Exception("Longs not equal:"+l+" : "+lv);
    }

    return true;
  }

  @Override
  protected void indexr(Object... fields) throws Exception {
    SolrInputDocument doc = getDoc(fields);
    indexDoc(doc);
  }
  
  private void attachStreamFactory(TupleStream tupleStream) {
    StreamContext streamContext = new StreamContext();
    streamContext.setStreamFactory(streamFactory);
    tupleStream.setStreamContext(streamContext);
  }
}
