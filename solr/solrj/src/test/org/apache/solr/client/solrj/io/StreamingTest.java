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
import java.io.Serializable;
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


  private void testSpacesInParams() throws Exception {

    String zkHost = zkServer.getZkAddress();

    Map params = mapParams("q","*:*","fl","id , a_s , a_i , a_f","sort", "a_f  asc , a_i  asc");

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

    Map paramsA = mapParams("q","*:*","fl","id,a_s,a_i,a_f","sort", "a_s asc,a_f asc", "partitionKeys", "none");
    CloudSolrStream stream = new CloudSolrStream(zkHost, "collection1", paramsA);
    ParallelStream pstream = new ParallelStream(zkHost, "collection1", stream, 2, new AscFieldComp("a_s"));

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

    Map params = mapParams("q","*:*","fl","id,a_s,a_i,a_f","sort", "a_f asc,a_i asc", "partitionKeys", "a_f");
    CloudSolrStream stream = new CloudSolrStream(zkHost, "collection1", params);
    UniqueStream ustream = new UniqueStream(stream, new AscFieldComp("a_f"));
    ParallelStream pstream = new ParallelStream(zkHost, "collection1", ustream, 2, new AscFieldComp("a_f"));
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

    Map params = mapParams("q","*:*","fl","id,a_s,a_i","sort", "a_i asc");
    CloudSolrStream stream = new CloudSolrStream(zkHost, "collection1", params);
    RankStream rstream = new RankStream(stream, 3, new DescFieldComp("a_i"));
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

    Map params = mapParams("q","*:*","fl","id,a_s,a_i","sort", "a_i asc", "partitionKeys", "a_i");
    CloudSolrStream stream = new CloudSolrStream(zkHost, "collection1", params);
    RankStream rstream = new RankStream(stream, 11, new DescFieldComp("a_i"));
    ParallelStream pstream = new ParallelStream(zkHost, "collection1", rstream, 2, new DescFieldComp("a_i"));
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

    //Test with spaces in the parameter lists.
    Map paramsA = mapParams("q","*:*","fl","id,a_s, a_i,  a_f","sort", "a_s asc  ,  a_f   asc");
    CloudSolrStream stream = new CloudSolrStream(zkHost, "collection1", paramsA);
    ReducerStream rstream = new ReducerStream(stream, new AscFieldComp("a_s"));

    List<Tuple> tuples = getTuples(rstream);

    assert(tuples.size() == 3);
    assertOrder(tuples, 0,3,4);

    Tuple t0 = tuples.get(0);
    List<Map> maps0 = t0.getMaps();
    assertMaps(maps0, 0, 2,1, 9);

    Tuple t1 = tuples.get(1);
    List<Map> maps1 = t1.getMaps();
    assertMaps(maps1, 3, 5, 7, 8);

    Tuple t2 = tuples.get(2);
    List<Map> maps2 = t2.getMaps();
    assertMaps(maps2, 4, 6);



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

    Map paramsA = mapParams("q","*:*","fl","id,a_s,a_i,a_f","sort", "a_s asc,a_f asc", "partitionKeys", "a_s");
    CloudSolrStream stream = new CloudSolrStream(zkHost, "collection1", paramsA);
    ReducerStream rstream = new ReducerStream(stream, new AscFieldComp("a_s"));
    ParallelStream pstream = new ParallelStream(zkHost, "collection1", rstream, 2, new AscFieldComp("a_s"));

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

    paramsA = mapParams("q","*:*","fl","id,a_s,a_i,a_f","sort", "a_s desc,a_f asc", "partitionKeys", "a_s");
    stream = new CloudSolrStream(zkHost, "collection1", paramsA);
    rstream = new ReducerStream(stream, new DescFieldComp("a_s"));
    pstream = new ParallelStream(zkHost, "collection1", rstream, 2, new DescFieldComp("a_s"));

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

  private void testTuple() throws Exception {

    indexr(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "5.1", "s_multi", "a", "s_multi", "b", "i_multi", "1", "i_multi", "2", "f_multi", "1.2", "f_multi", "1.3");

    commit();

    String zkHost = zkServer.getZkAddress();

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

    //Test ascending
    Map paramsA = mapParams("q","id:(4 1 8 7 9)","fl","id,a_s,a_i","sort", "a_i asc", "partitionKeys", "a_i");
    CloudSolrStream streamA = new CloudSolrStream(zkHost, "collection1", paramsA);

    Map paramsB = mapParams("q","id:(0 2 3 6)","fl","id,a_s,a_i","sort", "a_i asc", "partitionKeys", "a_i");
    CloudSolrStream streamB = new CloudSolrStream(zkHost, "collection1", paramsB);

    MergeStream mstream = new MergeStream(streamA, streamB, new AscFieldComp("a_i"));
    ParallelStream pstream = new ParallelStream(zkHost, "collection1", mstream, 2, new AscFieldComp("a_i"));
    List<Tuple> tuples = getTuples(pstream);

    assert(tuples.size() == 9);
    assertOrder(tuples, 0,1,2,3,4,7,6,8,9);

    //Test descending
    paramsA = mapParams("q","id:(4 1 8 9)","fl","id,a_s,a_i","sort", "a_i desc", "partitionKeys", "a_i");
    streamA = new CloudSolrStream(zkHost, "collection1", paramsA);

    paramsB = mapParams("q","id:(0 2 3 6)","fl","id,a_s,a_i","sort", "a_i desc", "partitionKeys", "a_i");
    streamB = new CloudSolrStream(zkHost, "collection1", paramsB);

    mstream = new MergeStream(streamA, streamB, new DescFieldComp("a_i"));
    pstream = new ParallelStream(zkHost, "collection1", mstream, 2, new DescFieldComp("a_i"));
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

    //Test ascending
    Map paramsA = mapParams("q","id:(4 1 8 7 9)","fl","id,a_s,a_i","sort", "a_i asc", "partitionKeys", "a_i");
    CloudSolrStream streamA = new CloudSolrStream(zkHost, "collection1", paramsA);

    Map paramsB = mapParams("q","id:(0 2 3 6)","fl","id,a_s,a_i","sort", "a_i asc", "partitionKeys", "a_i");
    CloudSolrStream streamB = new CloudSolrStream(zkHost, "collection1", paramsB);

    MergeStream mstream = new MergeStream(streamA, streamB, new AscFieldComp("a_i"));
    CountStream cstream = new CountStream(mstream);
    ParallelStream pstream = new ParallelStream(zkHost, "collection1", cstream, 2, new AscFieldComp("a_i"));
    List<Tuple> tuples = getTuples(pstream);

    assert(tuples.size() == 9);
    Map<String, Tuple> eofTuples = pstream.getEofTuples();
    assert(eofTuples.size() == 2); // There should be an EOF Tuple for each worker.

    long totalCount = 0;

    Iterator<Tuple> it = eofTuples.values().iterator();
    while(it.hasNext()) {
      Tuple t = it.next();
      totalCount += t.getLong("count");
    }

    assert(tuples.size() == totalCount);

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

    assert(tuples.size() == 5);
    assertOrder(tuples, 0,2,1,3,4);

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
    testParallelEOF();
    testParallelUniqueStream();
    testParallelRankStream();
    testParallelMergeStream();
    testParallelReducerStream();
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
}
