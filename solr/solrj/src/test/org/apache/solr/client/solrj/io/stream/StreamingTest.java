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
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.ComparatorOrder;
import org.apache.solr.client.solrj.io.comp.FieldComparator;
import org.apache.solr.client.solrj.io.comp.MultipleFieldComparator;
import org.apache.solr.client.solrj.io.eq.FieldEqualitor;
import org.apache.solr.client.solrj.io.ops.GroupOperation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParser;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.client.solrj.io.stream.metrics.Bucket;
import org.apache.solr.client.solrj.io.stream.metrics.CountMetric;
import org.apache.solr.client.solrj.io.stream.metrics.MaxMetric;
import org.apache.solr.client.solrj.io.stream.metrics.MeanMetric;
import org.apache.solr.client.solrj.io.stream.metrics.Metric;
import org.apache.solr.client.solrj.io.stream.metrics.MinMetric;
import org.apache.solr.client.solrj.io.stream.metrics.SumMetric;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.routing.RequestReplicaListTransformerGenerator;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
*  All base tests will be done with CloudSolrStream. Under the covers CloudSolrStream uses SolrStream so
*  SolrStream will get fully exercised through these tests.
*
**/

@SolrTestCaseJ4.SuppressSSL
@LuceneTestCase.SuppressCodecs({"Lucene3x", "Lucene40","Lucene41","Lucene42","Lucene45"})
public class StreamingTest extends SolrCloudTestCase {

public static final String COLLECTIONORALIAS = "streams";
public static final String MULTI_REPLICA_COLLECTIONORALIAS = "streams-multi-replica";

private static final StreamFactory streamFactory = new StreamFactory()
    .withFunctionName("search", CloudSolrStream.class)
    .withFunctionName("merge", MergeStream.class)
    .withFunctionName("unique", UniqueStream.class)
    .withFunctionName("top", RankStream.class)
    .withFunctionName("reduce", ReducerStream.class)
    .withFunctionName("group", GroupOperation.class)
    .withFunctionName("rollup", RollupStream.class)
    .withFunctionName("parallel", ParallelStream.class);

private static String zkHost;

private static int numShards;
private static int numWorkers;
private static boolean useAlias;

@BeforeClass
public static void configureCluster() throws Exception {
  numShards = random().nextInt(2) + 1;  //1 - 3
  numWorkers = numShards > 2 ? random().nextInt(numShards - 1) + 1 : numShards;
  configureCluster(numShards)
      .addConfig("conf", getFile("solrj").toPath().resolve("solr").resolve("configsets").resolve("streaming").resolve("conf"))
      .configure();

  String collection;
  useAlias = random().nextBoolean();
  if (useAlias) {
    collection = COLLECTIONORALIAS + "_collection";
  } else {
    collection = COLLECTIONORALIAS;
  }
  CollectionAdminRequest.createCollection(collection, "conf", numShards, 1)
      .process(cluster.getSolrClient());
  cluster.waitForActiveCollection(collection, numShards, numShards);
  if (useAlias) {
    CollectionAdminRequest.createAlias(COLLECTIONORALIAS, collection).process(cluster.getSolrClient());
  }

  zkHost = cluster.getZkServer().getZkAddress();
  streamFactory.withCollectionZkHost(COLLECTIONORALIAS, zkHost);

  // Set up multi-replica collection
  if (useAlias) {
    collection = MULTI_REPLICA_COLLECTIONORALIAS + "_collection";
  } else {
    collection = MULTI_REPLICA_COLLECTIONORALIAS;
  }
  CollectionAdminRequest.createCollection(collection, "conf", numShards, 1, 1, 1)
      .setMaxShardsPerNode(numShards * 3)
      .process(cluster.getSolrClient());
  cluster.waitForActiveCollection(collection, numShards, numShards * 3);
  if (useAlias) {
    CollectionAdminRequest.createAlias(MULTI_REPLICA_COLLECTIONORALIAS, collection).process(cluster.getSolrClient());
  }
  streamFactory.withCollectionZkHost(MULTI_REPLICA_COLLECTIONORALIAS, zkHost);
}

private static final String id = "id";

@Before
public void clearCollection() throws Exception {
  new UpdateRequest()
      .deleteByQuery("*:*")
      .commit(cluster.getSolrClient(), COLLECTIONORALIAS);
}

@Test
public void testUniqueStream() throws Exception {

  //Test CloudSolrStream and UniqueStream
  new UpdateRequest()
      .add(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "0")
      .add(id, "2", "a_s", "hello2", "a_i", "2", "a_f", "0")
      .add(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3")
      .add(id, "4", "a_s", "hello4", "a_i", "4", "a_f", "4")
      .add(id, "1", "a_s", "hello1", "a_i", "1", "a_f", "1")
      .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

  StreamContext streamContext = new StreamContext();
  SolrClientCache solrClientCache = new SolrClientCache();
  streamContext.setSolrClientCache(solrClientCache);
  try {
    SolrParams sParams = StreamingTest.mapParams("q", "*:*", "fl", "id,a_s,a_i,a_f", "sort", "a_f asc,a_i asc");
    CloudSolrStream stream = new CloudSolrStream(zkHost, COLLECTIONORALIAS, sParams);
    UniqueStream ustream = new UniqueStream(stream, new FieldEqualitor("a_f"));
    ustream.setStreamContext(streamContext);
    List<Tuple> tuples = getTuples(ustream);
    assertEquals(4, tuples.size());
    assertOrder(tuples, 0, 1, 3, 4);
  } finally {
    solrClientCache.close();
  }
}

@Test
public void testSpacesInParams() throws Exception {

  SolrParams sParams = StreamingTest.mapParams("q", "*:*", "fl", "id , a_s , a_i , a_f", "sort", "a_f  asc , a_i  asc");

  //CloudSolrStream compares the values of the sort with the fl field.
  //The constructor will throw an exception if the sort fields do not the
  //a value in the field list.

  CloudSolrStream stream = new CloudSolrStream("", "collection1", sParams);
  stream.close();
}

@Test
public void testNonePartitionKeys() throws Exception {

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
  try {

    SolrParams sParamsA = StreamingTest.mapParams("q", "*:*", "fl", "id,a_s,a_i,a_f", "sort", "a_s asc,a_f asc", "partitionKeys", "none", "qt", "/export");
    CloudSolrStream stream = new CloudSolrStream(zkHost, COLLECTIONORALIAS, sParamsA);
    ParallelStream pstream = parallelStream(stream, new FieldComparator("a_s", ComparatorOrder.ASCENDING));
    attachStreamFactory(pstream);
    pstream.setStreamContext(streamContext);
    List<Tuple> tuples = getTuples(pstream);

    assert (tuples.size() == (10 * numWorkers)); // Each tuple will be double counted.
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

  StreamContext streamContext = new StreamContext();
  SolrClientCache solrClientCache = new SolrClientCache();
  streamContext.setSolrClientCache(solrClientCache);

  try {

    SolrParams sParams = mapParams("q", "*:*", "fl", "id,a_s,a_i,a_f", "sort", "a_f asc,a_i asc", "partitionKeys", "a_f", "qt", "/export");
    CloudSolrStream stream = new CloudSolrStream(zkHost, COLLECTIONORALIAS, sParams);
    UniqueStream ustream = new UniqueStream(stream, new FieldEqualitor("a_f"));
    ParallelStream pstream = parallelStream(ustream, new FieldComparator("a_f", ComparatorOrder.ASCENDING));
    attachStreamFactory(pstream);
    pstream.setStreamContext(streamContext);
    List<Tuple> tuples = getTuples(pstream);
    assertEquals(5, tuples.size());
    assertOrder(tuples, 0, 1, 3, 4, 6);

    //Test the eofTuples

    Map<String, Tuple> eofTuples = pstream.getEofTuples();
    assertEquals(numWorkers, eofTuples.size()); //There should be an EOF tuple for each worker.
  }finally {
    solrClientCache.close();
  }

}

@Test
public void testMultipleFqClauses() throws Exception {

  new UpdateRequest()
      .add(id, "0", "a_ss", "hello0", "a_ss", "hello1", "a_i", "0", "a_f", "0")
      .add(id, "2", "a_ss", "hello2", "a_i", "2", "a_f", "0")
  .add(id, "3", "a_ss", "hello3", "a_i", "3", "a_f", "3")
      .add(id, "4", "a_ss", "hello4", "a_i", "4", "a_f", "4")
      .add(id, "1", "a_ss", "hello1", "a_i", "1", "a_f", "1")
      .add(id, "5", "a_ss", "hello1", "a_i", "10", "a_f", "1")
      .add(id, "6", "a_ss", "hello1", "a_i", "11", "a_f", "5")
      .add(id, "7", "a_ss", "hello1", "a_i", "12", "a_f", "5")
      .add(id, "8", "a_ss", "hello1", "a_i", "13", "a_f", "4")
      .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

  streamFactory.withCollectionZkHost(COLLECTIONORALIAS, zkHost);

  StreamContext streamContext = new StreamContext();
  SolrClientCache solrClientCache = new SolrClientCache();
  streamContext.setSolrClientCache(solrClientCache);

  try {
    ModifiableSolrParams params = new ModifiableSolrParams(mapParams("q", "*:*", "fl", "id,a_i",
        "sort", "a_i asc", "fq", "a_ss:hello0", "fq", "a_ss:hello1"));
    CloudSolrStream stream = new CloudSolrStream(zkHost, COLLECTIONORALIAS, params);
    stream.setStreamContext(streamContext);
    List<Tuple> tuples = getTuples(stream);
    assertEquals("Multiple fq clauses should have been honored", 1, tuples.size());
    assertEquals("should only have gotten back document 0", "0", tuples.get(0).getString("id"));
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

  StreamContext streamContext = new StreamContext();
  SolrClientCache solrClientCache = new SolrClientCache();
  streamContext.setSolrClientCache(solrClientCache);
  try {
    SolrParams sParams = mapParams("q", "*:*", "fl", "id,a_s,a_i", "sort", "a_i asc");
    CloudSolrStream stream = new CloudSolrStream(zkHost, COLLECTIONORALIAS, sParams);
    RankStream rstream = new RankStream(stream, 3, new FieldComparator("a_i", ComparatorOrder.DESCENDING));
    rstream.setStreamContext(streamContext);
    List<Tuple> tuples = getTuples(rstream);
    assertEquals(3, tuples.size());
    assertOrder(tuples, 4, 3, 2);
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

  StreamContext streamContext = new StreamContext();
  SolrClientCache solrClientCache = new SolrClientCache();
  streamContext.setSolrClientCache(solrClientCache);
  try {
    SolrParams sParams = mapParams("q", "*:*", "fl", "id,a_s,a_i", "sort", "a_i asc", "partitionKeys", "a_i", "qt", "/export");
    CloudSolrStream stream = new CloudSolrStream(zkHost, COLLECTIONORALIAS, sParams);
    RankStream rstream = new RankStream(stream, 11, new FieldComparator("a_i", ComparatorOrder.DESCENDING));
    ParallelStream pstream = parallelStream(rstream, new FieldComparator("a_i", ComparatorOrder.DESCENDING));
    attachStreamFactory(pstream);
    pstream.setStreamContext(streamContext);
    List<Tuple> tuples = getTuples(pstream);

    assertEquals(10, tuples.size());
    assertOrder(tuples, 10, 9, 8, 7, 6, 5, 4, 3, 2, 0);
  } finally {
    solrClientCache.close();
  }

}

@Test
  public void testTrace() throws Exception {

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

    try {
      //Test with spaces in the parameter lists.
      SolrParams sParamsA = mapParams("q", "*:*", "fl", "id,a_s, a_i,a_f", "sort", "a_s asc,a_f   asc");
      CloudSolrStream stream = new CloudSolrStream(zkHost, COLLECTIONORALIAS, sParamsA);
      stream.setTrace(true);
      stream.setStreamContext(streamContext);
      List<Tuple> tuples = getTuples(stream);
      assertEquals(COLLECTIONORALIAS, tuples.get(0).get("_COLLECTION_"));
      assertEquals(COLLECTIONORALIAS, tuples.get(1).get("_COLLECTION_"));
      assertEquals(COLLECTIONORALIAS, tuples.get(2).get("_COLLECTION_"));
      assertEquals(COLLECTIONORALIAS, tuples.get(3).get("_COLLECTION_"));
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

    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);

    try {
      //Test with spaces in the parameter lists.
      SolrParams sParamsA = mapParams("q", "*:*", "fl", "id,a_s, a_i,  a_f", "sort", "a_s asc  ,  a_f   asc");
      CloudSolrStream stream = new CloudSolrStream(zkHost, COLLECTIONORALIAS, sParamsA);
      ReducerStream rstream = new ReducerStream(stream,
          new FieldEqualitor("a_s"),
          new GroupOperation(new FieldComparator("a_f", ComparatorOrder.ASCENDING), 5));

      rstream.setStreamContext(streamContext);
      List<Tuple> tuples = getTuples(rstream);

      assertEquals(3, tuples.size());

      Tuple t0 = tuples.get(0);
      @SuppressWarnings({"rawtypes"})
      List<Map> maps0 = t0.getMaps("group");
      assertMaps(maps0, 0, 2, 1, 9);

      Tuple t1 = tuples.get(1);
      @SuppressWarnings({"rawtypes"})
      List<Map> maps1 = t1.getMaps("group");
      assertMaps(maps1, 3, 5, 7, 8);

      Tuple t2 = tuples.get(2);
      @SuppressWarnings({"rawtypes"})
      List<Map> maps2 = t2.getMaps("group");
      assertMaps(maps2, 4, 6);

      //Test with spaces in the parameter lists using a comparator
      sParamsA = mapParams("q", "*:*", "fl", "id,a_s, a_i,  a_f", "sort", "a_s asc  ,  a_f   asc");
      stream = new CloudSolrStream(zkHost, COLLECTIONORALIAS, sParamsA);
      rstream = new ReducerStream(stream,
          new FieldComparator("a_s", ComparatorOrder.ASCENDING),
          new GroupOperation(new FieldComparator("a_f", ComparatorOrder.DESCENDING), 5));
      rstream.setStreamContext(streamContext);
      tuples = getTuples(rstream);

      assertEquals(3, tuples.size());

      t0 = tuples.get(0);
      maps0 = t0.getMaps("group");
      assertMaps(maps0, 9, 1, 2, 0);

      t1 = tuples.get(1);
      maps1 = t1.getMaps("group");
      assertMaps(maps1, 8, 7, 5, 3);

      t2 = tuples.get(2);
      maps2 = t2.getMaps("group");
      assertMaps(maps2, 6, 4);
    } finally {
      solrClientCache.close();
    }
  }

  @Test
  public void testZeroReducerStream() throws Exception {

    //Gracefully handle zero results
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

    try {
      //Test with spaces in the parameter lists.
      SolrParams sParamsA = mapParams("q", "blah", "fl", "id,a_s, a_i,  a_f", "sort", "a_s asc  ,  a_f   asc");
      CloudSolrStream stream = new CloudSolrStream(zkHost, COLLECTIONORALIAS, sParamsA);
      ReducerStream rstream = new ReducerStream(stream,
          new FieldEqualitor("a_s"),
          new GroupOperation(new FieldComparator("a_f", ComparatorOrder.ASCENDING), 5));
      rstream.setStreamContext(streamContext);
      List<Tuple> tuples = getTuples(rstream);

      assertEquals(0, tuples.size());
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

    try {
      SolrParams sParamsA = mapParams("q", "*:*", "fl", "id,a_s,a_i,a_f", "sort", "a_s asc,a_f asc", "partitionKeys", "a_s", "qt", "/export");
      CloudSolrStream stream = new CloudSolrStream(zkHost, COLLECTIONORALIAS, sParamsA);

      ReducerStream rstream = new ReducerStream(stream,
          new FieldEqualitor("a_s"),
          new GroupOperation(new FieldComparator("a_f", ComparatorOrder.DESCENDING), 5));
      ParallelStream pstream = parallelStream(rstream, new FieldComparator("a_s", ComparatorOrder.ASCENDING));
      attachStreamFactory(pstream);
      pstream.setStreamContext(streamContext);
      List<Tuple> tuples = getTuples(pstream);

      assertEquals(3, tuples.size());

      Tuple t0 = tuples.get(0);
      @SuppressWarnings({"rawtypes"})
      List<Map> maps0 = t0.getMaps("group");
      assertMaps(maps0, 9, 1, 2, 0);

      Tuple t1 = tuples.get(1);
      @SuppressWarnings({"rawtypes"})
      List<Map> maps1 = t1.getMaps("group");
      assertMaps(maps1, 8, 7, 5, 3);

      Tuple t2 = tuples.get(2);
      @SuppressWarnings({"rawtypes"})
      List<Map> maps2 = t2.getMaps("group");
      assertMaps(maps2, 6, 4);

      //Test Descending with Ascending subsort

      sParamsA = mapParams("q", "*:*", "fl", "id,a_s,a_i,a_f", "sort", "a_s desc,a_f asc", "partitionKeys", "a_s", "qt", "/export");
      stream = new CloudSolrStream(zkHost, COLLECTIONORALIAS, sParamsA);

      rstream = new ReducerStream(stream,
          new FieldEqualitor("a_s"),
          new GroupOperation(new FieldComparator("a_f", ComparatorOrder.ASCENDING), 3));
      pstream = parallelStream(rstream, new FieldComparator("a_s", ComparatorOrder.DESCENDING));
      attachStreamFactory(pstream);
      pstream.setStreamContext(streamContext);
      tuples = getTuples(pstream);

      assertEquals(3, tuples.size());

      t0 = tuples.get(0);
      maps0 = t0.getMaps("group");
      assertMaps(maps0, 4, 6);

      t1 = tuples.get(1);
      maps1 = t1.getMaps("group");
      assertMaps(maps1, 3, 5, 7);

      t2 = tuples.get(2);
      maps2 = t2.getMaps("group");
      assertMaps(maps2, 0, 2, 1);
    } finally {
      solrClientCache.close();
    }
  }

  @Test
  @Ignore
  public void testExceptionStream() throws Exception {

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
    //Test an error that comes originates from the /select handler
    try {
      SolrParams sParamsA = mapParams("q", "*:*", "fl", "a_s,a_i,a_f,blah", "sort", "blah asc");
      CloudSolrStream stream = new CloudSolrStream(zkHost, COLLECTIONORALIAS, sParamsA);
      ExceptionStream estream = new ExceptionStream(stream);
      estream.setStreamContext(streamContext);
      Tuple t = getTuple(estream);
      assertTrue(t.EOF);
      assertTrue(t.EXCEPTION);
      assertTrue(t.getException().contains("sort param field can't be found: blah"));

      //Test an error that comes originates from the /export handler
      sParamsA = mapParams("q", "*:*", "fl", "a_s,a_i,a_f,score", "sort", "a_s asc", "qt", "/export");
      stream = new CloudSolrStream(zkHost, COLLECTIONORALIAS, sParamsA);
      estream = new ExceptionStream(stream);
      estream.setStreamContext(streamContext);
      t = getTuple(estream);
      assertTrue(t.EOF);
      assertTrue(t.EXCEPTION);
      //The /export handler will pass through a real exception.
      assertTrue(t.getException().contains("undefined field:"));
    } finally {
      solrClientCache.close();
    }
  }

  @Test
  @Ignore
  public void testParallelExceptionStream() throws Exception {

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

    SolrParams sParamsA = mapParams("q", "*:*", "fl", "a_s,a_i,a_f,blah", "sort", "blah asc");
    CloudSolrStream stream = new CloudSolrStream(zkHost, COLLECTIONORALIAS, sParamsA);
    ParallelStream pstream = new ParallelStream(zkHost, COLLECTIONORALIAS, stream, 2, new FieldComparator("blah", ComparatorOrder.ASCENDING));
    ExceptionStream estream = new ExceptionStream(pstream);
    Tuple t = getTuple(estream);
    assertTrue(t.EOF);
    assertTrue(t.EXCEPTION);
    //ParallelStream requires that partitionKeys be set.
    assertTrue(t.getException().contains("When numWorkers > 1 partitionKeys must be set."));


    //Test an error that originates from the /select handler
    sParamsA = mapParams("q", "*:*", "fl", "a_s,a_i,a_f,blah", "sort", "blah asc", "partitionKeys", "a_s", "qt", "/export");
    stream = new CloudSolrStream(zkHost, COLLECTIONORALIAS, sParamsA);
    pstream = new ParallelStream(zkHost, COLLECTIONORALIAS, stream, 2, new FieldComparator("blah", ComparatorOrder.ASCENDING));
    estream = new ExceptionStream(pstream);
    t = getTuple(estream);
    assertTrue(t.EOF);
    assertTrue(t.EXCEPTION);
    assertTrue(t.getException().contains("sort param field can't be found: blah"));


    //Test an error that originates from the /export handler
    sParamsA = mapParams("q", "*:*", "fl", "a_s,a_i,a_f,score", "sort", "a_s asc", "qt", "/export", "partitionKeys", "a_s");
    stream = new CloudSolrStream(zkHost, COLLECTIONORALIAS, sParamsA);
    pstream = new ParallelStream(zkHost, COLLECTIONORALIAS, stream, 2, new FieldComparator("a_s", ComparatorOrder.ASCENDING));
    estream = new ExceptionStream(pstream);
    t = getTuple(estream);
    assertTrue(t.EOF);
    assertTrue(t.EXCEPTION);
    //The /export handler will pass through a real exception.
    assertTrue(t.getException().contains("undefined field:"));
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

    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);

    try {
      SolrParams sParamsA = mapParams("q", "*:*");

      Metric[] metrics = {new SumMetric("a_i"),
          new SumMetric("a_f"),
          new MinMetric("a_i"),
          new MinMetric("a_f"),
          new MaxMetric("a_i"),
          new MaxMetric("a_f"),
          new MeanMetric("a_i"),
          new MeanMetric("a_f"),
          new CountMetric()};

      StatsStream statsStream = new StatsStream(zkHost, COLLECTIONORALIAS, sParamsA, metrics);
      statsStream.setStreamContext(streamContext);
      List<Tuple> tuples = getTuples(statsStream);

      assertEquals(1, tuples.size());

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

      assertEquals(70, sumi.longValue());
      assertEquals(55.0, sumf.doubleValue(), 0.01);
      assertEquals(0.0, mini.doubleValue(), 0.01);
      assertEquals(1.0, minf.doubleValue(), 0.01);
      assertEquals(14.0, maxi.doubleValue(), 0.01);
      assertEquals(10.0, maxf.doubleValue(), 0.01);
      assertEquals(7.0, avgi.doubleValue(), .01);
      assertEquals(5.5, avgf.doubleValue(), .001);
      assertEquals(10, count.doubleValue(), .01);
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

    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);

    try {
      SolrParams sParamsA = mapParams("q", "*:*", "fl", "a_s,a_i,a_f", "sort", "a_s asc");

      Bucket[] buckets = {new Bucket("a_s")};

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

      FacetStream facetStream = new FacetStream(zkHost, COLLECTIONORALIAS, sParamsA, buckets, metrics, sorts, 100);

      List<Tuple> tuples = getTuples(facetStream);

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

      assertEquals("hello4", bucket);
      assertEquals(15, sumi.longValue());
      assertEquals(11.0, sumf.doubleValue(), 0.01);
      assertEquals(4.0, mini.doubleValue(), 0.01);
      assertEquals(4.0, minf.doubleValue(), 0.01);
      assertEquals(11.0, maxi.doubleValue(), 0.01);
      assertEquals(7.0, maxf.doubleValue(), 0.01);
      assertEquals(7.5, avgi.doubleValue(), 0.01);
      assertEquals(5.5, avgf.doubleValue(), 0.01);
      assertEquals(2, count.doubleValue(), 0.01);

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

      assertEquals("hello0", bucket);
      assertEquals(17, sumi.doubleValue(), .01);
      assertEquals(18, sumf.doubleValue(), .01);
      assertEquals(0.0, mini.doubleValue(), .01);
      assertEquals(1.0, minf.doubleValue(), .01);
      assertEquals(14.0, maxi.doubleValue(), .01);
      assertEquals(10.0, maxf.doubleValue(), .01);
      assertEquals(4.25, avgi.doubleValue(), .01);
      assertEquals(4.5, avgf.doubleValue(), .01);
      assertEquals(4, count.doubleValue(), .01);

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

      assertEquals("hello3", bucket);
      assertEquals(38.0, sumi.doubleValue(), 0.01);
      assertEquals(26.0, sumf.doubleValue(), 0.01);
      assertEquals(3.0, mini.doubleValue(), 0.01);
      assertEquals(3.0, minf.doubleValue(), 0.01);
      assertEquals(13.0, maxi.doubleValue(), 0.01);
      assertEquals(9.0, maxf.doubleValue(), 0.01);
      assertEquals(9.5, avgi.doubleValue(), 0.01);
      assertEquals(6.5, avgf.doubleValue(), 0.01);
      assertEquals(4, count.doubleValue(), 0.01);


      //Reverse the Sort.

      sorts[0] = new FieldComparator("sum(a_i)", ComparatorOrder.DESCENDING);

      facetStream = new FacetStream(zkHost, COLLECTIONORALIAS, sParamsA, buckets, metrics, sorts, 100);

      tuples = getTuples(facetStream);

      assertEquals(3, tuples.size());

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

      assertEquals("hello3", bucket);
      assertEquals(38, sumi.doubleValue(), 0.1);
      assertEquals(26, sumf.doubleValue(), 0.1);
      assertEquals(3, mini.doubleValue(), 0.1);
      assertEquals(3, minf.doubleValue(), 0.1);
      assertEquals(13, maxi.doubleValue(), 0.1);
      assertEquals(9, maxf.doubleValue(), 0.1);
      assertEquals(9.5, avgi.doubleValue(), 0.1);
      assertEquals(6.5, avgf.doubleValue(), 0.1);
      assertEquals(4, count.doubleValue(), 0.1);

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

      assertEquals("hello0", bucket);
      assertEquals(17, sumi.doubleValue(), 0.01);
      assertEquals(18, sumf.doubleValue(), 0.01);
      assertEquals(0, mini.doubleValue(), 0.01);
      assertEquals(1, minf.doubleValue(), 0.01);
      assertEquals(14, maxi.doubleValue(), 0.01);
      assertEquals(10, maxf.doubleValue(), 0.01);
      assertEquals(4.25, avgi.doubleValue(), 0.01);
      assertEquals(4.5, avgf.doubleValue(), 0.01);
      assertEquals(4, count.doubleValue(), 0.01);

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

      assertEquals("hello4", bucket);
      assertEquals(15, sumi.longValue());
      assertEquals(11, sumf.doubleValue(), 0.01);
      assertEquals(4.0, mini.doubleValue(), 0.01);
      assertEquals(4.0, minf.doubleValue(), 0.01);
      assertEquals(11.0, maxi.doubleValue(), 0.01);
      assertEquals(7.0, maxf.doubleValue(), 0.01);
      assertEquals(7.5, avgi.doubleValue(), 0.01);
      assertEquals(5.5, avgf.doubleValue(), 0.01);
      assertEquals(2, count.doubleValue(), 0.01);


      //Test index sort

      sorts[0] = new FieldComparator("a_s", ComparatorOrder.DESCENDING);


      facetStream = new FacetStream(zkHost, COLLECTIONORALIAS, sParamsA, buckets, metrics, sorts, 100);
      facetStream.setStreamContext(streamContext);

      tuples = getTuples(facetStream);

      assertEquals(3, tuples.size());


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


      assertEquals("hello4", bucket);
      assertEquals(15, sumi.longValue());
      assertEquals(11, sumf.doubleValue(), 0.01);
      assertEquals(4, mini.doubleValue(), 0.01);
      assertEquals(4, minf.doubleValue(), 0.01);
      assertEquals(11, maxi.doubleValue(), 0.01);
      assertEquals(7, maxf.doubleValue(), 0.01);
      assertEquals(7.5, avgi.doubleValue(), 0.01);
      assertEquals(5.5, avgf.doubleValue(), 0.01);
      assertEquals(2, count.doubleValue(), 0.01);

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

      assertEquals("hello0", bucket);
      assertEquals(17, sumi.doubleValue(), 0.01);
      assertEquals(18, sumf.doubleValue(), 0.01);
      assertEquals(0, mini.doubleValue(), 0.01);
      assertEquals(1, minf.doubleValue(), 0.01);
      assertEquals(14, maxi.doubleValue(), 0.01);
      assertEquals(10, maxf.doubleValue(), 0.01);
      assertEquals(4.25, avgi.doubleValue(), 0.01);
      assertEquals(4.5, avgf.doubleValue(), 0.01);
      assertEquals(4, count.doubleValue(), 0.01);

      //Test index sort

      sorts[0] = new FieldComparator("a_s", ComparatorOrder.ASCENDING);

      facetStream = new FacetStream(zkHost, COLLECTIONORALIAS, sParamsA, buckets, metrics, sorts, 100);
      facetStream.setStreamContext(streamContext);
      tuples = getTuples(facetStream);

      assertEquals(3, tuples.size());

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

      assertEquals("hello0", bucket);
      assertEquals(17, sumi.doubleValue(), 0.01);
      assertEquals(18, sumf.doubleValue(), 0.01);
      assertEquals(0, mini.doubleValue(), 0.01);
      assertEquals(1, minf.doubleValue(), 0.01);
      assertEquals(14, maxi.doubleValue(), 0.01);
      assertEquals(10, maxf.doubleValue(), 0.01);
      assertEquals(4.25, avgi.doubleValue(), 0.0001);
      assertEquals(4.5, avgf.doubleValue(), 0.001);
      assertEquals(4, count.doubleValue(), 0.01);

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

      assertEquals("hello3", bucket);
      assertEquals(38, sumi.doubleValue(), 0.01);
      assertEquals(26, sumf.doubleValue(), 0.01);
      assertEquals(3, mini.doubleValue(), 0.01);
      assertEquals(3, minf.doubleValue(), 0.01);
      assertEquals(13, maxi.doubleValue(), 0.01);
      assertEquals(9, maxf.doubleValue(), 0.01);
      assertEquals(9.5, avgi.doubleValue(), 0.01);
      assertEquals(6.5, avgf.doubleValue(), 0.01);
      assertEquals(4, count.doubleValue(), 0.01);

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

      assertEquals("hello4", bucket);
      assertEquals(15, sumi.longValue());
      assertEquals(11.0, sumf.doubleValue(), 0.1);
      assertEquals(4.0, mini.doubleValue(), 0.1);
      assertEquals(4.0, minf.doubleValue(), 0.1);
      assertEquals(11.0, maxi.doubleValue(), 0.1);
      assertEquals(7.0, maxf.doubleValue(), 0.1);
      assertEquals(7.5, avgi.doubleValue(), 0.1);
      assertEquals(5.5, avgf.doubleValue(), 0.1);
      assertEquals(2, count.doubleValue(), 0.1);

      sorts[0] = new FieldComparator("a_s", ComparatorOrder.ASCENDING);

      facetStream = new FacetStream(zkHost, COLLECTIONORALIAS, sParamsA, buckets, metrics, sorts, -1);
      facetStream.setStreamContext(streamContext);
      tuples = getTuples(facetStream);

      assertEquals(3, tuples.size());

    } finally {
      solrClientCache.close();
    }
  }


  String[] docPairs(int base, String sSeq) {
    List<String> pairs = new ArrayList<>();
    final int iSeq = base * 100;
    pairs.add(id);
    pairs.add(sSeq + base); // aaa1
    pairs.add("s_sing");
    pairs.add(Integer.toString(iSeq + 1)); // 101
    pairs.add("i_sing");
    pairs.add(Integer.toString(iSeq + 2)); // 102
    pairs.add("f_sing");
    pairs.add(Float.toString(iSeq + 3)); // 103.0
    pairs.add("l_sing");
    pairs.add(Long.toString(iSeq + 4)); // 104
    pairs.add("d_sing");
    pairs.add(Double.toString(iSeq + 5)); // 105
    pairs.add("dt_sing");
    pairs.add(String.format(Locale.ROOT, "2000-01-01T%02d:00:00Z", base)); // Works as long as we add fewer than 60 docs
    pairs.add("b_sing");
    pairs.add((base % 2) == 0 ? "T" : "F"); // Tricky

    String[] ret = new String[pairs.size()];
    return pairs.toArray(ret);
  }

  // Select and export should be identical sort orders I think.
  private void checkSort(JettySolrRunner jetty, String field, String sortDir, String[] fields) throws IOException, SolrServerException {

    // Comes back after after LUCENE-7548
//    SolrQuery query = new SolrQuery("*:*");
//    query.addSort(field, ("asc".equals(sortDir) ? SolrQuery.ORDER.asc : SolrQuery.ORDER.desc));
//    query.addSort("id", SolrQuery.ORDER.asc);
//    query.addField("id");
//    query.addField(field);
//    query.setRequestHandler("standard");
//    query.setRows(100);
//
//    List<String> selectOrder = new ArrayList<>();
//
//    String url = jetty.getBaseUrl() + "/" + COLLECTION;
//
//    try (HttpSolrClient client = getHttpSolrClient(url)) {
//      client.setConnectionTimeout(DEFAULT_CONNECTION_TIMEOUT);
//      QueryResponse rsp = client.query(query);
//      for (SolrDocument doc : rsp.getResults()) {
//        selectOrder.add((String) doc.getFieldValue("id"));
//      }
//    }
//    SolrParams exportParams = mapParams("q", "*:*", "qt", "/export", "fl", "id," + field, "sort", field + " " + sortDir + ",id asc");
//    try (CloudSolrStream solrStream = new CloudSolrStream(zkHost, COLLECTIONORALIAS, exportParams)) {
//      List<Tuple> tuples = getTuples(solrStream);
//      assertEquals("There should be exactly 32 responses returned", 32, tuples.size());
//      // Since the getTuples method doesn't return the EOF tuple, these two entries should be the same size.
//      assertEquals("Tuple count should exactly match sort array size for field " + field + " sort order " + sortDir, selectOrder.size(), tuples.size());
//
//      for (int idx = 0; idx < selectOrder.size(); ++idx) { // Tuples should be in lock step with the orders from select.
//        assertEquals("Order for missing docValues fields wrong for field '" + field + "' sort direction '" + sortDir,
//            tuples.get(idx).getString("id"), selectOrder.get(idx));
//      }
//    }

    // Remove below and uncomment above after LUCENE-7548
    List<String> selectOrder = ("asc".equals(sortDir)) ? Arrays.asList(ascOrder) : Arrays.asList(descOrder);
    List<String> selectOrderBool = ("asc".equals(sortDir)) ? Arrays.asList(ascOrderBool) : Arrays.asList(descOrderBool);
    SolrParams exportParams = mapParams("q", "*:*", "qt", "/export", "fl", "id," + field, "sort", field + " " + sortDir + ",id asc");
    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);
    try (CloudSolrStream solrStream = new CloudSolrStream(zkHost, COLLECTIONORALIAS, exportParams)) {
      solrStream.setStreamContext(streamContext);
      List<Tuple> tuples = getTuples(solrStream);
      assertEquals("There should be exactly 32 responses returned", 32, tuples.size());
      // Since the getTuples method doesn't return the EOF tuple, these two entries should be the same size.
      assertEquals("Tuple count should exactly match sort array size for field " + field + " sort order " + sortDir, selectOrder.size(), tuples.size());

      for (int idx = 0; idx < selectOrder.size(); ++idx) { // Tuples should be in lock step with the orders passed in.
        assertEquals("Order for missing docValues fields wrong for field '" + field + "' sort direction '" + sortDir +
                "' RESTORE GETTING selectOrder from select statement after LUCENE-7548",
            tuples.get(idx).getString("id"), (field.startsWith("b_") ? selectOrderBool.get(idx) : selectOrder.get(idx)));
      }
    } finally {
      solrClientCache.close();
    }
  }

  static final String[] voidIds = new String[]{
      "iii1",
      "eee1",
      "aaa1",
      "ooo1",
      "iii2",
      "eee2",
      "aaa2",
      "ooo2",
      "iii3",
      "eee3",
      "aaa3",
      "ooo3"
  };

  private void checkReturnValsForEmpty(String[] fields) throws IOException {

    Set<String> voids = new HashSet<>(Arrays.asList(voidIds));

    StringBuilder fl = new StringBuilder("id");
    for (String f : fields) {
      fl.append(",").append(f);
    }
    SolrParams sParams = mapParams("q", "*:*", "qt", "/export", "fl", fl.toString(), "sort", "id asc");

    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);

    try (CloudSolrStream solrStream = new CloudSolrStream(zkHost, COLLECTIONORALIAS, sParams)) {
      solrStream.setStreamContext(streamContext);
      List<Tuple> tuples = getTuples(solrStream);
      assertEquals("There should be exactly 32 responses returned", 32, tuples.size());

      for (Tuple tuple : tuples) {
        String id = tuple.getString("id");
        if (voids.contains(id)) {
          for (String f : fields) {
            assertNull("Should have returned a void for field " + f + " doc " + id, tuple.get(f));
          }
        } else {
          for (String f : fields) {
            assertNotNull("Should have returned a value for field " + f + " doc " + id, tuple.get(f));
          }
        }
      }
    } finally {
      solrClientCache.close();
    }
  }

  // Goes away after after LUCENE-7548
  final static String[] ascOrder = new String[]{
      "aaa1", "aaa2", "aaa3", "eee1",
      "eee2", "eee3", "iii1", "iii2",
      "iii3", "ooo1", "ooo2", "ooo3",
      "aaa4", "eee4", "iii4", "ooo4",
      "aaa5", "eee5", "iii5", "ooo5",
      "aaa6", "eee6", "iii6", "ooo6",
      "aaa7", "eee7", "iii7", "ooo7",
      "aaa8", "eee8", "iii8", "ooo8"
  };

  // Goes away after after LUCENE-7548
  final static String[] descOrder = new String[]{
      "aaa8", "eee8", "iii8", "ooo8",
      "aaa7", "eee7", "iii7", "ooo7",
      "aaa6", "eee6", "iii6", "ooo6",
      "aaa5", "eee5", "iii5", "ooo5",
      "aaa4", "eee4", "iii4", "ooo4",
      "aaa1", "aaa2", "aaa3", "eee1",
      "eee2", "eee3", "iii1", "iii2",
      "iii3", "ooo1", "ooo2", "ooo3"
  };


  // Goes away after after LUCENE-7548
  final static String[] ascOrderBool = new String[]{
      "aaa1", "aaa2", "aaa3", "eee1",
      "eee2", "eee3", "iii1", "iii2",
      "iii3", "ooo1", "ooo2", "ooo3",
      "aaa5", "aaa7", "eee5", "eee7",
      "iii5", "iii7", "ooo5", "ooo7",
      "aaa4", "aaa6", "aaa8", "eee4",
      "eee6", "eee8", "iii4", "iii6",
      "iii8", "ooo4", "ooo6", "ooo8"
  };

  // Goes away after after LUCENE-7548
  final static String[] descOrderBool = new String[]{
      "aaa4", "aaa6", "aaa8", "eee4",
      "eee6", "eee8", "iii4", "iii6",
      "iii8", "ooo4", "ooo6", "ooo8",
      "aaa5", "aaa7", "eee5", "eee7",
      "iii5", "iii7", "ooo5", "ooo7",
      "aaa1", "aaa2", "aaa3", "eee1",
      "eee2", "eee3", "iii1", "iii2",
      "iii3", "ooo1", "ooo2", "ooo3",
  };

  @Test
  public void testMissingFields() throws Exception {

    new UpdateRequest()
        // Some docs with nothing at all for any of the "interesting" fields.
        .add(id, "iii1")
        .add(id, "eee1")
        .add(id, "aaa1")
        .add(id, "ooo1")

        .add(id, "iii2")
        .add(id, "eee2")
        .add(id, "aaa2")
        .add(id, "ooo2")

        .add(id, "iii3")
        .add(id, "eee3")
        .add(id, "aaa3")
        .add(id, "ooo3")

        // Docs with values in for all of the types we want to sort on.

        .add(docPairs(4, "iii"))
        .add(docPairs(4, "eee"))
        .add(docPairs(4, "aaa"))
        .add(docPairs(4, "ooo"))

        .add(docPairs(5, "iii"))
        .add(docPairs(5, "eee"))
        .add(docPairs(5, "aaa"))
        .add(docPairs(5, "ooo"))

        .add(docPairs(6, "iii"))
        .add(docPairs(6, "eee"))
        .add(docPairs(6, "aaa"))
        .add(docPairs(6, "ooo"))

        .add(docPairs(7, "iii"))
        .add(docPairs(7, "eee"))
        .add(docPairs(7, "aaa"))
        .add(docPairs(7, "ooo"))

        .add(docPairs(8, "iii"))
        .add(docPairs(8, "eee"))
        .add(docPairs(8, "aaa"))
        .add(docPairs(8, "ooo"))

        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    JettySolrRunner jetty = cluster.getJettySolrRunners().get(0);


    String[] fields = new String[]{"s_sing", "i_sing", "f_sing", "l_sing", "d_sing", "dt_sing", "b_sing" };


    for (String f : fields) {
      checkSort(jetty, f, "asc", fields);
      checkSort(jetty, f, "desc", fields);
    }

    checkReturnValsForEmpty(fields);

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

    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);

    try {

      SolrParams sParamsA = mapParams("q", "*:*", "fl", "a_i,a_f");

      Bucket[] buckets = {new Bucket("level1_s"), new Bucket("level2_s")};

      Metric[] metrics = {new SumMetric("a_i"),
          new CountMetric()};

      FieldComparator[] sorts = {new FieldComparator("sum(a_i)", ComparatorOrder.DESCENDING), new FieldComparator("sum(a_i)", ComparatorOrder.DESCENDING)};

      FacetStream facetStream = new FacetStream(
          zkHost,
          COLLECTIONORALIAS,
          sParamsA,
          buckets,
          metrics,
          sorts,
          100);
      facetStream.setStreamContext(streamContext);
      List<Tuple> tuples = getTuples(facetStream);
      assertEquals(6, tuples.size());

      Tuple tuple = tuples.get(0);
      String bucket1 = tuple.getString("level1_s");
      String bucket2 = tuple.getString("level2_s");
      Double sumi = tuple.getDouble("sum(a_i)");
      Double count = tuple.getDouble("count(*)");

      assertEquals("hello3", bucket1);
      assertEquals("b", bucket2);
      assertEquals(35, sumi.longValue());
      assertEquals(3, count, 0.1);

      tuple = tuples.get(1);
      bucket1 = tuple.getString("level1_s");
      bucket2 = tuple.getString("level2_s");
      sumi = tuple.getDouble("sum(a_i)");
      count = tuple.getDouble("count(*)");

      assertEquals("hello0", bucket1);
      assertEquals("b", bucket2);
      assertEquals(15, sumi.longValue());
      assertEquals(2, count, 0.1);

      tuple = tuples.get(2);
      bucket1 = tuple.getString("level1_s");
      bucket2 = tuple.getString("level2_s");
      sumi = tuple.getDouble("sum(a_i)");
      count = tuple.getDouble("count(*)");

      assertEquals("hello4", bucket1);
      assertEquals("b", bucket2);
      assertEquals(11, sumi.longValue());
      assertEquals(1, count.doubleValue(), 0.1);

      tuple = tuples.get(3);
      bucket1 = tuple.getString("level1_s");
      bucket2 = tuple.getString("level2_s");
      sumi = tuple.getDouble("sum(a_i)");
      count = tuple.getDouble("count(*)");

      assertEquals("hello4", bucket1);
      assertEquals("a", bucket2);
      assertEquals(4, sumi.longValue());
      assertEquals(1, count.doubleValue(), 0.1);

      tuple = tuples.get(4);
      bucket1 = tuple.getString("level1_s");
      bucket2 = tuple.getString("level2_s");
      sumi = tuple.getDouble("sum(a_i)");
      count = tuple.getDouble("count(*)");

      assertEquals("hello3", bucket1);
      assertEquals("a", bucket2);
      assertEquals(3, sumi.longValue());
      assertEquals(1, count.doubleValue(), 0.1);

      tuple = tuples.get(5);
      bucket1 = tuple.getString("level1_s");
      bucket2 = tuple.getString("level2_s");
      sumi = tuple.getDouble("sum(a_i)");
      count = tuple.getDouble("count(*)");

      assertEquals("hello0", bucket1);
      assertEquals("a", bucket2);
      assertEquals(2, sumi.longValue());
      assertEquals(2, count.doubleValue(), 0.1);

      sorts[0] = new FieldComparator("level1_s", ComparatorOrder.DESCENDING);
      sorts[1] = new FieldComparator("level2_s", ComparatorOrder.DESCENDING);
      facetStream = new FacetStream(
          zkHost,
          COLLECTIONORALIAS,
          sParamsA,
          buckets,
          metrics,
          sorts,
          100);
      facetStream.setStreamContext(streamContext);
      tuples = getTuples(facetStream);
      assertEquals(6, tuples.size());

      tuple = tuples.get(0);
      bucket1 = tuple.getString("level1_s");
      bucket2 = tuple.getString("level2_s");
      sumi = tuple.getDouble("sum(a_i)");
      count = tuple.getDouble("count(*)");

      assertEquals("hello4", bucket1);
      assertEquals("b", bucket2);
      assertEquals(11, sumi.longValue());
      assertEquals(1, count, 0.1);

      tuple = tuples.get(1);
      bucket1 = tuple.getString("level1_s");
      bucket2 = tuple.getString("level2_s");
      sumi = tuple.getDouble("sum(a_i)");
      count = tuple.getDouble("count(*)");

      assertEquals("hello4", bucket1);
      assertEquals("a", bucket2);
      assertEquals(4, sumi.longValue());
      assertEquals(1, count.doubleValue(), 0.1);

      tuple = tuples.get(2);
      bucket1 = tuple.getString("level1_s");
      bucket2 = tuple.getString("level2_s");
      sumi = tuple.getDouble("sum(a_i)");
      count = tuple.getDouble("count(*)");

      assertEquals("hello3", bucket1);
      assertEquals("b", bucket2);
      assertEquals(35, sumi.longValue());
      assertEquals(3, count.doubleValue(), 0.1);

      tuple = tuples.get(3);
      bucket1 = tuple.getString("level1_s");
      bucket2 = tuple.getString("level2_s");
      sumi = tuple.getDouble("sum(a_i)");
      count = tuple.getDouble("count(*)");

      assertEquals("hello3", bucket1);
      assertEquals("a", bucket2);
      assertEquals(3, sumi.longValue());
      assertEquals(1, count.doubleValue(), 0.1);

      tuple = tuples.get(4);
      bucket1 = tuple.getString("level1_s");
      bucket2 = tuple.getString("level2_s");
      sumi = tuple.getDouble("sum(a_i)");
      count = tuple.getDouble("count(*)");

      assertEquals("hello0", bucket1);
      assertEquals("b", bucket2);
      assertEquals(15, sumi.longValue());
      assertEquals(2, count.doubleValue(), 0.1);

      tuple = tuples.get(5);
      bucket1 = tuple.getString("level1_s");
      bucket2 = tuple.getString("level2_s");
      sumi = tuple.getDouble("sum(a_i)");
      count = tuple.getDouble("count(*)");

      assertEquals("hello0", bucket1);
      assertEquals("a", bucket2);
      assertEquals(2, sumi.longValue());
      assertEquals(2, count.doubleValue(), 0.1);
    } finally {
      solrClientCache.close();
    }
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
    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);

    try {
      SolrParams sParamsA = mapParams("q", "*:*", "fl", "a_s,a_i,a_f", "sort", "a_s asc");
      CloudSolrStream stream = new CloudSolrStream(zkHost, COLLECTIONORALIAS, sParamsA);

      Bucket[] buckets = {new Bucket("a_s")};

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
      rollupStream.setStreamContext(streamContext);
      List<Tuple> tuples = getTuples(rollupStream);

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


      assertEquals("hello0", bucket);
      assertEquals(17, sumi.doubleValue(), 0.001);
      assertEquals(18, sumf.doubleValue(), 0.001);
      assertEquals(0, mini.doubleValue(), 0.001);
      assertEquals(1, minf.doubleValue(), 0.001);
      assertEquals(14, maxi.doubleValue(), 0.001);
      assertEquals(10, maxf.doubleValue(), 0.001);
      assertEquals(4.25, avgi.doubleValue(), 0.001);
      assertEquals(4.5, avgf.doubleValue(), 0.001);
      assertEquals(4, count.doubleValue(), 0.001);


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

      assertEquals("hello3", bucket);
      assertEquals(38, sumi.doubleValue(), 0.001);
      assertEquals(26, sumf.doubleValue(), 0.001);
      assertEquals(3, mini.doubleValue(), 0.001);
      assertEquals(3, minf.doubleValue(), 0.001);
      assertEquals(13, maxi.doubleValue(), 0.001);
      assertEquals(9, maxf.doubleValue(), 0.001);
      assertEquals(9.5, avgi.doubleValue(), 0.001);
      assertEquals(6.5, avgf.doubleValue(), 0.001);
      assertEquals(4, count.doubleValue(), 0.001);


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

      assertEquals("hello4", bucket);
      assertEquals(15, sumi.longValue());
      assertEquals(11, sumf.doubleValue(), 0.01);
      assertEquals(4, mini.doubleValue(), 0.01);
      assertEquals(4, minf.doubleValue(), 0.01);
      assertEquals(11, maxi.doubleValue(), 0.01);
      assertEquals(7, maxf.doubleValue(), 0.01);
      assertEquals(7.5, avgi.doubleValue(), 0.01);
      assertEquals(5.5, avgf.doubleValue(), 0.01);
      assertEquals(2, count.doubleValue(), 0.01);

      // Test will null metrics
      rollupStream = new RollupStream(stream, buckets, metrics);
      rollupStream.setStreamContext(streamContext);
      tuples = getTuples(rollupStream);

      assert (tuples.size() == 3);
      tuple = tuples.get(0);
      bucket = tuple.getString("a_s");
      assertTrue(bucket.equals("hello0"));

      tuple = tuples.get(1);
      bucket = tuple.getString("a_s");
      assertTrue(bucket.equals("hello3"));

      tuple = tuples.get(2);
      bucket = tuple.getString("a_s");
      assertTrue(bucket.equals("hello4"));


      //Test will null value in the grouping field
      new UpdateRequest()
          .add(id, "12", "a_s", null, "a_i", "14", "a_f", "10")
          .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

      sParamsA = mapParams("q", "*:*", "fl", "a_s,a_i,a_f", "sort", "a_s asc", "qt", "/export");
      stream = new CloudSolrStream(zkHost, COLLECTIONORALIAS, sParamsA);
      Bucket[] buckets1 = {new Bucket("a_s")};

      Metric[] metrics1 = {new SumMetric("a_i"),
          new SumMetric("a_f"),
          new MinMetric("a_i"),
          new MinMetric("a_f"),
          new MaxMetric("a_i"),
          new MaxMetric("a_f"),
          new MeanMetric("a_i"),
          new MeanMetric("a_f"),
          new CountMetric()};

      rollupStream = new RollupStream(stream, buckets1, metrics1);
      rollupStream.setStreamContext(streamContext);
      tuples = getTuples(rollupStream);
      //Check that we've got the extra NULL bucket
      assertEquals(4, tuples.size());
      tuple = tuples.get(0);
      assertEquals("NULL", tuple.getString("a_s"));

      sumi = tuple.getDouble("sum(a_i)");
      sumf = tuple.getDouble("sum(a_f)");
      mini = tuple.getDouble("min(a_i)");
      minf = tuple.getDouble("min(a_f)");
      maxi = tuple.getDouble("max(a_i)");
      maxf = tuple.getDouble("max(a_f)");
      avgi = tuple.getDouble("avg(a_i)");
      avgf = tuple.getDouble("avg(a_f)");
      count = tuple.getDouble("count(*)");

      assertEquals(14, sumi.doubleValue(), 0.01);
      assertEquals(10, sumf.doubleValue(), 0.01);
      assertEquals(14, mini.doubleValue(), 0.01);
      assertEquals(10, minf.doubleValue(), 0.01);
      assertEquals(14, maxi.doubleValue(), 0.01);
      assertEquals(10, maxf.doubleValue(), 0.01);
      assertEquals(14, avgi.doubleValue(), 0.01);
      assertEquals(10, avgf.doubleValue(), 0.01);
      assertEquals(1, count.doubleValue(), 0.01);
    } finally {
      solrClientCache.close();
    }
  }

  @Test
  public void testDaemonTopicStream() throws Exception {
    Assume.assumeTrue(!useAlias);

    StreamContext context = new StreamContext();
    SolrClientCache cache = new SolrClientCache();
    context.setSolrClientCache(cache);

    try {
      SolrParams sParams = mapParams("q", "a_s:hello0", "rows", "500", "fl", "id");

      TopicStream topicStream = new TopicStream(zkHost,
          COLLECTIONORALIAS,
          COLLECTIONORALIAS,
          "50000000",
          -1,
          1000000, sParams);

      DaemonStream daemonStream = new DaemonStream(topicStream, "daemon1", 1000, 500);
      daemonStream.setStreamContext(context);

      daemonStream.open();
      CheckDaemonStream(context, daemonStream);

      // We should get an error if we try to open an already-open stream.
      final IOException ex = expectThrows(IOException.class, () -> {
        daemonStream.open();
      });
      assertEquals("Should have an intelligible exception message", ex.getMessage(), "There is already an open daemon named 'daemon1', no action taken.");
      daemonStream.close();

      // We should be able to close then re-open the stream, then close it again, see SOLR-13408
      daemonStream.open();
      CheckDaemonStream(context, daemonStream);
      daemonStream.close();
    } finally {
      cache.close();
    }
  }

  private void CheckDaemonStream(StreamContext context, DaemonStream daemonStream) throws IOException, SolrServerException {
    // Wait for the checkpoint
    JettySolrRunner jetty = cluster.getJettySolrRunners().get(0);

    SolrParams sParams1 = mapParams("qt", "/get", "ids", "50000000", "fl", "id");
    int count = 0;
    while (count == 0) {
      SolrStream solrStream = new SolrStream(jetty.getBaseUrl().toString() + "/" + COLLECTIONORALIAS, sParams1);
      solrStream.setStreamContext(context);
      List<Tuple> tuples = getTuples(solrStream);
      count = tuples.size();
      if (count > 0) {
        Tuple t = tuples.get(0);
        assertTrue(t.getLong("id") == 50000000);
      } else {
        System.out.println("###### Waiting for checkpoint #######:" + count);
      }
    }

    new UpdateRequest()
        .add(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "1")
        .add(id, "2", "a_s", "hello0", "a_i", "2", "a_f", "2")
        .add(id, "3", "a_s", "hello0", "a_i", "3", "a_f", "3")
        .add(id, "4", "a_s", "hello0", "a_i", "4", "a_f", "4")
        .add(id, "1", "a_s", "hello0", "a_i", "1", "a_f", "5")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    for (int i = 0; i < 5; i++) {
      daemonStream.read();
    }

    new UpdateRequest()
        .add(id, "5", "a_s", "hello0", "a_i", "4", "a_f", "4")
        .add(id, "6", "a_s", "hello0", "a_i", "4", "a_f", "4")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    for (int i = 0; i < 2; i++) {
      daemonStream.read();
    }

    daemonStream.shutdown();

    Tuple tuple = daemonStream.read();

    assertTrue(tuple.EOF);
  }

  @Test
  public void testRollupWithNoParallel() throws Exception {

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

    try {
      //Intentionally adding partitionKeys to trigger SOLR-12674
      SolrParams sParamsA = mapParams("q", "*:*", "fl", "a_s,a_i,a_f", "sort", "a_s asc", "partitionKeys", "a_s", "qt", "/export" );
      CloudSolrStream stream = new CloudSolrStream(zkHost, COLLECTIONORALIAS, sParamsA);

      Bucket[] buckets = {new Bucket("a_s")};

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
      rollupStream.setStreamContext(streamContext);
      List<Tuple> tuples = getTuples(rollupStream);
      assertEquals(3, tuples.size());


      List<String> shardUrls = TupleStream.getShards(cluster.getZkServer().getZkAddress(), COLLECTIONORALIAS, streamContext);
      ModifiableSolrParams solrParams = new ModifiableSolrParams();
      solrParams.add("qt", "/stream");
      solrParams.add("expr", "rollup(search(" + COLLECTIONORALIAS + ",q=\"*:*\",fl=\"a_s,a_i,a_f\",sort=\"a_s desc\",partitionKeys=\"a_s\", qt=\"/export\"),over=\"a_s\")\n");
      SolrStream solrStream = new SolrStream(shardUrls.get(0), solrParams);
      streamContext = new StreamContext();
      solrStream.setStreamContext(streamContext);
      tuples = getTuples(solrStream);
      assert (tuples.size() == 3);
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

    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);

    try {
      SolrParams sParamsA = mapParams("q", "*:*", "fl", "a_s,a_i,a_f", "sort", "a_s asc", "partitionKeys", "a_s", "qt", "/export");
      CloudSolrStream stream = new CloudSolrStream(zkHost, COLLECTIONORALIAS, sParamsA);

      Bucket[] buckets = {new Bucket("a_s")};

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
      ParallelStream parallelStream = parallelStream(rollupStream, new FieldComparator("a_s", ComparatorOrder.ASCENDING));
      attachStreamFactory(parallelStream);
      parallelStream.setStreamContext(streamContext);
      List<Tuple> tuples = getTuples(parallelStream);

      assertEquals(3, tuples.size());

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

      assertEquals("hello0", bucket);
      assertEquals(17, sumi.doubleValue(), 0.001);
      assertEquals(18, sumf.doubleValue(), 0.001);
      assertEquals(0, mini.doubleValue(), 0.001);
      assertEquals(1, minf.doubleValue(), 0.001);
      assertEquals(14, maxi.doubleValue(), 0.001);
      assertEquals(10, maxf.doubleValue(), 0.001);
      assertEquals(4.25, avgi.doubleValue(), 0.001);
      assertEquals(4.5, avgf.doubleValue(), 0.001);
      assertEquals(4, count.doubleValue(), 0.001);

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

      assertEquals("hello3", bucket);
      assertEquals(38, sumi.doubleValue(), 0.001);
      assertEquals(26, sumf.doubleValue(), 0.001);
      assertEquals(3, mini.doubleValue(), 0.001);
      assertEquals(3, minf.doubleValue(), 0.001);
      assertEquals(13, maxi.doubleValue(), 0.001);
      assertEquals(9, maxf.doubleValue(), 0.001);
      assertEquals(9.5, avgi.doubleValue(), 0.001);
      assertEquals(6.5, avgf.doubleValue(), 0.001);
      assertEquals(4, count.doubleValue(), 0.001);

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

      assertEquals("hello4", bucket);
      assertEquals(15, sumi.longValue());
      assertEquals(11, sumf.doubleValue(), 0.001);
      assertEquals(4, mini.doubleValue(), 0.001);
      assertEquals(4, minf.doubleValue(), 0.001);
      assertEquals(11, maxi.doubleValue(), 0.001);
      assertEquals(7, maxf.doubleValue(), 0.001);
      assertEquals(7.5, avgi.doubleValue(), 0.001);
      assertEquals(5.5, avgf.doubleValue(), 0.001);
      assertEquals(2, count.doubleValue(), 0.001);
    } finally {
      solrClientCache.close();
    }
  }

  @Test
  // commented out on: 17-Feb-2019   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 6-Sep-2018
  public void testZeroParallelReducerStream() throws Exception {

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
    try {
      SolrParams sParamsA = mapParams("q", "a_s:blah", "fl", "id,a_s,a_i,a_f", "sort", "a_s asc,a_f asc", "partitionKeys", "a_s", "qt", "/export");
      CloudSolrStream stream = new CloudSolrStream(zkHost, COLLECTIONORALIAS, sParamsA);
      ReducerStream rstream = new ReducerStream(stream,
          new FieldEqualitor("a_s"),
          new GroupOperation(new FieldComparator("a_s", ComparatorOrder.ASCENDING), 2));
      ParallelStream pstream = parallelStream(rstream, new FieldComparator("a_s", ComparatorOrder.ASCENDING));
      attachStreamFactory(pstream);
      pstream.setStreamContext(streamContext);
      List<Tuple> tuples = getTuples(pstream);
      assert (tuples.size() == 0);
    } finally {
      solrClientCache.close();
    }

  }

  @Test
  public void testTuple() throws Exception {

    new UpdateRequest()
        .add(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "5.1", "s_multi", "a", "s_multi", "b", "i_multi",
            "1", "i_multi", "2", "f_multi", "1.2", "f_multi", "1.3")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);

    try {
      SolrParams sParams = mapParams("q", "*:*", "fl", "id,a_s,a_i,a_f,s_multi,i_multi,f_multi", "sort", "a_s asc");
      CloudSolrStream stream = new CloudSolrStream(zkHost, COLLECTIONORALIAS, sParams);
      stream.setStreamContext(streamContext);
      List<Tuple> tuples = getTuples(stream);
      Tuple tuple = tuples.get(0);

      String s = tuple.getString("a_s");
      assertEquals("hello0", s);


      long l = tuple.getLong("a_i");
      assertEquals(0, l);

      double d = tuple.getDouble("a_f");
      assertEquals(5.1, d, 0.001);


      List<String> stringList = tuple.getStrings("s_multi");
      assertEquals("a", stringList.get(0));
      assertEquals("b", stringList.get(1));

      List<Long> longList = tuple.getLongs("i_multi");
      assertEquals(1, longList.get(0).longValue());
      assertEquals(2, longList.get(1).longValue());

      List<Double> doubleList = tuple.getDoubles("f_multi");
      assertEquals(1.2, doubleList.get(0).doubleValue(), 0.001);
      assertEquals(1.3, doubleList.get(1).doubleValue(), 0.001);
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

    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);

    try {
      //Test ascending
      SolrParams sParamsA = mapParams("q", "id:(4 1)", "fl", "id,a_s,a_i", "sort", "a_i asc");
      CloudSolrStream streamA = new CloudSolrStream(zkHost, COLLECTIONORALIAS, sParamsA);

      SolrParams sParamsB = mapParams("q", "id:(0 2 3)", "fl", "id,a_s,a_i", "sort", "a_i asc");
      CloudSolrStream streamB = new CloudSolrStream(zkHost, COLLECTIONORALIAS, sParamsB);

      MergeStream mstream = new MergeStream(streamA, streamB, new FieldComparator("a_i", ComparatorOrder.ASCENDING));
      mstream.setStreamContext(streamContext);
      List<Tuple> tuples = getTuples(mstream);

      assertEquals(5, tuples.size());
      assertOrder(tuples, 0, 1, 2, 3, 4);

      //Test descending
      sParamsA = mapParams("q", "id:(4 1)", "fl", "id,a_s,a_i", "sort", "a_i desc");
      streamA = new CloudSolrStream(zkHost, COLLECTIONORALIAS, sParamsA);

      sParamsB = mapParams("q", "id:(0 2 3)", "fl", "id,a_s,a_i", "sort", "a_i desc");
      streamB = new CloudSolrStream(zkHost, COLLECTIONORALIAS, sParamsB);

      mstream = new MergeStream(streamA, streamB, new FieldComparator("a_i", ComparatorOrder.DESCENDING));
      mstream.setStreamContext(streamContext);
      tuples = getTuples(mstream);

      assertEquals(5, tuples.size());
      assertOrder(tuples, 4, 3, 2, 1, 0);

      //Test compound sort

      sParamsA = mapParams("q", "id:(2 4 1)", "fl", "id,a_s,a_i,a_f", "sort", "a_f asc,a_i asc");
      streamA = new CloudSolrStream(zkHost, COLLECTIONORALIAS, sParamsA);

      sParamsB = mapParams("q", "id:(0 3)", "fl", "id,a_s,a_i,a_f", "sort", "a_f asc,a_i asc");
      streamB = new CloudSolrStream(zkHost, COLLECTIONORALIAS, sParamsB);

      mstream = new MergeStream(streamA, streamB, new MultipleFieldComparator(new FieldComparator("a_f", ComparatorOrder.ASCENDING), new FieldComparator("a_i", ComparatorOrder.ASCENDING)));
      mstream.setStreamContext(streamContext);
      tuples = getTuples(mstream);

      assertEquals(5, tuples.size());
      assertOrder(tuples, 0, 2, 1, 3, 4);

      sParamsA = mapParams("q", "id:(2 4 1)", "fl", "id,a_s,a_i,a_f", "sort", "a_f asc,a_i desc");
      streamA = new CloudSolrStream(zkHost, COLLECTIONORALIAS, sParamsA);

      sParamsB = mapParams("q", "id:(0 3)", "fl", "id,a_s,a_i,a_f", "sort", "a_f asc,a_i desc");
      streamB = new CloudSolrStream(zkHost, COLLECTIONORALIAS, sParamsB);

      mstream = new MergeStream(streamA, streamB, new MultipleFieldComparator(new FieldComparator("a_f", ComparatorOrder.ASCENDING), new FieldComparator("a_i", ComparatorOrder.DESCENDING)));
      mstream.setStreamContext(streamContext);
      tuples = getTuples(mstream);

      assertEquals(5, tuples.size());
      assertOrder(tuples, 2, 0, 1, 3, 4);
    } finally {
      solrClientCache.close();
    }
  }

  @Test
  // commented out on: 17-Feb-2019   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 6-Sep-2018
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

    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);

    try {
      //Test ascending
      SolrParams sParamsA = mapParams("q", "id:(4 1 8 7 9)", "fl", "id,a_s,a_i", "sort", "a_i asc", "partitionKeys", "a_i", "qt", "/export");
      CloudSolrStream streamA = new CloudSolrStream(zkHost, COLLECTIONORALIAS, sParamsA);

      SolrParams sParamsB = mapParams("q", "id:(0 2 3 6)", "fl", "id,a_s,a_i", "sort", "a_i asc", "partitionKeys", "a_i", "qt", "/export");
      CloudSolrStream streamB = new CloudSolrStream(zkHost, COLLECTIONORALIAS, sParamsB);

      MergeStream mstream = new MergeStream(streamA, streamB, new FieldComparator("a_i", ComparatorOrder.ASCENDING));
      ParallelStream pstream = parallelStream(mstream, new FieldComparator("a_i", ComparatorOrder.ASCENDING));
      attachStreamFactory(pstream);
      pstream.setStreamContext(streamContext);
      List<Tuple> tuples = getTuples(pstream);

      assertEquals(9, tuples.size());
      assertOrder(tuples, 0, 1, 2, 3, 4, 7, 6, 8, 9);

      //Test descending
      sParamsA = mapParams("q", "id:(4 1 8 9)", "fl", "id,a_s,a_i", "sort", "a_i desc", "partitionKeys", "a_i", "qt", "/export");
      streamA = new CloudSolrStream(zkHost, COLLECTIONORALIAS, sParamsA);

      sParamsB = mapParams("q", "id:(0 2 3 6)", "fl", "id,a_s,a_i", "sort", "a_i desc", "partitionKeys", "a_i", "qt", "/export");
      streamB = new CloudSolrStream(zkHost, COLLECTIONORALIAS, sParamsB);

      mstream = new MergeStream(streamA, streamB, new FieldComparator("a_i", ComparatorOrder.DESCENDING));
      pstream = parallelStream(mstream, new FieldComparator("a_i", ComparatorOrder.DESCENDING));
      attachStreamFactory(pstream);
      pstream.setStreamContext(streamContext);
      tuples = getTuples(pstream);

      assertEquals(8, tuples.size());
      assertOrder(tuples, 9, 8, 6, 4, 3, 2, 1, 0);
    } finally {
      solrClientCache.close();
    }
  }

  @Test
  public void testParallelEOF() throws Exception {

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

    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);

    try {
      //Test ascending
      SolrParams sParamsA = mapParams("q", "id:(4 1 8 7 9)", "fl", "id,a_s,a_i", "sort", "a_i asc", "partitionKeys", "a_i", "qt", "/export");
      CloudSolrStream streamA = new CloudSolrStream(zkHost, COLLECTIONORALIAS, sParamsA);

      SolrParams sParamsB = mapParams("q", "id:(0 2 3 6)", "fl", "id,a_s,a_i", "sort", "a_i asc", "partitionKeys", "a_i", "qt", "/export");
      CloudSolrStream streamB = new CloudSolrStream(zkHost, COLLECTIONORALIAS, sParamsB);

      MergeStream mstream = new MergeStream(streamA, streamB, new FieldComparator("a_i", ComparatorOrder.ASCENDING));
      ParallelStream pstream = parallelStream(mstream, new FieldComparator("a_i", ComparatorOrder.ASCENDING));
      attachStreamFactory(pstream);
      pstream.setStreamContext(streamContext);
      List<Tuple> tuples = getTuples(pstream);

      assertEquals(9, tuples.size());
      Map<String, Tuple> eofTuples = pstream.getEofTuples();
      assertEquals(numWorkers, eofTuples.size()); // There should be an EOF Tuple for each worker.
    } finally {
      solrClientCache.close();
    }
  }

  @Test
  public void streamTests() throws Exception {

    new UpdateRequest()
        .add(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "0")
        .add(id, "2", "a_s", "hello2", "a_i", "2", "a_f", "0")
        .add(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3")
        .add(id, "4", "a_s", "hello4", "a_i", "4", "a_f", "4")
        .add(id, "1", "a_s", "hello1", "a_i", "1", "a_f", "1")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);


    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);
    //Basic CloudSolrStream Test with Descending Sort

    try {
      SolrParams sParams = mapParams("q", "*:*", "fl", "id,a_s,a_i", "sort", "a_i desc");
      CloudSolrStream stream = new CloudSolrStream(zkHost, COLLECTIONORALIAS, sParams);
      stream.setStreamContext(streamContext);
      List<Tuple> tuples = getTuples(stream);

      assertEquals(5, tuples.size());
      assertOrder(tuples, 4, 3, 2, 1, 0);

      //With Ascending Sort
      sParams = mapParams("q", "*:*", "fl", "id,a_s,a_i", "sort", "a_i asc");
      stream = new CloudSolrStream(zkHost, COLLECTIONORALIAS, sParams);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);

      assertEquals(5, tuples.size());
      assertOrder(tuples, 0, 1, 2, 3, 4);


      //Test compound sort
      sParams = mapParams("q", "*:*", "fl", "id,a_s,a_i,a_f", "sort", "a_f asc,a_i desc");
      stream = new CloudSolrStream(zkHost, COLLECTIONORALIAS, sParams);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);

      assertEquals(5, tuples.size());
      assertOrder(tuples, 2, 0, 1, 3, 4);


      sParams = mapParams("q", "*:*", "fl", "id,a_s,a_i,a_f", "sort", "a_f asc,a_i asc");
      stream = new CloudSolrStream(zkHost, COLLECTIONORALIAS, sParams);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);

      assertEquals(5, tuples.size());
      assertOrder(tuples, 0, 2, 1, 3, 4);
    } finally {
      solrClientCache.close();
    }
  }

  @Test
  public void testDateBoolSorting() throws Exception {

    new UpdateRequest()
        .add(id, "0", "b_sing", "false", "dt_sing", "1981-03-04T01:02:03.78Z")
        .add(id, "3", "b_sing", "true", "dt_sing", "1980-03-04T01:02:03.78Z")
        .add(id, "2", "b_sing", "false", "dt_sing", "1981-04-04T01:02:03.78Z")
        .add(id, "1", "b_sing", "true", "dt_sing", "1980-04-04T01:02:03.78Z")
        .add(id, "4", "b_sing", "true", "dt_sing", "1980-04-04T01:02:03.78Z")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);


    trySortWithQt("/export");
    trySortWithQt("/select");
  }
  private void trySortWithQt(String which) throws Exception {
    //Basic CloudSolrStream Test bools desc

    SolrParams sParams = mapParams("q", "*:*", "qt", which, "fl", "id,b_sing", "sort", "b_sing asc,id asc");
    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);
    CloudSolrStream stream = new CloudSolrStream(zkHost, COLLECTIONORALIAS, sParams);

    try  {
      stream.setStreamContext(streamContext);
      List<Tuple> tuples = getTuples(stream);

      assertEquals(5, tuples.size());
      assertOrder(tuples, 0, 2, 1, 3, 4);

      //Basic CloudSolrStream Test bools desc
      sParams = mapParams("q", "*:*", "qt", which, "fl", "id,b_sing", "sort", "b_sing desc,id desc");
      stream = new CloudSolrStream(zkHost, COLLECTIONORALIAS, sParams);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);

      assertEquals(5, tuples.size());
      assertOrder(tuples, 4, 3, 1, 2, 0);

      //Basic CloudSolrStream Test dates desc
      sParams = mapParams("q", "*:*", "qt", which, "fl", "id,dt_sing", "sort", "dt_sing desc,id asc");
      stream = new CloudSolrStream(zkHost, COLLECTIONORALIAS, sParams);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);

      assertEquals(5, tuples.size());
      assertOrder(tuples, 2, 0, 1, 4, 3);

      //Basic CloudSolrStream Test ates desc
      sParams = mapParams("q", "*:*", "qt", which, "fl", "id,dt_sing", "sort", "dt_sing asc,id desc");
      stream = new CloudSolrStream(zkHost, COLLECTIONORALIAS, sParams);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);

      assertEquals (5,tuples.size());
      assertOrder(tuples, 3, 4, 1, 0, 2);
    } finally {
      solrClientCache.close();
    }

  }


  @Test
  public void testAllValidExportTypes() throws Exception {

    //Test whether all the expected types are actually returned, including booleans and dates.
    // The contract is that the /select and /export handlers return the same format, so we can test this once each
    // way
    new UpdateRequest()
        .add(id, "0", "i_sing", "11", "i_multi", "12", "i_multi", "13",
            "l_sing", "14", "l_multi", "15", "l_multi", "16",
            "f_sing", "1.70", "f_multi", "1.80", "f_multi", "1.90",
            "d_sing", "1.20", "d_multi", "1.21", "d_multi", "1.22",
            "s_sing", "single", "s_multi", "sm1", "s_multi", "sm2",
            "dt_sing", "1980-01-02T11:11:33.89Z", "dt_multi", "1981-03-04T01:02:03.78Z", "dt_multi", "1981-05-24T04:05:06.99Z",
            "b_sing", "true", "b_multi", "false", "b_multi", "true"
        )
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    tryWithQt("/export");
    tryWithQt("/select");
  }

  // We should be getting the exact same thing back with both the export and select handlers, so test
  private void tryWithQt(String which) throws IOException {
    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);
    SolrParams sParams = StreamingTest.mapParams("q", "*:*", "qt", which, "fl",
        "id,i_sing,i_multi,l_sing,l_multi,f_sing,f_multi,d_sing,d_multi,dt_sing,dt_multi,s_sing,s_multi,b_sing,b_multi",
        "sort", "i_sing asc");
    try (CloudSolrStream stream = new CloudSolrStream(zkHost, COLLECTIONORALIAS, sParams)) {

      stream.setStreamContext(streamContext);
      Tuple tuple = getTuple(stream); // All I really care about is that all the fields are returned. There's

      assertEquals("Integers should be returned", 11, tuple.getLong("i_sing").longValue());
      assertEquals("MV should be returned for i_multi", 12, tuple.getLongs("i_multi").get(0).longValue());
      assertEquals("MV should be returned for i_multi", 13, tuple.getLongs("i_multi").get(1).longValue());

      assertEquals("longs should be returned", 14, tuple.getLong("l_sing").longValue());
      assertEquals("MV should be returned for l_multi", 15, tuple.getLongs("l_multi").get(0).longValue());
      assertEquals("MV should be returned for l_multi", 16, tuple.getLongs("l_multi").get(1).longValue());

      assertEquals("floats should be returned", 1.7, tuple.getDouble("f_sing").doubleValue(), 0.001);
      assertEquals("MV should be returned for f_multi", 1.8, tuple.getDoubles("f_multi").get(0).doubleValue(), 0.001);
      assertEquals("MV should be returned for f_multi", 1.9, tuple.getDoubles("f_multi").get(1).doubleValue(), 0.001);

      assertEquals("doubles should be returned", 1.2, tuple.getDouble("d_sing").doubleValue(), 0.001);
      assertEquals("MV should be returned for d_multi", 1.21, tuple.getDoubles("d_multi").get(0).doubleValue(), 0.001);
      assertEquals("MV should be returned for d_multi", 1.22, tuple.getDoubles("d_multi").get(1).doubleValue(), 0.001);

      assertTrue("Strings should be returned", tuple.getString("s_sing").equals("single"));
      assertTrue("MV should be returned for s_multi", tuple.getStrings("s_multi").get(0).equals("sm1"));
      assertTrue("MV should be returned for s_multi", tuple.getStrings("s_multi").get(1).equals("sm2"));

      assertTrue("Dates should be returned as Strings", tuple.getString("dt_sing").equals("1980-01-02T11:11:33.890Z"));
      assertTrue("MV dates should be returned as Strings for dt_multi", tuple.getStrings("dt_multi").get(0).equals("1981-03-04T01:02:03.780Z"));
      assertTrue("MV dates should be returned as Strings for dt_multi", tuple.getStrings("dt_multi").get(1).equals("1981-05-24T04:05:06.990Z"));

      // Also test native type conversion
      Date dt = new Date(Instant.parse("1980-01-02T11:11:33.890Z").toEpochMilli());
      assertTrue("Dates should be returned as Dates", tuple.getDate("dt_sing").equals(dt));
      dt = new Date(Instant.parse("1981-03-04T01:02:03.780Z").toEpochMilli());
      assertTrue("MV dates should be returned as Dates for dt_multi", tuple.getDates("dt_multi").get(0).equals(dt));
      dt = new Date(Instant.parse("1981-05-24T04:05:06.990Z").toEpochMilli());
      assertTrue("MV dates should be returned as Dates  for dt_multi", tuple.getDates("dt_multi").get(1).equals(dt));

      assertTrue("Booleans should be returned", tuple.getBool("b_sing"));
      assertFalse("MV boolean should be returned for b_multi", tuple.getBools("b_multi").get(0));
      assertTrue("MV boolean should be returned for b_multi", tuple.getBools("b_multi").get(1));
    } finally {
      solrClientCache.close();
    }

  }

  @Test
  public void testTupleStreamGetShardsPreference() throws Exception {
    StreamContext streamContext = new StreamContext();
    streamContext.setSolrClientCache(new SolrClientCache());
    streamContext.setRequestReplicaListTransformerGenerator(new RequestReplicaListTransformerGenerator(ShardParams.SHARDS_PREFERENCE_REPLICA_TYPE + ":TLOG", null, null, null));

    streamContext.setRequestParams(mapParams(ShardParams.SHARDS_PREFERENCE, ShardParams.SHARDS_PREFERENCE_REPLICA_TYPE + ":nrt"));

    try {
      ZkStateReader zkStateReader = cluster.getSolrClient().getZkStateReader();
      List<String> strings = zkStateReader.aliasesManager.getAliases().resolveAliases(MULTI_REPLICA_COLLECTIONORALIAS);
      String collName = strings.size() > 0 ? strings.get(0) : MULTI_REPLICA_COLLECTIONORALIAS;
      Map<String, String> replicaTypeMap = mapReplicasToReplicaType(zkStateReader.getClusterState().getCollectionOrNull(collName));

      // Test from extra params
      SolrParams sParams = mapParams("q", "*:*", ShardParams.SHARDS_PREFERENCE, ShardParams.SHARDS_PREFERENCE_REPLICA_TYPE + ":pull");
      testTupleStreamSorting(streamContext, sParams, "PULL", replicaTypeMap);

      // Test defaults from streamContext.getParams()
      testTupleStreamSorting(streamContext, new ModifiableSolrParams(), "NRT", replicaTypeMap);

      // Test defaults from the RLTG
      streamContext.setRequestParams(new ModifiableSolrParams());
      testTupleStreamSorting(streamContext, new ModifiableSolrParams(), "TLOG", replicaTypeMap);
    } finally {
      streamContext.getSolrClientCache().close();
    }
  }

  public void testTupleStreamSorting(StreamContext streamContext, SolrParams solrParams, String replicaType, Map<String, String> replicaTypeMap) throws Exception {
    List<String> shards = TupleStream.getShards(cluster.getZkClient().getZkServerAddress(), MULTI_REPLICA_COLLECTIONORALIAS, streamContext, solrParams);
    for (String shard : shards) {
      assertEquals(shard, replicaType.toUpperCase(Locale.ROOT), replicaTypeMap.getOrDefault(shard, "").toUpperCase(Locale.ROOT));
    }
  }

  protected List<Tuple> getTuples(TupleStream tupleStream) throws IOException {
    tupleStream.open();
    List<Tuple> tuples = new ArrayList<>();
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
      String tip = (String)t.get("id");
      if(!tip.equals(Integer.toString(val))) {
        throw new Exception("Found value:"+tip+" expecting:"+val);
      }
      ++i;
    }
    return true;
  }

  protected boolean assertGroupOrder(Tuple tuple, int... ids) throws Exception {
    @SuppressWarnings({"rawtypes"})
    List group = (List)tuple.get("tuples");
    int i=0;
    for(int val : ids) {
      @SuppressWarnings({"rawtypes"})
      Map t = (Map)group.get(i);
      Long tip = (Long)t.get("id");
      if(tip.intValue() != val) {
        throw new Exception("Found value:"+tip.intValue()+" expecting:"+val);
      }
      ++i;
    }
    return true;
  }

  protected boolean assertMaps(@SuppressWarnings({"rawtypes"})List<Map> maps, int... ids) throws Exception {
    if(maps.size() != ids.length) {
      throw new Exception("Expected id count != actual map count:"+ids.length+":"+maps.size());
    }

    int i=0;
    for(int val : ids) {
      @SuppressWarnings({"rawtypes"})
      Map t = maps.get(i);
      String tip = (String)t.get("id");
      if(!tip.equals(Integer.toString(val))) {
        throw new Exception("Found value:"+tip+" expecting:"+val);
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

  private void attachStreamFactory(TupleStream tupleStream) {
    StreamContext streamContext = new StreamContext();
    streamContext.setStreamFactory(streamFactory);
    tupleStream.setStreamContext(streamContext);
  }

  public static SolrParams mapParams(String... vals) {
    ModifiableSolrParams params = new ModifiableSolrParams();
    assertEquals("Parameters passed in here must be in pairs!", 0, (vals.length % 2));
    for (int idx = 0; idx < vals.length; idx += 2) {
      params.add(vals[idx], vals[idx + 1]);
    }
    if(random().nextBoolean()) params.add("wt","javabin");
    return params;
  }

  private ParallelStream parallelStream(TupleStream stream, FieldComparator comparator) throws IOException {
    ParallelStream pstream = new ParallelStream(zkHost, COLLECTIONORALIAS, stream, numWorkers, comparator);
    return pstream;
  }

  public void testCloudSolrStreamWithoutStreamContext() throws Exception {
    SolrParams sParams = StreamingTest.mapParams("q", "*:*", "fl", "id", "sort", "id asc");
    try (CloudSolrStream stream = new CloudSolrStream(zkHost, COLLECTIONORALIAS, sParams)) {
      stream.open();
    }
  }

  @Test
  public void testCloudStreamClientCache() throws Exception {

    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    solrClientCache.getCloudSolrClient(zkHost);
    streamContext.setSolrClientCache(solrClientCache);

    String expr = "search(" + MULTI_REPLICA_COLLECTIONORALIAS + ",q=*:*,fl=\"a_i\", qt=\"/export\", sort=\"a_i asc\")";
    try (CloudSolrStream stream = new CloudSolrStream(StreamExpressionParser.parse(expr), streamFactory)) {
      stream.setStreamContext(streamContext);
      stream.open();
      Tuple t = stream.read();
      while (!t.EOF) {
        // no-op ... just want to iterate over the tuples
        t = stream.read();
      }

      List<String> baseUrls = new LinkedList<>();
      ZkStateReader zkStateReader = cluster.getSolrClient().getZkStateReader();
      List<String> resolved = zkStateReader.aliasesManager.getAliases().resolveAliases(MULTI_REPLICA_COLLECTIONORALIAS);
      Set<String> liveNodes = zkStateReader.getClusterState().getLiveNodes();
      int expectedNumStreams = 0;
      for (String coll : resolved) {
        DocCollection dcoll = zkStateReader.getCollection(coll);
        for (Slice slice : dcoll.getSlices()) {
          ++expectedNumStreams; // one Stream per slice
          for (Replica r : slice.getReplicas()) {
            if (r.isActive(liveNodes)) {
              baseUrls.add(r.getBaseUrl());
            }
          }
        }
      }
      List<TupleStream> solrStreams = stream.children();
      assertEquals(expectedNumStreams, solrStreams.size());
      for (TupleStream next : solrStreams) {
        SolrStream ss = (SolrStream)next;
        assertTrue(baseUrls.contains(ss.getBaseUrl())); // SolrStream uses the baseUrl of the replica and not the coreUrl
      }

    } finally {
      solrClientCache.close();
    }
  }
}
