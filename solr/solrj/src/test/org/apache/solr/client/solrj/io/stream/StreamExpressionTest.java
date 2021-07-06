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

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.zip.GZIPOutputStream;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.io.ClassificationEvaluation;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.ComparatorOrder;
import org.apache.solr.client.solrj.io.comp.FieldComparator;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParser;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.client.solrj.io.stream.metrics.*;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.CoreDescriptor;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

@Slow
@SolrTestCaseJ4.SuppressSSL
@LuceneTestCase.SuppressCodecs({"Lucene3x", "Lucene40","Lucene41","Lucene42","Lucene45"})
public class StreamExpressionTest extends SolrCloudTestCase {

  private static final String COLLECTIONORALIAS = "collection1";
  private static final String FILESTREAM_COLLECTION = "filestream_collection";
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
    if (useAlias) {
      CollectionAdminRequest.createAlias(COLLECTIONORALIAS, collection).process(cluster.getSolrClient());
    }

    // Create a collection for use by the filestream() expression, and place some files there for it to read.
    CollectionAdminRequest.createCollection(FILESTREAM_COLLECTION, "conf", 1, 1).process(cluster.getSolrClient());
    cluster.waitForActiveCollection(FILESTREAM_COLLECTION, 1, 1);
    final Path dataDir = findUserFilesDataDir();
    populateFileStreamData(dataDir);
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

      Map<String, List<String>> shardsMap = new HashMap<>();
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
  public void testSearchFacadeStream() throws Exception {

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
      solrParams.add("expr", "sort(search("+COLLECTIONORALIAS+"), by=\"a_i asc\")");
      SolrStream solrStream = new SolrStream(shardUrls.get(0), solrParams);
      solrStream.setStreamContext(streamContext);
      tuples = getTuples(solrStream);
      assert (tuples.size() == 5);
      assertOrder(tuples, 0, 1, 2, 3, 4);
      assertLong(tuples.get(0), "a_i", 0);
      assertDouble(tuples.get(0), "a_f", 0);
      assertString(tuples.get(0), "a_s", "hello0");

      assertLong(tuples.get(1), "a_i", 1);
      assertDouble(tuples.get(1), "a_f", 1);
      assertString(tuples.get(1), "a_s", "hello1");

      assertLong(tuples.get(2), "a_i", 2);
      assertDouble(tuples.get(2), "a_f", 0);
      assertString(tuples.get(2), "a_s", "hello2");

      assertLong(tuples.get(3), "a_i", 3);
      assertDouble(tuples.get(3), "a_f", 3);
      assertString(tuples.get(3), "a_s", "hello3");

      assertLong(tuples.get(4), "a_i", 4);
      assertDouble(tuples.get(4), "a_f", 4);
      assertString(tuples.get(4), "a_s", "hello4");

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
    String oldVal = System.getProperty("StreamingExpressionMacros", "false");
    System.setProperty("StreamingExpressionMacros", "true");
    try {
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
      assertOrder(tuples, 0, 1, 3, 4);

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
    } finally {
      System.setProperty("StreamingExpressionMacros", oldVal);
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
  public void testRandomStream() throws Exception {

    UpdateRequest update = new UpdateRequest();
    for(int idx = 0; idx < 1000; ++idx){
      String idxString = Integer.toString(idx);
      update.add(id,idxString, "a_s", "hello" + idxString, "a_i", idxString, "a_f", idxString);
    }
    update.commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    StreamExpression expression;
    TupleStream stream;

    StreamFactory factory = new StreamFactory()
        .withCollectionZkHost(COLLECTIONORALIAS, cluster.getZkServer().getZkAddress())
        .withFunctionName("random", RandomFacadeStream.class);


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

      //Exercise the DeepRandomStream with higher rows

      expression = StreamExpressionParser.parse("random(" + COLLECTIONORALIAS + ", q=\"*:*\", rows=\"10001\", fl=\"id, a_i\")");
      stream = factory.constructStream(expression);
      stream.setStreamContext(context);
      List<Tuple> tuples10 = getTuples(stream);
      assert (tuples10.size() == 1000);

      expression = StreamExpressionParser.parse("random(" + COLLECTIONORALIAS + ", q=\"*:*\", rows=\"10001\", fl=\"id, a_i\")");
      stream = factory.constructStream(expression);
      stream.setStreamContext(context);
      List<Tuple> tuples11 = getTuples(stream);
      assert (tuples11.size() == 1000);

      different = false;
      for (int i = 0; i < tuples10.size(); i++) {
        Tuple tuple1 = tuples10.get(i);
        Tuple tuple2 = tuples11.get(i);
        if (!tuple1.get("id").equals(tuple2.get(id))) {
          different = true;
          break;
        }
      }

      assertTrue(different);

      Collections.sort(tuples10, new FieldComparator("id", ComparatorOrder.ASCENDING));
      Collections.sort(tuples11, new FieldComparator("id", ComparatorOrder.ASCENDING));

      for (int i = 0; i < tuples10.size(); i++) {
        Tuple tuple1 = tuples10.get(i);
        Tuple tuple2 = tuples11.get(i);
        if (!tuple1.get("id").equals(tuple2.get(id))) {
          assert(tuple1.getLong("id").equals(tuple2.get("a_i")));
        }
      }



      //Exercise the /stream handler
      ModifiableSolrParams sParams = new ModifiableSolrParams(StreamingTest.mapParams(CommonParams.QT, "/stream"));
      sParams.add("expr", "random(" + COLLECTIONORALIAS + ", q=\"*:*\", rows=\"1\", fl=\"id, a_i\")");
      JettySolrRunner jetty = cluster.getJettySolrRunner(0);
      SolrStream solrStream = new SolrStream(jetty.getBaseUrl().toString() + "/collection1", sParams);
      List<Tuple> tuples4 = getTuples(solrStream);
      assert (tuples4.size() == 1);
      //Assert no x-axis
      assertNull(tuples4.get(0).get("x"));


      sParams = new ModifiableSolrParams(StreamingTest.mapParams(CommonParams.QT, "/stream"));
      sParams.add("expr", "random(" + COLLECTIONORALIAS + ")");
      jetty = cluster.getJettySolrRunner(0);
      solrStream = new SolrStream(jetty.getBaseUrl().toString() + "/collection1", sParams);
      tuples4 = getTuples(solrStream);
      assert(tuples4.size() == 500);
      @SuppressWarnings({"rawtypes"})
      Map fields = tuples4.get(0).getFields();
      assert(fields.containsKey("id"));
      assert(fields.containsKey("a_f"));
      assert(fields.containsKey("a_i"));
      assert(fields.containsKey("a_s"));
      //Assert the x-axis:
      for(int i=0; i<tuples4.size(); i++) {
        assertEquals(tuples4.get(i).getLong("x").longValue(), i);
      }

    } finally {
      cache.close();
    }
  }

  @Test
  public void testKnnSearchStream() throws Exception {

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
      sParams.add("expr", "knnSearch(" + COLLECTIONORALIAS + ", id=\"1\", qf=\"a_t\", rows=\"4\", fl=\"id, score\", mintf=\"1\")");
      JettySolrRunner jetty = cluster.getJettySolrRunner(0);
      SolrStream solrStream = new SolrStream(jetty.getBaseUrl().toString() + "/collection1", sParams);
      List<Tuple> tuples = getTuples(solrStream);
      assertTrue(tuples.size() == 3);
      assertOrder(tuples, 2, 3, 4);

      sParams = new ModifiableSolrParams(StreamingTest.mapParams(CommonParams.QT, "/stream"));
      sParams.add("expr", "knnSearch(" + COLLECTIONORALIAS + ", id=\"1\", qf=\"a_t\", k=\"2\", fl=\"id, score\", mintf=\"1\")");
      solrStream = new SolrStream(jetty.getBaseUrl().toString() + "/collection1", sParams);
      tuples = getTuples(solrStream);
      assertTrue(tuples.size() == 2);
      assertOrder(tuples, 2, 3);

      sParams = new ModifiableSolrParams(StreamingTest.mapParams(CommonParams.QT, "/stream"));
      sParams.add("expr", "knnSearch(" + COLLECTIONORALIAS + ", id=\"1\", qf=\"a_t\", rows=\"4\", fl=\"id, score\", mintf=\"1\", maxdf=\"0\")");
      solrStream = new SolrStream(jetty.getBaseUrl().toString() + "/collection1", sParams);
      tuples = getTuples(solrStream);
      assertTrue(tuples.size() == 0);

      sParams = new ModifiableSolrParams(StreamingTest.mapParams(CommonParams.QT, "/stream"));
      sParams.add("expr", "knnSearch(" + COLLECTIONORALIAS + ", id=\"1\", qf=\"a_t\", rows=\"4\", fl=\"id, score\", mintf=\"1\", maxwl=\"1\")");
      solrStream = new SolrStream(jetty.getBaseUrl().toString() + "/collection1", sParams);
      tuples = getTuples(solrStream);
      assertTrue(tuples.size() == 0);

      sParams = new ModifiableSolrParams(StreamingTest.mapParams(CommonParams.QT, "/stream"));
      sParams.add("expr", "knnSearch(" + COLLECTIONORALIAS + ", id=\"1\", qf=\"a_t\", rows=\"2\", fl=\"id, score\", mintf=\"1\", minwl=\"20\")");
      solrStream = new SolrStream(jetty.getBaseUrl().toString() + "/collection1", sParams);
      tuples = getTuples(solrStream);
      assertTrue(tuples.size() == 0);

    } finally {
      cache.close();
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
    .withFunctionName("count", CountMetric.class)
    .withFunctionName("std", StdMetric.class)
    .withFunctionName("per", PercentileMetric.class)
    .withFunctionName("countDist", CountDistinctMetric.class);

    StreamExpression expression;
    TupleStream stream;
    List<Tuple> tuples;
    StreamContext streamContext = new StreamContext();
    SolrClientCache cache = new SolrClientCache();
    try {
      streamContext.setSolrClientCache(cache);
      String expr = "stats(" + COLLECTIONORALIAS + ", q=*:*, sum(a_i), sum(a_f), min(a_i), min(a_f), max(a_i), max(a_f), avg(a_i), avg(a_f), std(a_i), std(a_f), per(a_i, 50), per(a_f, 50), countDist(a_s), count(*))";
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
      Double stdi = tuple.getDouble("std(a_i)");
      Double stdf = tuple.getDouble("std(a_f)");
      Double peri = tuple.getDouble("per(a_i,50)");
      Double perf = tuple.getDouble("per(a_f,50)");
      Double count = tuple.getDouble("count(*)");
      Long countDist = tuple.getLong("countDist(a_s)");


      assertTrue(sumi.longValue() == 70);
      assertTrue(sumf.doubleValue() == 55.0D);
      assertTrue(mini.doubleValue() == 0.0D);
      assertTrue(minf.doubleValue() == 1.0D);
      assertTrue(maxi.doubleValue() == 14.0D);
      assertTrue(maxf.doubleValue() == 10.0D);
      assertTrue(avgi.doubleValue() == 7.0D);
      assertTrue(avgf.doubleValue() == 5.5D);
      assertTrue(stdi.doubleValue() == 5.196152422706632);
      assertTrue(stdf.doubleValue() == 2.8722813232690143);
      assertTrue(peri.doubleValue() == 7.0D);
      assertTrue(perf.doubleValue() == 5.5D);
      assertTrue(count.doubleValue() == 10);
      assertEquals(countDist.longValue(), 3L);



      //Test without query

      expr = "stats(" + COLLECTIONORALIAS + ", sum(a_i), sum(a_f), min(a_i), min(a_f), max(a_i), max(a_f), avg(a_i), avg(a_f), std(a_i), std(a_f), per(a_i, 50), per(a_f, 50), count(*))";
      expression = StreamExpressionParser.parse(expr);
      stream = factory.constructStream(expression);
      stream.setStreamContext(streamContext);

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
      stdi = tuple.getDouble("std(a_i)");
      stdf = tuple.getDouble("std(a_f)");
      peri = tuple.getDouble("per(a_i,50)");
      perf = tuple.getDouble("per(a_f,50)");
      count = tuple.getDouble("count(*)");

      assertTrue(sumi.longValue() == 70);
      assertTrue(sumf.doubleValue() == 55.0D);
      assertTrue(mini.doubleValue() == 0.0D);
      assertTrue(minf.doubleValue() == 1.0D);
      assertTrue(maxi.doubleValue() == 14.0D);
      assertTrue(maxf.doubleValue() == 10.0D);
      assertTrue(avgi.doubleValue() == 7.0D);
      assertTrue(avgf.doubleValue() == 5.5D);
      assertTrue(stdi.doubleValue() == 5.196152422706632);
      assertTrue(stdf.doubleValue() == 2.8722813232690143);
      assertTrue(peri.doubleValue() == 7.0D);
      assertTrue(perf.doubleValue() == 5.5D);
      assertTrue(count.doubleValue() == 10);

      //Test with shards parameter
      List<String> shardUrls = TupleStream.getShards(cluster.getZkServer().getZkAddress(), COLLECTIONORALIAS, streamContext);
      expr = "stats(myCollection, q=*:*, sum(a_i), sum(a_f), min(a_i), min(a_f), max(a_i), max(a_f), avg(a_i), avg(a_f), std(a_i), std(a_f), per(a_i, 50), per(a_f, 50), count(*))";
      Map<String, List<String>> shardsMap = new HashMap<>();
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
      stdi = tuple.getDouble("std(a_i)");
      stdf = tuple.getDouble("std(a_f)");
      peri = tuple.getDouble("per(a_i,50)");
      perf = tuple.getDouble("per(a_f,50)");
      count = tuple.getDouble("count(*)");

      assertTrue(sumi.longValue() == 70);
      assertTrue(sumf.doubleValue() == 55.0D);
      assertTrue(mini.doubleValue() == 0.0D);
      assertTrue(minf.doubleValue() == 1.0D);
      assertTrue(maxi.doubleValue() == 14.0D);
      assertTrue(maxf.doubleValue() == 10.0D);
      assertTrue(avgi.doubleValue() == 7.0D);
      assertTrue(avgf.doubleValue() == 5.5D);
      assertTrue(stdi.doubleValue() == 5.196152422706632);
      assertTrue(stdf.doubleValue() == 2.8722813232690143);
      assertTrue(peri.doubleValue() == 7.0D);
      assertTrue(perf.doubleValue() == 5.5D);
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
  public void testFacet2DStream() throws Exception {
    new UpdateRequest()
        .add(id, "0",  "diseases_s",  "stroke", "symptoms_s", "confusion", "cases_i", "10")
        .add(id, "1",  "diseases_s",  "cancer", "symptoms_s", "indigestion","cases_i", "5" )
        .add(id, "2",  "diseases_s",  "diabetes", "symptoms_s", "thirsty", "cases_i", "20")
        .add(id, "3",  "diseases_s",  "stroke", "symptoms_s", "confusion", "cases_i", "10")
        .add(id, "4",  "diseases_s",  "bronchus", "symptoms_s", "nausea", "cases_i", "25")
        .add(id, "5",  "diseases_s",  "bronchus", "symptoms_s", "cough", "cases_i", "10")
        .add(id, "6",  "diseases_s",  "bronchus", "symptoms_s", "cough", "cases_i", "10")
        .add(id, "7",  "diseases_s",  "heart attack", "symptoms_s", "indigestion", "cases_i", "5")
        .add(id, "8",  "diseases_s",  "diabetes", "symptoms_s", "urination", "cases_i", "10")
        .add(id, "9",  "diseases_s",  "diabetes", "symptoms_s", "thirsty", "cases_i", "20")
        .add(id, "10", "diseases_s", "diabetes", "symptoms_s", "thirsty", "cases_i", "20")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);
    StreamExpression expression;
    TupleStream stream;
    List<Tuple> tuples;

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    String expr = "facet2D(collection1, q=\"*:*\", x=\"diseases_s\", y=\"symptoms_s\", dimensions=\"3,1\", count(*))";
    paramsLoc.set("expr", expr);
    paramsLoc.set("qt", "/stream");

    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    tuples = getTuples(solrStream);

    assertEquals(tuples.size(), 3);

    Tuple tuple1 = tuples.get(0);
    assertEquals(tuple1.getString("diseases_s"), "diabetes");
    assertEquals(tuple1.getString("symptoms_s"), "thirsty");
    assertEquals(tuple1.getLong("count(*)").longValue(), 3);

    Tuple tuple2 = tuples.get(1);
    assertEquals(tuple2.getString("diseases_s"), "bronchus");
    assertEquals(tuple2.getString("symptoms_s"), "cough");
    assertEquals(tuple2.getLong("count(*)").longValue(), 2);

    Tuple tuple3 = tuples.get(2);
    assertEquals(tuple3.getString("diseases_s"), "stroke");
    assertEquals(tuple3.getString("symptoms_s"), "confusion");
    assertEquals(tuple3.getLong("count(*)").longValue(), 2);


    paramsLoc = new ModifiableSolrParams();
    expr = "facet2D(collection1, x=\"diseases_s\", y=\"symptoms_s\", dimensions=\"3,1\")";
    paramsLoc.set("expr", expr);
    paramsLoc.set("qt", "/stream");

    solrStream = new SolrStream(url, paramsLoc);

    context = new StreamContext();
    solrStream.setStreamContext(context);
    tuples = getTuples(solrStream);

    assertEquals(tuples.size(), 3);

    tuple1 = tuples.get(0);
    assertEquals(tuple1.getString("diseases_s"), "diabetes");
    assertEquals(tuple1.getString("symptoms_s"), "thirsty");
    assertEquals(tuple1.getString("count(*)"), "3");

    tuple2 = tuples.get(1);
    assertEquals(tuple2.getString("diseases_s"), "bronchus");
    assertEquals(tuple2.getString("symptoms_s"), "cough");
    assertEquals(tuple2.getString("count(*)"), "2");

    tuple3 = tuples.get(2);
    assertEquals(tuple3.getString("diseases_s"), "stroke");
    assertEquals(tuple3.getString("symptoms_s"), "confusion");
    assertEquals(tuple3.getString("count(*)"), "2");

    paramsLoc = new ModifiableSolrParams();
    expr = "facet2D(collection1, q=\"*:*\", x=\"diseases_s\", y=\"symptoms_s\", dimensions=\"3,1\", sum(cases_i))";
    paramsLoc.set("expr", expr);
    paramsLoc.set("qt", "/stream");

    solrStream = new SolrStream(url, paramsLoc);

    context = new StreamContext();
    solrStream.setStreamContext(context);
    tuples = getTuples(solrStream);

    assertEquals(tuples.size(), 3);


    tuple1 = tuples.get(0);
    assertEquals(tuple1.getString("diseases_s"), "diabetes");
    assertEquals(tuple1.getString("symptoms_s"), "thirsty");
    assertEquals(tuple1.getLong("sum(cases_i)").longValue(), 60L);

    tuple2 = tuples.get(1);
    assertEquals(tuple2.getString("diseases_s"), "bronchus");
    assertEquals(tuple2.getString("symptoms_s"), "nausea");
    assertEquals(tuple2.getLong("sum(cases_i)").longValue(), 25L);


    tuple3 = tuples.get(2);
    assertEquals(tuple3.getString("diseases_s"), "stroke");
    assertEquals(tuple3.getString("symptoms_s"), "confusion");
    assertEquals(tuple3.getLong("sum(cases_i)").longValue(), 20L);


    paramsLoc = new ModifiableSolrParams();
    expr = "facet2D(collection1, q=\"*:*\", x=\"diseases_s\", y=\"symptoms_s\", dimensions=\"3,1\", avg(cases_i))";
    paramsLoc.set("expr", expr);
    paramsLoc.set("qt", "/stream");

    solrStream = new SolrStream(url, paramsLoc);

    context = new StreamContext();
    solrStream.setStreamContext(context);
    tuples = getTuples(solrStream);

    assertEquals(tuples.size(), 3);

    tuple1 = tuples.get(0);
    assertEquals(tuple1.getString("diseases_s"), "diabetes");
    assertEquals(tuple1.getString("symptoms_s"), "thirsty");
    assertEquals(tuple1.getLong("avg(cases_i)").longValue(), 20);

    tuple2 = tuples.get(1);
    assertEquals(tuple2.getString("diseases_s"), "bronchus");
    assertEquals(tuple2.getString("symptoms_s"), "nausea");
    assertEquals(tuple2.getLong("avg(cases_i)").longValue(), 25);

    tuple3 = tuples.get(2);
    assertEquals(tuple3.getString("diseases_s"), "stroke");
    assertEquals(tuple3.getString("symptoms_s"), "confusion");
    assertEquals(tuple3.getLong("avg(cases_i)").longValue(), 10);

    paramsLoc = new ModifiableSolrParams();
    expr = "facet2D(collection1, q=\"*:*\", x=\"diseases_s\", y=\"symptoms_s\", dimensions=\"2,2\")";
    paramsLoc.set("expr", expr);
    paramsLoc.set("qt", "/stream");

    solrStream = new SolrStream(url, paramsLoc);

    context = new StreamContext();
    solrStream.setStreamContext(context);
    tuples = getTuples(solrStream);

    assertEquals(tuples.size(), 4);

    tuple1 = tuples.get(0);
    assertEquals(tuple1.getString("diseases_s"), "diabetes");
    assertEquals(tuple1.getString("symptoms_s"), "thirsty");
    assertEquals(tuple1.getLong("count(*)").longValue(), 3);

    tuple2 = tuples.get(1);
    assertEquals(tuple2.getString("diseases_s"), "diabetes");
    assertEquals(tuple2.getString("symptoms_s"), "urination");
    assertEquals(tuple2.getLong("count(*)").longValue(), 1);


    tuple3 = tuples.get(2);
    assertEquals(tuple3.getString("diseases_s"), "bronchus");
    assertEquals(tuple3.getString("symptoms_s"), "cough");
    assertEquals(tuple3.getLong("count(*)").longValue(), 2);

    Tuple tuple4 = tuples.get(3);
    assertEquals(tuple4.getString("diseases_s"), "bronchus");
    assertEquals(tuple4.getString("symptoms_s"), "nausea");
    assertEquals(tuple4.getLong("count(*)").longValue(), 1);
  }


  @Test
  public void testDrillStream() throws Exception {

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

    List<Tuple> tuples;

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    String expr = "rollup(select(drill("
        + "                            collection1, "
        + "                            q=\"*:*\", "
        + "                            fl=\"a_s, a_f\", "
        + "                            sort=\"a_s desc\", "
        + "                            rollup(input(), over=\"a_s\", count(*), sum(a_f)))," +
        "                        a_s, count(*) as cnt, sum(a_f) as saf)," +
        "                  over=\"a_s\"," +
        "                  sum(cnt), sum(saf)"
        + ")";
    paramsLoc.set("expr", expr);
    paramsLoc.set("qt", "/stream");

    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    tuples = getTuples(solrStream);

    Tuple tuple = tuples.get(0);
    String bucket = tuple.getString("a_s");

    Double count = tuple.getDouble("sum(cnt)");
    Double saf = tuple.getDouble("sum(saf)");

    assertTrue(bucket.equals("hello4"));
    assertEquals(count.doubleValue(), 2, 0);
    assertEquals(saf.doubleValue(), 11, 0);

    tuple = tuples.get(1);
    bucket = tuple.getString("a_s");
    count = tuple.getDouble("sum(cnt)");
    saf = tuple.getDouble("sum(saf)");

    assertTrue(bucket.equals("hello3"));
    assertEquals(count.doubleValue(), 4, 0);
    assertEquals(saf.doubleValue(), 26, 0);

    tuple = tuples.get(2);
    bucket = tuple.getString("a_s");
    count = tuple.getDouble("sum(cnt)");
    saf = tuple.getDouble("sum(saf)");

    assertTrue(bucket.equals("hello0"));
    assertTrue(count.doubleValue() == 4);
    assertEquals(saf.doubleValue(), 18, 0);

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
      .withFunctionName("std", StdMetric.class)
      .withFunctionName("per", PercentileMetric.class)
      .withFunctionName("count", CountMetric.class)
      .withFunctionName("countDist", CountDistinctMetric.class);

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
              +   "std(a_i), std(a_f),"
              +   "per(a_i, 50), per(a_f, 50),"
              +   "count(*), countDist(a_i)"
              + ")";

    stream = factory.constructStream(clause);
    tuples = getTuples(stream);

    assert(tuples.size() == 3);


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
    Double stdi = tuple.getDouble("std(a_i)");
    Double stdf = tuple.getDouble("std(a_f)");
    Double peri = tuple.getDouble("per(a_i,50)");
    Double perf = tuple.getDouble("per(a_f,50)");
    Long countDist = tuple.getLong("countDist(a_i)");
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
    assertTrue(stdi.doubleValue() == 3.5D);
    assertTrue(stdf.doubleValue() == 1.5D);
    assertTrue(peri.doubleValue() == 7.5D);
    assertTrue(perf.doubleValue() ==  5.5D);
    assertEquals(countDist.longValue(), 2);


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
    stdi = tuple.getDouble("std(a_i)");
    stdf = tuple.getDouble("std(a_f)");
    peri = tuple.getDouble("per(a_i,50)");
    perf = tuple.getDouble("per(a_f,50)");
    countDist = tuple.getLong("countDist(a_i)");


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
    assertTrue(stdi.doubleValue() == 5.673402858955108D);
    assertTrue(stdf.doubleValue() == 3.5D);
    assertTrue(peri.doubleValue() == 1.5D);
    assertTrue(perf.doubleValue() ==  3.5D);
    assertEquals(countDist.longValue(), 4);


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
    stdi = tuple.getDouble("std(a_i)");
    stdf = tuple.getDouble("std(a_f)");
    peri = tuple.getDouble("per(a_i,50)");
    perf = tuple.getDouble("per(a_f,50)");
    countDist = tuple.getLong("countDist(a_i)");

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
    assertTrue(stdi.doubleValue() == 3.905124837953327D);
    assertTrue(stdf.doubleValue() == 2.29128784747792D);
    assertTrue(peri.doubleValue() == 11.0D);
    assertTrue(perf.doubleValue() ==  7.0D);
    assertEquals(countDist.longValue(), 4);

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
        +   "std(a_i), std(a_f),"
        +   "per(a_i, 50), per(a_f, 50),"
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
    stdi = tuple.getDouble("std(a_i)");
    stdf = tuple.getDouble("std(a_f)");
    peri = tuple.getDouble("per(a_i,50)");
    perf = tuple.getDouble("per(a_f,50)");

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
    assertTrue(stdi.doubleValue() == 3.905124837953327D);
    assertTrue(stdf.doubleValue() == 2.29128784747792D);
    assertTrue(peri.doubleValue() == 11.0D);
    assertTrue(perf.doubleValue() ==  7.0D);


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
    stdi = tuple.getDouble("std(a_i)");
    stdf = tuple.getDouble("std(a_f)");
    peri = tuple.getDouble("per(a_i,50)");
    perf = tuple.getDouble("per(a_f,50)");

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
    assertTrue(stdi.doubleValue() == 5.673402858955108D);
    assertTrue(stdf.doubleValue() == 3.5D);
    assertTrue(peri.doubleValue() == 1.5D);
    assertTrue(perf.doubleValue() ==  3.5D);

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
    stdi = tuple.getDouble("std(a_i)");
    stdf = tuple.getDouble("std(a_f)");
    peri = tuple.getDouble("per(a_i,50)");
    perf = tuple.getDouble("per(a_f,50)");

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
    assertTrue(stdi.doubleValue() == 3.5D);
    assertTrue(stdf.doubleValue() == 1.5D);
    assertTrue(peri.doubleValue() == 7.5D);
    assertTrue(perf.doubleValue() ==  5.5D);


    clause = "facet("
        +   "collection1, "
        +   "q=\"*:*\", "
        +   "fl=\"a_s,a_i,a_f\", "
        +   "sort=\"a_s asc\", "
        +   "buckets=\"a_s\", "
        +   "bucketSorts=\"sum(a_i) desc\", "
        +   "rows=2, "
        +   "sum(a_i), sum(a_f), "
        +   "min(a_i), min(a_f), "
        +   "max(a_i), max(a_f), "
        +   "avg(a_i), avg(a_f), "
        +   "count(*)"
        + ")";

    stream = factory.constructStream(clause);
    tuples = getTuples(stream);


    //Test rows

    tuple = tuples.get(0);
    assertEquals(tuples.size(), 2);

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


    clause = "facet("
        +   "collection1, "
        +   "q=\"*:*\", "
        +   "fl=\"a_s,a_i,a_f\", "
        +   "sort=\"a_s asc\", "
        +   "buckets=\"a_s\", "
        +   "bucketSorts=\"sum(a_i) desc\", "
        +   "rows=2, offset=1, method=dvhash, refine=true,"
        +   "sum(a_i), sum(a_f), "
        +   "min(a_i), min(a_f), "
        +   "max(a_i), max(a_f), "
        +   "avg(a_i), avg(a_f), "
        +   "count(*)"
        + ")";

    stream = factory.constructStream(clause);
    tuples = getTuples(stream);


    //Test offset

    tuple = tuples.get(0);
    assertEquals(tuples.size(), 2);

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
        +   "std(a_i), std(a_f),"
        +   "per(a_i, 50), per(a_f, 50),"
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
    stdi = tuple.getDouble("std(a_i)");
    stdf = tuple.getDouble("std(a_f)");
    peri = tuple.getDouble("per(a_i,50)");
    perf = tuple.getDouble("per(a_f,50)");

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
    assertTrue(stdi.doubleValue() == 3.5D);
    assertTrue(stdf.doubleValue() == 1.5D);
    assertTrue(peri.doubleValue() == 7.5D);
    assertTrue(perf.doubleValue() ==  5.5D);

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
    stdi = tuple.getDouble("std(a_i)");
    stdf = tuple.getDouble("std(a_f)");
    peri = tuple.getDouble("per(a_i,50)");
    perf = tuple.getDouble("per(a_f,50)");

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
    assertTrue(stdi.doubleValue() == 3.905124837953327D);
    assertTrue(stdf.doubleValue() == 2.29128784747792D);
    assertTrue(peri.doubleValue() == 11.0D);
    assertTrue(perf.doubleValue() ==  7.0D);


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
    stdi = tuple.getDouble("std(a_i)");
    stdf = tuple.getDouble("std(a_f)");
    peri = tuple.getDouble("per(a_i,50)");
    perf = tuple.getDouble("per(a_f,50)");

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
    assertTrue(stdi.doubleValue() == 5.673402858955108D);
    assertTrue(stdf.doubleValue() == 3.5D);
    assertTrue(peri.doubleValue() == 1.5D);
    assertTrue(perf.doubleValue() ==  3.5D);


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
  public void testMultiCollection() throws Exception {

    CollectionAdminRequest.createCollection("collection2", "conf", 2, 1).process(cluster.getSolrClient());
    cluster.waitForActiveCollection("collection2", 2, 2);

    new UpdateRequest()
        .add(id, "0", "a_s", "hello", "a_i", "0", "a_f", "0", "s_multi", "aaaa", "test_dt", getDateString("2016", "5", "1"), "i_multi", "4", "i_multi", "7")
        .add(id, "2", "a_s", "hello", "a_i", "2", "a_f", "0", "s_multi", "aaaa1", "test_dt", getDateString("2016", "5", "1"), "i_multi", "44", "i_multi", "77")
        .add(id, "3", "a_s", "hello", "a_i", "3", "a_f", "3", "s_multi", "aaaa2", "test_dt", getDateString("2016", "5", "1"), "i_multi", "444", "i_multi", "777")
        .add(id, "4", "a_s", "hello", "a_i", "4", "a_f", "4", "s_multi", "aaaa3", "test_dt", getDateString("2016", "5", "1"), "i_multi", "4444", "i_multi", "7777")
        .add(id, "1", "a_s", "hello", "a_i", "1", "a_f", "1", "s_multi", "aaaa4", "test_dt", getDateString("2016", "5", "1"), "i_multi", "44444", "i_multi", "77777")
        .commit(cluster.getSolrClient(), "collection1");

    new UpdateRequest()
        .add(id, "10", "a_s", "hello", "a_i", "10", "a_f", "0", "s_multi", "aaaa", "test_dt", getDateString("2016", "5", "1"), "i_multi", "4", "i_multi", "7")
        .add(id, "12", "a_s", "hello", "a_i", "12", "a_f", "0", "s_multi", "aaaa1", "test_dt", getDateString("2016", "5", "1"), "i_multi", "44", "i_multi", "77")
        .add(id, "13", "a_s", "hello", "a_i", "13", "a_f", "3", "s_multi", "aaaa2", "test_dt", getDateString("2016", "5", "1"),  "i_multi", "444", "i_multi", "777")
        .add(id, "14", "a_s", "hello", "a_i", "14", "a_f", "4", "s_multi", "aaaa3", "test_dt", getDateString("2016", "5", "1"), "i_multi", "4444", "i_multi", "7777")
        .add(id, "11", "a_s", "hello", "a_i", "11", "a_f", "1", "s_multi", "aaaa4", "test_dt", getDateString("2016", "5", "1"), "i_multi", "44444", "i_multi", "77777")
        .commit(cluster.getSolrClient(), "collection2");


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
      solrParams.add("expr", "search(\"collection1, collection2\", q=\"*:*\", fl=\"id, a_i\", rows=50, sort=\"a_i asc\")");
      SolrStream solrStream = new SolrStream(shardUrls.get(0), solrParams);
      solrStream.setStreamContext(streamContext);
      tuples = getTuples(solrStream);
      assert (tuples.size() == 10);
      assertOrder(tuples, 0, 1, 2, 3, 4,10,11,12,13,14);

      //Test with export handler, different code path.

      solrParams = new ModifiableSolrParams();
      solrParams.add("qt", "/stream");
      solrParams.add("expr", "search(\"collection1, collection2\", q=\"*:*\", fl=\"id, a_i\", sort=\"a_i asc\", qt=\"/export\")");
      solrStream = new SolrStream(shardUrls.get(0), solrParams);
      solrStream.setStreamContext(streamContext);
      tuples = getTuples(solrStream);
      assert (tuples.size() == 10);
      assertOrder(tuples, 0, 1, 2, 3, 4,10,11,12,13,14);


      solrParams = new ModifiableSolrParams();
      solrParams.add("qt", "/stream");
      solrParams.add("expr", "facet(\"collection1, collection2\", q=\"*:*\", buckets=\"a_s\", bucketSorts=\"count(*) asc\", count(*))");
      solrStream = new SolrStream(shardUrls.get(0), solrParams);
      solrStream.setStreamContext(streamContext);
      tuples = getTuples(solrStream);
      assert (tuples.size() == 1);
      Tuple tuple = tuples.get(0);
      assertEquals(tuple.getString("a_s"), "hello");
      assertEquals(tuple.getLong("count(*)").longValue(), 10);

      String expr = "timeseries(\"collection1, collection2\", q=\"*:*\", " +
              "start=\"2016-01-01T01:00:00.000Z\", " +
              "end=\"2016-12-01T01:00:00.000Z\", " +
              "gap=\"+1YEAR\", " +
              "field=\"test_dt\", " +
              "format=\"yyyy\","+
              "count(*))";

      solrParams = new ModifiableSolrParams();
      solrParams.add("qt", "/stream");
      solrParams.add("expr", expr);
      solrStream = new SolrStream(shardUrls.get(0), solrParams);
      solrStream.setStreamContext(streamContext);
      tuples = getTuples(solrStream);
      assert (tuples.size() == 1);
      tuple = tuples.get(0);
      assertEquals(tuple.getString("test_dt"), "2016");
      assertEquals(tuple.getLong("count(*)").longValue(), 10);

      //Test parallel

      solrParams = new ModifiableSolrParams();
      solrParams.add("qt", "/stream");
      solrParams.add("expr", "parallel(collection1, sort=\"a_i asc\", workers=2, search(\"collection1, collection2\", q=\"*:*\", fl=\"id, a_i\", sort=\"a_i asc\", qt=\"/export\", partitionKeys=\"a_s\"))");
      solrStream = new SolrStream(shardUrls.get(0), solrParams);
      solrStream.setStreamContext(streamContext);
      tuples = getTuples(solrStream);
      assert (tuples.size() == 10);
      assertOrder(tuples, 0, 1, 2, 3, 4,10,11,12,13,14);

    } finally {
      CollectionAdminRequest.deleteCollection("collection2").process(cluster.getSolrClient());
      solrClientCache.close();
    }


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
      .withFunctionName("count", CountMetric.class)
      .withFunctionName("std", StdMetric.class)
      .withFunctionName("per", PercentileMetric.class);

    // Basic test
    clause = "facet("
              +   "collection1, "
              +   "q=\"*:*\", "
              +   "buckets=\"level1_s, level2_s\", "
              +   "bucketSorts=\"sum(a_i) desc, sum(a_i) desc\", "
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
        +   "bucketSorts=\"level1_s desc, level2_s desc\", "
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

    //Add sorts for percentile

    clause = "facet("
        +   "collection1, "
        +   "q=\"*:*\", "
        +   "buckets=\"level1_s, level2_s\", "
        +   "bucketSorts=\"per(a_i, 50) desc, std(a_i) desc\", "
        +   "bucketSizeLimit=100, "
        +   "std(a_i), per(a_i,50)"
        + ")";

    stream = factory.constructStream(clause);
    tuples = getTuples(stream);

    assert(tuples.size() == 6);

    tuple = tuples.get(0);
    bucket1 = tuple.getString("level1_s");
    bucket2 = tuple.getString("level2_s");
    double stdi = tuple.getDouble("std(a_i)");
    double peri = tuple.getDouble("per(a_i,50)");

    assertTrue(bucket1.equals("hello3"));
    assertTrue(bucket2.equals("b"));
    assertTrue(stdi  == 1.2472191289246535D);
    assertTrue(peri == 12.0D);

    tuple = tuples.get(1);
    bucket1 = tuple.getString("level1_s");
    bucket2 = tuple.getString("level2_s");
    stdi = tuple.getDouble("std(a_i)");
    peri = tuple.getDouble("per(a_i,50)");


    assertTrue(bucket1.equals("hello4"));
    assertTrue(bucket2.equals("b"));
    assertTrue(stdi  == 0.0D);
    assertTrue(peri == 11.0);

    tuple = tuples.get(2);
    bucket1 = tuple.getString("level1_s");
    bucket2 = tuple.getString("level2_s");
    stdi = tuple.getDouble("std(a_i)");
    peri = tuple.getDouble("per(a_i,50)");


    assertTrue(bucket1.equals("hello0"));
    assertTrue(bucket2.equals("b"));
    assertTrue(stdi  == 6.5D);
    assertTrue(peri == 7.5D);

    tuple = tuples.get(3);
    bucket1 = tuple.getString("level1_s");
    bucket2 = tuple.getString("level2_s");
    stdi = tuple.getDouble("std(a_i)");
    peri = tuple.getDouble("per(a_i,50)");

    assertTrue(bucket1.equals("hello4"));
    assertTrue(bucket2.equals("a"));
    assertTrue(stdi  == 0.0D);
    assertTrue(peri == 4.0D);

    tuple = tuples.get(4);
    bucket1 = tuple.getString("level1_s");
    bucket2 = tuple.getString("level2_s");
    stdi = tuple.getDouble("std(a_i)");
    peri = tuple.getDouble("per(a_i,50)");

    assertTrue(bucket1.equals("hello3"));
    assertTrue(bucket2.equals("a"));
    assertTrue(stdi  == 0.0D);
    assertTrue(peri == 3.0D);

    tuple = tuples.get(5);
    bucket1 = tuple.getString("level1_s");
    bucket2 = tuple.getString("level2_s");
    stdi = tuple.getDouble("std(a_i)");
    peri = tuple.getDouble("per(a_i,50)");

    assertTrue(bucket1.equals("hello0"));
    assertTrue(bucket2.equals("a"));
    assertTrue(stdi  == 1.0D);
    assertTrue(peri == 1.0D);

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
  // commented 4-Sep-2018 @LuceneTestCase.BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 2-Aug-2018
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
        "end=\"2017-12-01T01:00:00.000Z\", " +
        "gap=\"+1YEAR\", " +
        "field=\"test_dt\", " +
        "count(*), sum(price_f), max(price_f), min(price_f), avg(price_f), std(price_f), per(price_f, 50), countDist(id))";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", expr);
    paramsLoc.set("qt", "/stream");

    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);

    assertTrue(tuples.size() == 5);

    assertTrue(tuples.get(0).get("test_dt").equals("2013-01-01T01:00:00Z"));
    assertTrue(tuples.get(0).getLong("count(*)").equals(100L));
    assertTrue(tuples.get(0).getDouble("sum(price_f)").equals(10000D));
    assertTrue(tuples.get(0).getDouble("max(price_f)").equals(100D));
    assertTrue(tuples.get(0).getDouble("min(price_f)").equals(100D));
    assertTrue(tuples.get(0).getDouble("avg(price_f)").equals(100D));
    assertTrue(tuples.get(0).getDouble("std(price_f)").equals(0D));
    assertTrue(tuples.get(0).getDouble("per(price_f,50)").equals(100D));
    assertTrue(tuples.get(0).getLong("countDist(id)").equals(100L));


    assertTrue(tuples.get(1).get("test_dt").equals("2014-01-01T01:00:00Z"));
    assertTrue(tuples.get(1).getLong("count(*)").equals(50L));
    assertTrue(tuples.get(1).getDouble("sum(price_f)").equals(25000D));
    assertTrue(tuples.get(1).getDouble("max(price_f)").equals(500D));
    assertTrue(tuples.get(1).getDouble("min(price_f)").equals(500D));
    assertTrue(tuples.get(1).getDouble("avg(price_f)").equals(500D));
    assertTrue(tuples.get(1).getDouble("std(price_f)").equals(0D));
    assertTrue(tuples.get(1).getDouble("per(price_f,50)").equals(500D));
    assertTrue(tuples.get(1).getLong("countDist(id)").equals(50L));


    assertTrue(tuples.get(2).get("test_dt").equals("2015-01-01T01:00:00Z"));
    assertTrue(tuples.get(2).getLong("count(*)").equals(50L));
    assertTrue(tuples.get(2).getDouble("sum(price_f)").equals(15000D));
    assertTrue(tuples.get(2).getDouble("max(price_f)").equals(300D));
    assertTrue(tuples.get(2).getDouble("min(price_f)").equals(300D));
    assertTrue(tuples.get(2).getDouble("avg(price_f)").equals(300D));
    assertTrue(tuples.get(2).getDouble("std(price_f)").equals(0D));
    assertTrue(tuples.get(2).getDouble("per(price_f,50)").equals(300D));
    assertTrue(tuples.get(2).getLong("countDist(id)").equals(50L));


    assertTrue(tuples.get(3).get("test_dt").equals("2016-01-01T01:00:00Z"));
    assertTrue(tuples.get(3).getLong("count(*)").equals(50L));
    assertTrue(tuples.get(3).getDouble("sum(price_f)").equals(20000D));
    assertTrue(tuples.get(3).getDouble("max(price_f)").equals(400D));
    assertTrue(tuples.get(3).getDouble("min(price_f)").equals(400D));
    assertTrue(tuples.get(3).getDouble("avg(price_f)").equals(400D));
    assertTrue(tuples.get(3).getDouble("std(price_f)").equals(0D));
    assertTrue(tuples.get(3).getDouble("per(price_f,50)").equals(400D));
    assertTrue(tuples.get(3).getLong("countDist(id)").equals(50L));


    assertTrue(tuples.get(4).get("test_dt").equals("2017-01-01T01:00:00Z"));
    assertEquals((long)tuples.get(4).getLong("count(*)"), 0L);
    assertEquals(tuples.get(4).getDouble("sum(price_f)"), 0D, 0);
    assertEquals(tuples.get(4).getDouble("max(price_f)"),0D, 0);
    assertEquals(tuples.get(4).getDouble("min(price_f)"), 0D, 0);
    assertTrue(tuples.get(4).getDouble("avg(price_f)").equals(0D));
    assertTrue(tuples.get(4).getDouble("std(price_f)").equals(0D));
    assertTrue(tuples.get(4).getDouble("per(price_f,50)").equals(0D));
    assertTrue(tuples.get(4).getLong("countDist(id)").equals(0L));

    expr = "timeseries("+COLLECTIONORALIAS+", q=\"*:*\", start=\"2013-01-01T01:00:00.000Z\", " +
        "end=\"2016-12-01T01:00:00.000Z\", " +
        "gap=\"+1YEAR\", " +
        "field=\"test_dt\", " +
        "format=\"yyyy\", " +
        "count(*), sum(price_f), max(price_f), min(price_f), avg(price_f), std(price_f), per(price_f, 50))";
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
    assertTrue(tuples.get(0).getDouble("avg(price_f)").equals(100D));
    assertTrue(tuples.get(0).getDouble("std(price_f)").equals(0D));
    assertTrue(tuples.get(0).getDouble("per(price_f,50)").equals(100D));

    assertTrue(tuples.get(1).get("test_dt").equals("2014"));
    assertTrue(tuples.get(1).getLong("count(*)").equals(50L));
    assertTrue(tuples.get(1).getDouble("sum(price_f)").equals(25000D));
    assertTrue(tuples.get(1).getDouble("max(price_f)").equals(500D));
    assertTrue(tuples.get(1).getDouble("min(price_f)").equals(500D));
    assertTrue(tuples.get(1).getDouble("avg(price_f)").equals(500D));
    assertTrue(tuples.get(1).getDouble("std(price_f)").equals(0D));
    assertTrue(tuples.get(1).getDouble("per(price_f,50)").equals(500D));


    assertTrue(tuples.get(2).get("test_dt").equals("2015"));
    assertTrue(tuples.get(2).getLong("count(*)").equals(50L));
    assertTrue(tuples.get(2).getDouble("sum(price_f)").equals(15000D));
    assertTrue(tuples.get(2).getDouble("max(price_f)").equals(300D));
    assertTrue(tuples.get(2).getDouble("min(price_f)").equals(300D));
    assertTrue(tuples.get(2).getDouble("avg(price_f)").equals(300D));
    assertTrue(tuples.get(2).getDouble("std(price_f)").equals(0D));
    assertTrue(tuples.get(2).getDouble("per(price_f,50)").equals(300D));

    assertTrue(tuples.get(3).get("test_dt").equals("2016"));
    assertTrue(tuples.get(3).getLong("count(*)").equals(50L));
    assertTrue(tuples.get(3).getDouble("sum(price_f)").equals(20000D));
    assertTrue(tuples.get(3).getDouble("max(price_f)").equals(400D));
    assertTrue(tuples.get(3).getDouble("min(price_f)").equals(400D));
    assertTrue(tuples.get(3).getDouble("avg(price_f)").equals(400D));
    assertTrue(tuples.get(3).getDouble("std(price_f)").equals(0D));
    assertTrue(tuples.get(3).getDouble("per(price_f,50)").equals(400D));


    expr = "timeseries("+COLLECTIONORALIAS+", q=\"*:*\", start=\"2013-01-01T01:00:00.000Z\", " +
        "end=\"2016-12-01T01:00:00.000Z\", " +
        "gap=\"+1YEAR\", " +
        "field=\"test_dt\", " +
        "format=\"yyyy-MM\", " +
        "count(*), sum(price_f), max(price_f), min(price_f), avg(price_f), std(price_f), per(price_f, 50))";
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
    assertTrue(tuples.get(0).getDouble("avg(price_f)").equals(100D));
    assertTrue(tuples.get(0).getDouble("std(price_f)").equals(0D));
    assertTrue(tuples.get(0).getDouble("per(price_f,50)").equals(100D));

    assertTrue(tuples.get(1).get("test_dt").equals("2014-01"));
    assertTrue(tuples.get(1).getLong("count(*)").equals(50L));
    assertTrue(tuples.get(1).getDouble("sum(price_f)").equals(25000D));
    assertTrue(tuples.get(1).getDouble("max(price_f)").equals(500D));
    assertTrue(tuples.get(1).getDouble("min(price_f)").equals(500D));
    assertTrue(tuples.get(1).getDouble("avg(price_f)").equals(500D));
    assertTrue(tuples.get(1).getDouble("std(price_f)").equals(0D));
    assertTrue(tuples.get(1).getDouble("per(price_f,50)").equals(500D));

    assertTrue(tuples.get(2).get("test_dt").equals("2015-01"));
    assertTrue(tuples.get(2).getLong("count(*)").equals(50L));
    assertTrue(tuples.get(2).getDouble("sum(price_f)").equals(15000D));
    assertTrue(tuples.get(2).getDouble("max(price_f)").equals(300D));
    assertTrue(tuples.get(2).getDouble("min(price_f)").equals(300D));
    assertTrue(tuples.get(2).getDouble("avg(price_f)").equals(300D));
    assertTrue(tuples.get(2).getDouble("std(price_f)").equals(0D));
    assertTrue(tuples.get(2).getDouble("per(price_f,50)").equals(300D));

    assertTrue(tuples.get(3).get("test_dt").equals("2016-01"));
    assertTrue(tuples.get(3).getLong("count(*)").equals(50L));
    assertTrue(tuples.get(3).getDouble("sum(price_f)").equals(20000D));
    assertTrue(tuples.get(3).getDouble("max(price_f)").equals(400D));
    assertTrue(tuples.get(3).getDouble("min(price_f)").equals(400D));
    assertTrue(tuples.get(3).getDouble("avg(price_f)").equals(400D));
    assertTrue(tuples.get(3).getDouble("std(price_f)").equals(0D));
    assertTrue(tuples.get(3).getDouble("per(price_f,50)").equals(400D));


    expr = "timeseries("+COLLECTIONORALIAS+", q=\"*:*\", start=\"2012-01-01T01:00:00.000Z\", " +
        "end=\"2016-12-01T01:00:00.000Z\", " +
        "gap=\"+1YEAR\", " +
        "field=\"test_dt\", " +
        "format=\"yyyy-MM\", " +
        "count(*), sum(price_f), max(price_f), min(price_f), avg(price_f), std(price_f), per(price_f, 50))";
    paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", expr);
    paramsLoc.set("qt", "/stream");

    solrStream = new SolrStream(url, paramsLoc);

    solrStream.setStreamContext(context);
    tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 5);
    assertTrue(tuples.get(0).get("test_dt").equals("2012-01"));
    assertTrue(tuples.get(0).getLong("count(*)").equals(0L));
    assertTrue(tuples.get(0).getDouble("sum(price_f)") == 0);
    assertTrue(tuples.get(0).getDouble("max(price_f)") == 0);
    assertTrue(tuples.get(0).getDouble("min(price_f)") == 0);
    assertTrue(tuples.get(0).getDouble("avg(price_f)").equals(0D));
    assertTrue(tuples.get(0).getDouble("std(price_f)").equals(0D));
    assertTrue(tuples.get(0).getDouble("per(price_f,50)").equals(0D));

    assertTrue(tuples.get(1).get("test_dt").equals("2013-01"));
    assertTrue(tuples.get(1).getLong("count(*)").equals(100L));
    assertTrue(tuples.get(1).getDouble("sum(price_f)").equals(10000D));
    assertTrue(tuples.get(1).getDouble("max(price_f)").equals(100D));
    assertTrue(tuples.get(1).getDouble("min(price_f)").equals(100D));
    assertTrue(tuples.get(1).getDouble("avg(price_f)").equals(100D));
    assertTrue(tuples.get(1).getDouble("std(price_f)").equals(0D));
    assertTrue(tuples.get(1).getDouble("per(price_f,50)").equals(100D));

    assertTrue(tuples.get(2).get("test_dt").equals("2014-01"));
    assertTrue(tuples.get(2).getLong("count(*)").equals(50L));
    assertTrue(tuples.get(2).getDouble("sum(price_f)").equals(25000D));
    assertTrue(tuples.get(2).getDouble("max(price_f)").equals(500D));
    assertTrue(tuples.get(2).getDouble("min(price_f)").equals(500D));
    assertTrue(tuples.get(2).getDouble("avg(price_f)").equals(500D));
    assertTrue(tuples.get(2).getDouble("std(price_f)").equals(0D));
    assertTrue(tuples.get(2).getDouble("per(price_f,50)").equals(500D));

    assertTrue(tuples.get(3).get("test_dt").equals("2015-01"));
    assertTrue(tuples.get(3).getLong("count(*)").equals(50L));
    assertTrue(tuples.get(3).getDouble("sum(price_f)").equals(15000D));
    assertTrue(tuples.get(3).getDouble("max(price_f)").equals(300D));
    assertTrue(tuples.get(3).getDouble("min(price_f)").equals(300D));
    assertTrue(tuples.get(3).getDouble("avg(price_f)").equals(300D));
    assertTrue(tuples.get(3).getDouble("std(price_f)").equals(0D));
    assertTrue(tuples.get(3).getDouble("per(price_f,50)").equals(300D));

    assertTrue(tuples.get(4).get("test_dt").equals("2016-01"));
    assertTrue(tuples.get(4).getLong("count(*)").equals(50L));
    assertTrue(tuples.get(4).getDouble("sum(price_f)").equals(20000D));
    assertTrue(tuples.get(4).getDouble("max(price_f)").equals(400D));
    assertTrue(tuples.get(4).getDouble("min(price_f)").equals(400D));
    assertTrue(tuples.get(4).getDouble("avg(price_f)").equals(400D));
    assertTrue(tuples.get(4).getDouble("std(price_f)").equals(0D));
    assertTrue(tuples.get(4).getDouble("per(price_f,50)").equals(400D));
  }

  @Test
  public void testTupleStream() throws Exception {
    UpdateRequest updateRequest = new UpdateRequest();
    updateRequest.add(id, "hello", "test_t", "l b c d c e");
    updateRequest.add(id, "hello1", "test_t", "l b c d c");
    updateRequest.commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    String expr = "search("+COLLECTIONORALIAS+", q=\"`c d c`\", fl=\"id,test_t\", sort=\"id desc\")";

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
    @SuppressWarnings({"unchecked", "rawtypes"})
    List<Map> results  = (List<Map>)tuples.get(0).get("results");
    assertTrue(results.get(0).get("id").equals("hello1"));
    assertTrue(results.get(0).get("test_t").equals("l b c d c"));
    assertTrue(results.get(1).get("id").equals("hello"));
    assertTrue(results.get(1).get("test_t").equals("l b c d c e"));

    assertTrue(tuples.get(0).getLong("sum").equals(2L));

  }

  @Test
  public void testSearchBacktick() throws Exception {
    UpdateRequest updateRequest = new UpdateRequest();
    updateRequest.add(id, "hello", "test_t", "l b c d c e");
    updateRequest.add(id, "hello1", "test_t", "l b c d c");
    updateRequest.commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    String expr = "search("+COLLECTIONORALIAS+", q=\"`c d c e`\", fl=\"id,test_t\", sort=\"id desc\")";

    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", expr);
    paramsLoc.set("qt", "/stream");

    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertTrue(tuples.size() == 1);
    Tuple tuple = tuples.get(0);
    assertTrue(tuple.get("id").equals("hello"));
    assertTrue(tuple.get("test_t").equals("l b c d c e"));
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
    cluster.waitForActiveCollection("destinationCollection", 2, 2);

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
      ClassificationEvaluation evaluation = ClassificationEvaluation.create(lastModel.getFields());
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
  public void testFeaturesSelectionStream() throws Exception {
    Assume.assumeTrue(!useAlias);

    CollectionAdminRequest.createCollection("destinationCollection", "ml", 2, 1).process(cluster.getSolrClient());
    cluster.waitForActiveCollection("destinationCollection", 2, 2);

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
      expression = StreamExpressionParser.parse("update(destinationCollection, " + featuresExpression + ")");
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

      Map<String, List<String>> shardsMap = new HashMap<>();
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
  public void tooLargeForGetRequest() throws IOException, SolrServerException {
    // Test expressions which are larger than GET can handle
    UpdateRequest updateRequest = new UpdateRequest();
    for (int i = 0; i < 10; i++) {
      updateRequest.add(id, "a"+i, "test_t", "a b c d m l");
    }
    for(int i=1; i<=50; i++) {
      updateRequest.add(id, "id_"+(i),"test_dt", getDateString("2016", "5", "1"), "price_f", "400.00");
    }
    updateRequest.commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    SolrClientCache cache = new SolrClientCache();
    StreamContext streamContext = new StreamContext();
    streamContext.setSolrClientCache(cache);
    // use filter() to allow being parsed as 'terms in set' query instead of a (weighted/scored) BooleanQuery
    // so we don't trip too many boolean clauses
    String longQuery = "\"filter(id:(" + IntStream.range(0, 4000).mapToObj(i -> "a").collect(Collectors.joining(" ", "", "")) + "))\"";

    try {
      assertSuccess("significantTerms("+COLLECTIONORALIAS+", q="+longQuery+", field=\"test_t\", limit=3, minTermLength=1, maxDocFreq=\".5\")", streamContext);
      String expr = "timeseries("+COLLECTIONORALIAS+", q="+longQuery+", start=\"2013-01-01T01:00:00.000Z\", " +
              "end=\"2016-12-01T01:00:00.000Z\", " +
              "gap=\"+1YEAR\", " +
              "field=\"test_dt\", " +
              "format=\"yyyy\", " +
              "count(*), sum(price_f), max(price_f), min(price_f))";
      assertSuccess(expr, streamContext);
      expr = "facet("
                    +   "collection1, "
                    +   "q="+longQuery+", "
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
      assertSuccess(expr, streamContext);
      expr = "stats(" + COLLECTIONORALIAS + ", q="+longQuery+", sum(a_i), sum(a_f), min(a_i), min(a_f), max(a_i), max(a_f), avg(a_i), avg(a_f), count(*))";
      assertSuccess(expr, streamContext);
      expr = "search(" + COLLECTIONORALIAS + ", q="+longQuery+", fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\")";
      assertSuccess(expr, streamContext);
      expr = "random(" + COLLECTIONORALIAS + ", q="+longQuery+", rows=\"1000\", fl=\"id, a_i\")";
      assertSuccess(expr, streamContext);
    } finally {
      cache.close();
    }
  }

  @Test
  public void testCatStreamSingleFile() throws Exception {
    final String catStream = "cat(\"topLevel1.txt\")";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", catStream);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+FILESTREAM_COLLECTION;

    SolrStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertEquals(4, tuples.size());

    for (int i = 0; i < 4; i++) {
      Tuple t = tuples.get(i);
      assertEquals("topLevel1.txt line " + String.valueOf(i+1), t.get("line"));
      assertEquals("topLevel1.txt", t.get("file"));
    }
  }

  @Test
  public void testCatStreamSingleGzipFile() throws Exception {
    final String catStream = "cat(\"topLevel1.txt.gz\")";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", catStream);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+FILESTREAM_COLLECTION;

    SolrStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertEquals(4, tuples.size());

    for (int i = 0; i < 4; i++) {
      Tuple t = tuples.get(i);
      assertEquals("topLevel1.txt.gz line " + String.valueOf(i+1), t.get("line"));
      assertEquals("topLevel1.txt.gz", t.get("file"));
    }
  }

  @Test
  public void testCatStreamEmptyFile() throws Exception {
    final String catStream = "cat(\"topLevel-empty.txt\")";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", catStream);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+FILESTREAM_COLLECTION;

    SolrStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);

    assertEquals(0, tuples.size());
  }

  @Test
  public void testCatStreamMultipleFilesOneEmpty() throws Exception {
    final String catStream = "cat(\"topLevel1.txt,topLevel-empty.txt\")";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", catStream);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+FILESTREAM_COLLECTION;

    SolrStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);

    assertEquals(4, tuples.size());

    for (int i = 0; i < 4; i++) {
      Tuple t = tuples.get(i);
      assertEquals("topLevel1.txt line " + String.valueOf(i+1), t.get("line"));
      assertEquals("topLevel1.txt", t.get("file"));
    }
  }

  @Test
  public void testCatStreamMaxLines() throws Exception {
    final String catStream = "cat(\"topLevel1.txt\", maxLines=2)";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", catStream);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+FILESTREAM_COLLECTION;

    SolrStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertEquals(2, tuples.size());

    for (int i = 0; i < 2; i++) {
      Tuple t = tuples.get(i);
      assertEquals("topLevel1.txt line " + String.valueOf(i+1), t.get("line"));
      assertEquals("topLevel1.txt", t.get("file"));
    }
  }

  @Test
  public void testCatStreamDirectoryCrawl() throws Exception {
    final String catStream = "cat(\"directory1\")";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", catStream);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+FILESTREAM_COLLECTION;

    SolrStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertEquals(8, tuples.size());

    final String expectedSecondLevel1Path = "directory1" + File.separator + "secondLevel1.txt";
    for (int i = 0; i < 4; i++) {
      Tuple t = tuples.get(i);
      assertEquals("secondLevel1.txt line " + String.valueOf(i+1), t.get("line"));
      assertEquals(expectedSecondLevel1Path, t.get("file"));
    }

    final String expectedSecondLevel2Path = "directory1" + File.separator + "secondLevel2.txt";
    for (int i = 4; i < 8; i++) {
      Tuple t = tuples.get(i);
      assertEquals("secondLevel2.txt line " + String.valueOf(i - 3), t.get("line"));
      assertEquals(expectedSecondLevel2Path, t.get("file"));
    }
  }

  @Test
  public void testCatStreamMultipleExplicitFiles() throws Exception {
    final String catStream = "cat(\"topLevel1.txt,directory1" + File.separator + "secondLevel2.txt\")";
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", catStream);
    paramsLoc.set("qt", "/stream");
    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+FILESTREAM_COLLECTION;

    SolrStream solrStream = new SolrStream(url, paramsLoc);

    StreamContext context = new StreamContext();
    solrStream.setStreamContext(context);
    List<Tuple> tuples = getTuples(solrStream);
    assertEquals(8, tuples.size());

    for (int i = 0; i < 4; i++) {
      Tuple t = tuples.get(i);
      assertEquals("topLevel1.txt line " + String.valueOf(i+1), t.get("line"));
      assertEquals("topLevel1.txt", t.get("file"));
    }

    final String expectedSecondLevel2Path = "directory1" + File.separator + "secondLevel2.txt";
    for (int i = 4; i < 8; i++) {
      Tuple t = tuples.get(i);
      assertEquals("secondLevel2.txt line " + String.valueOf(i - 3), t.get("line"));
      assertEquals(expectedSecondLevel2Path, t.get("file"));
    }
  }

  private void assertSuccess(String expr, StreamContext streamContext) throws IOException {
    ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
    paramsLoc.set("expr", expr);
    paramsLoc.set("qt", "/stream");

    String url = cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+COLLECTIONORALIAS;
    TupleStream solrStream = new SolrStream(url, paramsLoc);
    solrStream.setStreamContext(streamContext);
    getTuples(solrStream);
  }


  private static Path findUserFilesDataDir() {
    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      final String baseDir = cluster.getBaseDir().toAbsolutePath().toString();
      for (CoreDescriptor coreDescriptor : jetty.getCoreContainer().getCoreDescriptors()) {
        if (coreDescriptor.getCollectionName().equals(FILESTREAM_COLLECTION)) {
          return jetty.getCoreContainer().getUserFilesPath();
        }
      }
    }

    throw new IllegalStateException("Unable to determine data-dir for: "+ FILESTREAM_COLLECTION);
  }

  /**
   * Creates a tree of files underneath a provided data-directory.
   *
   * The filetree created looks like:
   *
   * dataDir
   *   |- topLevel1.txt
   *   |- topLevel2.txt
   *   |- topLevel-empty.txt
   *   |- directory1
   *        |- secondLevel1.txt
   *        |- secondLevel2.txt
   *
   * Each file contains 4 lines.  Each line looks like: "<filename> line <linenumber>"
   */
  private static void populateFileStreamData(Path dataDir) throws Exception {
    Files.createDirectories(dataDir);
    Files.createDirectories(dataDir.resolve("directory1"));

    populateFileWithGzipData(dataDir.resolve("topLevel1.txt.gz"));
    populateFileWithData(dataDir.resolve("topLevel1.txt"));
    populateFileWithData(dataDir.resolve("topLevel2.txt"));
    Files.createFile(dataDir.resolve("topLevel-empty.txt"));
    populateFileWithData(dataDir.resolve("directory1").resolve("secondLevel1.txt"));
    populateFileWithData(dataDir.resolve("directory1").resolve("secondLevel2.txt"));
  }

  private static void populateFileWithData(Path dataFile) throws Exception {
    Files.createFile(dataFile);
    try (final BufferedWriter writer = Files.newBufferedWriter(dataFile, StandardCharsets.UTF_8)) {
      for (int i = 1; i <=4; i++) {
        writer.write(dataFile.getFileName() + " line " + i);
        writer.newLine();
      }
    }
  }

  private static void populateFileWithGzipData(Path dataFile) throws Exception {
    Files.createFile(dataFile);
    try (final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(dataFile.toFile())), StandardCharsets.UTF_8))) {
      for (int i = 1; i <=4; i++) {
        writer.write(dataFile.getFileName() + " line " + i);
        writer.newLine();
      }
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

  public boolean assertDouble(Tuple tuple, String fieldName, double d) throws Exception {
    double dv = tuple.getDouble(fieldName);
    if(dv != d) {
      throw new Exception("Doubles not equal:"+d+" : "+dv);
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

  private void assertTopicRun(TupleStream stream, String... idArray) throws Exception {
    long version = -1;
    int count = 0;
    List<String> ids = new ArrayList<>();
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
    List<String> texts = new ArrayList<>();
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
