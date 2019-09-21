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

package org.apache.solr.cloud;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.api.collections.TimeRoutedAlias;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.CompositeIdRouter;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.ImplicitDocRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.util.DateMathParser;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.solr.client.solrj.RoutedAliasTypes.TIME;

/**
 * Direct http tests of the CreateRoutedAlias functionality.
 */
@SolrTestCaseJ4.SuppressSSL
public class CreateRoutedAliasTest extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2).configure();

//    final Properties properties = new Properties();
//    properties.setProperty("immutable", "true"); // we won't modify it in this test
//    new ConfigSetAdminRequest.Create()
//        .setConfigSetName(configName)
//        .setBaseConfigSetName("_default")
//        .setNewConfigSetProperties(properties)
//        .process(cluster.getSolrClient());
  }

  private CloudSolrClient solrClient;

  @Before
  public void doBefore() throws Exception {
    solrClient = getCloudSolrClient(cluster);
  }

  @After
  public void doAfter() throws Exception {
    cluster.deleteAllCollections(); // deletes aliases too

    if (null != solrClient) {
      solrClient.close();
      solrClient = null;
    }
  }

  // This is a fairly complete test where we set many options and see that it both affected the created
  //  collection and that the alias metadata was saved accordingly
  @Test
  public void testV2() throws Exception {
    // note we don't use TZ in this test, thus it's UTC
    final String aliasName = getSaferTestName();

    String createNode = cluster.getRandomJetty(random()).getNodeName();

    final String baseUrl = cluster.getRandomJetty(random()).getBaseUrl().toString();
    //TODO fix Solr test infra so that this /____v2/ becomes /api/
    HttpPost post = new HttpPost(baseUrl + "/____v2/c");
    post.setEntity(new StringEntity("{\n" +
        "  \"create-alias\" : {\n" +
        "    \"name\": \"" + aliasName + "\",\n" +
        "    \"router\" : {\n" +
        "      \"name\": \"time\",\n" +
        "      \"field\": \"evt_dt\",\n" +
        "      \"start\":\"NOW/DAY\",\n" + // small window for test failure once a day.
        "      \"interval\":\"+2HOUR\",\n" +
        "      \"maxFutureMs\":\"14400000\"\n" +
        "    },\n" +
        //TODO should we use "NOW=" param?  Won't work with v2 and is kinda a hack any way since intended for distrib
        "    \"create-collection\" : {\n" +
        "      \"router\": {\n" +
        "        \"name\":\"implicit\",\n" +
        "        \"field\":\"foo_s\"\n" +
        "      },\n" +
        "      \"shards\":\"foo,bar\",\n" +
        "      \"config\":\"_default\",\n" +
        "      \"tlogReplicas\":1,\n" +
        "      \"pullReplicas\":1,\n" +
        "      \"maxShardsPerNode\":4,\n" + // note: we also expect the 'policy' to work fine
        "      \"nodeSet\": '" + createNode + "',\n" +
        "      \"properties\" : {\n" +
        "        \"foobar\":\"bazbam\",\n" +
        "        \"foobar2\":\"bazbam2\"\n" +
        "      }\n" +
        "    }\n" +
        "  }\n" +
        "}", ContentType.APPLICATION_JSON));
    assertSuccess(post);

    Date startDate = DateMathParser.parseMath(new Date(), "NOW/DAY");
    String initialCollectionName = TimeRoutedAlias.formatCollectionNameFromInstant(aliasName, startDate.toInstant());
    // small chance could fail due to "NOW"; see above
    assertCollectionExists(initialCollectionName);

    Thread.sleep(1000);
    // Test created collection:
    final DocCollection coll = solrClient.getClusterStateProvider().getState(initialCollectionName).get();
    //System.err.println(coll);
    //TODO how do we assert the configSet ?
    assertEquals(ImplicitDocRouter.class, coll.getRouter().getClass());
    assertEquals("foo_s", ((Map)coll.get("router")).get("field"));
    assertEquals(2, coll.getSlices().size()); // numShards
    assertEquals(4, coll.getSlices().stream()
        .mapToInt(s -> s.getReplicas().size()).sum()); // num replicas
    // we didn't ask for any NRT replicas
    assertEquals(0, coll.getSlices().stream()
        .mapToInt(s -> s.getReplicas(r -> r.getType() == Replica.Type.NRT).size()).sum());
    //assertEquals(1, coll.getNumNrtReplicas().intValue()); // TODO seems to be erroneous; I figured 'null'
    assertEquals(1, coll.getNumTlogReplicas().intValue()); // per-shard
    assertEquals(1, coll.getNumPullReplicas().intValue()); // per-shard
    assertEquals(4, coll.getMaxShardsPerNode());
    //TODO SOLR-11877 assertEquals(2, coll.getStateFormat());
    assertTrue("nodeSet didn't work?",
        coll.getSlices().stream().flatMap(s -> s.getReplicas().stream())
            .map(Replica::getNodeName).allMatch(createNode::equals));

    // Test Alias metadata:
    Aliases aliases = cluster.getSolrClient().getZkStateReader().getAliases();
    Map<String, String> collectionAliasMap = aliases.getCollectionAliasMap();
    assertEquals(initialCollectionName, collectionAliasMap.get(aliasName));
    Map<String, String> meta = aliases.getCollectionAliasProperties(aliasName);
    //System.err.println(new TreeMap(meta));
    assertEquals("evt_dt",meta.get("router.field"));
    assertEquals("_default",meta.get("create-collection.collection.configName"));
    assertEquals("foo_s",meta.get("create-collection.router.field"));
    assertEquals("bazbam",meta.get("create-collection.property.foobar"));
    assertEquals("bazbam2",meta.get("create-collection.property.foobar2"));
    assertEquals(createNode,meta.get("create-collection.createNodeSet"));
  }

  @Test
  public void testV1() throws Exception {
    final String aliasName = getSaferTestName();
    final String baseUrl = cluster.getRandomJetty(random()).getBaseUrl().toString();
    Instant start = Instant.now().truncatedTo(ChronoUnit.HOURS); // mostly make sure no millis
    HttpGet get = new HttpGet(baseUrl + "/admin/collections?action=CREATEALIAS" +
        "&wt=xml" +
        "&name=" + aliasName +
        "&router.field=evt_dt" +
        "&router.name=time" +
        "&router.start=" + start +
        "&router.interval=%2B30MINUTE" +
        "&create-collection.collection.configName=_default" +
        "&create-collection.router.field=foo_s" +
        "&create-collection.numShards=1" +
        "&create-collection.replicationFactor=2");
    assertSuccess(get);

    String initialCollectionName = TimeRoutedAlias.formatCollectionNameFromInstant(aliasName, start);
    assertCollectionExists(initialCollectionName);

    // Test created collection:
    final DocCollection coll = solrClient.getClusterStateProvider().getState(initialCollectionName).get();
    //TODO how do we assert the configSet ?
    assertEquals(CompositeIdRouter.class, coll.getRouter().getClass());
    assertEquals("foo_s", ((Map)coll.get("router")).get("field"));
    assertEquals(1, coll.getSlices().size()); // numShards
    assertEquals(2, coll.getReplicationFactor().intValue()); // num replicas
    //TODO SOLR-11877 assertEquals(2, coll.getStateFormat());

    // Test Alias metadata
    Aliases aliases = cluster.getSolrClient().getZkStateReader().getAliases();
    Map<String, String> collectionAliasMap = aliases.getCollectionAliasMap();
    String alias = collectionAliasMap.get(aliasName);
    assertNotNull(alias);
    Map<String, String> meta = aliases.getCollectionAliasProperties(aliasName);
    assertNotNull(meta);
    assertEquals("evt_dt",meta.get("router.field"));
    assertEquals("_default",meta.get("create-collection.collection.configName"));
    assertEquals(null,meta.get("start"));
  }

  // TZ should not affect the first collection name if absolute date given for start
  @Test
  public void testTimezoneAbsoluteDate() throws Exception {
    final String aliasName = getSaferTestName();
    try (SolrClient client = getCloudSolrClient(cluster)) {
      CollectionAdminRequest.createTimeRoutedAlias(
          aliasName,
          "2018-01-15T00:00:00Z",
          "+30MINUTE",
          "evt_dt",
          CollectionAdminRequest.createCollection("_ignored_", "_default", 1, 1)
      )
          .setTimeZone(TimeZone.getTimeZone("GMT-10"))
          .process(client);
    }

    assertCollectionExists(aliasName + TIME.getSeparatorPrefix() +"2018-01-15");
  }

  @Test
  public void testCollectionNamesMustBeAbsent() throws Exception {
    CollectionAdminRequest.createCollection("collection1meta", "_default", 2, 1).process(cluster.getSolrClient());
    CollectionAdminRequest.createCollection("collection2meta", "_default", 1, 1).process(cluster.getSolrClient());

    cluster.waitForActiveCollection("collection1meta", 2, 2);
    cluster.waitForActiveCollection("collection2meta", 1, 1);

    waitForState("Expected collection1 to be created with 2 shards and 1 replica", "collection1meta", clusterShape(2, 2));
    waitForState("Expected collection2 to be created with 1 shard and 1 replica", "collection2meta", clusterShape(1, 1));
    ZkStateReader zkStateReader = cluster.getSolrClient().getZkStateReader();
    zkStateReader.createClusterStateWatchersAndUpdate();

    final String baseUrl = cluster.getRandomJetty(random()).getBaseUrl().toString();
    HttpGet get = new HttpGet(baseUrl + "/admin/collections?action=CREATEALIAS" +
        "&wt=json" +
        "&name=" + getTestName() +
        "&collections=collection1meta,collection2meta" +
        "&router.field=evt_dt" +
        "&router.name=time" +
        "&router.start=2018-01-15T00:00:00Z" +
        "&router.interval=%2B30MINUTE" +
        "&create-collection.collection.configName=_default" +
        "&create-collection.numShards=1");
    assertFailure(get, "Collections cannot be specified");
  }

  @Test
  public void testAliasNameMustBeValid() throws Exception {
    final String baseUrl = cluster.getRandomJetty(random()).getBaseUrl().toString();
    HttpGet get = new HttpGet(baseUrl + "/admin/collections?action=CREATEALIAS" +
        "&wt=json" +
        "&name=735741!45" +  // ! not allowed
        "&router.field=evt_dt" +
        "&router.name=time" +
        "&router.start=2018-01-15T00:00:00Z" +
        "&router.interval=%2B30MINUTE" +
        "&create-collection.collection.configName=_default" +
        "&create-collection.numShards=1");
    assertFailure(get, "Invalid alias");
  }

  @Test
  public void testRandomRouterNameFails() throws Exception {
    final String aliasName = getSaferTestName();
    final String baseUrl = cluster.getRandomJetty(random()).getBaseUrl().toString();
    HttpGet get = new HttpGet(baseUrl + "/admin/collections?action=CREATEALIAS" +
        "&wt=json" +
        "&name=" + aliasName +
        "&router.field=evt_dt" +
        "&router.name=tiafasme" + //bad
        "&router.start=2018-01-15T00:00:00Z" +
        "&router.interval=%2B30MINUTE" +
        "&create-collection.collection.configName=_default" +
        "&create-collection.numShards=1");
    assertFailure(get, " is not in supported types, ");
  }

  @Test
  public void testTimeStampWithMsFails() throws Exception {
    final String aliasName = getSaferTestName();
    final String baseUrl = cluster.getRandomJetty(random()).getBaseUrl().toString();
    HttpGet get = new HttpGet(baseUrl + "/admin/collections?action=CREATEALIAS" +
        "&wt=json" +
        "&name=" + aliasName +
        "&router.field=evt_dt" +
        "&router.name=time" +
        "&router.start=2018-01-15T00:00:00.001Z" + // bad: no milliseconds permitted
        "&router.interval=%2B30MINUTE" +
        "&create-collection.collection.configName=_default" +
        "&create-collection.numShards=1");
    assertFailure(get, "Date or date math for start time includes milliseconds");
  }

  @Test
  public void testBadDateMathIntervalFails() throws Exception {
    final String aliasName = getSaferTestName();
    final String baseUrl = cluster.getRandomJetty(random()).getBaseUrl().toString();
    HttpGet get = new HttpGet(baseUrl + "/admin/collections?action=CREATEALIAS" +
        "&wt=json" +
        "&name=" + aliasName +
        "&router.field=evt_dt" +
        "&router.name=time" +
        "&router.start=2018-01-15T00:00:00Z" +
        "&router.interval=%2B30MINUTEx" + // bad; trailing 'x'
        "&router.maxFutureMs=60000" +
        "&create-collection.collection.configName=_default" +
        "&create-collection.numShards=1");
    assertFailure(get, "Unit not recognized");
  }

  @Test
  public void testNegativeFutureFails() throws Exception {
    final String aliasName = getSaferTestName();
    final String baseUrl = cluster.getRandomJetty(random()).getBaseUrl().toString();
    HttpGet get = new HttpGet(baseUrl + "/admin/collections?action=CREATEALIAS" +
        "&wt=json" +
        "&name=" + aliasName +
        "&router.field=evt_dt" +
        "&router.name=time" +
        "&router.start=2018-01-15T00:00:00Z" +
        "&router.interval=%2B30MINUTE" +
        "&router.maxFutureMs=-60000" + // bad: negative
        "&create-collection.collection.configName=_default" +
        "&create-collection.numShards=1");
    assertFailure(get, "must be >= 0");
  }

  @Test
  public void testUnParseableFutureFails() throws Exception {
    final String aliasName = "testAlias";
    final String baseUrl = cluster.getRandomJetty(random()).getBaseUrl().toString();
    HttpGet get = new HttpGet(baseUrl + "/admin/collections?action=CREATEALIAS" +
        "&wt=json" +
        "&name=" + aliasName +
        "&router.field=evt_dt" +
        "&router.name=time" +
        "&router.start=2018-01-15T00:00:00Z" +
        "&router.interval=%2B30MINUTE" +
        "&router.maxFutureMs=SixtyThousandMilliseconds" + // bad
        "&create-collection.collection.configName=_default" +
        "&create-collection.numShards=1");
    assertFailure(get, "SixtyThousandMilliseconds"); //TODO improve SolrParams.getLong
  }

  private void assertSuccess(HttpUriRequest msg) throws IOException {
    CloseableHttpClient httpClient = (CloseableHttpClient) solrClient.getHttpClient();
    try (CloseableHttpResponse response = httpClient.execute(msg)) {
      if (200 != response.getStatusLine().getStatusCode()) {
        System.err.println(EntityUtils.toString(response.getEntity()));
        fail("Unexpected status: " + response.getStatusLine());
      }
    }
  }

  private void assertFailure(HttpUriRequest msg, String expectedErrorSubstring) throws IOException {
    CloseableHttpClient httpClient = (CloseableHttpClient) solrClient.getHttpClient();
    try (CloseableHttpResponse response = httpClient.execute(msg)) {
      assertEquals(400, response.getStatusLine().getStatusCode());
      String entity = EntityUtils.toString(response.getEntity());
      assertTrue("Didn't find expected error string within response: " + entity,
          entity.contains(expectedErrorSubstring));
    }
  }

  private void assertCollectionExists(String name) throws IOException, SolrServerException {
    solrClient.getClusterStateProvider().connect(); // TODO get rid of this
    //  https://issues.apache.org/jira/browse/SOLR-9784?focusedCommentId=16332729

    assertNotNull(name + " not found", solrClient.getClusterStateProvider().getState(name));
    // note: could also do:
    //List collections = CollectionAdminRequest.listCollections(solrClient);
  }

  // not testing collection parameters, those should inherit error checking from the collection creation code.
}
