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
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.ClusterStateProvider;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.ZkClientClusterStateProvider;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.solr.common.cloud.ZkStateReader.ALIASES;

public class AliasIntegrationTest extends SolrCloudTestCase {

  private CloseableHttpClient httpClient;
  private CloudSolrClient solrClient;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    solrClient = getCloudSolrClient(cluster);
    httpClient = (CloseableHttpClient) solrClient.getHttpClient();
  }

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    IOUtils.close(solrClient, httpClient);

    cluster.deleteAllCollections(); // note: deletes aliases too
  }

  @Test
  public void testProperties() throws Exception {
    CollectionAdminRequest.createCollection("collection1meta", "conf", 2, 1).process(cluster.getSolrClient());
    CollectionAdminRequest.createCollection("collection2meta", "conf", 1, 1).process(cluster.getSolrClient());

    cluster.waitForActiveCollection("collection1meta", 2, 2);
    cluster.waitForActiveCollection("collection2meta", 1, 1);

    waitForState("Expected collection1 to be created with 2 shards and 1 replica", "collection1meta", clusterShape(2, 2));
    waitForState("Expected collection2 to be created with 1 shard and 1 replica", "collection2meta", clusterShape(1, 1));
    ZkStateReader zkStateReader = cluster.getSolrClient().getZkStateReader();
    zkStateReader.createClusterStateWatchersAndUpdate();
    List<String> aliases = zkStateReader.getAliases().resolveAliases("meta1");
    assertEquals(1, aliases.size());
    assertEquals("meta1", aliases.get(0));
    final ZkStateReader.AliasesManager aliasesManager = zkStateReader.aliasesManager;

    aliasesManager.applyModificationAndExportToZk(a -> a.cloneWithCollectionAlias("meta1", "collection1meta,collection2meta"));
    aliases = zkStateReader.getAliases().resolveAliases("meta1");
    assertEquals(2, aliases.size());
    assertEquals("collection1meta", aliases.get(0));
    assertEquals("collection2meta", aliases.get(1));
    //ensure we have the back-compat format in ZK:
    final byte[] rawBytes = zkStateReader.getZkClient().getData(ALIASES, null, null, true);
    //noinspection unchecked
    assertTrue(((Map<String,Map<String,?>>)Utils.fromJSON(rawBytes)).get("collection").get("meta1") instanceof String);

    // set properties
    aliasesManager.applyModificationAndExportToZk(a1 ->
        a1.cloneWithCollectionAliasProperties("meta1", "foo", "bar"));
    Map<String, String> meta = zkStateReader.getAliases().getCollectionAliasProperties("meta1");
    assertNotNull(meta);
    assertTrue(meta.containsKey("foo"));
    assertEquals("bar", meta.get("foo"));

    // set more properties
    aliasesManager.applyModificationAndExportToZk( a1 ->
        a1.cloneWithCollectionAliasProperties("meta1", "foobar", "bazbam"));
    meta = zkStateReader.getAliases().getCollectionAliasProperties("meta1");
    assertNotNull(meta);

    // old properties still there
    assertTrue(meta.containsKey("foo"));
    assertEquals("bar", meta.get("foo"));

    // new properties added
    assertTrue(meta.containsKey("foobar"));
    assertEquals("bazbam", meta.get("foobar"));

    // remove properties
    aliasesManager.applyModificationAndExportToZk(a1 ->
        a1.cloneWithCollectionAliasProperties("meta1", "foo", null));
    meta = zkStateReader.getAliases().getCollectionAliasProperties("meta1");
    assertNotNull(meta);

    // verify key was removed
    assertFalse(meta.containsKey("foo"));

    // but only the specified key was removed
    assertTrue(meta.containsKey("foobar"));
    assertEquals("bazbam", meta.get("foobar"));

    // removal of non existent key should succeed.
    aliasesManager.applyModificationAndExportToZk(a2 ->
        a2.cloneWithCollectionAliasProperties("meta1", "foo", null));

    // chained invocations
    aliasesManager.applyModificationAndExportToZk(a1 ->
        a1.cloneWithCollectionAliasProperties("meta1", "foo2", "bazbam")
        .cloneWithCollectionAliasProperties("meta1", "foo3", "bazbam2"));

    // some other independent update (not overwritten)
    aliasesManager.applyModificationAndExportToZk(a1 ->
        a1.cloneWithCollectionAlias("meta3", "collection1meta,collection2meta"));

    // competing went through
    assertEquals("collection1meta,collection2meta", zkStateReader.getAliases().getCollectionAliasMap().get("meta3"));

    meta = zkStateReader.getAliases().getCollectionAliasProperties("meta1");
    assertNotNull(meta);

    // old properties still there
    assertTrue(meta.containsKey("foobar"));
    assertEquals("bazbam", meta.get("foobar"));

    // competing update not overwritten
    assertEquals("collection1meta,collection2meta", zkStateReader.getAliases().getCollectionAliasMap().get("meta3"));

    // new properties added
    assertTrue(meta.containsKey("foo2"));
    assertEquals("bazbam", meta.get("foo2"));
    assertTrue(meta.containsKey("foo3"));
    assertEquals("bazbam2", meta.get("foo3"));

    // now check that an independently constructed ZkStateReader can see what we've done.
    // i.e. the data is really in zookeeper
    try (SolrZkClient zkClient = new SolrZkClient(cluster.getZkServer().getZkAddress(), 30000)) {
      ZkController.createClusterZkNodes(zkClient);
      try (ZkStateReader zkStateReader2 = new ZkStateReader(zkClient)) {
        zkStateReader2.createClusterStateWatchersAndUpdate();

        meta = zkStateReader2.getAliases().getCollectionAliasProperties("meta1");
        assertNotNull(meta);

        // verify key was removed in independent view
        assertFalse(meta.containsKey("foo"));

        // but only the specified key was removed
        assertTrue(meta.containsKey("foobar"));
        assertEquals("bazbam", meta.get("foobar"));
      }
    }

    // check removal leaves no props behind
    assertEquals(0, zkStateReader.getAliases()
        .cloneWithCollectionAlias("meta1", null) // not persisted to zk on purpose
        .getCollectionAliasProperties("meta1")
        .size());
  }

  @Test
  public void testModifyPropertiesV2() throws Exception {
    final String aliasName = getSaferTestName();
    ZkStateReader zkStateReader = createColectionsAndAlias(aliasName);
    final String baseUrl = cluster.getRandomJetty(random()).getBaseUrl().toString();
    //TODO fix Solr test infra so that this /____v2/ becomes /api/
    HttpPost post = new HttpPost(baseUrl + "/____v2/c");
    post.setEntity(new StringEntity("{\n" +
        "\"set-alias-property\" : {\n" +
        "  \"name\": \"" + aliasName + "\",\n" +
        "  \"properties\" : {\n" +
        "    \"foo\": \"baz\",\n" +
        "    \"bar\": \"bam\"\n" +
        "    }\n" +
        //TODO should we use "NOW=" param?  Won't work with v2 and is kinda a hack any way since intended for distrib
        "  }\n" +
        "}", ContentType.APPLICATION_JSON));
    assertSuccess(post);
    checkFooAndBarMeta(aliasName, zkStateReader);
  }

  @Test
  public void testModifyPropertiesV1() throws Exception {
    // note we don't use TZ in this test, thus it's UTC
    final String aliasName = getSaferTestName();
    ZkStateReader zkStateReader = createColectionsAndAlias(aliasName);
    final String baseUrl = cluster.getRandomJetty(random()).getBaseUrl().toString();
    HttpGet get = new HttpGet(baseUrl + "/admin/collections?action=ALIASPROP" +
        "&wt=xml" +
        "&name=" + aliasName +
        "&property.foo=baz" +
        "&property.bar=bam");
    assertSuccess(get);
    checkFooAndBarMeta(aliasName, zkStateReader);
  }

  @Test
  public void testModifyPropertiesCAR() throws Exception {
    // note we don't use TZ in this test, thus it's UTC
    final String aliasName = getSaferTestName();
    ZkStateReader zkStateReader = createColectionsAndAlias(aliasName);
    CollectionAdminRequest.SetAliasProperty setAliasProperty = CollectionAdminRequest.setAliasProperty(aliasName);
    setAliasProperty.addProperty("foo","baz");
    setAliasProperty.addProperty("bar","bam");
    setAliasProperty.process(cluster.getSolrClient());
    checkFooAndBarMeta(aliasName, zkStateReader);

    // now verify we can delete
    setAliasProperty = CollectionAdminRequest.setAliasProperty(aliasName);
    setAliasProperty.addProperty("foo","");
    setAliasProperty.process(cluster.getSolrClient());
    setAliasProperty = CollectionAdminRequest.setAliasProperty(aliasName);
    setAliasProperty.addProperty("bar",null);
    setAliasProperty.process(cluster.getSolrClient());
    setAliasProperty = CollectionAdminRequest.setAliasProperty(aliasName);

    // whitespace value
    setAliasProperty.addProperty("foo"," ");
    setAliasProperty.process(cluster.getSolrClient());


  }

  @Test
  public void testClusterStateProviderAPI() throws Exception {
    final String aliasName = getSaferTestName();
    
    // pick an arbitrary node, and use it's cloudManager to assert that (an instance of)
    // the ClusterStateProvider API reflects alias changes made by remote clients
    final SolrCloudManager cloudManager = cluster.getRandomJetty(random())
      .getCoreContainer().getZkController().getSolrCloudManager();

    // allthough the purpose of this test is to verify that the ClusterStateProvider API
    // works as a "black box" for inspecting alias information, we'll be doing some "grey box"
    // introspection of the underlying ZKNodeVersion to first verify that alias updates have
    // propogated to our randomly selected node before making assertions against the
    // ClusterStateProvider API...
    //
    // establish a baseline version for future waitForAliasesUpdate calls
    int lastVersion = waitForAliasesUpdate(-1, cloudManager.getClusterStateProvider());

    // create the alias and wait for it to propogate
    createColectionsAndAlias(aliasName);
    lastVersion = waitForAliasesUpdate(-1, cloudManager.getClusterStateProvider());

    // assert ClusterStateProvider sees the alias
    ClusterStateProvider stateProvider = cloudManager.getClusterStateProvider();
    List<String> collections = stateProvider.resolveAlias(aliasName);
    assertEquals(collections.toString(), 2, collections.size());
    assertTrue(collections.toString(), collections.contains("collection1meta"));
    assertTrue(collections.toString(), collections.contains("collection2meta"));

    // modify the alias to have some properties
    CollectionAdminRequest.SetAliasProperty setAliasProperty = CollectionAdminRequest.setAliasProperty(aliasName);
    setAliasProperty.addProperty("foo","baz");
    setAliasProperty.addProperty("bar","bam");
    setAliasProperty.process(cluster.getSolrClient());
    lastVersion = waitForAliasesUpdate(lastVersion, cloudManager.getClusterStateProvider());

    // assert ClusterStateProvider sees the new props (and still sees correct collections)
    stateProvider = cloudManager.getClusterStateProvider();
    Map<String, String> props = stateProvider.getAliasProperties(aliasName);
    assertEquals(props.toString(), 2, props.size());
    assertEquals(props.toString(), "baz", props.get("foo"));
    assertEquals(props.toString(), "bam", props.get("bar"));
    collections = stateProvider.resolveAlias(aliasName);
    assertEquals(collections.toString(), 2, collections.size());
    assertTrue(collections.toString(), collections.contains("collection1meta"));
    assertTrue(collections.toString(), collections.contains("collection2meta"));

    assertFalse("should not be a routed alias", stateProvider.isRoutedAlias(aliasName));
    
    // now make it a routed alias, according to the criteria in the API
    setAliasProperty = CollectionAdminRequest.setAliasProperty(aliasName);
    setAliasProperty.addProperty(CollectionAdminParams.ROUTER_PREFIX + "foo","baz");
    setAliasProperty.process(cluster.getSolrClient());
    lastVersion = waitForAliasesUpdate(lastVersion, cloudManager.getClusterStateProvider());
    
    // assert ClusterStateProvider sees it's routed...
    stateProvider = cloudManager.getClusterStateProvider();
    assertTrue("should be a routed alias", stateProvider.isRoutedAlias(aliasName));

    expectThrows(SolrException.class, () -> {
      String resolved = cloudManager.getClusterStateProvider().resolveSimpleAlias(aliasName);
      fail("this is not a simple alias but it resolved to " + resolved);
      });
  }

  /** 
   * Does a "grey box" assertion that the ClusterStateProvider is a ZkClientClusterStateProvider
   * and then waits for it's underlying ZkStateReader to see the updated aliases, 
   * returning the current ZNodeVersion for the aliases
   */
  private int waitForAliasesUpdate(int lastVersion, ClusterStateProvider stateProvider)
    throws Exception {

    assertTrue("this method does grey box introspection which requires that " +
               "the stateProvider be a ZkClientClusterStateProvider",
               stateProvider instanceof ZkClientClusterStateProvider);
    return waitForAliasesUpdate(lastVersion,
                                ((ZkClientClusterStateProvider)stateProvider).getZkStateReader());
  }
    
  private int waitForAliasesUpdate(int lastVersion, ZkStateReader zkStateReader) throws Exception {
    TimeOut timeOut = new TimeOut(30, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    while (!timeOut.hasTimedOut()) {
      zkStateReader.aliasesManager.update();
      Aliases aliases = zkStateReader.getAliases();
      if (aliases.getZNodeVersion() > lastVersion) {
        return aliases.getZNodeVersion();
      } else if (aliases.getZNodeVersion() < lastVersion) {
        fail("unexpected znode version, expected  greater than " + lastVersion + " but was " + aliases.getZNodeVersion());
      }
      timeOut.sleep(1000);
    }
    if (timeOut.hasTimedOut()) {
      fail("timed out waiting for aliases to update");
    }
    return -1;
  }

  private void checkFooAndBarMeta(String aliasName, ZkStateReader zkStateReader) throws Exception {
    zkStateReader.aliasesManager.update(); // ensure our view is up to date
    Map<String, String> meta = zkStateReader.getAliases().getCollectionAliasProperties(aliasName);
    assertNotNull(meta);
    assertTrue(meta.containsKey("foo"));
    assertEquals("baz", meta.get("foo"));
    assertTrue(meta.containsKey("bar"));
    assertEquals("bam", meta.get("bar"));
  }

  private ZkStateReader createColectionsAndAlias(String aliasName) throws SolrServerException, IOException, KeeperException, InterruptedException {
    CollectionAdminRequest.createCollection("collection1meta", "conf", 2, 1).process(cluster.getSolrClient());
    CollectionAdminRequest.createCollection("collection2meta", "conf", 1, 1).process(cluster.getSolrClient());

    cluster.waitForActiveCollection("collection1meta", 2, 2);
    cluster.waitForActiveCollection("collection2meta", 1, 1);

    waitForState("Expected collection1 to be created with 2 shards and 1 replica", "collection1meta", clusterShape(2, 2));
    waitForState("Expected collection2 to be created with 1 shard and 1 replica", "collection2meta", clusterShape(1, 1));
    ZkStateReader zkStateReader = cluster.getSolrClient().getZkStateReader();
    zkStateReader.createClusterStateWatchersAndUpdate();
    List<String> aliases = zkStateReader.getAliases().resolveAliases(aliasName);
    assertEquals(1, aliases.size());
    assertEquals(aliasName, aliases.get(0));
    UnaryOperator<Aliases> op6 = a -> a.cloneWithCollectionAlias(aliasName, "collection1meta,collection2meta");
    final ZkStateReader.AliasesManager aliasesManager = zkStateReader.aliasesManager;

    aliasesManager.applyModificationAndExportToZk(op6);
    aliases = zkStateReader.getAliases().resolveAliases(aliasName);
    assertEquals(2, aliases.size());
    assertEquals("collection1meta", aliases.get(0));
    assertEquals("collection2meta", aliases.get(1));
    return zkStateReader;
  }

  private void assertSuccess(HttpUriRequest msg) throws IOException {
    try (CloseableHttpResponse response = httpClient.execute(msg)) {
      if (200 != response.getStatusLine().getStatusCode()) {
        System.err.println(EntityUtils.toString(response.getEntity()));
        fail("Unexpected status: " + response.getStatusLine());
      }
    }
  }
  // Rather a long title, but it's common to recommend when people need to re-index for any reason that they:
  // 1> create a new collection
  // 2> index the corpus to the new collection and verify it
  // 3> create an alias pointing to the new collection WITH THE SAME NAME as their original collection
  // 4> delete the old collection.
  //
  // They may or may not have an alias already pointing to the old collection that's being replaced.
  // If they don't already have an alias, this leaves them with:
  //
  // > a collection named old_collection
  // > a collection named new_collection
  // > an alias old_collection->new_collection
  //
  // What happens when they delete old_collection now?
  //
  // Current behavior is that delete "does the right thing" and deletes old_collection rather than new_collection,
  // but if this behavior changes it could be disastrous for users so this test insures that this behavior.
  //
  @Test
  public void testDeleteAliasWithExistingCollectionName() throws Exception {
    CollectionAdminRequest.createCollection("collection_old", "conf", 2, 1).process(cluster.getSolrClient());
    CollectionAdminRequest.createCollection("collection_new", "conf", 1, 1).process(cluster.getSolrClient());

    cluster.waitForActiveCollection("collection_old", 2, 2);
    cluster.waitForActiveCollection("collection_new", 1, 1);

    waitForState("Expected collection_old to be created with 2 shards and 1 replica", "collection_old", clusterShape(2, 2));
    waitForState("Expected collection_new to be created with 1 shard and 1 replica", "collection_new", clusterShape(1, 1));

    new UpdateRequest()
        .add("id", "6", "a_t", "humpty dumpy sat on a wall")
        .add("id", "7", "a_t", "humpty dumpy3 sat on a walls")
        .add("id", "8", "a_t", "humpty dumpy2 sat on a walled")
        .commit(cluster.getSolrClient(), "collection_old");

    new UpdateRequest()
        .add("id", "1", "a_t", "humpty dumpy sat on an unfortunate wall")
        .commit(cluster.getSolrClient(), "collection_new");

    QueryResponse res = cluster.getSolrClient().query("collection_old", new SolrQuery("*:*"));
    assertEquals(3, res.getResults().getNumFound());

    ZkStateReader zkStateReader = cluster.getSolrClient().getZkStateReader();
    int lastVersion = zkStateReader.aliasesManager.getAliases().getZNodeVersion();
    // Let's insure we have a "handle" to the old collection
    CollectionAdminRequest.createAlias("collection_old_reserve", "collection_old").process(cluster.getSolrClient());
    lastVersion = waitForAliasesUpdate(lastVersion, zkStateReader);

    // This is the critical bit. The alias uses the _old collection name.
    CollectionAdminRequest.createAlias("collection_old", "collection_new").process(cluster.getSolrClient());
    lastVersion = waitForAliasesUpdate(lastVersion, zkStateReader);

    // aliases: collection_old->collection_new, collection_old_reserve -> collection_old -> collection_new
    // collections: collection_new and collection_old

    // Now we should only see the doc in collection_new through the collection_old alias
    res = cluster.getSolrClient().query("collection_old", new SolrQuery("*:*"));
    assertEquals(1, res.getResults().getNumFound());

    // Now we should still transitively see collection_new
    res = cluster.getSolrClient().query("collection_old_reserve", new SolrQuery("*:*"));
    assertEquals(1, res.getResults().getNumFound());

    // Now delete the old collection. This should fail since the collection_old_reserve points to collection_old
    RequestStatusState delResp = CollectionAdminRequest.deleteCollection("collection_old").processAndWait(cluster.getSolrClient(), 60);
    assertEquals("Should have failed to delete collection: ", delResp, RequestStatusState.FAILED);

    // assure ourselves that the old colletion is, indeed, still there.
    assertNotNull("collection_old should exist!", cluster.getSolrClient().getZkStateReader().getClusterState().getCollectionOrNull("collection_old"));

    // Now we should still succeed using the alias collection_old which points to collection_new
    // aliase: collection_old -> collection_new, collection_old_reserve -> collection_old -> collection_new
    // collections: collection_old, collection_new
    res = cluster.getSolrClient().query("collection_old", new SolrQuery("*:*"));
    assertEquals(1, res.getResults().getNumFound());

    Aliases aliases = cluster.getSolrClient().getZkStateReader().getAliases();
    assertTrue("collection_old should point to collection_new", aliases.resolveAliases("collection_old").contains("collection_new"));
    assertTrue("collection_old_reserve should point to collection_new", aliases.resolveAliases("collection_old_reserve").contains("collection_new"));

    // Clean up
    CollectionAdminRequest.deleteAlias("collection_old_reserve").processAndWait(cluster.getSolrClient(), 60);
    lastVersion = waitForAliasesUpdate(lastVersion, zkStateReader);
    CollectionAdminRequest.deleteAlias("collection_old").processAndWait(cluster.getSolrClient(), 60);
    lastVersion = waitForAliasesUpdate(lastVersion, zkStateReader);
    CollectionAdminRequest.deleteCollection("collection_new").processAndWait(cluster.getSolrClient(), 60);
    CollectionAdminRequest.deleteCollection("collection_old").processAndWait(cluster.getSolrClient(), 60);
    // collection_old already deleted as well as collection_old_reserve

    assertNull("collection_old_reserve should be gone", cluster.getSolrClient().getZkStateReader().getAliases().getCollectionAliasMap().get("collection_old_reserve"));
    assertNull("collection_old should be gone", cluster.getSolrClient().getZkStateReader().getAliases().getCollectionAliasMap().get("collection_old"));

    assertFalse("collection_new should be gone",
        cluster.getSolrClient().getZkStateReader().getClusterState().hasCollection("collection_new"));

    assertFalse("collection_old should be gone",
        cluster.getSolrClient().getZkStateReader().getClusterState().hasCollection("collection_old"));
  }

  // While writing the above test I wondered what happens when an alias points to two collections and one of them
  // is deleted.
  @Test
  public void testDeleteOneOfTwoCollectionsAliased() throws Exception {
    CollectionAdminRequest.createCollection("collection_one", "conf", 2, 1).process(cluster.getSolrClient());
    CollectionAdminRequest.createCollection("collection_two", "conf", 1, 1).process(cluster.getSolrClient());

    cluster.waitForActiveCollection("collection_one", 2, 2);
    cluster.waitForActiveCollection("collection_two", 1, 1);

    waitForState("Expected collection_one to be created with 2 shards and 1 replica", "collection_one", clusterShape(2, 2));
    waitForState("Expected collection_two to be created with 1 shard and 1 replica", "collection_two", clusterShape(1, 1));

    new UpdateRequest()
        .add("id", "1", "a_t", "humpty dumpy sat on a wall")
        .commit(cluster.getSolrClient(), "collection_one");


    new UpdateRequest()
        .add("id", "10", "a_t", "humpty dumpy sat on a high wall")
        .add("id", "11", "a_t", "humpty dumpy sat on a low wall")
        .commit(cluster.getSolrClient(), "collection_two");

    ZkStateReader zkStateReader = cluster.getSolrClient().getZkStateReader();
    int lastVersion = zkStateReader.aliasesManager.getAliases().getZNodeVersion();

    // Create an alias pointing to both
    CollectionAdminRequest.createAlias("collection_alias_pair", "collection_one,collection_two").process(cluster.getSolrClient());
    lastVersion = waitForAliasesUpdate(lastVersion, zkStateReader);

    QueryResponse res = cluster.getSolrClient().query("collection_alias_pair", new SolrQuery("*:*"));
    assertEquals(3, res.getResults().getNumFound());

    // Now delete one of the collections, should fail since an alias points to it.
    RequestStatusState delResp = CollectionAdminRequest.deleteCollection("collection_one").processAndWait(cluster.getSolrClient(), 60);
    // failed because the collection is a part of a compound alias
    assertEquals("Should have failed to delete collection: ", delResp, RequestStatusState.FAILED);

    CollectionAdminRequest.Delete delete = CollectionAdminRequest.deleteCollection("collection_alias_pair");
    delResp = delete.processAndWait(cluster.getSolrClient(), 60);
    // failed because we tried to delete an alias with followAliases=false
    assertEquals("Should have failed to delete alias: ", delResp, RequestStatusState.FAILED);

    delete.setFollowAliases(true);
    delResp = delete.processAndWait(cluster.getSolrClient(), 60);
    // failed because we tried to delete compound alias
    assertEquals("Should have failed to delete collection: ", delResp, RequestStatusState.FAILED);

    CollectionAdminRequest.createAlias("collection_alias_one", "collection_one").process(cluster.getSolrClient());
    lastVersion = waitForAliasesUpdate(lastVersion, zkStateReader);

    delete = CollectionAdminRequest.deleteCollection("collection_one");
    delResp = delete.processAndWait(cluster.getSolrClient(), 60);
    // failed because we tried to delete collection referenced by multiple aliases
    assertEquals("Should have failed to delete collection: ", delResp, RequestStatusState.FAILED);

    delete = CollectionAdminRequest.deleteCollection("collection_alias_one");
    delete.setFollowAliases(true);
    delResp = delete.processAndWait(cluster.getSolrClient(), 60);
    // failed because we tried to delete collection referenced by multiple aliases
    assertEquals("Should have failed to delete collection: ", delResp, RequestStatusState.FAILED);

    CollectionAdminRequest.deleteAlias("collection_alias_one").process(cluster.getSolrClient());
    lastVersion = waitForAliasesUpdate(lastVersion, zkStateReader);

    // Now redefine the alias to only point to collection two
    CollectionAdminRequest.createAlias("collection_alias_pair", "collection_two").process(cluster.getSolrClient());
    lastVersion = waitForAliasesUpdate(lastVersion, zkStateReader);

    //Delete collection_one.
    delResp = CollectionAdminRequest.deleteCollection("collection_one").processAndWait(cluster.getSolrClient(), 60);

    assertEquals("Should not have failed to delete collection, it was removed from the alias: ", delResp, RequestStatusState.COMPLETED);

    // Should only see two docs now in second collection
    res = cluster.getSolrClient().query("collection_alias_pair", new SolrQuery("*:*"));
    assertEquals(2, res.getResults().getNumFound());

    // We shouldn't be able to ping the deleted collection directly as
    // was deleted (and, assuming that it only points to collection_old).
    try {
      cluster.getSolrClient().query("collection_one", new SolrQuery("*:*"));
      fail("should have failed");
    } catch (SolrServerException | SolrException se) {

    }

    // Clean up
    CollectionAdminRequest.deleteAlias("collection_alias_pair").processAndWait(cluster.getSolrClient(), 60);
    CollectionAdminRequest.deleteCollection("collection_two").processAndWait(cluster.getSolrClient(), 60);
    // collection_one already deleted
    lastVersion = waitForAliasesUpdate(lastVersion, zkStateReader);

    assertNull("collection_alias_pair should be gone",
        cluster.getSolrClient().getZkStateReader().getAliases().getCollectionAliasMap().get("collection_alias_pair"));

    assertFalse("collection_one should be gone",
        cluster.getSolrClient().getZkStateReader().getClusterState().hasCollection("collection_one"));

    assertFalse("collection_two should be gone",
        cluster.getSolrClient().getZkStateReader().getClusterState().hasCollection("collection_two"));

  }


  @Test
  public void test() throws Exception {
    CollectionAdminRequest.createCollection("collection1", "conf", 2, 1).process(cluster.getSolrClient());
    CollectionAdminRequest.createCollection("collection2", "conf", 1, 1).process(cluster.getSolrClient());

    cluster.waitForActiveCollection("collection1", 2, 2);
    cluster.waitForActiveCollection("collection2", 1, 1);

    waitForState("Expected collection1 to be created with 2 shards and 1 replica", "collection1", clusterShape(2, 2));
    waitForState("Expected collection2 to be created with 1 shard and 1 replica", "collection2", clusterShape(1, 1));

    new UpdateRequest()
        .add("id", "6", "a_t", "humpty dumpy sat on a wall")
        .add("id", "7", "a_t", "humpty dumpy3 sat on a walls")
        .add("id", "8", "a_t", "humpty dumpy2 sat on a walled")
        .commit(cluster.getSolrClient(), "collection1");

    new UpdateRequest()
        .add("id", "9", "a_t", "humpty dumpy sat on a wall")
        .add("id", "10", "a_t", "humpty dumpy3 sat on a walls")
        .commit(cluster.getSolrClient(), "collection2");

    ///////////////
    // make sure there's only one level of alias
    ZkStateReader zkStateReader = cluster.getSolrClient().getZkStateReader();
    int lastVersion = zkStateReader.aliasesManager.getAliases().getZNodeVersion();

    CollectionAdminRequest.deleteAlias("collection1").process(cluster.getSolrClient());
    CollectionAdminRequest.createAlias("testalias1", "collection1").process(cluster.getSolrClient());

    // verify proper resolution on the server-side
    lastVersion = waitForAliasesUpdate(lastVersion, zkStateReader);

    Aliases aliases = zkStateReader.getAliases();
    List<String> collections = aliases.resolveAliases("testalias1");
    assertEquals(collections.toString(), 1, collections.size());
    assertTrue(collections.contains("collection1"));

    // ensure that the alias is visible in the API
    assertEquals("collection1",
        new CollectionAdminRequest.ListAliases().process(cluster.getSolrClient()).getAliases().get("testalias1"));

    // search for alias
    searchSeveralWays("testalias1", new SolrQuery("*:*"), 3);

    // Use a comma delimited list, one of which is an alias
    searchSeveralWays("testalias1,collection2", new SolrQuery("*:*"), 5);

    ///////////////
    // test alias pointing to two collections.  collection2 first because it's not on every node
    CollectionAdminRequest.createAlias("testalias2", "collection2,collection1").process(cluster.getSolrClient());

    lastVersion = waitForAliasesUpdate(lastVersion, zkStateReader);

    searchSeveralWays("testalias2", new SolrQuery("*:*"), 5);

    ///////////////
    // update alias
    CollectionAdminRequest.createAlias("testalias2", "collection2").process(cluster.getSolrClient());
    lastVersion = waitForAliasesUpdate(lastVersion, zkStateReader);

    searchSeveralWays("testalias2", new SolrQuery("*:*"), 2);

    ///////////////
    // alias pointing to alias.  One level of indirection is supported; more than that is not (may or may not work)
    CollectionAdminRequest.createAlias("testalias3", "testalias2").process(cluster.getSolrClient());
    lastVersion = waitForAliasesUpdate(lastVersion, zkStateReader);
    searchSeveralWays("testalias3", new SolrQuery("*:*"), 2);

    ///////////////
    // Test 2 aliases pointing to the same collection
    CollectionAdminRequest.createAlias("testalias4", "collection2").process(cluster.getSolrClient());
    lastVersion = waitForAliasesUpdate(lastVersion, zkStateReader);
    CollectionAdminRequest.createAlias("testalias5", "collection2").process(cluster.getSolrClient());
    lastVersion = waitForAliasesUpdate(lastVersion, zkStateReader);

    // add one document to testalias4, thus to collection2
    new UpdateRequest()
        .add("id", "11", "a_t", "humpty dumpy4 sat on a walls")
        .commit(cluster.getSolrClient(), "testalias4"); // thus gets added to collection2

    searchSeveralWays("testalias4", new SolrQuery("*:*"), 3);
    //searchSeveralWays("testalias4,testalias5", new SolrQuery("*:*"), 3);

    ///////////////
    // use v2 API
    new V2Request.Builder("/collections")
        .withMethod(SolrRequest.METHOD.POST)
        .withPayload("{\"create-alias\": {\"name\": \"testalias6\", collections:[\"collection2\",\"collection1\"]}}")
        .build().process(cluster.getSolrClient());

    searchSeveralWays("testalias6", new SolrQuery("*:*"), 6);

    // add one document to testalias6. this should fail because it's a multi-collection non-routed alias
    try {
      new UpdateRequest()
          .add("id", "12", "a_t", "humpty dumpy5 sat on a walls")
          .commit(cluster.getSolrClient(), "testalias6");
      fail("Update to a multi-collection non-routed alias should fail");
    } catch (SolrException e) {
      // expected
      assertEquals(e.toString(), SolrException.ErrorCode.BAD_REQUEST.code, e.code());
    }

    ///////////////
    for (int i = 1; i <= 6 ; i++) {
      CollectionAdminRequest.deleteAlias("testalias" + i).process(cluster.getSolrClient());
      lastVersion = waitForAliasesUpdate(lastVersion, zkStateReader);
    }

    SolrException e = expectThrows(SolrException.class, () -> {
      SolrQuery q = new SolrQuery("*:*");
      q.set("collection", "testalias1");
      cluster.getSolrClient().query(q);
    });
    assertTrue("Unexpected exception message: " + e.getMessage(), e.getMessage().contains("Collection not found: testalias1"));
  }

  private void searchSeveralWays(String collectionList, SolrParams solrQuery, int expectedNumFound) throws IOException, SolrServerException {
    searchSeveralWays(collectionList, solrQuery, res -> assertEquals(expectedNumFound, res.getResults().getNumFound()));
  }

  private void searchSeveralWays(String collectionList, SolrParams solrQuery, Consumer<QueryResponse> responseConsumer) throws IOException, SolrServerException {
    if (random().nextBoolean()) {
      // cluster's CloudSolrClient
      responseConsumer.accept(cluster.getSolrClient().query(collectionList, solrQuery));
    } else {
      // new CloudSolrClient (random shardLeadersOnly)
      try (CloudSolrClient solrClient = getCloudSolrClient(cluster)) {
        if (random().nextBoolean()) {
          solrClient.setDefaultCollection(collectionList);
          responseConsumer.accept(solrClient.query(null, solrQuery));
        } else {
          responseConsumer.accept(solrClient.query(collectionList, solrQuery));
        }
      }
    }

    // note: collectionList could be null when we randomly recurse and put the actual collection list into the
    //  "collection" param and some bugs value into collectionList (including null).  Only CloudSolrClient supports null.
    if (collectionList != null) {
      // HttpSolrClient
      JettySolrRunner jetty = cluster.getRandomJetty(random());
      if (random().nextBoolean()) {
        try (HttpSolrClient client = getHttpSolrClient(jetty.getBaseUrl().toString() + "/" + collectionList)) {
          responseConsumer.accept(client.query(null, solrQuery));
        }
      } else {
        try (HttpSolrClient client = getHttpSolrClient(jetty.getBaseUrl().toString())) {
          responseConsumer.accept(client.query(collectionList, solrQuery));
        }
      }

      // Recursively do again; this time with the &collection= param
      if (solrQuery.get("collection") == null) {
        // put in "collection" param
        ModifiableSolrParams newParams = new ModifiableSolrParams(solrQuery);
        newParams.set("collection", collectionList);
        String maskedColl = new String[]{null, "bogus", "collection2", "collection1"}[random().nextInt(4)];
        searchSeveralWays(maskedColl, newParams, responseConsumer);
      }
    }
  }

  @Test
  public void testErrorChecks() throws Exception {
    CollectionAdminRequest.createCollection("testErrorChecks-collection", "conf", 2, 1).process(cluster.getSolrClient());

    cluster.waitForActiveCollection("testErrorChecks-collection", 2, 2);
    waitForState("Expected testErrorChecks-collection to be created with 2 shards and 1 replica", "testErrorChecks-collection", clusterShape(2, 2));

    ignoreException(".");

    // Invalid Alias name
    SolrException e = expectThrows(SolrException.class, () ->
        CollectionAdminRequest.createAlias("test:alias", "testErrorChecks-collection").process(cluster.getSolrClient()));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST, SolrException.ErrorCode.getErrorCode(e.code()));

    // Target collection doesn't exists
    e = expectThrows(SolrException.class, () ->
        CollectionAdminRequest.createAlias("testalias", "doesnotexist").process(cluster.getSolrClient()));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST, SolrException.ErrorCode.getErrorCode(e.code()));
    assertTrue(e.getMessage().contains("Can't create collection alias for collections='doesnotexist', 'doesnotexist' is not an existing collection or alias"));

    // One of the target collections doesn't exist
    e = expectThrows(SolrException.class, () ->
        CollectionAdminRequest.createAlias("testalias", "testErrorChecks-collection,doesnotexist").process(cluster.getSolrClient()));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST, SolrException.ErrorCode.getErrorCode(e.code()));
    assertTrue(e.getMessage().contains("Can't create collection alias for collections='testErrorChecks-collection,doesnotexist', 'doesnotexist' is not an existing collection or alias"));

    // Valid
    CollectionAdminRequest.createAlias("testalias", "testErrorChecks-collection").process(cluster.getSolrClient());
    // TODO dubious; remove?
    CollectionAdminRequest.createAlias("testalias2", "testalias").process(cluster.getSolrClient());

    // Alias + invalid
    e = expectThrows(SolrException.class, () ->
        CollectionAdminRequest.createAlias("testalias3", "testalias2,doesnotexist").process(cluster.getSolrClient()));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST, SolrException.ErrorCode.getErrorCode(e.code()));
    unIgnoreException(".");

    CollectionAdminRequest.deleteAlias("testalias").process(cluster.getSolrClient());
    CollectionAdminRequest.deleteAlias("testalias2").process(cluster.getSolrClient());
    CollectionAdminRequest.deleteCollection("testErrorChecks-collection");
  }

}
