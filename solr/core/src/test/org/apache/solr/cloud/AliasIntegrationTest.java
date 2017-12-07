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
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.Utils;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.solr.common.cloud.ZkStateReader.ALIASES;

public class AliasIntegrationTest extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  @Test
  public void testMetadata() throws Exception {
    CollectionAdminRequest.createCollection("collection1meta", "conf", 2, 1).process(cluster.getSolrClient());
    CollectionAdminRequest.createCollection("collection2meta", "conf", 1, 1).process(cluster.getSolrClient());
    waitForState("Expected collection1 to be created with 2 shards and 1 replica", "collection1meta", clusterShape(2, 1));
    waitForState("Expected collection2 to be created with 1 shard and 1 replica", "collection2meta", clusterShape(1, 1));
    ZkStateReader zkStateReader = cluster.getSolrClient().getZkStateReader();
    zkStateReader.createClusterStateWatchersAndUpdate();
    List<String> aliases = zkStateReader.getAliases().resolveAliases("meta1");
    assertEquals(1, aliases.size());
    assertEquals("meta1", aliases.get(0));
    UnaryOperator<Aliases> op6 = a -> a.cloneWithCollectionAlias("meta1", "collection1meta,collection2meta");
    final ZkStateReader.AliasesManager aliasesHolder = zkStateReader.aliasesHolder;

    aliasesHolder.applyModificationAndExportToZk(op6);
    aliases = zkStateReader.getAliases().resolveAliases("meta1");
    assertEquals(2, aliases.size());
    assertEquals("collection1meta", aliases.get(0));
    assertEquals("collection2meta", aliases.get(1));
    //ensure we have the back-compat format in ZK:
    final byte[] rawBytes = zkStateReader.getZkClient().getData(ALIASES, null, null, true);
    assertTrue(((Map<String,Map<String,?>>)Utils.fromJSON(rawBytes)).get("collection").get("meta1") instanceof String);

    // set metadata
    UnaryOperator<Aliases> op5 = a -> a.cloneWithCollectionAliasMetadata("meta1", "foo", "bar");
    aliasesHolder.applyModificationAndExportToZk(op5);
    Map<String, String> meta = zkStateReader.getAliases().getCollectionAliasMetadata("meta1");
    assertNotNull(meta);
    assertTrue(meta.containsKey("foo"));
    assertEquals("bar", meta.get("foo"));

    // set more metadata
    UnaryOperator<Aliases> op4 = a -> a.cloneWithCollectionAliasMetadata("meta1", "foobar", "bazbam");
    aliasesHolder.applyModificationAndExportToZk(op4);
    meta = zkStateReader.getAliases().getCollectionAliasMetadata("meta1");
    assertNotNull(meta);

    // old metadata still there
    assertTrue(meta.containsKey("foo"));
    assertEquals("bar", meta.get("foo"));

    // new metadata added
    assertTrue(meta.containsKey("foobar"));
    assertEquals("bazbam", meta.get("foobar"));

    // remove metadata
    UnaryOperator<Aliases> op3 = a -> a.cloneWithCollectionAliasMetadata("meta1", "foo", null);
    aliasesHolder.applyModificationAndExportToZk(op3);
    meta = zkStateReader.getAliases().getCollectionAliasMetadata("meta1");
    assertNotNull(meta);

    // verify key was removed
    assertFalse(meta.containsKey("foo"));

    // but only the specified key was removed
    assertTrue(meta.containsKey("foobar"));
    assertEquals("bazbam", meta.get("foobar"));

    // removal of non existent key should succeed.
    UnaryOperator<Aliases> op2 = a -> a.cloneWithCollectionAliasMetadata("meta1", "foo", null);
    aliasesHolder.applyModificationAndExportToZk(op2);

    // chained invocations
    UnaryOperator<Aliases> op1 = a ->
        a.cloneWithCollectionAliasMetadata("meta1", "foo2", "bazbam")
        .cloneWithCollectionAliasMetadata("meta1", "foo3", "bazbam2");
    aliasesHolder.applyModificationAndExportToZk(op1);

    // some other independent update (not overwritten)
    UnaryOperator<Aliases> op = a -> a.cloneWithCollectionAlias("meta3", "collection1meta,collection2meta");
    aliasesHolder.applyModificationAndExportToZk(op);

    // competing went through
    assertEquals("collection1meta,collection2meta", zkStateReader.getAliases().getCollectionAliasMap().get("meta3"));

    meta = zkStateReader.getAliases().getCollectionAliasMetadata("meta1");
    assertNotNull(meta);

    // old metadata still there
    assertTrue(meta.containsKey("foobar"));
    assertEquals("bazbam", meta.get("foobar"));

    // competing update not overwritten
    assertEquals("collection1meta,collection2meta", zkStateReader.getAliases().getCollectionAliasMap().get("meta3"));

    // new metadata added
    assertTrue(meta.containsKey("foo2"));
    assertEquals("bazbam", meta.get("foo2"));
    assertTrue(meta.containsKey("foo3"));
    assertEquals("bazbam2", meta.get("foo3"));

    // now check that an independently constructed ZkStateReader can see what we've done.
    // i.e. the data is really in zookeeper
    String zkAddress = cluster.getZkServer().getZkAddress();
    boolean createdZKSR = false;
    try(SolrZkClient zkClient = new SolrZkClient(zkAddress, 30000)) {

      ZkController.createClusterZkNodes(zkClient);

      zkStateReader = new ZkStateReader(zkClient);
      createdZKSR = true;
      zkStateReader.createClusterStateWatchersAndUpdate();

      meta = zkStateReader.getAliases().getCollectionAliasMetadata("meta1");
      assertNotNull(meta);

      // verify key was removed in independent view
      assertFalse(meta.containsKey("foo"));

      // but only the specified key was removed
      assertTrue(meta.containsKey("foobar"));
      assertEquals("bazbam", meta.get("foobar"));

      Aliases a = zkStateReader.getAliases();
      Aliases clone = a.cloneWithCollectionAlias("meta1", null);
      meta = clone.getCollectionAliasMetadata("meta1");
      assertEquals(0,meta.size());
    } finally {
      if (createdZKSR) {
        zkStateReader.close();
      }
    }
  }
  
  @Test
  public void test() throws Exception {
    CollectionAdminRequest.createCollection("collection1", "conf", 2, 1).process(cluster.getSolrClient());
    CollectionAdminRequest.createCollection("collection2", "conf", 1, 1).process(cluster.getSolrClient());
    waitForState("Expected collection1 to be created with 2 shards and 1 replica", "collection1", clusterShape(2, 1));
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
    CollectionAdminRequest.createAlias("testalias1", "collection1").process(cluster.getSolrClient());
    sleepToAllowZkPropagation();
    // ensure that the alias has been registered
    assertEquals("collection1",
        new CollectionAdminRequest.ListAliases().process(cluster.getSolrClient()).getAliases().get("testalias1"));

    // search for alias
    searchSeveralWays("testalias1", new SolrQuery("*:*"), 3);

    // Use a comma delimited list, one of which is an alias
    searchSeveralWays("testalias1,collection2", new SolrQuery("*:*"), 5);

    ///////////////
    // test alias pointing to two collections.  collection2 first because it's not on every node
    CollectionAdminRequest.createAlias("testalias2", "collection2,collection1").process(cluster.getSolrClient());

    searchSeveralWays("testalias2", new SolrQuery("*:*"), 5);

    ///////////////
    // update alias
    CollectionAdminRequest.createAlias("testalias2", "collection2").process(cluster.getSolrClient());
    sleepToAllowZkPropagation();

    searchSeveralWays("testalias2", new SolrQuery("*:*"), 2);

    ///////////////
    // alias pointing to alias.  One level of indirection is supported; more than that is not (may or may not work)
    // TODO dubious; remove?
    CollectionAdminRequest.createAlias("testalias3", "testalias2").process(cluster.getSolrClient());
    searchSeveralWays("testalias3", new SolrQuery("*:*"), 2);

    ///////////////
    // Test 2 aliases pointing to the same collection
    CollectionAdminRequest.createAlias("testalias4", "collection2").process(cluster.getSolrClient());
    CollectionAdminRequest.createAlias("testalias5", "collection2").process(cluster.getSolrClient());

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

    // add one document to testalias6, which will route to collection2 because it's the first
    new UpdateRequest()
        .add("id", "12", "a_t", "humpty dumpy5 sat on a walls")
        .commit(cluster.getSolrClient(), "testalias6"); // thus gets added to collection2
    searchSeveralWays("collection2", new SolrQuery("*:*"), 4);

    ///////////////
    for (int i = 1; i <= 6 ; i++) {
      CollectionAdminRequest.deleteAlias("testalias" + i).process(cluster.getSolrClient());
    }
    sleepToAllowZkPropagation();

    SolrException e = expectThrows(SolrException.class, () -> {
      SolrQuery q = new SolrQuery("*:*");
      q.set("collection", "testalias1");
      cluster.getSolrClient().query(q);
    });
    assertTrue("Unexpected exception message: " + e.getMessage(), e.getMessage().contains("Collection not found: testalias1"));
  }

  /**
   * Sleep a bit to allow Zookeeper state propagation.
   *
   * Solr's view of the cluster is eventually consistent. *Eventually* all nodes and CloudSolrClients will be aware of
   * alias changes, but not immediately. If a newly created alias is queried, things should work right away since Solr
   * will attempt to see if it needs to get the latest aliases when it can't otherwise resolve the name.  However
   * modifications to an alias will take some time.
   */
  private void sleepToAllowZkPropagation() {
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
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

  public void testErrorChecks() throws Exception {
    CollectionAdminRequest.createCollection("testErrorChecks-collection", "conf", 2, 1).process(cluster.getSolrClient());
    waitForState("Expected testErrorChecks-collection to be created with 2 shards and 1 replica", "testErrorChecks-collection", clusterShape(2, 1));
    
    ignoreException(".");
    
    // Invalid Alias name
    SolrException e = expectThrows(SolrException.class, () -> {
      CollectionAdminRequest.createAlias("test:alias", "testErrorChecks-collection").process(cluster.getSolrClient());
    });
    assertEquals(SolrException.ErrorCode.BAD_REQUEST, SolrException.ErrorCode.getErrorCode(e.code()));

    // Target collection doesn't exists
    e = expectThrows(SolrException.class, () -> {
      CollectionAdminRequest.createAlias("testalias", "doesnotexist").process(cluster.getSolrClient());
    });
    assertEquals(SolrException.ErrorCode.BAD_REQUEST, SolrException.ErrorCode.getErrorCode(e.code()));
    assertTrue(e.getMessage().contains("Can't create collection alias for collections='doesnotexist', 'doesnotexist' is not an existing collection or alias"));

    // One of the target collections doesn't exist
    e = expectThrows(SolrException.class, () -> {
      CollectionAdminRequest.createAlias("testalias", "testErrorChecks-collection,doesnotexist").process(cluster.getSolrClient());
    });
    assertEquals(SolrException.ErrorCode.BAD_REQUEST, SolrException.ErrorCode.getErrorCode(e.code()));
    assertTrue(e.getMessage().contains("Can't create collection alias for collections='testErrorChecks-collection,doesnotexist', 'doesnotexist' is not an existing collection or alias"));

    // Valid
    CollectionAdminRequest.createAlias("testalias", "testErrorChecks-collection").process(cluster.getSolrClient());
    // TODO dubious; remove?
    CollectionAdminRequest.createAlias("testalias2", "testalias").process(cluster.getSolrClient());

    // Alias + invalid
    e = expectThrows(SolrException.class, () -> {
      CollectionAdminRequest.createAlias("testalias3", "testalias2,doesnotexist").process(cluster.getSolrClient());
    });
    assertEquals(SolrException.ErrorCode.BAD_REQUEST, SolrException.ErrorCode.getErrorCode(e.code()));
    unIgnoreException(".");

    CollectionAdminRequest.deleteAlias("testalias").process(cluster.getSolrClient());
    CollectionAdminRequest.deleteAlias("testalias2").process(cluster.getSolrClient());
    CollectionAdminRequest.deleteCollection("testErrorChecks-collection");
  }

}
