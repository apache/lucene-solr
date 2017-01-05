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

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrException;
import org.junit.BeforeClass;
import org.junit.Test;

public class AliasIntegrationTest extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
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

    CollectionAdminRequest.createAlias("testalias", "collection1").process(cluster.getSolrClient());

    // search for alias
    QueryResponse res = cluster.getSolrClient().query("testalias", new SolrQuery("*:*"));
    assertEquals(3, res.getResults().getNumFound());
    
    // search for alias with random non cloud client
    JettySolrRunner jetty = cluster.getRandomJetty(random());
    try (HttpSolrClient client = getHttpSolrClient(jetty.getBaseUrl().toString() + "/testalias")) {
      res = client.query(new SolrQuery("*:*"));
      assertEquals(3, res.getResults().getNumFound());
    }

    // create alias, collection2 first because it's not on every node
    CollectionAdminRequest.createAlias("testalias", "collection2,collection1").process(cluster.getSolrClient());
    
    // search with new cloud client
    try (CloudSolrClient cloudSolrClient = getCloudSolrClient(cluster.getZkServer().getZkAddress(), random().nextBoolean())) {
      cloudSolrClient.setParallelUpdates(random().nextBoolean());
      SolrQuery query = new SolrQuery("*:*");
      query.set("collection", "testalias");
      res = cloudSolrClient.query(query);
      assertEquals(5, res.getResults().getNumFound());

      // Try with setDefaultCollection
      query = new SolrQuery("*:*");
      cloudSolrClient.setDefaultCollection("testalias");
      res = cloudSolrClient.query(query);
      assertEquals(5, res.getResults().getNumFound());
    }

    // search for alias with random non cloud client
    jetty = cluster.getRandomJetty(random());
    try (HttpSolrClient client = getHttpSolrClient(jetty.getBaseUrl().toString() + "/testalias")) {
      SolrQuery query = new SolrQuery("*:*");
      query.set("collection", "testalias");
      res = client.query(query);
      assertEquals(5, res.getResults().getNumFound());

      // now without collections param
      query = new SolrQuery("*:*");
      res = client.query(query);
      assertEquals(5, res.getResults().getNumFound());
    }

    // update alias
    CollectionAdminRequest.createAlias("testalias", "collection2").process(cluster.getSolrClient());

    // search for alias
    SolrQuery query = new SolrQuery("*:*");
    query.set("collection", "testalias");
    res = cluster.getSolrClient().query(query);
    assertEquals(2, res.getResults().getNumFound());
    
    // set alias to two collections
    CollectionAdminRequest.createAlias("testalias", "collection1,collection2").process(cluster.getSolrClient());

    query = new SolrQuery("*:*");
    query.set("collection", "testalias");
    res = cluster.getSolrClient().query(query);
    assertEquals(5, res.getResults().getNumFound());
    
    // try a std client
    // search 1 and 2, but have no collections param
    query = new SolrQuery("*:*");
    try (HttpSolrClient client = getHttpSolrClient(jetty.getBaseUrl().toString() + "/testalias")) {
      res = client.query(query);
      assertEquals(5, res.getResults().getNumFound());
    }

    CollectionAdminRequest.createAlias("testalias", "collection2").process(cluster.getSolrClient());
    
    // a second alias
    CollectionAdminRequest.createAlias("testalias2", "collection2").process(cluster.getSolrClient());

    try (HttpSolrClient client = getHttpSolrClient(jetty.getBaseUrl().toString() + "/testalias")) {
      new UpdateRequest()
          .add("id", "11", "a_t", "humpty dumpy4 sat on a walls")
          .commit(cluster.getSolrClient(), "testalias");
      res = client.query(query);
      assertEquals(3, res.getResults().getNumFound());
    }

    CollectionAdminRequest.createAlias("testalias", "collection2,collection1").process(cluster.getSolrClient());
    
    query = new SolrQuery("*:*");
    query.set("collection", "testalias");
    res = cluster.getSolrClient().query(query);
    assertEquals(6, res.getResults().getNumFound());

    CollectionAdminRequest.deleteAlias("testalias").process(cluster.getSolrClient());
    CollectionAdminRequest.deleteAlias("testalias2").process(cluster.getSolrClient());

    SolrException e = expectThrows(SolrException.class, () -> {
      SolrQuery q = new SolrQuery("*:*");
      q.set("collection", "testalias");
      cluster.getSolrClient().query(q);
    });
    assertTrue("Unexpected exception message: " + e.getMessage(), e.getMessage().contains("Collection not found: testalias"));
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
