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
import java.util.function.Consumer;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
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

    // ensure that the alias has been registered
    assertEquals("collection1",
        new CollectionAdminRequest.ListAliases().process(cluster.getSolrClient()).getAliases().get("testalias"));

    // search for alias
    searchSeveralWays("testalias", new SolrQuery("*:*"), 3);

    // Use a comma delimited list, one of which is an alias
    searchSeveralWays("testalias,collection2", new SolrQuery("*:*"), 5);

    // create alias, collection2 first because it's not on every node
    CollectionAdminRequest.createAlias("testalias", "collection2,collection1").process(cluster.getSolrClient());

    searchSeveralWays("testalias", new SolrQuery("*:*"), 5);

    // update alias
    CollectionAdminRequest.createAlias("testalias", "collection2").process(cluster.getSolrClient());

    searchSeveralWays("testalias", new SolrQuery("*:*"), 2);

    // set alias to two collections
    CollectionAdminRequest.createAlias("testalias", "collection1,collection2").process(cluster.getSolrClient());
    searchSeveralWays("testalias", new SolrQuery("*:*"), 5);

    // alias pointing to alias (one level of indirection is supported; more than that is not (may or may not work)
    // TODO dubious; remove?
    CollectionAdminRequest.createAlias("testalias2", "testalias").process(cluster.getSolrClient());
    searchSeveralWays("testalias2", new SolrQuery("*:*"), 5);

    // Test 2 aliases pointing to the same collection
    CollectionAdminRequest.createAlias("testalias", "collection2").process(cluster.getSolrClient());
    CollectionAdminRequest.createAlias("testalias2", "collection2").process(cluster.getSolrClient());

    // add one document to testalias, thus to collection2
    new UpdateRequest()
        .add("id", "11", "a_t", "humpty dumpy4 sat on a walls")
        .commit(cluster.getSolrClient(), "testalias"); // thus gets added to collection2

    searchSeveralWays("testalias", new SolrQuery("*:*"), 3);

    CollectionAdminRequest.createAlias("testalias", "collection2,collection1").process(cluster.getSolrClient());

    searchSeveralWays("testalias", new SolrQuery("*:*"), 6);

    // add one document to testalias, which will route to collection2 because it's the first
    new UpdateRequest()
        .add("id", "12", "a_t", "humpty dumpy5 sat on a walls")
        .commit(cluster.getSolrClient(), "testalias"); // thus gets added to collection2
    searchSeveralWays("collection2", new SolrQuery("*:*"), 4);

    CollectionAdminRequest.deleteAlias("testalias").process(cluster.getSolrClient());
    CollectionAdminRequest.deleteAlias("testalias2").process(cluster.getSolrClient());

    SolrException e = expectThrows(SolrException.class, () -> {
      SolrQuery q = new SolrQuery("*:*");
      q.set("collection", "testalias");
      cluster.getSolrClient().query(q);
    });
    assertTrue("Unexpected exception message: " + e.getMessage(), e.getMessage().contains("Collection not found: testalias"));
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
