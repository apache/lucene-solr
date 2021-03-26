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

package org.apache.solr.update.processor;

import java.util.ArrayList;
import java.util.List;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.request.json.JsonQueryRequest;
import org.apache.solr.client.solrj.request.json.TermsFacetMap;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.json.BucketBasedJsonFacet;
import org.apache.solr.client.solrj.response.json.BucketJsonFacet;
import org.apache.solr.cloud.SolrCloudTestCase;

import org.apache.lucene.util.IOUtils;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Tests the ability to use {@link SignatureUpdateProcessorFactory} to generate uniqueKeys for "duplicate" documents
 * in cloud mode.
 */
public class TestCloudDeduplication extends SolrCloudTestCase {
  public final static String COLLECTION = "dedup_col";
  
  /** One client per node */
  private static final List<SolrClient> NODE_CLIENTS = new ArrayList<>(7);
  /** clients (including cloud client) for easy randomization and looping of collection level requests */
  private static final List<SolrClient> CLIENTS = new ArrayList<>(7);
  
  @BeforeClass
  public static void setupCluster() throws Exception {
    final int numShards = usually() ? 2 : 1;
    final int numReplicas = usually() ? 2 : 1;
    final int numNodes = 1 + (numShards * numReplicas);  // at least one node w/o any replicas
    configureCluster(numNodes) 
      .addConfig("conf", configset("dedup"))
      .configure();

    CLIENTS.add(cluster.getSolrClient());
    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      final SolrClient c = getHttpSolrClient(jetty.getBaseUrl().toString());
      NODE_CLIENTS.add(c);
      CLIENTS.add(c);
    }
    
    assertEquals("failed to create collection", 0,
                 CollectionAdminRequest
                 .createCollection(COLLECTION, "conf", numShards, numReplicas)
                 .process(cluster.getSolrClient()).getStatus());

    cluster.waitForActiveCollection(COLLECTION, numShards, numShards * numReplicas);

  }
  
  @AfterClass
  private static void closeClients() throws Exception {
    try {
      IOUtils.close(NODE_CLIENTS);
    } finally {
      NODE_CLIENTS.clear();
      CLIENTS.clear();
    }
  }

  @After
  public void clearCollection() throws Exception {
    assertEquals("DBQ failed", 0, cluster.getSolrClient().deleteByQuery(COLLECTION, "*:*").getStatus());
    assertEquals("commit failed", 0, cluster.getSolrClient().commit(COLLECTION).getStatus());
  }

  
  public void testRandomDocs() throws Exception {

    // index some random documents, using a mix-match of batches, to various SolrClients
    
    final int uniqueMod = atLeast(43);         // the number of unique sig values expected
    final int numBatches = atLeast(uniqueMod); // we'll add at least one doc per batch
    int docCounter = 0;
    for (int batchId = 0; batchId < numBatches; batchId++) {
      final UpdateRequest ureq = new UpdateRequest();
      final int batchSize = atLeast(2);
      for (int i = 0; i < batchSize; i++) {
        docCounter++;
        ureq.add(sdoc(// NOTE: No 'id' field, SignatureUpdateProcessor fills it in for us
                      "data_s", (docCounter % uniqueMod)));
      }
      assertEquals("add failed", 0, ureq.process(getRandClient(), COLLECTION).getStatus());
    }
    assertEquals("commit failed", 0, getRandClient().commit(COLLECTION).getStatus());
    
    assert docCounter > uniqueMod;
    
    // query our collection and confirm no duplicates on the signature field (using faceting)
    // Check every (node) for consistency...
    final JsonQueryRequest req = new JsonQueryRequest()
      .setQuery("*:*")
      .setLimit(0)
      .withFacet("data_facet", new TermsFacetMap("data_s").setLimit(uniqueMod + 1));
    for (SolrClient client : CLIENTS) {
      final QueryResponse rsp = req.process(client, COLLECTION);
      try {
        assertEquals(0, rsp.getStatus());
        assertEquals(uniqueMod, rsp.getResults().getNumFound());
        
        final BucketBasedJsonFacet facet = rsp.getJsonFacetingResponse().getBucketBasedFacets("data_facet");
        assertEquals(uniqueMod, facet.getBuckets().size());
        for (BucketJsonFacet bucket : facet.getBuckets()) {
          assertEquals("Bucket " + bucket.getVal(),
                       1, bucket.getCount());
        }
      } catch (AssertionError e) {
        throw new AssertionError(rsp + " + " + client + " => " + e.getMessage(), e);
      }
    }
  }
  
  /** 
   * returns a random SolrClient -- either a CloudSolrClient, or an HttpSolrClient pointed 
   * at a node in our cluster.
   */
  private static SolrClient getRandClient() {
    return CLIENTS.get(random().nextInt(CLIENTS.size()));
  }
}
