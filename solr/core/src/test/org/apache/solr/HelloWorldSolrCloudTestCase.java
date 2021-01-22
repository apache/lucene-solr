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
package org.apache.solr;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;

import org.junit.BeforeClass;
import org.junit.Test;

/**
 * How to use this test class:
 * #1 Run the test, e.g.
 *    in Eclipse 'Run As JUnit Test' or
 *    on the command line:  ./gradlew -p solr/core test --tests HelloWorldSolrCloudTestCase
 * #2 Modify the test, e.g.
 *    in setupCluster add further documents and then re-run the test.
 */
public class HelloWorldSolrCloudTestCase extends SolrCloudTestCase {

  private static final String COLLECTION = "hello_world" ;

  private static final int numShards = 3;
  private static final int numReplicas = 2;
  private static final int maxShardsPerNode = 2;
  private static final int nodeCount = (numShards*numReplicas + (maxShardsPerNode-1))/maxShardsPerNode;

  private static final String id = "id";

  @BeforeClass
  public static void setupCluster() throws Exception {

    // create and configure cluster
    configureCluster(nodeCount)
        .addConfig("conf", configset("cloud-dynamic"))
        .configure();

    // create an empty collection
    CollectionAdminRequest.createCollection(COLLECTION, "conf", numShards, numReplicas)
        .setMaxShardsPerNode(maxShardsPerNode)
        .process(cluster.getSolrClient());

    // add a document
    final SolrInputDocument doc1 = sdoc(id, "1",
        "title_s", "Here comes the sun",
        "artist_s", "The Beatles",
        "popularity_i", "123");
    new UpdateRequest()
        .add(doc1)
        .commit(cluster.getSolrClient(), COLLECTION);

    // add further document(s) here
    // TODO
  }

  @Test
  public void testHighestScoring() throws Exception {
    final SolrQuery solrQuery = new SolrQuery("q", "*:*", "fl", "id,popularity_i", "sort", "popularity_i desc", "rows", "1");
    final CloudSolrClient cloudSolrClient = cluster.getSolrClient();
    final QueryResponse rsp = cloudSolrClient.query(COLLECTION, solrQuery);
    assertEquals(1, rsp.getResults().size());
    assertEquals("1", rsp.getResults().get(0).getFieldValue(id));
  }

  @Test
  public void testLowestScoring() throws Exception {
    final SolrQuery solrQuery = new SolrQuery("q", "*:*", "fl", "id,popularity_i", "sort", "popularity_i asc", "rows", "1");
    final CloudSolrClient cloudSolrClient = cluster.getSolrClient();
    final QueryResponse rsp = cloudSolrClient.query(COLLECTION, solrQuery);
    assertEquals(1, rsp.getResults().size());
    assertEquals("1", rsp.getResults().get(0).getFieldValue(id));
  }

}

