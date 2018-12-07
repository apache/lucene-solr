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

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.SolrException;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class SplitShardTest extends SolrCloudTestCase {

  private final String COLLECTION_NAME = "splitshardtest-collection";

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    cluster.deleteAllCollections();
  }

  @Test
  public void doTest() throws IOException, SolrServerException {
    CollectionAdminRequest
        .createCollection(COLLECTION_NAME, "conf", 2, 1)
        .setMaxShardsPerNode(100)
        .process(cluster.getSolrClient());
    
    cluster.waitForActiveCollection(COLLECTION_NAME, 2, 2);
    
    CollectionAdminRequest.SplitShard splitShard = CollectionAdminRequest.splitShard(COLLECTION_NAME)
        .setNumSubShards(5)
        .setShardName("shard1");
    splitShard.process(cluster.getSolrClient());
    waitForState("Timed out waiting for sub shards to be active. Number of active shards=" +
            cluster.getSolrClient().getZkStateReader().getClusterState().getCollection(COLLECTION_NAME).getActiveSlices().size(),
        COLLECTION_NAME, activeClusterShape(6, 7));

    try {
      splitShard = CollectionAdminRequest.splitShard(COLLECTION_NAME).setShardName("shard2").setNumSubShards(10);
      splitShard.process(cluster.getSolrClient());
      fail("SplitShard should throw an exception when numSubShards > 8");
    } catch (HttpSolrClient.RemoteSolrException ex) {
      assertTrue(ex.getMessage().contains("A shard can only be split into 2 to 8 subshards in one split request."));
    }

    try {
      splitShard = CollectionAdminRequest.splitShard(COLLECTION_NAME).setShardName("shard2").setNumSubShards(1);
      splitShard.process(cluster.getSolrClient());
      fail("SplitShard should throw an exception when numSubShards < 2");
    } catch (HttpSolrClient.RemoteSolrException ex) {
      assertTrue(ex.getMessage().contains("A shard can only be split into 2 to 8 subshards in one split request. Provided numSubShards=1"));
    }
  }

  @Test
  public void multipleOptionsSplitTest() throws IOException, SolrServerException {
    CollectionAdminRequest.SplitShard splitShard = CollectionAdminRequest.splitShard(COLLECTION_NAME)
        .setNumSubShards(5)
        .setRanges("0-c,d-7fffffff")
        .setShardName("shard1");
    boolean expectedException = false;
    try {
      splitShard.process(cluster.getSolrClient());
      fail("An exception should have been thrown");
    } catch (SolrException ex) {
      expectedException = true;
    }
    assertTrue("Expected SolrException but it didn't happen", expectedException);
  }

}
