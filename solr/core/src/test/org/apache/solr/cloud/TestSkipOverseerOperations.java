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
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.common.cloud.LiveNodesPredicate;
import org.apache.solr.common.cloud.ZkStateReader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestSkipOverseerOperations extends SolrCloudTestCase {

  @Before
  public void setupCluster() throws Exception {
    System.setProperty("solr.directoryFactory", "solr.StandardDirectoryFactory");
    System.setProperty("solr.ulog.numRecordsToKeep", "1000");

    configureCluster(3)
        .addConfig("config", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();
  }
  
  @After
  public void tearDown() throws Exception {
    shutdownCluster();
    super.tearDown();
  }
  
  public void testSkipLeaderOperations() throws Exception {

    String overseerLeader = getOverseerLeader();
    
    assertNotNull(overseerLeader);
    assertTrue(overseerLeader.length() > 0);
    
    List<JettySolrRunner> notOverseerNodes = cluster.getJettySolrRunners()
        .stream()
        .filter(solrRunner -> !solrRunner.getNodeName().equals(overseerLeader))
        .collect(Collectors.toList());
    
    assertEquals(2, notOverseerNodes.size());
    
    String collection = "collection1";
    CollectionAdminRequest
        .createCollection(collection, 2, 1)
        .setCreateNodeSet(notOverseerNodes
            .stream()
            .map(JettySolrRunner::getNodeName)
            .collect(Collectors.joining(","))
        )
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection("collection1", 2, 2);

    ZkStateReader reader = cluster.getSolrClient().getZkStateReader();
    
    List<String> nodes = new ArrayList<>();
    for (JettySolrRunner solrRunner : notOverseerNodes) {
      nodes.add(solrRunner.getNodeName());
    }
    
    for (JettySolrRunner solrRunner : notOverseerNodes) {
      solrRunner.stop();
    }
    
    for (JettySolrRunner solrRunner : notOverseerNodes) {
      cluster.waitForJettyToStop(solrRunner);
    }
    
    reader.waitForLiveNodes(30, TimeUnit.SECONDS, new LiveNodesPredicate() {
      
      @Override
      public boolean matches(SortedSet<String> oldLiveNodes, SortedSet<String> newLiveNodes) {
        boolean success = true;
        for (String lostNodeName : nodes) {
          if (newLiveNodes.contains(lostNodeName)) {
            success = false;
            break;
          }
        }
        
        return success;
      }
    });
    
    waitForState("Expected single liveNode", collection,
        (liveNodes, collectionState) -> liveNodes.size() == 1);

    CollectionAdminResponse resp = CollectionAdminRequest.getOverseerStatus().process(cluster.getSolrClient());
    for (JettySolrRunner solrRunner : notOverseerNodes) {
      solrRunner.start();
    }
    
    cluster.waitForAllNodes(30);

    waitForState("Expected 2x1 for collection: " + collection, collection,
        clusterShape(2, 2));
    CollectionAdminResponse resp2 = CollectionAdminRequest.getOverseerStatus().process(cluster.getSolrClient());
    assertEquals(getNumLeaderOpeations(resp), getNumLeaderOpeations(resp2));
    CollectionAdminRequest.deleteCollection(collection).process(cluster.getSolrClient());
  }

  @Test
  // commented out on: 17-Feb-2019   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 20-Sep-2018
  public void testSkipDownOperations() throws Exception {
    String overseerLeader = getOverseerLeader();
    List<JettySolrRunner> notOverseerNodes = cluster.getJettySolrRunners()
        .stream()
        .filter(solrRunner -> !solrRunner.getNodeName().equals(overseerLeader))
        .collect(Collectors.toList());
    String collection = "collection2";
    CollectionAdminRequest
        .createCollection(collection, 2, 2)
        .setCreateNodeSet(notOverseerNodes
            .stream()
            .map(JettySolrRunner::getNodeName)
            .collect(Collectors.joining(","))
        )
        .setMaxShardsPerNode(2)
        .process(cluster.getSolrClient());
    
    cluster.waitForActiveCollection(collection, 2, 4);
    
    ZkStateReader reader = cluster.getSolrClient().getZkStateReader();
    
    List<String> nodes = new ArrayList<>();
    for (JettySolrRunner solrRunner : notOverseerNodes) {
      nodes.add(solrRunner.getNodeName());
    }
    
    for (JettySolrRunner solrRunner : notOverseerNodes) {
      solrRunner.stop();
    }
    for (JettySolrRunner solrRunner : notOverseerNodes) {
      cluster.waitForJettyToStop(solrRunner);
    }
    
    reader.waitForLiveNodes(30, TimeUnit.SECONDS, new LiveNodesPredicate() {
      
      @Override
      public boolean matches(SortedSet<String> oldLiveNodes, SortedSet<String> newLiveNodes) {
        boolean success = true;
        for (String lostNodeName : nodes) {
          if (newLiveNodes.contains(lostNodeName)) {
            success = false;
            break;
          }
        }
        
        return success;
      }
    });
    
    waitForState("Expected single liveNode", collection,
        (liveNodes, collectionState) -> liveNodes.size() == 1);

    CollectionAdminResponse resp = CollectionAdminRequest.getOverseerStatus().process(cluster.getSolrClient());
    for (JettySolrRunner solrRunner : notOverseerNodes) {
      solrRunner.start();
    }
    cluster.waitForAllNodes(30);
    waitForState("Expected 2x2 for collection: " + collection, collection,
        clusterShape(2, 4));
    CollectionAdminResponse resp2 = CollectionAdminRequest.getOverseerStatus().process(cluster.getSolrClient());
    // 2 for recovering state, 4 for active state
    assertEquals(getNumStateOpeations(resp) + 6, getNumStateOpeations(resp2));
    CollectionAdminRequest.deleteCollection(collection).process(cluster.getSolrClient());
  }

  private int getNumLeaderOpeations(CollectionAdminResponse resp) {
    return (int) resp.getResponse().findRecursive("overseer_operations", "leader", "requests");
  }

  private int getNumStateOpeations(CollectionAdminResponse resp) {
    return (int) resp.getResponse().findRecursive("overseer_operations", "state", "requests");
  }

  private String getOverseerLeader() throws IOException, SolrServerException {
    CollectionAdminResponse resp = CollectionAdminRequest.getOverseerStatus().process(cluster.getSolrClient());
    return  (String) resp.getResponse().get("leader");
  }

}
