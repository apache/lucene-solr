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
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.junit.BeforeClass;

public class TestShardsWithSingleReplica extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("solr.directoryFactory", "solr.StandardDirectoryFactory");
    System.setProperty("solr.ulog.numRecordsToKeep", "1000");

    configureCluster(3)
        .addConfig("config", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();
  }
  
  public void testSkipLeaderOperations() throws Exception {
    String overseerLeader = getOverseerLeader();
    List<JettySolrRunner> notOverseerNodes = cluster.getJettySolrRunners()
        .stream()
        .filter(solrRunner -> !solrRunner.getNodeName().equals(overseerLeader))
        .collect(Collectors.toList());
    String collection = "collection1";
    CollectionAdminRequest
        .createCollection(collection, 2, 1)
        .setCreateNodeSet(notOverseerNodes
            .stream()
            .map(JettySolrRunner::getNodeName)
            .collect(Collectors.joining(","))
        )
        .process(cluster.getSolrClient());

    for (JettySolrRunner solrRunner : notOverseerNodes) {
      cluster.stopJettySolrRunner(solrRunner);
    }
    waitForState("Expected empty liveNodes", collection,
        (liveNodes, collectionState) -> liveNodes.size() == 1);

    CollectionAdminResponse resp = CollectionAdminRequest.getOverseerStatus().process(cluster.getSolrClient());
    for (JettySolrRunner solrRunner : notOverseerNodes) {
      cluster.startJettySolrRunner(solrRunner);
    }

    waitForState("Expected 2x1 for collection: " + collection, collection,
        clusterShape(2, 1));
    CollectionAdminResponse resp2 = CollectionAdminRequest.getOverseerStatus().process(cluster.getSolrClient());
    assertEquals(getNumLeaderOpeations(resp), getNumLeaderOpeations(resp2));
  }

  private int getNumLeaderOpeations(CollectionAdminResponse resp) {
    return (int) resp.getResponse().findRecursive("overseer_operations", "leader", "requests");
  }

  private String getOverseerLeader() throws IOException, SolrServerException {
    CollectionAdminResponse resp = CollectionAdminRequest.getOverseerStatus().process(cluster.getSolrClient());
    return  (String) resp.getResponse().get("leader");
  }

}
