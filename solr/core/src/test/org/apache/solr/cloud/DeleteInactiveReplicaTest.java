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

import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;
import org.apache.solr.util.FileUtils;
import org.apache.solr.util.TimeOut;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteInactiveReplicaTest extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(4)
        .addConfig("conf", configset("cloud-minimal"))
        .withProperty(ZkStateReader.LEGACY_CLOUD, "false")
        .configure();
  }

  @Test
  public void deleteInactiveReplicaTest() throws Exception {

    String collectionName = "delDeadColl";
    int replicationFactor = 2;
    int numShards = 2;
    int maxShardsPerNode = ((((numShards + 1) * replicationFactor) / cluster.getJettySolrRunners().size())) + 1;

    CollectionAdminRequest.createCollection(collectionName, "conf", numShards, replicationFactor)
        .setMaxShardsPerNode(maxShardsPerNode)
        .process(cluster.getSolrClient());
    waitForState("Expected a cluster of 2 shards and 2 replicas", collectionName, (n, c) -> {
      return DocCollection.isFullyActive(n, c, numShards, replicationFactor);
    });

    DocCollection collectionState = getCollectionState(collectionName);

    Slice shard = getRandomShard(collectionState);
    Replica replica = getRandomReplica(shard);
    JettySolrRunner jetty = cluster.getReplicaJetty(replica);
    CoreDescriptor replicaCd;
    try (SolrCore core = jetty.getCoreContainer().getCore(replica.getCoreName())) {
      replicaCd = core.getCoreDescriptor();
    }
    cluster.stopJettySolrRunner(jetty);

    waitForState("Expected replica " + replica.getName() + " on down node to be removed from cluster state", collectionName, (n, c) -> {
      Replica r = c.getReplica(replica.getCoreName());
      return r == null || r.getState() != Replica.State.ACTIVE;
    });

    if (log.isInfoEnabled()) {
      log.info("Removing replica {}/{} ", shard.getName(), replica.getName());
    }
    CollectionAdminRequest.deleteReplica(collectionName, shard.getName(), replica.getName())
        .process(cluster.getSolrClient());
    waitForState("Expected deleted replica " + replica.getName() + " to be removed from cluster state", collectionName, (n, c) -> {
      return c.getReplica(replica.getCoreName()) == null;
    });

    cluster.startJettySolrRunner(jetty);
    log.info("restarted jetty");
    TimeOut timeOut = new TimeOut(60, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    timeOut.waitFor("Expected data dir and instance dir of " + replica.getName() + " is deleted", ()
        -> !Files.exists(replicaCd.getInstanceDir()) && !FileUtils.fileExists(replicaCd.getDataDir()));

    // Check that we can't create a core with no coreNodeName
    try (SolrClient queryClient = getHttpSolrClient(jetty.getBaseUrl().toString())) {
      Exception e = expectThrows(Exception.class, () -> {
        CoreAdminRequest.Create createRequest = new CoreAdminRequest.Create();
        createRequest.setCoreName("testcore");
        createRequest.setCollection(collectionName);
        createRequest.setShardId("shard2");
        queryClient.request(createRequest);
      });
      assertTrue("Unexpected error message: " + e.getMessage(), e.getMessage().contains("coreNodeName missing"));

    }
  }

}
