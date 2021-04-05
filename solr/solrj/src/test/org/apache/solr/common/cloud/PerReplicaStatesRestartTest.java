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
package org.apache.solr.common.cloud;

import java.lang.invoke.MethodHandles;

import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PerReplicaStatesRestartTest extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public void testPRSRestart() throws Exception {
    String testCollection = "prs_restart_test";
    MiniSolrCloudCluster cluster1 =
        configureCluster(1)
        .addConfig("conf1", getFile("solrj").toPath().resolve("solr").resolve("configsets").resolve("streaming").resolve("conf"))
        .withJettyConfig(jetty -> jetty.enableV2(true))
        .configure();
    try {
      CollectionAdminRequest.createCollection(testCollection, "conf1", 1, 1)
      .setPerReplicaState(Boolean.TRUE)
      .process(cluster1.getSolrClient());
      cluster1.waitForActiveCollection(testCollection, 1, 1);

      DocCollection c = cluster1.getSolrClient().getZkStateReader().getCollection(testCollection);
      c.forEachReplica((s, replica) -> assertNotNull(replica.getReplicaState()));
      String collectionPath = ZkStateReader.getCollectionPath(testCollection);
      PerReplicaStates prs = PerReplicaStates.fetch(collectionPath, cluster.getZkClient(), null);
      assertEquals(1, prs.states.size());

      JettySolrRunner jsr = cluster1.startJettySolrRunner();
      assertEquals(2,cluster1.getJettySolrRunners().size());

      // Now let's do an add replica
      CollectionAdminRequest
      .addReplicaToShard(testCollection, "shard1")
      .process(cluster1.getSolrClient());
      cluster1.waitForActiveCollection(testCollection, 1, 2);
      prs = PerReplicaStates.fetch(collectionPath, cluster.getZkClient(), null);
      assertEquals(2, prs.states.size());
      c = cluster1.getSolrClient().getZkStateReader().getCollection(testCollection);
      prs.states.forEachEntry((s, state) -> assertEquals(Replica.State.ACTIVE, state.state));

      String replicaName = null;
      for (Replica r : c.getSlice("shard1").getReplicas()) {
        if(r.getNodeName() .equals(jsr.getNodeName())) {
          replicaName = r.getName();
        }
      }

      if(replicaName != null) {
        log.info("restarting the node : {}, state.json v: {} downreplica :{}", jsr.getNodeName(), c.getZNodeVersion(), replicaName);
        jsr.stop();
        c = cluster1.getSolrClient().getZkStateReader().getCollection(testCollection);
        log.info("after down node, state.json v: {}", c.getZNodeVersion());
        prs =  PerReplicaStates.fetch(collectionPath, cluster.getZkClient(), null);
        PerReplicaStates.State st = prs.get(replicaName);
        assertNotEquals(Replica.State.ACTIVE, st.state);
        jsr.start();
        cluster1.waitForActiveCollection(testCollection, 1, 2);
        prs =  PerReplicaStates.fetch(collectionPath, cluster.getZkClient(), null);
        prs.states.forEachEntry((s, state) -> assertEquals(Replica.State.ACTIVE, state.state));
      }

    } finally {
      cluster1.shutdown();
    }

  }
}
