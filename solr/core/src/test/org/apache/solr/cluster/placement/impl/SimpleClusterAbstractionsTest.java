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

package org.apache.solr.cluster.placement.impl;

import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.cluster.Cluster;
import org.apache.solr.cluster.Node;
import org.apache.solr.cluster.Replica;
import org.apache.solr.cluster.Shard;
import org.apache.solr.cluster.SolrCollection;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Slice;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Locale;
import java.util.Set;

/**
 *
 */
public class SimpleClusterAbstractionsTest extends SolrCloudTestCase {

  private static final String COLLECTION = SimpleClusterAbstractionsTest.class.getName() + "_collection";

  private static SolrCloudManager cloudManager;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(3)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
    cloudManager = cluster.getJettySolrRunner(0).getCoreContainer().getZkController().getSolrCloudManager();
    CollectionAdminRequest.createCollection(COLLECTION, "conf", 2, 2)
        .process(cluster.getSolrClient());
  }

  @Test
  public void testBasic() throws Exception {
    ClusterState clusterState = cloudManager.getClusterStateProvider().getClusterState();
    Cluster cluster = new SimpleClusterAbstractionsImpl.ClusterImpl(cloudManager);
    assertNotNull(cluster);
    Set<Node> nodes = cluster.getLiveNodes();
    nodes.forEach(n -> assertTrue("missing node " + n, clusterState.liveNodesContain(n.getName())));

    DocCollection docCollection = clusterState.getCollection(COLLECTION);
    SolrCollection collection = cluster.getCollection(COLLECTION);
    // XXX gah ... can't assert anything about collection properties !!!??
    // things like router or other collection props, like eg. special placement policy

    assertNotNull(collection);
    for (String shardName : docCollection.getSlicesMap().keySet()) {
      Slice slice = docCollection.getSlice(shardName);
      Shard shard = collection.getShard(shardName);
      // XXX can't assert shard range ... because it's not in the API! :(

      assertNotNull("missing shard " + shardName, shard);
      assertNotNull("no leader in shard " + shard, shard.getLeader());
      Replica replica = shard.getLeader();
      assertEquals(slice.getLeader().getName(), replica.getReplicaName());
      slice.getReplicas().forEach(sreplica -> {
        Replica r = shard.getReplica(sreplica.getName());
        assertNotNull("missing replica " + sreplica.getName(), r);
        assertEquals(r.getCoreName(), sreplica.getCoreName());
        assertEquals(r.getNode().getName(), sreplica.getNodeName());
        assertEquals(r.getState().toString().toLowerCase(Locale.ROOT), sreplica.getState().toString());
        assertEquals(r.getType().toString(), sreplica.getType().toString());
      });
    }
  }
}
