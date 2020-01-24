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
import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slow
public class ClusterStateUpdateTest extends SolrCloudTestCase  {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  public void setUp() throws Exception {
    super.setUp();
    configureCluster(3)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  @BeforeClass
  public static void beforeClass() {
    System.setProperty("solrcloud.skip.autorecovery", "true");
  }

  @AfterClass
  public static void afterClass() throws InterruptedException, IOException {
    System.clearProperty("solrcloud.skip.autorecovery");
    System.clearProperty("genericCoreNodeNames");
  }
  
  @Test
  public void testCoreRegistration() throws Exception {
    System.setProperty("solrcloud.update.delay", "1");

    assertEquals(0, CollectionAdminRequest.createCollection("testcore", "conf", 1, 1)
        .setCreateNodeSet(cluster.getJettySolrRunner(0).getNodeName())
        .process(cluster.getSolrClient()).getStatus());

    ZkController zkController2 = cluster.getJettySolrRunner(1).getCoreContainer().getZkController();

    String host = zkController2.getHostName();
    
    // slight pause - TODO: takes an oddly long amount of time to schedule tasks
    // with almost no delay ...
    ClusterState clusterState2 = null;
    Map<String,Slice> slices = null;
    for (int i = 75; i > 0; i--) {
      clusterState2 = zkController2.getClusterState();
      DocCollection docCollection = clusterState2.getCollectionOrNull("testcore");
      slices = docCollection == null ? null : docCollection.getSlicesMap();
      
      if (slices != null && slices.containsKey("shard1")
          && slices.get("shard1").getReplicasMap().size() > 0) {
        break;
      }
      Thread.sleep(500);
    }

    assertNotNull(slices);
    assertTrue(slices.containsKey("shard1"));

    Slice slice = slices.get("shard1");
    assertEquals("shard1", slice.getName());

    Map<String,Replica> shards = slice.getReplicasMap();

    assertEquals(1, shards.size());

    // assert this is core of container1
    Replica zkProps = shards.values().iterator().next();

    assertNotNull(zkProps);

    assertEquals(host + ":" +cluster.getJettySolrRunner(0).getLocalPort()+"_solr", zkProps.getStr(ZkStateReader.NODE_NAME_PROP));

    assertTrue(zkProps.getStr(ZkStateReader.BASE_URL_PROP).contains("http://" + host + ":"+cluster.getJettySolrRunner(0).getLocalPort()+"/solr")
      || zkProps.getStr(ZkStateReader.BASE_URL_PROP).contains("https://" + host + ":"+cluster.getJettySolrRunner(0).getLocalPort()+"/solr") );

    // assert there are 3 live nodes
    Set<String> liveNodes = clusterState2.getLiveNodes();
    assertNotNull(liveNodes);
    assertEquals(3, liveNodes.size());

    // shut down node 2
    JettySolrRunner j = cluster.stopJettySolrRunner(2);

    // slight pause (15s timeout) for watch to trigger
    for(int i = 0; i < (5 * 15); i++) {
      if(zkController2.getClusterState().getLiveNodes().size() == 2) {
        break;
      }
      Thread.sleep(200);
    }
    
    cluster.waitForJettyToStop(j);

    assertEquals(2, zkController2.getClusterState().getLiveNodes().size());

    cluster.getJettySolrRunner(1).stop();
    cluster.getJettySolrRunner(1).start();
    
    // pause for watch to trigger
    for(int i = 0; i < 200; i++) {
      if (cluster.getJettySolrRunner(0).getCoreContainer().getZkController().getClusterState().liveNodesContain(
          cluster.getJettySolrRunner(1).getCoreContainer().getZkController().getNodeName())) {
        break;
      }
      Thread.sleep(100);
    }

    assertTrue(cluster.getJettySolrRunner(0).getCoreContainer().getZkController().getClusterState().liveNodesContain(
        cluster.getJettySolrRunner(1).getCoreContainer().getZkController().getNodeName()));

    // core.close();  // don't close - this core is managed by container1 now
  }
}
