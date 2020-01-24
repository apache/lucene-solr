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

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.is;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.LuceneTestCase.AwaitsFix;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient.RemoteSolrException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterStateUtil;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.MockDirectoryFactory;
import org.apache.solr.core.SolrCore;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AwaitsFix(bugUrl="https://issues.apache.org/jira/browse/SOLR-13237")
public class LeaderTragicEventTest extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("solr.mscheduler", "org.apache.solr.core.MockConcurrentMergeScheduler");
    System.setProperty(MockDirectoryFactory.SOLR_TESTS_USING_MOCK_DIRECTORY_WRAPPER, "true");

    configureCluster(2)
        .addConfig("config", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();
  }

  @AfterClass
  public static void cleanup() {
    System.clearProperty("solr.mscheduler");
    System.clearProperty(MockDirectoryFactory.SOLR_TESTS_USING_MOCK_DIRECTORY_WRAPPER);
  }


  @Test
  public void test() throws Exception {
    final String collection = "collection1";
    cluster.getSolrClient().setDefaultCollection(collection);
    CollectionAdminRequest
        .createCollection(collection, "config", 1, 2)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(collection, 1, 2);
    try {
      List<String> addedIds = new ArrayList<>();
      Replica oldLeader = corruptLeader(collection, addedIds);

      waitForState("Timeout waiting for new replica become leader", collection, (liveNodes, collectionState) -> {
        Slice slice = collectionState.getSlice("shard1");

        if (slice.getReplicas().size() != 2) return false;
        if (slice.getLeader() == null) return false;
        if (slice.getLeader().getName().equals(oldLeader.getName())) return false;

        return true;
      });
      ClusterStateUtil.waitForAllActiveAndLiveReplicas(cluster.getSolrClient().getZkStateReader(), collection, 120000);
      Slice shard = getCollectionState(collection).getSlice("shard1");
      assertNotSame(shard.getLeader().getNodeName(), oldLeader.getNodeName());
      assertEquals(getNonLeader(shard).getNodeName(), oldLeader.getNodeName());

      for (String id : addedIds) {
        assertNotNull(cluster.getSolrClient().getById(collection,id));
      }
      log.info("The test success oldLeader:{} currentState:{}", oldLeader, getCollectionState(collection));

    } finally {
      CollectionAdminRequest.deleteCollection(collection).process(cluster.getSolrClient());
    }
  }

  private Replica corruptLeader(String collection, List<String> addedIds) throws IOException {
    DocCollection dc = getCollectionState(collection);
    Replica oldLeader = dc.getLeader("shard1");
    log.info("Corrupt leader : {}", oldLeader);

    CoreContainer leaderCC = cluster.getReplicaJetty(oldLeader).getCoreContainer();
    SolrCore leaderCore = leaderCC.getCores().iterator().next();
    MockDirectoryWrapper mockDir = (MockDirectoryWrapper) leaderCore.getDirectoryFactory()
        .get(leaderCore.getIndexDir(), DirectoryFactory.DirContext.DEFAULT, leaderCore.getSolrConfig().indexConfig.lockType);
    leaderCore.getDirectoryFactory().release(mockDir);

    try (HttpSolrClient solrClient = new HttpSolrClient.Builder(dc.getLeader("shard1").getCoreUrl()).build()) {
      for (int i = 0; i < 100; i++) {
        new UpdateRequest()
            .add("id", i + "")
            .process(solrClient);
        solrClient.commit();
        addedIds.add(i + "");

        for (String file : mockDir.listAll()) {
          if (file.contains("segments_")) continue;
          if (file.endsWith("si")) continue;
          if (file.endsWith("fnm")) continue;
          if (random().nextBoolean()) continue;

          try {
            mockDir.corruptFiles(Collections.singleton(file));
          } catch (RuntimeException | FileNotFoundException | NoSuchFileException e) {
            // merges can lead to this exception
          }
        }
      }
    } catch (Exception e) {
      log.info("Corrupt leader ex: ", e);
      
      // solrClient.add/commit would throw RemoteSolrException with error code 500 or 
      // 404(when the leader replica is already deleted by giveupLeadership)
      if (e instanceof RemoteSolrException) {
        SolrException se = (SolrException) e;
        assertThat(se.code(), anyOf(is(500), is(404)));
      } else if (!(e instanceof AlreadyClosedException)) {
        throw new RuntimeException("Unexpected exception", e);
      }
      //else expected
    }
    return oldLeader;
  }

  private Replica getNonLeader(Slice slice) {
    if (slice.getReplicas().size() <= 1) return null;
    return slice.getReplicas(rep -> !rep.getName().equals(slice.getLeader().getName())).get(0);
  }

  @Test
  public void testOtherReplicasAreNotActive() throws Exception {
    final String collection = "collection2";
    cluster.getSolrClient().setDefaultCollection(collection);
    int numReplicas = random().nextInt(2) + 1;
    // won't do anything if leader is the only one active replica in the shard
    CollectionAdminRequest
        .createCollection(collection, "config", 1, numReplicas)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(collection, 1, numReplicas);

    try {
      JettySolrRunner otherReplicaJetty = null;
      if (numReplicas == 2) {
        Slice shard = getCollectionState(collection).getSlice("shard1");
        otherReplicaJetty = cluster.getReplicaJetty(getNonLeader(shard));
        log.info("Stop jetty node : {} state:{}", otherReplicaJetty.getBaseUrl(), getCollectionState(collection));
        otherReplicaJetty.stop();
        cluster.waitForJettyToStop(otherReplicaJetty);
        waitForState("Timeout waiting for replica get down", collection, (liveNodes, collectionState) -> getNonLeader(collectionState.getSlice("shard1")).getState() != Replica.State.ACTIVE);
      }

      Replica oldLeader = corruptLeader(collection, new ArrayList<>());

      if (otherReplicaJetty != null) {
        otherReplicaJetty.start();
        cluster.waitForNode(otherReplicaJetty, 30);
      }

      Replica leader = getCollectionState(collection).getSlice("shard1").getLeader();
      assertEquals(leader.getName(), oldLeader.getName());
    } finally {
      CollectionAdminRequest.deleteCollection(collection).process(cluster.getSolrClient());
    }
  }


}
