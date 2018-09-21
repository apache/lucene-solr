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

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.util.MockCoreContainer.MockCoreDescriptor;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

/**
 * Test for {@link LeaderInitiatedRecoveryThread}
 */
@Deprecated
@SolrTestCaseJ4.SuppressSSL
public class TestLeaderInitiatedRecoveryThread extends AbstractFullDistribZkTestBase {

  public TestLeaderInitiatedRecoveryThread() {
    sliceCount = 1;
    fixShardCount(2);
  }

  @Test
  //17-Aug-2018 commented @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 09-Apr-2018
  @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 20-Sep-2018
  public void testPublishDownState() throws Exception {
    waitForRecoveriesToFinish(true);

    final String leaderCoreNodeName = shardToLeaderJetty.get(SHARD1).coreNodeName;
    final CloudJettyRunner leaderRunner = shardToLeaderJetty.get(SHARD1);
    CoreContainer coreContainer = leaderRunner.jetty.getCoreContainer();
    ZkController zkController = coreContainer.getZkController();

    CloudJettyRunner notLeader = null;
    for (CloudJettyRunner cloudJettyRunner : shardToJetty.get(SHARD1)) {
      if (cloudJettyRunner != leaderRunner) {
        notLeader = cloudJettyRunner;
        break;
      }
    }
    assertNotNull(notLeader);
    Replica replica = cloudClient.getZkStateReader().getClusterState().getCollection(DEFAULT_COLLECTION).getReplica(notLeader.coreNodeName);
    ZkCoreNodeProps replicaCoreNodeProps = new ZkCoreNodeProps(replica);
    
    MockCoreDescriptor cd = new MockCoreDescriptor() {
      public CloudDescriptor getCloudDescriptor() {
        return new CloudDescriptor(shardToLeaderJetty.get(SHARD1).info.getStr(ZkStateReader.CORE_NAME_PROP), new Properties(), this) {
          @Override
          public String getCoreNodeName() {
            return shardToLeaderJetty.get(SHARD1).info.getStr(ZkStateReader.CORE_NODE_NAME_PROP);
          }
          @Override
          public boolean isLeader() {
            return true;
          }
        };
      }
    };

    /*
     1. Test that publishDownState throws exception when zkController.isReplicaInRecoveryHandling == false
      */
    try {
      
      LeaderInitiatedRecoveryThread thread = new LeaderInitiatedRecoveryThread(zkController, coreContainer,
          DEFAULT_COLLECTION, SHARD1, replicaCoreNodeProps, 1, cd);
      assertFalse(zkController.isReplicaInRecoveryHandling(replicaCoreNodeProps.getCoreUrl()));
      thread.run();
      fail("publishDownState should not have succeeded because replica url is not marked in leader initiated recovery in ZkController");
    } catch (SolrException e) {
      assertTrue(e.code() == SolrException.ErrorCode.INVALID_STATE.code);
    }


    /*
     2. Test that a non-live replica cannot be put into LIR or down state
      */
    LeaderInitiatedRecoveryThread thread = new LeaderInitiatedRecoveryThread(zkController, coreContainer,
        DEFAULT_COLLECTION, SHARD1, replicaCoreNodeProps, 1, cd);
    // kill the replica
    int children = cloudClient.getZkStateReader().getZkClient().getChildren("/live_nodes", null, true).size();
    ChaosMonkey.stop(notLeader.jetty);
    TimeOut timeOut = new TimeOut(60, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    while (!timeOut.hasTimedOut()) {
      if (children > cloudClient.getZkStateReader().getZkClient().getChildren("/live_nodes", null, true).size()) {
        break;
      }
      Thread.sleep(500);
    }
    assertTrue(children > cloudClient.getZkStateReader().getZkClient().getChildren("/live_nodes", null, true).size());

    int cversion = getOverseerCversion();
    // Thread should not publish LIR and down state for node which is not live, regardless of whether forcePublish is true or false
    assertFalse(thread.publishDownState(replicaCoreNodeProps.getCoreName(), replica.getName(), replica.getNodeName(), replicaCoreNodeProps.getCoreUrl(), false));
    // lets assert that we did not publish anything to overseer queue, simplest way is to assert that cversion of overseer queue zk node is still the same
    assertEquals(cversion, getOverseerCversion());

    assertFalse(thread.publishDownState(replicaCoreNodeProps.getCoreName(), replica.getName(), replica.getNodeName(), replicaCoreNodeProps.getCoreUrl(), true));
    // lets assert that we did not publish anything to overseer queue
    assertEquals(cversion, getOverseerCversion());


    /*
    3. Test that if ZK connection loss then thread should not attempt to publish down state even if forcePublish=true
     */
    ChaosMonkey.start(notLeader.jetty);
    waitForRecoveriesToFinish(true);

    thread = new LeaderInitiatedRecoveryThread(zkController, coreContainer,
        DEFAULT_COLLECTION, SHARD1, replicaCoreNodeProps, 1, cd) {
      @Override
      protected void updateLIRState(String replicaCoreNodeName) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "", new KeeperException.ConnectionLossException());
      }
    };
    assertFalse(thread.publishDownState(replicaCoreNodeProps.getCoreName(), replica.getName(), replica.getNodeName(), replicaCoreNodeProps.getCoreUrl(), false));
    assertFalse(thread.publishDownState(replicaCoreNodeProps.getCoreName(), replica.getName(), replica.getNodeName(), replicaCoreNodeProps.getCoreUrl(), true));
    assertNull(zkController.getLeaderInitiatedRecoveryState(DEFAULT_COLLECTION, SHARD1, replica.getName()));


    /*
     4. Test that if ZK connection loss or session expired then thread should not attempt to publish down state even if forcePublish=true
      */
    thread = new LeaderInitiatedRecoveryThread(zkController, coreContainer,
        DEFAULT_COLLECTION, SHARD1, replicaCoreNodeProps, 1, cd) {
      @Override
      protected void updateLIRState(String replicaCoreNodeName) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "", new KeeperException.SessionExpiredException());
      }
    };
    assertFalse(thread.publishDownState(replicaCoreNodeProps.getCoreName(), replica.getName(), replica.getNodeName(), replicaCoreNodeProps.getCoreUrl(), false));
    assertFalse(thread.publishDownState(replicaCoreNodeProps.getCoreName(), replica.getName(), replica.getNodeName(), replicaCoreNodeProps.getCoreUrl(), true));
    assertNull(zkController.getLeaderInitiatedRecoveryState(DEFAULT_COLLECTION, SHARD1, replica.getName()));


    /*
     5. Test that any exception other then ZK connection loss or session expired should publish down state only if forcePublish=true
      */
    thread = new LeaderInitiatedRecoveryThread(zkController, coreContainer,
        DEFAULT_COLLECTION, SHARD1, replicaCoreNodeProps, 1, cd) {
      @Override
      protected void updateLIRState(String replicaCoreNodeName) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "bogus exception");
      }
    };
    // the following should return true because regardless of the bogus exception in setting LIR state, we still want recovery commands to be sent,
    // however the following will not publish a down state
    cversion = getOverseerCversion();
    assertTrue(thread.publishDownState(replicaCoreNodeProps.getCoreName(), replica.getName(), replica.getNodeName(), replicaCoreNodeProps.getCoreUrl(), false));

    // lets assert that we did not publish anything to overseer queue, simplest way is to assert that cversion of overseer queue zk node is still the same
    assertEquals(cversion, getOverseerCversion());

    assertTrue(thread.publishDownState(replicaCoreNodeProps.getCoreName(), replica.getName(), replica.getNodeName(), replicaCoreNodeProps.getCoreUrl(), true));
    // this should have published a down state so assert that cversion has incremented
    assertTrue(getOverseerCversion() > cversion);

    timeOut = new TimeOut(30, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    while (!timeOut.hasTimedOut()) {
      Replica r = cloudClient.getZkStateReader().getClusterState().getCollection(DEFAULT_COLLECTION).getReplica(replica.getName());
      if (r.getState() == Replica.State.DOWN) {
        break;
      }
      Thread.sleep(500);
    }

    assertNull(zkController.getLeaderInitiatedRecoveryState(DEFAULT_COLLECTION, SHARD1, replica.getName()));
    assertEquals(Replica.State.DOWN, cloudClient.getZkStateReader().getClusterState().getCollection(DEFAULT_COLLECTION).getReplica(replica.getName()).getState());

    /*
    6. Test that non-leader cannot set LIR nodes
     */

    coreContainer = notLeader.jetty.getCoreContainer();
    zkController = coreContainer.getZkController();

    thread = new LeaderInitiatedRecoveryThread(zkController, coreContainer,
        DEFAULT_COLLECTION, SHARD1, replicaCoreNodeProps, 1, coreContainer.getCores().iterator().next().getCoreDescriptor()) {
      @Override
      protected void updateLIRState(String replicaCoreNodeName) {
        try {
          super.updateLIRState(replicaCoreNodeName);
        } catch (Exception e) {
          assertTrue(e instanceof ZkController.NotLeaderException);
          throw e;
        }
      }
    };
    cversion = getOverseerCversion();
    assertFalse(thread.publishDownState(replicaCoreNodeProps.getCoreName(), replica.getName(), replica.getNodeName(), replicaCoreNodeProps.getCoreUrl(), false));
    assertEquals(cversion, getOverseerCversion());

    /*
     7. assert that we can write a LIR state if everything else is fine
      */
    // reset the zkcontroller to the one from the leader
    coreContainer = leaderRunner.jetty.getCoreContainer();
    zkController = coreContainer.getZkController();
    thread = new LeaderInitiatedRecoveryThread(zkController, coreContainer,
        DEFAULT_COLLECTION, SHARD1, replicaCoreNodeProps, 1, coreContainer.getCores().iterator().next().getCoreDescriptor());
    thread.publishDownState(replicaCoreNodeProps.getCoreName(), replica.getName(), replica.getNodeName(), replicaCoreNodeProps.getCoreUrl(), false);
    timeOut = new TimeOut(30, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    while (!timeOut.hasTimedOut()) {
      Replica.State state = zkController.getLeaderInitiatedRecoveryState(DEFAULT_COLLECTION, SHARD1, replica.getName());
      if (state == Replica.State.DOWN) {
        break;
      }
      Thread.sleep(500);
    }
    assertNotNull(zkController.getLeaderInitiatedRecoveryStateObject(DEFAULT_COLLECTION, SHARD1, replica.getName()));
    assertEquals(Replica.State.DOWN, zkController.getLeaderInitiatedRecoveryState(DEFAULT_COLLECTION, SHARD1, replica.getName()));

    /*
    7. Test that
     */
  }

  protected int getOverseerCversion() throws KeeperException, InterruptedException {
    Stat stat = new Stat();
    cloudClient.getZkStateReader().getZkClient().getData("/overseer/queue", null, stat, true);
    return stat.getCversion();
  }

}
