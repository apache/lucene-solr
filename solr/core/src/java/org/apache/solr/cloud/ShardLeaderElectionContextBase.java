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
import java.util.List;
import java.util.ArrayList;

import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.PerReplicaStates;
import org.apache.solr.common.cloud.PerReplicaStatesOps;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkCmdExecutor;
import org.apache.solr.common.cloud.ZkMaintenanceUtils;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.RetryUtil;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.OpResult.SetDataResult;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ShardLeaderElectionContextBase extends ElectionContext {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  protected final SolrZkClient zkClient;
  protected String shardId;
  protected String collection;
  protected LeaderElector leaderElector;
  protected ZkStateReader zkStateReader;
  protected ZkController zkController;
  private Integer leaderZkNodeParentVersion;

  // Prevents a race between cancelling and becoming leader.
  private final Object lock = new Object();

  public ShardLeaderElectionContextBase(LeaderElector leaderElector,
                                        final String shardId, final String collection, final String coreNodeName,
                                        ZkNodeProps props, ZkController zkController) {
    super(coreNodeName, ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection
        + "/leader_elect/" + shardId, ZkStateReader.getShardLeadersPath(
        collection, shardId), props, zkController.getZkClient());
    this.leaderElector = leaderElector;
    this.zkStateReader = zkController.getZkStateReader();
    this.zkClient = zkStateReader.getZkClient();
    this.zkController = zkController;
    this.shardId = shardId;
    this.collection = collection;

    String parent = ZkMaintenanceUtils.getZkParent(leaderPath);
    ZkCmdExecutor zcmd = new ZkCmdExecutor(30000);
    // only if /collections/{collection} exists already do we succeed in creating this path
    log.info("make sure parent is created {}", parent);
    try {
      zcmd.ensureExists(parent, (byte[]) null, CreateMode.PERSISTENT, zkClient, 2);
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void cancelElection() throws InterruptedException, KeeperException {
    super.cancelElection();
    synchronized (lock) {
      if (leaderZkNodeParentVersion != null) {
        // no problem
        // no problem
        try {
          // We need to be careful and make sure we *only* delete our own leader registration node.
          // We do this by using a multi and ensuring the parent znode of the leader registration node
          // matches the version we expect - there is a setData call that increments the parent's znode
          // version whenever a leader registers.
          log.debug("Removing leader registration node on cancel: {} {}", leaderPath, leaderZkNodeParentVersion);
          List<Op> ops = new ArrayList<>(2);
          String parent = ZkMaintenanceUtils.getZkParent(leaderPath);
          ops.add(Op.check(parent, leaderZkNodeParentVersion));
          ops.add(Op.delete(leaderPath, -1));
          zkClient.multi(ops, true);
        } catch (InterruptedException e) {
          throw e;
        } catch (IllegalArgumentException e) {
          SolrException.log(log, e);
        }
        leaderZkNodeParentVersion = null;
      } else {
        log.info("No version found for ephemeral leader parent node, won't remove previous leader registration.");
      }
    }
  }

  @Override
  void runLeaderProcess(boolean weAreReplacement, int pauseBeforeStartMs)
      throws KeeperException, InterruptedException, IOException {
    // register as leader - if an ephemeral is already there, wait to see if it goes away

    String parent = ZkMaintenanceUtils.getZkParent(leaderPath);
    try {
      RetryUtil.retryOnThrowable(NodeExistsException.class, 60000, 5000, () -> {
        synchronized (lock) {
          log.info("Creating leader registration node {} after winning as {}", leaderPath, leaderSeqPath);
          List<Op> ops = new ArrayList<>(2);

          // We use a multi operation to get the parent nodes version, which will
          // be used to make sure we only remove our own leader registration node.
          // The setData call used to get the parent version is also the trigger to
          // increment the version. We also do a sanity check that our leaderSeqPath exists.

          ops.add(Op.check(leaderSeqPath, -1));
          ops.add(Op.create(leaderPath, Utils.toJSON(leaderProps), zkClient.getZkACLProvider().getACLsToAdd(leaderPath), CreateMode.EPHEMERAL));
          ops.add(Op.setData(parent, null, -1));
          List<OpResult> results;

          results = zkClient.multi(ops, true);
          for (OpResult result : results) {
            if (result.getType() == ZooDefs.OpCode.setData) {
              SetDataResult dresult = (SetDataResult) result;
              Stat stat = dresult.getStat();
              leaderZkNodeParentVersion = stat.getVersion();
              return;
            }
          }
          assert leaderZkNodeParentVersion != null;
        }
      });
    } catch (NoNodeException e) {
      log.info("Will not register as leader because it seems the election is no longer taking place.");
      return;
    } catch (Throwable t) {
      if (t instanceof OutOfMemoryError) {
        throw (OutOfMemoryError) t;
      }
      throw new SolrException(ErrorCode.SERVER_ERROR, "Could not register as the leader because creating the ephemeral registration node in ZooKeeper failed", t);
    }

    assert shardId != null;
    boolean isAlreadyLeader = false;
    if (zkStateReader.getClusterState() != null &&
        zkStateReader.getClusterState().getCollection(collection).getSlice(shardId).getReplicas().size() < 2) {
      Replica leader = zkStateReader.getLeader(collection, shardId);
      if (leader != null
          && leader.getNodeName().equals(leaderProps.get(ZkStateReader.NODE_NAME_PROP))
          && leader.getCoreName().equals(leaderProps.get(ZkStateReader.CORE_NAME_PROP))) {
        isAlreadyLeader = true;
      }
    }
    if (!isAlreadyLeader) {
      String nodeName = leaderProps.getStr(ZkStateReader.NODE_NAME_PROP);
      ZkNodeProps m = ZkNodeProps.fromKeyVals(Overseer.QUEUE_OPERATION, OverseerAction.LEADER.toLower(),
          ZkStateReader.SHARD_ID_PROP, shardId,
          ZkStateReader.COLLECTION_PROP, collection,
          ZkStateReader.NODE_NAME_PROP, nodeName,
          ZkStateReader.BASE_URL_PROP, zkStateReader.getBaseUrlForNodeName(nodeName),
          ZkStateReader.CORE_NAME_PROP, leaderProps.get(ZkStateReader.CORE_NAME_PROP),
          ZkStateReader.STATE_PROP, Replica.State.ACTIVE.toString());
      assert zkController != null;
      assert zkController.getOverseer() != null;
      DocCollection coll = zkStateReader.getCollection(this.collection);
      if (coll == null || coll.getStateFormat() < 2 || ZkController.sendToOverseer(coll, id)) {
        zkController.getOverseer().offerStateUpdate(Utils.toJSON(m));
      } else {
        PerReplicaStates prs = PerReplicaStates.fetch(coll.getZNode(), zkClient, coll.getPerReplicaStates());
        PerReplicaStatesOps.flipLeader(zkStateReader.getClusterState().getCollection(collection).getSlice(shardId).getReplicaNames(), id, prs)
            .persist(coll.getZNode(), zkStateReader.getZkClient());
      }
    }
  }

  public LeaderElector getLeaderElector() {
    return leaderElector;
  }

}