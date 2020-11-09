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
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
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
  protected volatile boolean closed;
  private volatile Integer leaderZkNodeParentVersion;

  public ShardLeaderElectionContextBase(final String coreNodeName, String electionPath, String leaderPath,
                                        Replica props, SolrZkClient zkClient) {
    super(coreNodeName, electionPath, leaderPath, props);
    this.zkClient = zkClient;
  }

  @Override
  public void close() {
    this.closed = true;
    try {
      super.close();
    } catch (Exception e) {
      ParWork.propagateInterrupt(e);
      log.error("Exception canceling election", e);
    } finally {
      leaderZkNodeParentVersion = null;
    }
  }

  @Override
  protected void cancelElection() throws InterruptedException, KeeperException {
    if (log.isDebugEnabled()) log.debug("cancelElection");
    if (!zkClient.isConnected()) {
      log.info("Can't cancel, zkClient is not connected");
      return;
    }
    super.cancelElection();
      try {
        if (leaderZkNodeParentVersion != null) {
          try {
//            if (!zkClient.exists(leaderSeqPath)) {
//              return;
//            }
            // We need to be careful and make sure we *only* delete our own leader registration node.
            // We do this by using a multi and ensuring the parent znode of the leader registration node
            // matches the version we expect - there is a setData call that increments the parent's znode
            // version whenever a leader registers.
            log.info("Removing leader registration node on cancel, parent node: {} {}", Paths.get(leaderPath).getParent().toString(), leaderZkNodeParentVersion);
            List<Op> ops = new ArrayList<>(3);
            ops.add(Op.check(Paths.get(leaderPath).getParent().toString(), leaderZkNodeParentVersion));
            ops.add(Op.delete(leaderSeqPath, -1));
            ops.add(Op.delete(leaderPath, -1));
            zkClient.multi(ops, false);
          } catch (KeeperException e) {
            if (e instanceof NoNodeException) {
              // okay
              return;
            }
            if (e instanceof KeeperException.SessionExpiredException) {
              log.warn("ZooKeeper session expired");
              throw e;
            }

            int i = 0;
            List<OpResult> results = e.getResults();
            for (OpResult result : results) {
              if (((OpResult.ErrorResult) result).getErr() == -101) {
                // no node, fine
              } else {
                if (result instanceof OpResult.ErrorResult) {
                  OpResult.ErrorResult dresult = (OpResult.ErrorResult) result;
                  if (dresult.getErr() != 0) {
                    log.error("op=" + i++ + " err=" + dresult.getErr());
                  }
                }
                throw new SolrException(ErrorCode.SERVER_ERROR, "Exception canceling election " + e.getPath(), e);
              }
            }

          } catch (InterruptedException | AlreadyClosedException e) {
            ParWork.propagateInterrupt(e, true);
            return;
          } catch (Exception e) {
            throw new SolrException(ErrorCode.SERVER_ERROR, "Exception canceling election", e);
          }
        } else {
          try {
            if (leaderSeqPath != null) {
              zkClient.delete(leaderSeqPath, -1);
            }
          } catch (NoNodeException e) {
            // fine
          }
          log.info("No version found for ephemeral leader parent node, won't remove previous leader registration.");
        }
      } catch (Exception e) {
        if (e instanceof InterruptedException) {
          ParWork.propagateInterrupt(e);
        }
        Stat stat = new Stat();
        zkClient.getData(Paths.get(leaderPath).getParent().toString(), null, stat);
        log.error("Exception trying to cancel election {} {} {}", stat.getVersion(), e.getClass().getName(), e.getMessage(), e);
      }
      leaderZkNodeParentVersion = null;
  }

  @Override
  synchronized void runLeaderProcess(ElectionContext context, boolean weAreReplacement, int pauseBeforeStartMs)
          throws KeeperException, InterruptedException, IOException {
    // register as leader - if an ephemeral is already there, wait to see if it goes away

    String parent = Paths.get(leaderPath).getParent().toString();
    List<String> errors = new ArrayList<>();

    try {

      if (leaderSeqPath == null) {
        throw new IllegalStateException("We have won as leader, but we have no leader election node known to us leaderPath " + leaderPath);
      }

      log.info("Creating leader registration node {} after winning as {} parent is {}", leaderPath, leaderSeqPath, parent);
      List<Op> ops = new ArrayList<>(3);

      // We use a multi operation to get the parent nodes version, which will
      // be used to make sure we only remove our own leader registration node.
      // The setData call used to get the parent version is also the trigger to
      // increment the version. We also do a sanity check that our leaderSeqPath exists.

      ops.add(Op.check(leaderSeqPath, -1));
      ops.add(Op.create(leaderPath, Utils.toJSON(leaderProps), zkClient.getZkACLProvider().getACLsToAdd(leaderPath), CreateMode.EPHEMERAL));
      ops.add(Op.setData(parent, null, -1));
      List<OpResult> results;

      results = zkClient.multi(ops, false);
      log.info("Results from call {}", results);
      Iterator<Op> it = ops.iterator();
      for (OpResult result : results) {
        if (result.getType() == ZooDefs.OpCode.setData) {
          SetDataResult dresult = (SetDataResult) result;
          Stat stat = dresult.getStat();
          leaderZkNodeParentVersion = stat.getVersion();
          log.info("Got leaderZkNodeParentVersion {}", leaderZkNodeParentVersion);
        }
      }
      //assert leaderZkNodeParentVersion != null;

    } catch (Throwable t) {
      ParWork.propagateInterrupt(t);
      throw new SolrException(ErrorCode.SERVER_ERROR, "Could not register as the leader because creating the ephemeral registration node in ZooKeeper failed: " + errors, t);
    }

  }

  @Override
  public boolean isClosed() {
    return closed || zkClient.isClosed();
  }
}
