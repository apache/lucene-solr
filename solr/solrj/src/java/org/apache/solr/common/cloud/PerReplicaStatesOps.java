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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.singletonList;

/**
 * This is a helper class that encapsulates various operations performed on the per-replica states
 * Do not directly manipulate the per replica states as it can become difficult to debug them
 */
public class PerReplicaStatesOps {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private PerReplicaStates rs;
  List<PerReplicaStates.Operation> ops;
  private boolean preOp = true;
  final Function<PerReplicaStates, List<PerReplicaStates.Operation>> fun;

  PerReplicaStatesOps(Function<PerReplicaStates, List<PerReplicaStates.Operation>> fun) {
    this.fun = fun;
  }

  /**
   * Persist a set of operations to Zookeeper
   */
  private void persist(List<PerReplicaStates.Operation> operations, String znode, SolrZkClient zkClient) throws KeeperException, InterruptedException {
    if (operations == null || operations.isEmpty()) return;
    if (log.isDebugEnabled()) {
      log.debug("Per-replica state being persisted for : '{}', ops: {}", znode, operations);
    }

    List<Op> ops = new ArrayList<>(operations.size());
    for (PerReplicaStates.Operation op : operations) {
      // the state of the replica is being updated
      String path = znode + "/" + op.state.asString;
      ops.add(op.typ == PerReplicaStates.Operation.Type.ADD ?
          Op.create(path, null, zkClient.getZkACLProvider().getACLsToAdd(path), CreateMode.PERSISTENT) :
          Op.delete(path, -1));
    }
    try {
      zkClient.multi(ops, true);
    } catch (KeeperException e) {
      log.error("multi op exception : " + e.getMessage() + zkClient.getChildren(znode, null, true));
      throw e;
    }

  }

  /**
   * There is a possibility that a replica may have some leftover entries. Delete them too.
   */
  private static List<PerReplicaStates.Operation> addDeleteStaleNodes(List<PerReplicaStates.Operation> ops, PerReplicaStates.State rs) {
    while (rs != null) {
      ops.add(new PerReplicaStates.Operation(PerReplicaStates.Operation.Type.DELETE, rs));
      rs = rs.duplicate;
    }
    return ops;
  }

  /**
   * This is a persist operation with retry if a write fails due to stale state
   */
  public void persist(String znode, SolrZkClient zkClient) throws KeeperException, InterruptedException {
    List<PerReplicaStates.Operation> operations = ops;
    for (int i = 0; i < PerReplicaStates.MAX_RETRIES; i++) {
      try {
        persist(operations, znode, zkClient);
        return;
      } catch (KeeperException.NodeExistsException | KeeperException.NoNodeException e) {
        // state is stale
        if(log.isInfoEnabled()) {
          log.info("Stale state for {}, attempt: {}. retrying...", znode, i);
        }
        operations = refresh(PerReplicaStates.fetch(znode, zkClient, null));
      }
    }
  }

  public PerReplicaStates getPerReplicaStates() {
    return rs;
  }

  /**
   * Change the state of a replica
   *
   * @param newState the new state
   */
  public static PerReplicaStatesOps flipState(String replica, Replica.State newState, PerReplicaStates rs) {
    return new PerReplicaStatesOps(prs -> {
      List<PerReplicaStates.Operation> operations = new ArrayList<>(2);
      PerReplicaStates.State existing = prs.get(replica);
      if (existing == null) {
        operations.add(new PerReplicaStates.Operation(PerReplicaStates.Operation.Type.ADD, new PerReplicaStates.State(replica, newState, Boolean.FALSE, 0)));
      } else {
        operations.add(new PerReplicaStates.Operation(PerReplicaStates.Operation.Type.ADD, new PerReplicaStates.State(replica, newState, existing.isLeader, existing.version + 1)));
        addDeleteStaleNodes(operations, existing);
      }
      if (log.isDebugEnabled()) {
        log.debug("flipState on {}, {} -> {}, ops :{}", prs.path, replica, newState, operations);
      }
      return operations;
    }).init(rs);
  }

  /**
   * Switch a collection from/to perReplicaState=true
   */
  public static PerReplicaStatesOps modifyCollection(DocCollection coll, boolean enable, PerReplicaStates rs) {
    return new PerReplicaStatesOps(prs -> enable ?
        enable(coll,prs) :
        disable(prs)).init(rs);

  }

  private static List<PerReplicaStates.Operation> enable(DocCollection coll, PerReplicaStates prs) {
    List<PerReplicaStates.Operation> result = new ArrayList<>();
    coll.forEachReplica((s, r) -> {
      PerReplicaStates.State st = prs.states.get(r.getName());
      int newVer = 0;
      if (st != null) {
        result.add(new PerReplicaStates.Operation(PerReplicaStates.Operation.Type.DELETE, st));
        newVer = st.version + 1;
      }
      result.add(new PerReplicaStates.Operation(PerReplicaStates.Operation.Type.ADD,
          new PerReplicaStates.State(r.getName(), r.getState(), r.isLeader(), newVer)));
    });
    return result;
  }

  private static List<PerReplicaStates.Operation> disable(PerReplicaStates prs) {
    List<PerReplicaStates.Operation> result = new ArrayList<>();
    prs.states.forEachEntry((s, state) -> result.add(new PerReplicaStates.Operation(PerReplicaStates.Operation.Type.DELETE, state)));
    return result;
  }

  /**
   * Flip the leader replica to a new one
   *
   * @param allReplicas allReplicas of the shard
   * @param next        next leader
   */
  public static PerReplicaStatesOps flipLeader(Set<String> allReplicas, String next, PerReplicaStates rs) {
    return new PerReplicaStatesOps(prs -> {
      List<PerReplicaStates.Operation> ops = new ArrayList<>();
      if (next != null) {
        PerReplicaStates.State st = prs.get(next);
        if (st != null) {
          if (!st.isLeader) {
            ops.add(new PerReplicaStates.Operation(PerReplicaStates.Operation.Type.ADD, new PerReplicaStates.State(st.replica, Replica.State.ACTIVE, Boolean.TRUE, st.version + 1)));
            ops.add(new PerReplicaStates.Operation(PerReplicaStates.Operation.Type.DELETE, st));
          }
          // else do not do anything, that node is the leader
        } else {
          // there is no entry for the new leader.
          // create one
          ops.add(new PerReplicaStates.Operation(PerReplicaStates.Operation.Type.ADD, new PerReplicaStates.State(next, Replica.State.ACTIVE, Boolean.TRUE, 0)));
        }
      }

      // now go through all other replicas and unset previous leader
      for (String r : allReplicas) {
        PerReplicaStates.State st = prs.get(r);
        if (st == null) continue;//unlikely
        if (!Objects.equals(r, next)) {
          if (st.isLeader) {
            //some other replica is the leader now. unset
            ops.add(new PerReplicaStates.Operation(PerReplicaStates.Operation.Type.ADD, new PerReplicaStates.State(st.replica, st.state, Boolean.FALSE, st.version + 1)));
            ops.add(new PerReplicaStates.Operation(PerReplicaStates.Operation.Type.DELETE, st));
          }
        }
      }
      if (log.isDebugEnabled()) {
        log.debug("flipLeader on:{}, {} -> {}, ops: {}", prs.path, allReplicas, next, ops);
      }
      return ops;
    }).init(rs);
  }

  /**
   * Delete a replica entry from per-replica states
   *
   * @param replica name of the replica to be deleted
   */
  public static PerReplicaStatesOps deleteReplica(String replica, PerReplicaStates rs) {
    return new PerReplicaStatesOps(prs -> {
      List<PerReplicaStates.Operation> result;
      if (prs == null) {
        result = Collections.emptyList();
      } else {
        PerReplicaStates.State state = prs.get(replica);
        result = addDeleteStaleNodes(new ArrayList<>(), state);
      }
      return result;
    }).init(rs);
  }

  public static PerReplicaStatesOps addReplica(String replica, Replica.State state, boolean isLeader, PerReplicaStates rs) {
    return new PerReplicaStatesOps(perReplicaStates -> singletonList(new PerReplicaStates.Operation(PerReplicaStates.Operation.Type.ADD,
        new PerReplicaStates.State(replica, state, isLeader, 0)))).init(rs);
  }

  /**
   * Mark the given replicas as DOWN
   */
  public static PerReplicaStatesOps downReplicas(List<String> replicas, PerReplicaStates rs) {
    return new PerReplicaStatesOps(prs -> {
      List<PerReplicaStates.Operation> operations = new ArrayList<>();
      for (String replica : replicas) {
        PerReplicaStates.State r = prs.get(replica);
        if (r != null) {
          if (r.state == Replica.State.DOWN && !r.isLeader) continue;
          operations.add(new PerReplicaStates.Operation(PerReplicaStates.Operation.Type.ADD, new PerReplicaStates.State(replica, Replica.State.DOWN, Boolean.FALSE, r.version + 1)));
          addDeleteStaleNodes(operations, r);
        } else {
          operations.add(new PerReplicaStates.Operation(PerReplicaStates.Operation.Type.ADD, new PerReplicaStates.State(replica, Replica.State.DOWN, Boolean.FALSE, 0)));
        }
      }
      if (log.isDebugEnabled()) {
        log.debug("for coll: {} down replicas {}, ops {}", prs, replicas, operations);
      }
      return operations;
    }).init(rs);
  }

  /**
   * Just creates and deletes a dummy entry so that the {@link Stat#getCversion()} of state.json
   * is updated
   */
  public static PerReplicaStatesOps touchChildren() {
    PerReplicaStatesOps result = new PerReplicaStatesOps(prs -> {
      List<PerReplicaStates.Operation> operations = new ArrayList<>(2);
      PerReplicaStates.State st = new PerReplicaStates.State(".dummy." + System.nanoTime(), Replica.State.DOWN, Boolean.FALSE, 0);
      operations.add(new PerReplicaStates.Operation(PerReplicaStates.Operation.Type.ADD, st));
      operations.add(new PerReplicaStates.Operation(PerReplicaStates.Operation.Type.DELETE, st));
      if (log.isDebugEnabled()) {
        log.debug("touchChildren {}", operations);
      }
      return operations;
    });
    result.preOp = false;
    result.ops = result.refresh(null);
    return result;
  }

  PerReplicaStatesOps init(PerReplicaStates rs) {
    if (rs == null) return null;
    get(rs);
    return this;
  }

  public List<PerReplicaStates.Operation> get() {
    return ops;
  }

  public List<PerReplicaStates.Operation> get(PerReplicaStates rs) {
    ops = refresh(rs);
    if (ops == null) ops = Collections.emptyList();
    this.rs = rs;
    return ops;
  }

  /**
   * To be executed before collection state.json is persisted
   */
  public boolean isPreOp() {
    return preOp;
  }

  /**
   * This method should compute the set of ZK operations for a given action
   * for instance, a state change may result in 2 operations on per-replica states (1 CREATE and 1 DELETE)
   * if a multi operation fails because the state got modified from behind,
   * refresh the operation and try again
   *
   * @param prs The latest state
   */
  List<PerReplicaStates.Operation> refresh(PerReplicaStates prs) {
    if (fun != null) return fun.apply(prs);
    return null;
  }

  @Override
  public String toString() {
    return ops.toString();
  }
}
