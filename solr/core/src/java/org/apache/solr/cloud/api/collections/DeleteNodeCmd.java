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

package org.apache.solr.cloud.api.collections;


import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.NamedList;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETEREPLICA;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;

public class DeleteNodeCmd implements OverseerCollectionMessageHandler.Cmd {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final OverseerCollectionMessageHandler ocmh;

  public DeleteNodeCmd(OverseerCollectionMessageHandler ocmh) {
    this.ocmh = ocmh;
  }

  @Override
  @SuppressWarnings({"unchecked"})
  public void call(ClusterState state, ZkNodeProps message, @SuppressWarnings({"rawtypes"})NamedList results) throws Exception {
    ocmh.checkRequired(message, "node");
    String node = message.getStr("node");
    List<ZkNodeProps> sourceReplicas = ReplaceNodeCmd.getReplicasOfNode(node, state);
    List<String> singleReplicas = verifyReplicaAvailability(sourceReplicas, state);
    if (!singleReplicas.isEmpty()) {
      results.add("failure", "Can't delete the only existing non-PULL replica(s) on node " + node + ": " + singleReplicas.toString());
    } else {
      cleanupReplicas(results, state, sourceReplicas, ocmh, node, message.getStr(ASYNC));
    }
  }

  // collect names of replicas that cannot be deleted
  static List<String> verifyReplicaAvailability(List<ZkNodeProps> sourceReplicas, ClusterState state) {
    List<String> res = new ArrayList<>();
    for (ZkNodeProps sourceReplica : sourceReplicas) {
      String coll = sourceReplica.getStr(COLLECTION_PROP);
      String shard = sourceReplica.getStr(SHARD_ID_PROP);
      String replicaName = sourceReplica.getStr(ZkStateReader.REPLICA_PROP);
      DocCollection collection = state.getCollection(coll);
      Slice slice = collection.getSlice(shard);
      if (slice.getReplicas().size() < 2) {
        // can't delete the only replica in existence
        res.add(coll + "/" + shard + "/" + replicaName + ", type=" + sourceReplica.getStr(ZkStateReader.REPLICA_TYPE));
      } else { // check replica types
        int otherNonPullReplicas = 0;
        for (Replica r : slice.getReplicas()) {
          if (!r.getName().equals(replicaName) && !r.getType().equals(Replica.Type.PULL)) {
            otherNonPullReplicas++;
          }
        }
        // can't delete - there are no other non-pull replicas
        if (otherNonPullReplicas == 0) {
          res.add(coll + "/" + shard + "/" + replicaName + ", type=" + sourceReplica.getStr(ZkStateReader.REPLICA_TYPE));
        }
      }
    }
    return res;
  }

  @SuppressWarnings({"unchecked"})
  static void cleanupReplicas(@SuppressWarnings({"rawtypes"})NamedList results,
                              ClusterState clusterState,
                              List<ZkNodeProps> sourceReplicas,
                              OverseerCollectionMessageHandler ocmh,
                              String node,
                              String async) throws InterruptedException {
    CountDownLatch cleanupLatch = new CountDownLatch(sourceReplicas.size());
    for (ZkNodeProps sourceReplica : sourceReplicas) {
      String coll = sourceReplica.getStr(COLLECTION_PROP);
      String shard = sourceReplica.getStr(SHARD_ID_PROP);
      String type = sourceReplica.getStr(ZkStateReader.REPLICA_TYPE);
      log.info("Deleting replica type={} for collection={} shard={} on node={}", type, coll, shard, node);
      @SuppressWarnings({"rawtypes"})
      NamedList deleteResult = new NamedList();
      try {
        if (async != null) sourceReplica = sourceReplica.plus(ASYNC, async);
        ((DeleteReplicaCmd)ocmh.commandMap.get(DELETEREPLICA)).deleteReplica(clusterState, sourceReplica.plus("parallel", "true"), deleteResult, () -> {
          cleanupLatch.countDown();
          if (deleteResult.get("failure") != null) {
            synchronized (results) {

              results.add("failure", String.format(Locale.ROOT, "Failed to delete replica for collection=%s shard=%s" +
                  " on node=%s", coll, shard, node));
            }
          }
        });
      } catch (KeeperException e) {
        log.warn("Error deleting ", e);
        cleanupLatch.countDown();
      } catch (Exception e) {
        log.warn("Error deleting ", e);
        cleanupLatch.countDown();
        throw e;
      }
    }
    log.debug("Waiting for delete node action to complete");
    cleanupLatch.await(5, TimeUnit.MINUTES);
  }


}
