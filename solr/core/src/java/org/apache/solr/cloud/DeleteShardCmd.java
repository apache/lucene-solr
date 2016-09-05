
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.solr.cloud.OverseerCollectionMessageHandler.Cmd;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.NODE_NAME_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETEREPLICA;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETESHARD;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;

public class DeleteShardCmd implements Cmd {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final OverseerCollectionMessageHandler ocmh;

  public DeleteShardCmd(OverseerCollectionMessageHandler ocmh) {
    this.ocmh = ocmh;
  }

  @Override
  public void call(ClusterState clusterState, ZkNodeProps message, NamedList results) throws Exception {
    String collectionName = message.getStr(ZkStateReader.COLLECTION_PROP);
    String sliceId = message.getStr(ZkStateReader.SHARD_ID_PROP);

    log.info("Delete shard invoked");
    Slice slice = clusterState.getSlice(collectionName, sliceId);

    if (slice == null) {
      if (clusterState.hasCollection(collectionName)) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "No shard with name " + sliceId + " exists for collection " + collectionName);
      } else {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No collection with the specified name exists: " + collectionName);
      }
    }
    // For now, only allow for deletions of Inactive slices or custom hashes (range==null).
    // TODO: Add check for range gaps on Slice deletion
    final Slice.State state = slice.getState();
    if (!(slice.getRange() == null || state == Slice.State.INACTIVE || state == Slice.State.RECOVERY
        || state == Slice.State.CONSTRUCTION)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "The slice: " + slice.getName() + " is currently " + state
          + ". Only non-active (or custom-hashed) slices can be deleted.");
    }

    if (state == Slice.State.RECOVERY)  {
      // mark the slice as 'construction' and only then try to delete the cores
      // see SOLR-9455
      DistributedQueue inQueue = Overseer.getStateUpdateQueue(ocmh.zkStateReader.getZkClient());
      Map<String, Object> propMap = new HashMap<>();
      propMap.put(Overseer.QUEUE_OPERATION, OverseerAction.UPDATESHARDSTATE.toLower());
      propMap.put(sliceId, Slice.State.CONSTRUCTION.toString());
      propMap.put(ZkStateReader.COLLECTION_PROP, collectionName);
      ZkNodeProps m = new ZkNodeProps(propMap);
      inQueue.offer(Utils.toJSON(m));
    }

    String asyncId = message.getStr(ASYNC);

    try {
      List<ZkNodeProps> replicas = getReplicasForSlice(collectionName, slice);
      CountDownLatch cleanupLatch = new CountDownLatch(replicas.size());
      for (ZkNodeProps r : replicas) {
        final ZkNodeProps replica = r.plus(message.getProperties()).plus("parallel", "true").plus(ASYNC, asyncId);
        log.info("Deleting replica for collection={} shard={} on node={}", replica.getStr(COLLECTION_PROP), replica.getStr(SHARD_ID_PROP), replica.getStr(CoreAdminParams.NODE));
        NamedList deleteResult = new NamedList();
        try {
          ((DeleteReplicaCmd)ocmh.commandMap.get(DELETEREPLICA)).deleteReplica(clusterState, replica, deleteResult, () -> {
            cleanupLatch.countDown();
            if (deleteResult.get("failure") != null) {
              synchronized (results) {
                results.add("failure", String.format(Locale.ROOT, "Failed to delete replica for collection=%s shard=%s" +
                    " on node=%s", replica.getStr(COLLECTION_PROP), replica.getStr(SHARD_ID_PROP), replica.getStr(NODE_NAME_PROP)));
              }
            }
            SimpleOrderedMap success = (SimpleOrderedMap) deleteResult.get("success");
            if (success != null) {
              synchronized (results)  {
                results.add("success", success);
              }
            }
          });
        } catch (KeeperException e) {
          log.warn("Error deleting replica: " + r, e);
          cleanupLatch.countDown();
        } catch (Exception e) {
          log.warn("Error deleting replica: " + r, e);
          cleanupLatch.countDown();
          throw e;
        }
      }
      log.debug("Waiting for delete shard action to complete");
      cleanupLatch.await(5, TimeUnit.MINUTES);

      ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION, DELETESHARD.toLower(), ZkStateReader.COLLECTION_PROP,
          collectionName, ZkStateReader.SHARD_ID_PROP, sliceId);
      ZkStateReader zkStateReader = ocmh.zkStateReader;
      Overseer.getStateUpdateQueue(zkStateReader.getZkClient()).offer(Utils.toJSON(m));

      // wait for a while until we don't see the shard
      TimeOut timeout = new TimeOut(30, TimeUnit.SECONDS);
      boolean removed = false;
      while (!timeout.hasTimedOut()) {
        Thread.sleep(100);
        DocCollection collection = zkStateReader.getClusterState().getCollection(collectionName);
        removed = collection.getSlice(sliceId) == null;
        if (removed) {
          Thread.sleep(100); // just a bit of time so it's more likely other readers see on return
          break;
        }
      }
      if (!removed) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "Could not fully remove collection: " + collectionName + " shard: " + sliceId);
      }

      log.info("Successfully deleted collection: " + collectionName + ", shard: " + sliceId);
    } catch (SolrException e) {
      throw e;
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Error executing delete operation for collection: " + collectionName + " shard: " + sliceId, e);
    }
  }

  private List<ZkNodeProps> getReplicasForSlice(String collectionName, Slice slice) {
    List<ZkNodeProps> sourceReplicas = new ArrayList<>();
    for (Replica replica : slice.getReplicas()) {
      ZkNodeProps props = new ZkNodeProps(
          COLLECTION_PROP, collectionName,
          SHARD_ID_PROP, slice.getName(),
          ZkStateReader.CORE_NAME_PROP, replica.getCoreName(),
          ZkStateReader.REPLICA_PROP, replica.getName(),
          CoreAdminParams.NODE, replica.getNodeName());
      sourceReplicas.add(props);
    }
    return sourceReplicas;
  }
}
