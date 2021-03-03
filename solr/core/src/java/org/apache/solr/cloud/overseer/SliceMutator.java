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
package org.apache.solr.cloud.overseer;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.apache.solr.client.solrj.cloud.DistribStateManager;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.api.collections.Assign;
import org.apache.solr.cloud.api.collections.OverseerCollectionMessageHandler;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.RoutingRule;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SliceMutator {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String PREFERRED_LEADER_PROP = OverseerCollectionMessageHandler.COLL_PROP_PREFIX + "preferredleader";

  public static final Set<String> SLICE_UNIQUE_BOOLEAN_PROPERTIES = ImmutableSet.of(PREFERRED_LEADER_PROP);

  protected final SolrCloudManager cloudManager;
  protected final DistribStateManager stateManager;

  public SliceMutator(SolrCloudManager cloudManager) {
    this.cloudManager = cloudManager;
    this.stateManager = cloudManager.getDistribStateManager();
  }

  public ClusterState addReplica(ClusterState clusterState, ZkNodeProps message) {
    if (log.isDebugEnabled()) log.debug("createReplica() {} ", message);
    String coll = message.getStr(ZkStateReader.COLLECTION_PROP);
    // if (!checkCollectionKeyExistence(message)) return ZkStateWriter.NO_OP;
    String slice = message.getStr(ZkStateReader.SHARD_ID_PROP);

    DocCollection collection = clusterState.getCollection(coll);

    String coreName;
    if (message.getStr(ZkStateReader.CORE_NAME_PROP) != null) {
      coreName = message.getStr(ZkStateReader.CORE_NAME_PROP);
    } else {
      coreName = Assign.buildSolrCoreName(collection, slice, Replica.Type.get(message.getStr(ZkStateReader.REPLICA_TYPE)));
    }
    Replica replica = new Replica(coreName,
        Utils.makeNonNullMap("id", String.valueOf(collection.getHighestReplicaId()),
                    ZkStateReader.STATE_PROP, Replica.State.DOWN,
                    ZkStateReader.NODE_NAME_PROP, message.getStr(ZkStateReader.NODE_NAME_PROP),
                    ZkStateReader.NUM_SHARDS_PROP, message.getStr(ZkStateReader.NUM_SHARDS_PROP),
                    "shards", message.getStr("shards"),
                    ZkStateReader.REPLICA_TYPE, message.get(ZkStateReader.REPLICA_TYPE)), coll, collection.getId(), slice, (Replica.NodeNameToBaseUrl) cloudManager.getClusterStateProvider());

    if (log.isDebugEnabled()) {
      log.debug("addReplica(ClusterState, ZkNodeProps) - end");
    }
    return clusterState.copyWith(coll, updateReplica((Replica.NodeNameToBaseUrl) cloudManager.getClusterStateProvider(), collection, slice, replica.getName(), replica));
  }

  public ClusterState removeReplica(ClusterState clusterState, ZkNodeProps message) {

    if (log.isDebugEnabled()) log.debug("removeReplica(ClusterState clusterState={}, ZkNodeProps message={}) - start", clusterState, message);

    String coreName = message.getStr(ZkStateReader.REPLICA_PROP);
    if (coreName == null) {
      coreName = message.getStr(ZkStateReader.CORE_NAME_PROP);
    }
    final String collection = message.getStr(ZkStateReader.COLLECTION_PROP);

    DocCollection coll = clusterState.getCollectionOrNull(collection);
    if (coll == null) {
      throw new IllegalStateException("No collection found to remove replica from collection=" + collection);
    }

    Map<String, Slice> newSlices = new LinkedHashMap<>(coll.getSlices().size() - 1);

    for (Slice slice : coll.getSlices()) {
      Replica replica = slice.getReplica(coreName);
      if (replica != null) {
        Map<String, Replica> newReplicas = slice.getReplicasCopy();
        newReplicas.remove(coreName);
        slice = new Slice(slice.getName(), newReplicas, slice.getProperties(), collection, coll.getId(), (Replica.NodeNameToBaseUrl) cloudManager.getClusterStateProvider());
      }
      newSlices.put(slice.getName(), slice);
    }


    if (log.isDebugEnabled()) {
      log.debug("removeReplica(ClusterState, ZkNodeProps) - end");
    }
    return clusterState.copyWith(collection, coll.copyWithSlices(newSlices));
  }

  public ClusterState setShardLeader(ClusterState clusterState, ZkNodeProps message) {
    if (log.isDebugEnabled()) {
      log.debug("setShardLeader(ClusterState clusterState={}, ZkNodeProps message={}) - start", clusterState, message);
    }

    String coreName = message.getStr(ZkStateReader.CORE_NAME_PROP);

    String collectionName = message.getStr(ZkStateReader.COLLECTION_PROP);
    String sliceName = message.getStr(ZkStateReader.SHARD_ID_PROP);
    DocCollection coll = clusterState.getCollectionOrNull(collectionName);

    if (coll == null) {
      log.error("Could not mark shard leader for non existing collection: {} {}", collectionName, message);
      return clusterState;
    }

    Slice slice = coll.getSlice(sliceName);

    if (slice == null) {
      log.error("Could not mark shard leader for non existing slice: {}", sliceName);
      return clusterState;
    }

    final Map<String, Replica> newReplicas = new LinkedHashMap<>();
    for (Replica replica : slice.getReplicas()) {
      // TODO: this should only be calculated once and cached somewhere?
      if (log.isDebugEnabled()) log.debug("examine for setting or unsetting as leader replica={}", replica);

      if (coreName.equals(replica.getName())) {
        if (log.isDebugEnabled()) log.debug("Set leader {}", replica.getName());
        replica = new ReplicaMutator(cloudManager).setLeader(replica);
      } else if (replica.getBool("leader", false)) {
        if (log.isDebugEnabled()) log.debug("Unset leader");
        replica = new ReplicaMutator(cloudManager).unsetLeader(replica);
      }

      newReplicas.put(replica.getName(), replica);
    }

    Map<String, Object> newSliceProps = slice.shallowCopy();
    newSliceProps.put(Slice.REPLICAS, newReplicas);
    slice = new Slice(slice.getName(), newReplicas, slice.getProperties(), collectionName, coll.getId(), (Replica.NodeNameToBaseUrl) cloudManager.getClusterStateProvider());

    clusterState = clusterState.copyWith(collectionName, CollectionMutator.updateSlice(collectionName, coll, slice));
    if (log.isDebugEnabled()) log.debug("setShardLeader {} {}", sliceName, clusterState);

    return clusterState;
  }

  public ClusterState updateShardState(ClusterState clusterState, ZkNodeProps message) {
    if (log.isDebugEnabled()) {
      log.debug("updateShardState(ClusterState clusterState={}, ZkNodeProps message={}) - start", clusterState, message);
    }

    String collectionName = message.getStr(ZkStateReader.COLLECTION_PROP);

    DocCollection collection = clusterState.getCollection(collectionName);
    Map<String, Slice> slicesCopy = new LinkedHashMap<>(collection.getSlicesMap());
    for (String key : message.keySet()) {
      if (ZkStateReader.COLLECTION_PROP.equals(key)) continue;
      if (Overseer.QUEUE_OPERATION.equals(key)) continue;
      if (key == null) continue;

      Slice slice = collection.getSlice(key);
      if (slice == null) {
        throw new RuntimeException("Overseer.updateShardState unknown slice: " + collectionName + " slice: " + key);
      }
      if (log.isDebugEnabled()) log.debug("Update shard state " + key + " to " + message.getStr(key));
      Map<String, Object> props = slice.shallowCopy();

      if (Slice.State.getState(message.getStr(key)) == Slice.State.ACTIVE) {
        props.remove(Slice.PARENT);
        props.remove("shard_parent_node");
        props.remove("shard_parent_zk_session");
      }
      props.put(ZkStateReader.STATE_PROP, message.getStr(key));
      // we need to use epoch time so that it's comparable across Overseer restarts
      props.put(ZkStateReader.STATE_TIMESTAMP_PROP, String.valueOf(cloudManager.getTimeSource().getEpochTimeNs()));
      Slice newSlice = new Slice(slice.getName(), slice.getReplicasCopy(), props, collectionName, collection.getId(), (Replica.NodeNameToBaseUrl) cloudManager.getClusterStateProvider());

      slicesCopy.put(slice.getName(), newSlice);
    }

    ClusterState cs = clusterState.copyWith(collectionName, collection.copyWithSlices(slicesCopy));
    if (log.isDebugEnabled()) log.debug("updateShardState return clusterstate={}", cs);
    return cs;
  }

  public ClusterState addRoutingRule(final ClusterState clusterState, ZkNodeProps message) {
    if (log.isDebugEnabled()) {
      log.debug("addRoutingRule(ClusterState clusterState={}, ZkNodeProps message={}) - start", clusterState, message);
    }

    String collectionName = message.getStr(ZkStateReader.COLLECTION_PROP);

    String shard = message.getStr(ZkStateReader.SHARD_ID_PROP);
    String routeKey = message.getStr("routeKey");
    String range = message.getStr("range");
    String targetCollection = message.getStr("targetCollection");
    String expireAt = message.getStr("expireAt");

    DocCollection collection = clusterState.getCollection(collectionName);
    Slice slice = collection.getSlice(shard);
    if (slice == null) {
      throw new RuntimeException("Overseer.addRoutingRule unknown collection: " + collectionName + " slice:" + shard);
    }

    Map<String, RoutingRule> routingRules = slice.getRoutingRules();
    if (routingRules == null)
      routingRules = new HashMap<>();
    RoutingRule r = routingRules.get(routeKey);
    if (r == null) {
      Map<String, Object> map = new HashMap<>();
      map.put("routeRanges", range);
      map.put("targetCollection", targetCollection);
      map.put("expireAt", expireAt);
      RoutingRule rule = new RoutingRule(routeKey, map);
      routingRules.put(routeKey, rule);
    } else {
      // add this range
      Map<String, Object> map = r.shallowCopy();
      map.put("routeRanges", map.get("routeRanges") + "," + range);
      map.put("expireAt", expireAt);
      routingRules.put(routeKey, new RoutingRule(routeKey, map));
    }

    Map<String, Object> props = slice.shallowCopy();
    props.put("routingRules", routingRules);

    Slice newSlice = new Slice(slice.getName(), slice.getReplicasCopy(), props, collectionName, collection.getId(), (Replica.NodeNameToBaseUrl) cloudManager.getClusterStateProvider());
    return clusterState.copyWith( collectionName,
        CollectionMutator.updateSlice(collectionName, collection, newSlice));
  }

  public ClusterState removeRoutingRule(final ClusterState clusterState, ZkNodeProps message) {
    if (log.isDebugEnabled()) {
      log.debug("removeRoutingRule(ClusterState clusterState={}, ZkNodeProps message={}) - start", clusterState, message);
    }

    String collectionName = message.getStr(ZkStateReader.COLLECTION_PROP);

    String shard = message.getStr(ZkStateReader.SHARD_ID_PROP);
    String routeKeyStr = message.getStr("routeKey");

    log.info("Overseer.removeRoutingRule invoked for collection: " + collectionName
            + " shard: " + shard + " routeKey: " + routeKeyStr);

    DocCollection collection = clusterState.getCollection(collectionName);
    Slice slice = collection.getSlice(shard);
    if (slice == null) {
      log.warn("Unknown collection: " + collectionName + " shard: " + shard);
      return null;
    }
    Map<String, RoutingRule> routingRules = slice.getRoutingRules();
    if (routingRules != null) {
      routingRules.remove(routeKeyStr); // no rules left
      Map<String, Object> props = slice.shallowCopy();
      props.put("routingRules", routingRules);
      Slice newSlice = new Slice(slice.getName(), slice.getReplicasCopy(), props, collectionName, collection.getId(), (Replica.NodeNameToBaseUrl) cloudManager.getClusterStateProvider());

      if (log.isDebugEnabled()) {
        log.debug("removeRoutingRule(ClusterState, ZkNodeProps) - end");
      }
      return clusterState.copyWith(collectionName, CollectionMutator.updateSlice(collectionName, collection, newSlice));
    }

    if (log.isDebugEnabled()) {
      log.debug("removeRoutingRule(ClusterState, ZkNodeProps) - end");
    }
    return null;
  }

  public static DocCollection updateReplica(Replica.NodeNameToBaseUrl nodeNameToBaseUrl, DocCollection collection, final String shard, String coreNodeName, final Replica replica) {
    Slice slice;
    if (log.isDebugEnabled()) {
      log.debug("updateReplica(DocCollection collection={}, Slice slice={}, String coreNodeName={}, Replica replica={}) - start", collection, shard, coreNodeName, replica);
    }
    slice = collection.getSlice(shard);
    if (slice == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Slice not found " + shard);
    }
    Map<String,Replica> replicasCopy = slice.getReplicasCopy();

    if (replica == null) {
      replicasCopy.remove(coreNodeName);
    } else {
      replicasCopy.put(replica.getName(), replica);
    }
    Slice newSlice = new Slice(slice.getName(), replicasCopy, slice.getProperties(), collection.getName(), collection.getId(),  nodeNameToBaseUrl);
    if (log.isDebugEnabled()) {
      log.debug("Old Slice: {}", slice);
      log.debug("New Slice: {}", newSlice);
    }
    return CollectionMutator.updateSlice(collection.getName(), collection, newSlice);
  }
}

