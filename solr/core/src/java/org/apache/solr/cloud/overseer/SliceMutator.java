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

import static org.apache.solr.cloud.overseer.CollectionMutator.checkCollectionKeyExistence;
import static org.apache.solr.common.util.Utils.makeMap;

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
    log.info("createReplica() {} ", message);
    String coll = message.getStr(ZkStateReader.COLLECTION_PROP);
    // if (!checkCollectionKeyExistence(message)) return ZkStateWriter.NO_OP;
    String slice = message.getStr(ZkStateReader.SHARD_ID_PROP);

    DocCollection collection = clusterState.getCollection(coll);

    String coreName;
    if (message.getStr(ZkStateReader.CORE_NAME_PROP) != null) {
      coreName = message.getStr(ZkStateReader.CORE_NAME_PROP);
    } else {
      coreName = Assign.buildSolrCoreName(collection, coll, slice, Replica.Type.get(message.getStr(ZkStateReader.REPLICA_TYPE)));
    }
    Replica replica = new Replica(coreName,
        Utils.makeNonNullMap(
                   ZkStateReader.BASE_URL_PROP, message.getStr(ZkStateReader.BASE_URL_PROP),
                    ZkStateReader.STATE_PROP, Replica.State.DOWN,
                    ZkStateReader.NODE_NAME_PROP, message.getStr(ZkStateReader.NODE_NAME_PROP),
                    ZkStateReader.NUM_SHARDS_PROP, message.getStr(ZkStateReader.NUM_SHARDS_PROP),
                    "shards", message.getStr("shards"),
                    ZkStateReader.REPLICA_TYPE, message.get(ZkStateReader.REPLICA_TYPE)), coll, slice);

    if (log.isDebugEnabled()) {
      log.debug("addReplica(ClusterState, ZkNodeProps) - end");
    }
    return clusterState.copyWith(coll, updateReplica(collection, slice, replica.getName(), replica));
  }

  public ClusterState removeReplica(ClusterState clusterState, ZkNodeProps message) {

    log.info("removeReplica(ClusterState clusterState={}, ZkNodeProps message={}) - start", clusterState, message);

    final String coreName = message.getStr(ZkStateReader.REPLICA_PROP);
    final String collection = message.getStr(ZkStateReader.COLLECTION_PROP);
    final String baseUrl = message.getStr(ZkStateReader.BASE_URL_PROP);

    DocCollection coll = clusterState.getCollectionOrNull(collection);
    if (coll == null) {
      throw new IllegalStateException("No collection found to remove replica from collection=" + collection);
    }

    Map<String, Slice> newSlices = new LinkedHashMap<>(coll.getSlices().size() - 1);

    for (Slice slice : coll.getSlices()) {
      Replica replica = slice.getReplica(coreName);
      if (replica != null && (baseUrl == null || baseUrl.equals(replica.getBaseUrl()))) {
        Map<String, Replica> newReplicas = slice.getReplicasCopy();
        newReplicas.remove(coreName);
        slice = new Slice(slice.getName(), newReplicas, slice.getProperties(), collection);
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

    StringBuilder sb = new StringBuilder();
    String baseUrl = message.getStr(ZkStateReader.BASE_URL_PROP);
    String coreName = message.getStr(ZkStateReader.CORE_NAME_PROP);
    sb.append(baseUrl);
    if (baseUrl != null && !baseUrl.endsWith("/")) sb.append("/");
    sb.append(coreName == null ? "" : coreName);
    if (!(sb.substring(sb.length() - 1).equals("/"))) sb.append("/");
    String leaderUrl = sb.length() > 0 ? sb.toString() : null;


    String collectionName = message.getStr(ZkStateReader.COLLECTION_PROP);
    String sliceName = message.getStr(ZkStateReader.SHARD_ID_PROP);
    DocCollection coll = clusterState.getCollectionOrNull(collectionName);

    if (coll == null) {
      log.error("Could not mark shard leader for non existing collection: {}", collectionName);
      return clusterState;
    }

    Slice slice = coll.getSlice(sliceName);

    if (slice == null) {
      log.error("Could not mark shard leader for non existing slice: {}", sliceName);
      return clusterState;
    }

    Replica oldLeader = slice.getLeader();
    final Map<String, Replica> newReplicas = new LinkedHashMap<>();
    for (Replica replica : slice.getReplicas()) {
      // TODO: this should only be calculated once and cached somewhere?
      if (log.isDebugEnabled()) log.debug("examine for setting or unsetting as leader replica={}", replica);

      if (coreName.equals(replica.getName())) {
        log.info("Set leader {}", replica.getName());
        replica = new ReplicaMutator(cloudManager).setLeader(replica);
      } else if (replica.getBool("leader", false)) {
        if (log.isDebugEnabled()) log.debug("Unset leader");
        replica = new ReplicaMutator(cloudManager).unsetLeader(replica);
      }

      newReplicas.put(replica.getName(), replica);
    }

    Map<String, Object> newSliceProps = slice.shallowCopy();
    newSliceProps.put(Slice.REPLICAS, newReplicas);
    slice = new Slice(slice.getName(), newReplicas, slice.getProperties(), collectionName);

    if (log.isDebugEnabled()) {
      log.debug("setShardLeader(ClusterState, ZkNodeProps) - end");
    }
    return clusterState.copyWith(collectionName, CollectionMutator.updateSlice(collectionName, coll, slice));
  }

  public ClusterState updateShardState(ClusterState clusterState, ZkNodeProps message) {
    if (log.isDebugEnabled()) {
      log.debug("updateShardState(ClusterState clusterState={}, ZkNodeProps message={}) - start", clusterState, message);
    }

    String collectionName = message.getStr(ZkStateReader.COLLECTION_PROP);
    log.info("Update shard state invoked for collection: " + collectionName + " with message: " + message);

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
      log.info("Update shard state " + key + " to " + message.getStr(key));
      Map<String, Object> props = slice.shallowCopy();

      if (Slice.State.getState(message.getStr(key)) == Slice.State.ACTIVE) {
        props.remove(Slice.PARENT);
        props.remove("shard_parent_node");
        props.remove("shard_parent_zk_session");
      }
      props.put(ZkStateReader.STATE_PROP, message.getStr(key));
      // we need to use epoch time so that it's comparable across Overseer restarts
      props.put(ZkStateReader.STATE_TIMESTAMP_PROP, String.valueOf(cloudManager.getTimeSource().getEpochTimeNs()));
      Slice newSlice = new Slice(slice.getName(), slice.getReplicasCopy(), props, collectionName);

      slicesCopy.put(slice.getName(), newSlice);
    }

    ClusterState cs = clusterState.copyWith(collectionName, collection.copyWithSlices(slicesCopy));
    log.info("updateShardState return clusterstate={}", cs);
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

    Slice newSlice = new Slice(slice.getName(), slice.getReplicasCopy(), props, collectionName);
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
      Slice newSlice = new Slice(slice.getName(), slice.getReplicasCopy(), props, collectionName);

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

  public static DocCollection updateReplica(DocCollection collection, final String shard, String coreNodeName, final Replica replica) {
    Slice slice;
    if (log.isDebugEnabled()) {
      log.debug("updateReplica(DocCollection collection={}, Slice slice={}, String coreNodeName={}, Replica replica={}) - start", collection, shard, coreNodeName, replica);
    }
    slice = collection.getSlice(shard);
    if (slice == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Slice not found " + slice);
    }
    Map<String,Replica> replicasCopy = slice.getReplicasCopy();

    if (replica == null) {
      replicasCopy.remove(coreNodeName);
    } else {
      replicasCopy.put(replica.getName(), replica);
    }
    Slice newSlice = new Slice(slice.getName(), replicasCopy, slice.getProperties(), collection.getName());
    if (log.isDebugEnabled()) {
      log.debug("Old Slice: {}", slice);
      log.debug("New Slice: {}", newSlice);
    }
    return CollectionMutator.updateSlice(collection.getName(), collection, newSlice);
  }
}

