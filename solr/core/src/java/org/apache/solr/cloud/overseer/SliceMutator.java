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
import org.apache.solr.client.solrj.impl.SolrClientCloudManager;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.api.collections.Assign;
import org.apache.solr.cloud.api.collections.OverseerCollectionMessageHandler;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.PerReplicaStatesOps;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.PerReplicaStates;
import org.apache.solr.common.cloud.RoutingRule;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
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
  protected final SolrZkClient zkClient;

  public SliceMutator(SolrCloudManager cloudManager) {
    this.cloudManager = cloudManager;
    this.stateManager = cloudManager.getDistribStateManager();
    this.zkClient = getZkClient(cloudManager);
  }

  static SolrZkClient getZkClient(SolrCloudManager cloudManager) {
    if (cloudManager instanceof SolrClientCloudManager) {
      SolrClientCloudManager manager = (SolrClientCloudManager) cloudManager;
      return manager.getZkClient();
    } else {
      return null;
    }
  }

  public ZkWriteCommand addReplica(ClusterState clusterState, ZkNodeProps message) {
    log.info("createReplica() {} ", message);
    String coll = message.getStr(ZkStateReader.COLLECTION_PROP);
    if (!checkCollectionKeyExistence(message)) return ZkStateWriter.NO_OP;
    String slice = message.getStr(ZkStateReader.SHARD_ID_PROP);
    DocCollection collection = clusterState.getCollection(coll);
    Slice sl = collection.getSlice(slice);
    if (sl == null) {
      log.error("Invalid Collection/Slice {}/{} ", coll, slice);
      return ZkStateWriter.NO_OP;
    }
    String coreNodeName;
    if (message.getStr(ZkStateReader.CORE_NODE_NAME_PROP) != null) {
      coreNodeName = message.getStr(ZkStateReader.CORE_NODE_NAME_PROP);
    } else {
      coreNodeName = Assign.assignCoreNodeName(stateManager, collection);
    }
    String nodeName = message.getStr(ZkStateReader.NODE_NAME_PROP);
    String baseUrl = Utils.getBaseUrlForNodeName(nodeName, cloudManager.getClusterStateProvider().getClusterProperty(ZkStateReader.URL_SCHEME, "http"));
    Replica replica = new Replica(coreNodeName,
        makeMap(
            ZkStateReader.CORE_NAME_PROP, message.getStr(ZkStateReader.CORE_NAME_PROP),
            ZkStateReader.STATE_PROP, message.getStr(ZkStateReader.STATE_PROP),
            ZkStateReader.NODE_NAME_PROP, nodeName,
            ZkStateReader.BASE_URL_PROP, baseUrl,
            ZkStateReader.REPLICA_TYPE, message.get(ZkStateReader.REPLICA_TYPE)), coll, slice);

    if (collection.isPerReplicaState()) {
      PerReplicaStates prs = PerReplicaStates.fetch(collection.getZNode(), zkClient, collection.getPerReplicaStates());
      return new ZkWriteCommand(coll, updateReplica(collection, sl, replica.getName(), replica),
          PerReplicaStatesOps.addReplica(replica.getName(), replica.getState(), replica.isLeader(), prs), true);
    } else {
      return new ZkWriteCommand(coll, updateReplica(collection, sl, replica.getName(), replica));
    }

  }

  public ZkWriteCommand removeReplica(ClusterState clusterState, ZkNodeProps message) {
    final String cnn = message.getStr(ZkStateReader.CORE_NODE_NAME_PROP);
    final String collection = message.getStr(ZkStateReader.COLLECTION_PROP);
    if (!checkCollectionKeyExistence(message)) return ZkStateWriter.NO_OP;

    DocCollection coll = clusterState.getCollectionOrNull(collection);
    if (coll == null) {
      // make sure we delete the zk nodes for this collection just to be safe
      return new ZkWriteCommand(collection, null);
    }

    Map<String, Slice> newSlices = new LinkedHashMap<>();
    final String nodeName = message.getStr(ZkStateReader.NODE_NAME_PROP);
    for (Slice slice : coll.getSlices()) {
      Replica replica = slice.getReplica(cnn);
      if (replica != null && (nodeName == null || nodeName.equals(replica.getNodeName()))) {
        Map<String, Replica> newReplicas = slice.getReplicasCopy();
        newReplicas.remove(cnn);
        slice = new Slice(slice.getName(), newReplicas, slice.getProperties(),collection);
      }
      newSlices.put(slice.getName(), slice);
    }


    if (coll.isPerReplicaState()) {
      PerReplicaStatesOps replicaOps = PerReplicaStatesOps.deleteReplica(cnn, PerReplicaStates.fetch(coll.getZNode(), zkClient, coll.getPerReplicaStates()) );
      return new ZkWriteCommand(collection, coll.copyWithSlices(newSlices), replicaOps, true);
    } else {
      return new ZkWriteCommand(collection, coll.copyWithSlices(newSlices));
    }
  }

  public ZkWriteCommand setShardLeader(ClusterState clusterState, ZkNodeProps message) {
    String leaderUrl = message.getStr(ZkStateReader.BASE_URL_PROP) != null ? ZkCoreNodeProps.getCoreUrl(message) : null;
    String collectionName = message.getStr(ZkStateReader.COLLECTION_PROP);
    String sliceName = message.getStr(ZkStateReader.SHARD_ID_PROP);
    DocCollection coll = clusterState.getCollectionOrNull(collectionName);

    if (coll == null) {
      log.error("Could not mark shard leader for non existing collection: {}", collectionName);
      return ZkStateWriter.NO_OP;
    }

    Map<String, Slice> slices = coll.getSlicesMap();
    Slice slice = slices.get(sliceName);

    Replica oldLeader = slice.getLeader();
    Replica newLeader = null;
    final Map<String, Replica> newReplicas = new LinkedHashMap<>();
    for (Replica replica : slice.getReplicas()) {
      // TODO: this should only be calculated once and cached somewhere?
      String coreURL = ZkCoreNodeProps.getCoreUrl(replica.getBaseUrl(), replica.getStr(ZkStateReader.CORE_NAME_PROP));

      if (replica == oldLeader && !coreURL.equals(leaderUrl)) {
        replica = new ReplicaMutator(cloudManager).unsetLeader(replica);
      } else if (coreURL.equals(leaderUrl)) {
        newLeader = replica = new ReplicaMutator(cloudManager).setLeader(replica);
      }

      newReplicas.put(replica.getName(), replica);
    }

    Map<String, Object> newSliceProps = slice.shallowCopy();
    newSliceProps.put(Slice.REPLICAS, newReplicas);
    slice = new Slice(slice.getName(), newReplicas, slice.getProperties(), collectionName);
    if (coll.isPerReplicaState()) {
      PerReplicaStates prs = PerReplicaStates.fetch(coll.getZNode(), zkClient, coll.getPerReplicaStates());
      return new ZkWriteCommand(collectionName, CollectionMutator.updateSlice(collectionName, coll, slice),
          PerReplicaStatesOps.flipLeader(
              slice.getReplicaNames(),
              newLeader == null ? null : newLeader.getName(),
              prs), false);
    } else {
      return new ZkWriteCommand(collectionName, CollectionMutator.updateSlice(collectionName, coll, slice));
    }
  }    

  public ZkWriteCommand updateShardState(ClusterState clusterState, ZkNodeProps message) {
    String collectionName = message.getStr(ZkStateReader.COLLECTION_PROP);
    if (!checkCollectionKeyExistence(message)) return ZkStateWriter.NO_OP;
    log.info("Update shard state invoked for collection: {} with message: {}", collectionName, message);

    DocCollection collection = clusterState.getCollection(collectionName);
    Map<String, Slice> slicesCopy = new LinkedHashMap<>(collection.getSlicesMap());
    for (String key : message.keySet()) {
      if (ZkStateReader.COLLECTION_PROP.equals(key)) continue;
      if (Overseer.QUEUE_OPERATION.equals(key)) continue;

      Slice slice = collection.getSlice(key);
      if (slice == null) {
        throw new RuntimeException("Overseer.updateShardState unknown collection: " + collectionName + " slice: " + key);
      }
      if (log.isInfoEnabled()) {
        log.info("Update shard state {} to {}", key, message.getStr(key));
      }
      Map<String, Object> props = slice.shallowCopy();
      
      if (Slice.State.getState(message.getStr(key)) == Slice.State.ACTIVE) {
        props.remove(Slice.PARENT);
        props.remove("shard_parent_node");
        props.remove("shard_parent_zk_session");
      }
      props.put(ZkStateReader.STATE_PROP, message.getStr(key));
      // we need to use epoch time so that it's comparable across Overseer restarts
      props.put(ZkStateReader.STATE_TIMESTAMP_PROP, String.valueOf(cloudManager.getTimeSource().getEpochTimeNs()));
      Slice newSlice = new Slice(slice.getName(), slice.getReplicasCopy(), props,collectionName);
      slicesCopy.put(slice.getName(), newSlice);
    }

    return new ZkWriteCommand(collectionName, collection.copyWithSlices(slicesCopy));
  }

  public ZkWriteCommand addRoutingRule(final ClusterState clusterState, ZkNodeProps message) {
    String collectionName = message.getStr(ZkStateReader.COLLECTION_PROP);
    if (!checkCollectionKeyExistence(message)) return ZkStateWriter.NO_OP;
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

    Slice newSlice = new Slice(slice.getName(), slice.getReplicasCopy(), props,collectionName);
    return new ZkWriteCommand(collectionName,
        CollectionMutator.updateSlice(collectionName, collection, newSlice));
  }

  public ZkWriteCommand removeRoutingRule(final ClusterState clusterState, ZkNodeProps message) {
    String collectionName = message.getStr(ZkStateReader.COLLECTION_PROP);
    if (!checkCollectionKeyExistence(message)) return ZkStateWriter.NO_OP;
    String shard = message.getStr(ZkStateReader.SHARD_ID_PROP);
    String routeKeyStr = message.getStr("routeKey");

    log.info("Overseer.removeRoutingRule invoked for collection: {} shard: {} routeKey: {}"
        , collectionName, shard, routeKeyStr);

    DocCollection collection = clusterState.getCollection(collectionName);
    Slice slice = collection.getSlice(shard);
    if (slice == null) {
      log.warn("Unknown collection: {} shard: {}", collectionName, shard);
      return ZkStateWriter.NO_OP;
    }
    Map<String, RoutingRule> routingRules = slice.getRoutingRules();
    if (routingRules != null) {
      routingRules.remove(routeKeyStr); // no rules left
      Map<String, Object> props = slice.shallowCopy();
      props.put("routingRules", routingRules);
      Slice newSlice = new Slice(slice.getName(), slice.getReplicasCopy(), props,collectionName);
      return new ZkWriteCommand(collectionName,
          CollectionMutator.updateSlice(collectionName, collection, newSlice));
    }

    return ZkStateWriter.NO_OP;
  }

  public static DocCollection updateReplica(DocCollection collection, final Slice slice, String coreNodeName, final Replica replica) {
    Map<String, Replica> replicasCopy = slice.getReplicasCopy();
    if (replica == null) {
      replicasCopy.remove(coreNodeName);
    } else {
      replicasCopy.put(replica.getName(), replica);
    }
    Slice newSlice = new Slice(slice.getName(), replicasCopy, slice.getProperties(), collection.getName());
    log.debug("Old Slice: {}", slice);
    log.debug("New Slice: {}", newSlice);
    return CollectionMutator.updateSlice(collection.getName(), collection, newSlice);
  }
}

