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

import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.cloud.DistribStateManager;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.api.collections.OverseerCollectionMessageHandler;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.overseer.CollectionMutator.checkCollectionKeyExistence;
import static org.apache.solr.cloud.overseer.CollectionMutator.checkKeyExistence;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class ReplicaMutator {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected final SolrCloudManager cloudManager;
  protected final DistribStateManager stateManager;

  public ReplicaMutator(SolrCloudManager cloudManager) {
    this.cloudManager = cloudManager;
    this.stateManager = cloudManager.getDistribStateManager();
  }

  protected Replica setProperty(Replica replica, String key, String value) {
    assert key != null;
    assert value != null;

    if (StringUtils.equalsIgnoreCase(replica.getStr(key), value))
      return replica; // already the value we're going to set

    Map<String, Object> replicaProps = new LinkedHashMap<>(replica.getProperties());
    replicaProps.put(key, value);
    return new Replica(replica.getName(), replicaProps, replica.getCollection(), replica.getCollectionId(), replica.getSlice(), replica.getBaseUrl());
  }

  protected Replica setProperty(Replica replica, String key, String value, String key2, String value2) {
    assert key != null;
    assert value != null;

    if (StringUtils.equalsIgnoreCase(replica.getStr(key), value))
      return replica; // already the value we're going to set

    Map<String, Object> replicaProps = new LinkedHashMap<>(replica.getProperties());
    replicaProps.put(key, value);
    replicaProps.put(key2, value2);
    return new Replica(replica.getName(), replicaProps, replica.getCollection(), replica.getCollectionId(), replica.getSlice(), (Replica.NodeNameToBaseUrl) cloudManager.getClusterStateProvider());
  }

  protected Replica unsetProperty(Replica replica, String key) {
    assert key != null;

    if (!replica.containsKey(key)) return replica;
    Map<String, Object> replicaProps = new LinkedHashMap<>(replica.getProperties());
    replicaProps.remove(key);
    return new Replica(replica.getName(), replicaProps, replica.getCollection(), replica.getCollectionId(), replica.getSlice(), (Replica.NodeNameToBaseUrl) cloudManager.getClusterStateProvider());
  }

  protected Replica setLeader(Replica replica) {
    return setProperty(replica, ZkStateReader.LEADER_PROP, "true", ZkStateReader.STATE_PROP, Replica.State.ACTIVE.toString());
  }

  protected Replica unsetLeader(Replica replica) {
    return unsetProperty(replica, ZkStateReader.LEADER_PROP);
  }

  protected Replica setState(Replica replica, String state) {
    assert state != null;

    return setProperty(replica, ZkStateReader.STATE_PROP, state);
  }

  public ClusterState addReplicaProperty(ClusterState clusterState, ZkNodeProps message) {
    if (!checkKeyExistence(message, ZkStateReader.COLLECTION_PROP) ||
        !checkKeyExistence(message, ZkStateReader.SHARD_ID_PROP) ||
        !checkKeyExistence(message, ZkStateReader.REPLICA_PROP) ||
        !checkKeyExistence(message, ZkStateReader.PROPERTY_PROP) ||
        !checkKeyExistence(message, ZkStateReader.PROPERTY_VALUE_PROP)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Overseer ADDREPLICAPROP requires " +
              ZkStateReader.COLLECTION_PROP + " and " + ZkStateReader.SHARD_ID_PROP + " and " +
              ZkStateReader.REPLICA_PROP + " and " + ZkStateReader.PROPERTY_PROP + " and " +
              ZkStateReader.PROPERTY_VALUE_PROP + " no action taken.");
    }

    String collectionName = message.getStr(ZkStateReader.COLLECTION_PROP);
    String sliceName = message.getStr(ZkStateReader.SHARD_ID_PROP);
    String replicaName = message.getStr(ZkStateReader.REPLICA_PROP);
    String property = message.getStr(ZkStateReader.PROPERTY_PROP).toLowerCase(Locale.ROOT);
    if (StringUtils.startsWith(property, OverseerCollectionMessageHandler.COLL_PROP_PREFIX) == false) {
      property = OverseerCollectionMessageHandler.COLL_PROP_PREFIX + property;
    }
    property = property.toLowerCase(Locale.ROOT);
    String propVal = message.getStr(ZkStateReader.PROPERTY_VALUE_PROP);
    String shardUnique = message.getStr(OverseerCollectionMessageHandler.SHARD_UNIQUE);

    boolean isUnique = false;

    if (SliceMutator.SLICE_UNIQUE_BOOLEAN_PROPERTIES.contains(property)) {
      if (StringUtils.isNotBlank(shardUnique) && Boolean.parseBoolean(shardUnique) == false) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Overseer ADDREPLICAPROP for " +
            property + " cannot have " + OverseerCollectionMessageHandler.SHARD_UNIQUE + " set to anything other than" +
            "'true'. No action taken");
      }
      isUnique = true;
    } else {
      isUnique = Boolean.parseBoolean(shardUnique);
    }

    DocCollection collection = clusterState.getCollection(collectionName);
    Replica replica = collection.getReplica(replicaName);

    if (replica == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Could not find collection/slice/replica " +
          collectionName + "/" + sliceName + "/" + replicaName + " no action taken.");
    }
    log.info("Setting property {} with value {} for collection {}", property, propVal, collectionName);
    log.debug("Full message: {}", message);
    if (StringUtils.equalsIgnoreCase(replica.getStr(property), propVal))
      return null; // already the value we're going to set

    // OK, there's no way we won't change the cluster state now
    Map<String, Replica> replicas = collection.getSlice(sliceName).getReplicasCopy();
    if (isUnique == false) {
      replicas.get(replicaName).getProperties().put(property, propVal);
    } else { // Set prop for this replica, but remove it for all others.
      for (Replica rep : replicas.values()) {
        if (rep.getName().equalsIgnoreCase(replicaName)) {
          rep.getProperties().put(property, propVal);
        } else {
          rep.getProperties().remove(property);
        }
      }
    }
    Slice newSlice = new Slice(sliceName, replicas, collection.getSlice(sliceName).shallowCopy(), collectionName,
        replica.getCollectionId(), (Replica.NodeNameToBaseUrl) cloudManager.getClusterStateProvider());
    DocCollection newCollection = CollectionMutator.updateSlice(collectionName, collection,
        newSlice);
    return clusterState.copyWith(collectionName, newCollection);
  }

  public ClusterState deleteReplicaProperty(ClusterState clusterState, ZkNodeProps message) {
    if (checkKeyExistence(message, ZkStateReader.COLLECTION_PROP) == false ||
        checkKeyExistence(message, ZkStateReader.SHARD_ID_PROP) == false ||
        checkKeyExistence(message, ZkStateReader.REPLICA_PROP) == false ||
        checkKeyExistence(message, ZkStateReader.PROPERTY_PROP) == false) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Overseer DELETEREPLICAPROP requires " +
              ZkStateReader.COLLECTION_PROP + " and " + ZkStateReader.SHARD_ID_PROP + " and " +
              ZkStateReader.REPLICA_PROP + " and " + ZkStateReader.PROPERTY_PROP + " no action taken.");
    }
    String collectionName = message.getStr(ZkStateReader.COLLECTION_PROP);
    String sliceName = message.getStr(ZkStateReader.SHARD_ID_PROP);
    String replicaName = message.getStr(ZkStateReader.REPLICA_PROP);
    String property = message.getStr(ZkStateReader.PROPERTY_PROP).toLowerCase(Locale.ROOT);
    if (StringUtils.startsWith(property, OverseerCollectionMessageHandler.COLL_PROP_PREFIX) == false) {
      property = OverseerCollectionMessageHandler.COLL_PROP_PREFIX + property;
    }

    DocCollection collection = clusterState.getCollection(collectionName);
    Replica replica = collection.getReplica(replicaName);

    if (replica == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Could not find collection/slice/replica " +
          collectionName + "/" + sliceName + "/" + replicaName + " no action taken.");
    }

    log.info("Deleting property {} for collection: {} slice: {} replica: {}", property, collectionName, sliceName, replicaName);
    log.debug("Full message: {}", message);
    String curProp = replica.getStr(property);
    if (curProp == null) return null; // not there anyway, nothing to do.

    Slice slice = collection.getSlice(sliceName);
    DocCollection newCollection = SliceMutator.updateReplica((Replica.NodeNameToBaseUrl) cloudManager.getClusterStateProvider(), collection,
        slice.getName(), replicaName, unsetProperty(replica, property));
    return clusterState.copyWith(collectionName, newCollection);
  }

  /**
   * Handles state updates
   */
  public ClusterState setState(ClusterState clusterState, ZkNodeProps message) {
    String collectionName = message.getStr(ZkStateReader.COLLECTION_PROP);
    String sliceName = message.getStr(ZkStateReader.SHARD_ID_PROP);

    if (collectionName == null || sliceName == null) {
      log.error("Invalid collection and slice {}", message);
      return null;
    }
    DocCollection collection = clusterState.getCollectionOrNull(collectionName);
    Slice slice = collection != null ? collection.getSlice(sliceName) : null;
    if (slice == null) {
      log.error("No such slice exists {}", message);
      return null;
    }

    return updateState(clusterState, message);
  }

  protected ClusterState updateState(ClusterState clusterState, ZkNodeProps message) {
    final String cName = message.getStr(ZkStateReader.COLLECTION_PROP);
    if (!checkCollectionKeyExistence(message)) return null;
    Integer numShards = message.getInt(ZkStateReader.NUM_SHARDS_PROP, null);
    log.debug("Update state numShards={} message={}", numShards, message);


    return updateState(clusterState,
        message, cName, numShards);
  }

  private ClusterState updateState(ClusterState clusterState, ZkNodeProps message, String collectionName, Integer numShards) {
    String sliceName = message.getStr(ZkStateReader.SHARD_ID_PROP);
    String coreName = message.getStr(ZkStateReader.CORE_NAME_PROP);

    DocCollection collection = clusterState.getCollectionOrNull(collectionName);
    if (collection.getReplica(coreName) == null) {
      log.info("Failed to update state because the replica does not exist, {}", message);
      return clusterState;
    }

   // MRM TODO:
//    if (coreNodeName == null) {
//      coreNodeName = ClusterStateMutator.getAssignedCoreNodeName(collection,
//          message.getStr(ZkStateReader.NODE_NAME_PROP), message.getStr(ZkStateReader.CORE_NAME_PROP));
//      if (coreNodeName != null) {
//        log.debug("node={} is already registered", coreNodeName);
//      } else {
//        if (!forceSetState) {
//          log.info("Failed to update state because the replica does not exist, {}", message);
//          return null;
//        }
//        // if coreNodeName is null, auto assign one
//        int replicas = 0;
//        DocCollection docCollection = prevState.getCollectionOrNull(collectionName);
//        if (docCollection != null) {
//          replicas = docCollection.getReplicas().size();
//        }
//
//        coreNodeName = Assign.assignCoreNodeName(stateManager, collectionName,replicas );
//      }
//      message.getProperties().put(ZkStateReader.CORE_NODE_NAME_PROP,
//          coreNodeName);
//    }

    // use the provided non null shardId
    if (sliceName == null) {
       throw new RuntimeException();
    }


    Slice slice = collection != null ?  collection.getSlice(sliceName) : null;
    Long id = -1l;

    Map<String, Object> replicaProps = new LinkedHashMap<>(message.getProperties());
    if (slice != null) {
      Replica oldReplica = slice.getReplica(coreName);

      if (oldReplica != null) {
        id = oldReplica.getCollectionId();
        if (oldReplica.containsKey(ZkStateReader.LEADER_PROP)) {
          replicaProps.put(ZkStateReader.LEADER_PROP, oldReplica.get(ZkStateReader.LEADER_PROP));
        }

        replicaProps.put(ZkStateReader.REPLICA_TYPE, oldReplica.getType().toString());
        // Move custom props over.
        for (Map.Entry<String, Object> ent : oldReplica.getProperties().entrySet()) {
          if (ent.getKey().startsWith(OverseerCollectionMessageHandler.COLL_PROP_PREFIX)) {
            replicaProps.put(ent.getKey(), ent.getValue());
          }
        }
      }
    }

    // we don't put these in the clusterstate
    replicaProps.remove(ZkStateReader.NUM_SHARDS_PROP);
    replicaProps.remove(ZkStateReader.CORE_NAME_PROP);
    replicaProps.remove(ZkStateReader.SHARD_ID_PROP);
    replicaProps.remove(ZkStateReader.COLLECTION_PROP);
    replicaProps.remove(Overseer.QUEUE_OPERATION);

    // remove any props with null values
    Set<Map.Entry<String, Object>> entrySet = replicaProps.entrySet();
    List<String> removeKeys = new ArrayList<>();
    for (Map.Entry<String, Object> entry : entrySet) {
      if (entry.getValue() == null) {
        removeKeys.add(entry.getKey());
      }
    }
    for (String removeKey : removeKeys) {
      replicaProps.remove(removeKey);
    }

    // remove shard specific properties
    String shardRange = (String) replicaProps.remove(ZkStateReader.SHARD_RANGE_PROP);
    String shardState = (String) replicaProps.remove(ZkStateReader.SHARD_STATE_PROP);
    String shardParent = (String) replicaProps.remove(ZkStateReader.SHARD_PARENT_PROP);


    Replica replica = new Replica(coreName, replicaProps, collectionName, id, sliceName, (Replica.NodeNameToBaseUrl) cloudManager.getClusterStateProvider());

    log.debug("Will update state for replica: {}", replica);

    Map<String, Object> sliceProps = null;
    Map<String, Replica> replicas;


      //prevState =checkAndCompleteShardSplit(prevState, collection, coreName, sliceName, replica);
      // get the current slice again because it may have been updated due to checkAndCompleteShardSplit method
      slice = collection.getSlice(sliceName);
      sliceProps = slice.getProperties();
      replicas = slice.getReplicasCopy();

    replicas.put(replica.getName(), replica);
    slice = new Slice(sliceName, replicas, sliceProps, collectionName, collection.getId(), (Replica.NodeNameToBaseUrl) cloudManager.getClusterStateProvider());

    DocCollection newCollection = CollectionMutator.updateSlice(collectionName, collection, slice);
    log.debug("Collection is now: {}", newCollection);
    return clusterState.copyWith(collectionName, newCollection);
  }
}

