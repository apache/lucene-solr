package org.apache.solr.cloud.overseer;

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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.solr.cloud.Assign;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.OverseerCollectionProcessor;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.OverseerCollectionProcessor.COLL_PROP_PREFIX;
import static org.apache.solr.cloud.overseer.CollectionMutator.checkCollectionKeyExistence;
import static org.apache.solr.cloud.overseer.CollectionMutator.checkKeyExistence;

public class ReplicaMutator {
  private static Logger log = LoggerFactory.getLogger(ReplicaMutator.class);

  protected final ZkStateReader zkStateReader;

  public ReplicaMutator(ZkStateReader reader) {
    this.zkStateReader = reader;
  }

  protected Replica setProperty(Replica replica, String key, String value) {
    assert key != null;
    assert value != null;

    if (StringUtils.equalsIgnoreCase(replica.getStr(key), value))
      return replica; // already the value we're going to set

    Map<String, Object> replicaProps = new LinkedHashMap<>(replica.getProperties());
    replicaProps.put(key, value);
    return new Replica(replica.getName(), replicaProps);
  }

  protected Replica unsetProperty(Replica replica, String key) {
    assert key != null;

    if (!replica.containsKey(key)) return replica;
    Map<String, Object> replicaProps = new LinkedHashMap<>(replica.getProperties());
    replicaProps.remove(key);
    return new Replica(replica.getName(), replicaProps);
  }

  protected Replica setLeader(Replica replica) {
    return setProperty(replica, ZkStateReader.LEADER_PROP, "true");
  }

  protected Replica unsetLeader(Replica replica) {
    return unsetProperty(replica, ZkStateReader.LEADER_PROP);
  }

  protected Replica setState(Replica replica, String state) {
    assert state != null;

    return setProperty(replica, ZkStateReader.STATE_PROP, state);
  }

  public ZkWriteCommand addReplicaProperty(ClusterState clusterState, ZkNodeProps message) {
    if (checkKeyExistence(message, ZkStateReader.COLLECTION_PROP) == false ||
        checkKeyExistence(message, ZkStateReader.SHARD_ID_PROP) == false ||
        checkKeyExistence(message, ZkStateReader.REPLICA_PROP) == false ||
        checkKeyExistence(message, ZkStateReader.PROPERTY_PROP) == false ||
        checkKeyExistence(message, ZkStateReader.PROPERTY_VALUE_PROP) == false) {
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
    if (StringUtils.startsWith(property, COLL_PROP_PREFIX) == false) {
      property = OverseerCollectionProcessor.COLL_PROP_PREFIX + property;
    }
    property = property.toLowerCase(Locale.ROOT);
    String propVal = message.getStr(ZkStateReader.PROPERTY_VALUE_PROP);
    String shardUnique = message.getStr(OverseerCollectionProcessor.SHARD_UNIQUE);

    boolean isUnique = false;

    if (SliceMutator.SLICE_UNIQUE_BOOLEAN_PROPERTIES.contains(property)) {
      if (StringUtils.isNotBlank(shardUnique) && Boolean.parseBoolean(shardUnique) == false) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Overseer ADDREPLICAPROP for " +
            property + " cannot have " + OverseerCollectionProcessor.SHARD_UNIQUE + " set to anything other than" +
            "'true'. No action taken");
      }
      isUnique = true;
    } else {
      isUnique = Boolean.parseBoolean(shardUnique);
    }

    Replica replica = clusterState.getReplica(collectionName, replicaName);

    if (replica == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Could not find collection/slice/replica " +
          collectionName + "/" + sliceName + "/" + replicaName + " no action taken.");
    }
    log.info("Setting property " + property + " with value: " + propVal +
        " for collection: " + collectionName + ". Full message: " + message);
    if (StringUtils.equalsIgnoreCase(replica.getStr(property), propVal)) return ZkStateWriter.NO_OP; // already the value we're going to set

    // OK, there's no way we won't change the cluster state now
    Map<String,Replica> replicas = clusterState.getSlice(collectionName, sliceName).getReplicasCopy();
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
    Slice newSlice = new Slice(sliceName, replicas, clusterState.getSlice(collectionName, sliceName).shallowCopy());
    DocCollection newCollection = CollectionMutator.updateSlice(collectionName, clusterState.getCollection(collectionName),
        newSlice);
    return new ZkWriteCommand(collectionName, newCollection);
  }

  public ZkWriteCommand deleteReplicaProperty(ClusterState clusterState, ZkNodeProps message) {
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
    if (StringUtils.startsWith(property, COLL_PROP_PREFIX) == false) {
      property = OverseerCollectionProcessor.COLL_PROP_PREFIX + property;
    }

    Replica replica = clusterState.getReplica(collectionName, replicaName);

    if (replica == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Could not find collection/slice/replica " +
          collectionName + "/" + sliceName + "/" + replicaName + " no action taken.");
    }

    log.info("Deleting property " + property + " for collection: " + collectionName +
        " slice " + sliceName + " replica " + replicaName + ". Full message: " + message);
    String curProp = replica.getStr(property);
    if (curProp == null) return ZkStateWriter.NO_OP; // not there anyway, nothing to do.

    log.info("Deleting property " + property + " for collection: " + collectionName +
        " slice " + sliceName + " replica " + replicaName + ". Full message: " + message);
    DocCollection collection = clusterState.getCollection(collectionName);
    Slice slice = collection.getSlice(sliceName);
    DocCollection newCollection = SliceMutator.updateReplica(collection,
        slice, replicaName, unsetProperty(replica, property));
    return new ZkWriteCommand(collectionName, newCollection);
  }

  public ZkWriteCommand setState(ClusterState clusterState, ZkNodeProps message) {
    if (Overseer.isLegacy(zkStateReader.getClusterProps())) {
      return updateState(clusterState, message);
    } else {
      return updateStateNew(clusterState, message);
    }
  }

  protected ZkWriteCommand updateState(final ClusterState prevState, ZkNodeProps message) {
    final String cName = message.getStr(ZkStateReader.COLLECTION_PROP);
    if (!checkCollectionKeyExistence(message)) return ZkStateWriter.NO_OP;
    Integer numShards = message.getInt(ZkStateReader.NUM_SHARDS_PROP, null);
    log.info("Update state numShards={} message={}", numShards, message);

    List<String> shardNames = new ArrayList<>();

    ZkWriteCommand writeCommand = null;
    ClusterState newState = null;

    //collection does not yet exist, create placeholders if num shards is specified
    boolean collectionExists = prevState.hasCollection(cName);
    if (!collectionExists && numShards != null) {
      ClusterStateMutator.getShardNames(numShards, shardNames);
      Map<String, Object> createMsg = ZkNodeProps.makeMap("name", cName);
      createMsg.putAll(message.getProperties());
      writeCommand = new ClusterStateMutator(zkStateReader).createCollection(prevState, new ZkNodeProps(createMsg));
      DocCollection collection = writeCommand.collection;
      newState = ClusterStateMutator.newState(prevState, cName, collection);
    }
    return updateState(newState != null ? newState : prevState,
        message, cName, numShards, collectionExists);
  }

  private ZkWriteCommand updateState(final ClusterState prevState, ZkNodeProps message, String collectionName, Integer numShards, boolean collectionExists) {
    String sliceName = message.getStr(ZkStateReader.SHARD_ID_PROP);

    String coreNodeName = message.getStr(ZkStateReader.CORE_NODE_NAME_PROP);
    if (coreNodeName == null) {
      coreNodeName = ClusterStateMutator.getAssignedCoreNodeName(prevState, message);
      if (coreNodeName != null) {
        log.info("node=" + coreNodeName + " is already registered");
      } else {
        // if coreNodeName is null, auto assign one
        coreNodeName = Assign.assignNode(collectionName, prevState);
      }
      message.getProperties().put(ZkStateReader.CORE_NODE_NAME_PROP,
          coreNodeName);
    }

    // use the provided non null shardId
    if (sliceName == null) {
      //get shardId from ClusterState
      sliceName = ClusterStateMutator.getAssignedId(prevState, coreNodeName, message);
      if (sliceName != null) {
        log.info("shard=" + sliceName + " is already registered");
      }
    }
    if (sliceName == null) {
      //request new shardId
      if (collectionExists) {
        // use existing numShards
        numShards = prevState.getCollection(collectionName).getSlices().size();
        log.info("Collection already exists with " + ZkStateReader.NUM_SHARDS_PROP + "=" + numShards);
      }
      sliceName = Assign.assignShard(collectionName, prevState, numShards);
      log.info("Assigning new node to shard shard=" + sliceName);
    }

    Slice slice = prevState.getSlice(collectionName, sliceName);

    Map<String, Object> replicaProps = new LinkedHashMap<>();

    replicaProps.putAll(message.getProperties());
    if (slice != null) {
      Replica oldReplica = slice.getReplicasMap().get(coreNodeName);
      if (oldReplica != null) {
        if (oldReplica.containsKey(ZkStateReader.LEADER_PROP)) {
          replicaProps.put(ZkStateReader.LEADER_PROP, oldReplica.get(ZkStateReader.LEADER_PROP));
        }
        // Move custom props over.
        for (Map.Entry<String, Object> ent : oldReplica.getProperties().entrySet()) {
          if (ent.getKey().startsWith(COLL_PROP_PREFIX)) {
            replicaProps.put(ent.getKey(), ent.getValue());
          }
        }
      }
    }

    // we don't put these in the clusterstate
    replicaProps.remove(ZkStateReader.NUM_SHARDS_PROP);
    replicaProps.remove(ZkStateReader.CORE_NODE_NAME_PROP);
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
    replicaProps.remove(ZkStateReader.CORE_NODE_NAME_PROP);
    // remove shard specific properties
    String shardRange = (String) replicaProps.remove(ZkStateReader.SHARD_RANGE_PROP);
    String shardState = (String) replicaProps.remove(ZkStateReader.SHARD_STATE_PROP);
    String shardParent = (String) replicaProps.remove(ZkStateReader.SHARD_PARENT_PROP);


    Replica replica = new Replica(coreNodeName, replicaProps);

    Map<String, Object> sliceProps = null;
    Map<String, Replica> replicas;

    DocCollection collection = prevState.getCollectionOrNull(collectionName);
    if (slice != null) {
      collection = prevState.getCollection(collectionName);
      collection = checkAndCompleteShardSplit(prevState, collection, coreNodeName, sliceName, replicaProps);
      // get the current slice again because it may have been updated due to checkAndCompleteShardSplit method
      slice = collection.getSlice(sliceName);
      sliceProps = slice.getProperties();
      replicas = slice.getReplicasCopy();
    } else {
      replicas = new HashMap<>(1);
      sliceProps = new HashMap<>();
      sliceProps.put(Slice.RANGE, shardRange);
      sliceProps.put(Slice.STATE, shardState);
      sliceProps.put(Slice.PARENT, shardParent);
    }

    replicas.put(replica.getName(), replica);
    slice = new Slice(sliceName, replicas, sliceProps);

    DocCollection newCollection = CollectionMutator.updateSlice(collectionName, collection, slice);
    return new ZkWriteCommand(collectionName, newCollection);
  }

  /**
   * Handles non-legacy state updates
   */
  protected ZkWriteCommand updateStateNew(ClusterState clusterState, final ZkNodeProps message) {
    String collection = message.getStr(ZkStateReader.COLLECTION_PROP);
    if (!checkCollectionKeyExistence(message)) return ZkStateWriter.NO_OP;
    String sliceName = message.getStr(ZkStateReader.SHARD_ID_PROP);

    if (collection == null || sliceName == null) {
      log.error("Invalid collection and slice {}", message);
      return ZkStateWriter.NO_OP;
    }
    Slice slice = clusterState.getSlice(collection, sliceName);
    if (slice == null) {
      log.error("No such slice exists {}", message);
      return ZkStateWriter.NO_OP;
    }

    return updateState(clusterState, message);
  }

  private DocCollection checkAndCompleteShardSplit(ClusterState prevState, DocCollection collection, String coreNodeName, String sliceName, Map<String, Object> replicaProps) {
    Slice slice = collection.getSlice(sliceName);
    Map<String, Object> sliceProps = slice.getProperties();
    String sliceState = slice.getState();
    if (Slice.RECOVERY.equals(sliceState)) {
      log.info("Shard: {} is in recovery state", sliceName);
      // is this replica active?
      if (ZkStateReader.ACTIVE.equals(replicaProps.get(ZkStateReader.STATE_PROP))) {
        log.info("Shard: {} is in recovery state and coreNodeName: {} is active", sliceName, coreNodeName);
        // are all other replicas also active?
        boolean allActive = true;
        for (Map.Entry<String, Replica> entry : slice.getReplicasMap().entrySet()) {
          if (coreNodeName.equals(entry.getKey())) continue;
          if (!Slice.ACTIVE.equals(entry.getValue().getStr(Slice.STATE))) {
            allActive = false;
            break;
          }
        }
        if (allActive) {
          log.info("Shard: {} - all replicas are active. Finding status of fellow sub-shards", sliceName);
          // find out about other sub shards
          Map<String, Slice> allSlicesCopy = new HashMap<>(collection.getSlicesMap());
          List<Slice> subShardSlices = new ArrayList<>();
          outer:
          for (Map.Entry<String, Slice> entry : allSlicesCopy.entrySet()) {
            if (sliceName.equals(entry.getKey()))
              continue;
            Slice otherSlice = entry.getValue();
            if (Slice.RECOVERY.equals(otherSlice.getState())) {
              if (slice.getParent() != null && slice.getParent().equals(otherSlice.getParent())) {
                log.info("Shard: {} - Fellow sub-shard: {} found", sliceName, otherSlice.getName());
                // this is a fellow sub shard so check if all replicas are active
                for (Map.Entry<String, Replica> sliceEntry : otherSlice.getReplicasMap().entrySet()) {
                  if (!ZkStateReader.ACTIVE.equals(sliceEntry.getValue().getStr(ZkStateReader.STATE_PROP))) {
                    allActive = false;
                    break outer;
                  }
                }
                log.info("Shard: {} - Fellow sub-shard: {} has all replicas active", sliceName, otherSlice.getName());
                subShardSlices.add(otherSlice);
              }
            }
          }
          if (allActive) {
            // hurray, all sub shard replicas are active
            log.info("Shard: {} - All replicas across all fellow sub-shards are now ACTIVE. Preparing to switch shard states.", sliceName);
            String parentSliceName = (String) sliceProps.remove(Slice.PARENT);

            Map<String, Object> propMap = new HashMap<>();
            propMap.put(Overseer.QUEUE_OPERATION, "updateshardstate");
            propMap.put(parentSliceName, Slice.INACTIVE);
            propMap.put(sliceName, Slice.ACTIVE);
            for (Slice subShardSlice : subShardSlices) {
              propMap.put(subShardSlice.getName(), Slice.ACTIVE);
            }
            propMap.put(ZkStateReader.COLLECTION_PROP, collection.getName());
            ZkNodeProps m = new ZkNodeProps(propMap);
            return new SliceMutator(zkStateReader).updateShardState(prevState, m).collection;
          }
        }
      }
    }
    return collection;
  }
}

