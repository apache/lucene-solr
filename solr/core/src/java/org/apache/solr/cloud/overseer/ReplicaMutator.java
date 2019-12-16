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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.cloud.DistribStateManager;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.VersionedData;
import org.apache.solr.cloud.CloudUtil;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.api.collections.Assign;
import org.apache.solr.cloud.api.collections.OverseerCollectionMessageHandler;
import org.apache.solr.cloud.api.collections.SplitShardCmd;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.TestInjection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.overseer.CollectionMutator.checkCollectionKeyExistence;
import static org.apache.solr.cloud.overseer.CollectionMutator.checkKeyExistence;
import static org.apache.solr.common.params.CommonParams.NAME;

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
    return new Replica(replica.getName(), replicaProps, replica.getCollection(), replica.getSlice());
  }

  protected Replica unsetProperty(Replica replica, String key) {
    assert key != null;

    if (!replica.containsKey(key)) return replica;
    Map<String, Object> replicaProps = new LinkedHashMap<>(replica.getProperties());
    replicaProps.remove(key);
    return new Replica(replica.getName(), replicaProps, replica.getCollection(), replica.getSlice());
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
      return ZkStateWriter.NO_OP; // already the value we're going to set

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
    Slice newSlice = new Slice(sliceName, replicas, collection.getSlice(sliceName).shallowCopy(),collectionName);
    DocCollection newCollection = CollectionMutator.updateSlice(collectionName, collection,
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
    if (curProp == null) return ZkStateWriter.NO_OP; // not there anyway, nothing to do.

    Slice slice = collection.getSlice(sliceName);
    DocCollection newCollection = SliceMutator.updateReplica(collection,
        slice, replicaName, unsetProperty(replica, property));
    return new ZkWriteCommand(collectionName, newCollection);
  }

  public ZkWriteCommand setState(ClusterState clusterState, ZkNodeProps message) {
    if (Overseer.isLegacy(cloudManager.getClusterStateProvider())) {
      return updateState(clusterState, message);
    } else {
      return updateStateNew(clusterState, message);
    }
  }

  protected ZkWriteCommand updateState(final ClusterState prevState, ZkNodeProps message) {
    final String cName = message.getStr(ZkStateReader.COLLECTION_PROP);
    if (!checkCollectionKeyExistence(message)) return ZkStateWriter.NO_OP;
    Integer numShards = message.getInt(ZkStateReader.NUM_SHARDS_PROP, null);
    log.debug("Update state numShards={} message={}", numShards, message);

    List<String> shardNames = new ArrayList<>();

    ZkWriteCommand writeCommand = null;
    ClusterState newState = null;

    //collection does not yet exist, create placeholders if num shards is specified
    boolean collectionExists = prevState.hasCollection(cName);
    if (!collectionExists && numShards != null) {
      ClusterStateMutator.getShardNames(numShards, shardNames);
      Map<String, Object> createMsg = Utils.makeMap(NAME, cName);
      createMsg.putAll(message.getProperties());
      writeCommand = new ClusterStateMutator(cloudManager).createCollection(prevState, new ZkNodeProps(createMsg));
      DocCollection collection = writeCommand.collection;
      newState = ClusterStateMutator.newState(prevState, cName, collection);
    }
    return updateState(newState != null ? newState : prevState,
        message, cName, numShards, collectionExists);
  }

  private ZkWriteCommand updateState(final ClusterState prevState, ZkNodeProps message, String collectionName, Integer numShards, boolean collectionExists) {
    String sliceName = message.getStr(ZkStateReader.SHARD_ID_PROP);
    String coreNodeName = message.getStr(ZkStateReader.CORE_NODE_NAME_PROP);
    boolean forceSetState = message.getBool(ZkStateReader.FORCE_SET_STATE_PROP, true);

    DocCollection collection = prevState.getCollectionOrNull(collectionName);
    if (!forceSetState && !CloudUtil.replicaExists(prevState, collectionName, sliceName, coreNodeName)) {
      log.info("Failed to update state because the replica does not exist, {}", message);
      return ZkStateWriter.NO_OP;
    }

    if (coreNodeName == null) {
      coreNodeName = ClusterStateMutator.getAssignedCoreNodeName(collection,
          message.getStr(ZkStateReader.NODE_NAME_PROP), message.getStr(ZkStateReader.CORE_NAME_PROP));
      if (coreNodeName != null) {
        log.debug("node=" + coreNodeName + " is already registered");
      } else {
        if (!forceSetState) {
          log.info("Failed to update state because the replica does not exist, {}", message);
          return ZkStateWriter.NO_OP;
        }
        // if coreNodeName is null, auto assign one
        coreNodeName = Assign.assignCoreNodeName(stateManager, collection);
      }
      message.getProperties().put(ZkStateReader.CORE_NODE_NAME_PROP,
          coreNodeName);
    }

    // use the provided non null shardId
    if (sliceName == null) {
      //get shardId from ClusterState
      sliceName = ClusterStateMutator.getAssignedId(collection, coreNodeName);
      if (sliceName != null) {
        log.debug("shard=" + sliceName + " is already registered");
      }
    }
    if (sliceName == null) {
      //request new shardId
      if (collectionExists) {
        // use existing numShards
        numShards = collection.getSlices().size();
        log.debug("Collection already exists with " + ZkStateReader.NUM_SHARDS_PROP + "=" + numShards);
      }
      sliceName = Assign.assignShard(collection, numShards);
      log.info("Assigning new node to shard shard=" + sliceName);
    }

    Slice slice = collection != null ?  collection.getSlice(sliceName) : null;

    Map<String, Object> replicaProps = new LinkedHashMap<>(message.getProperties());
    if (slice != null) {
      Replica oldReplica = slice.getReplica(coreNodeName);
      if (oldReplica != null) {
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


    Replica replica = new Replica(coreNodeName, replicaProps, collectionName, sliceName);

    log.debug("Will update state for replica: {}", replica);

    Map<String, Object> sliceProps = null;
    Map<String, Replica> replicas;

    if (slice != null) {
      collection = checkAndCompleteShardSplit(prevState, collection, coreNodeName, sliceName, replica);
      // get the current slice again because it may have been updated due to checkAndCompleteShardSplit method
      slice = collection.getSlice(sliceName);
      sliceProps = slice.getProperties();
      replicas = slice.getReplicasCopy();
    } else {
      replicas = new HashMap<>(1);
      sliceProps = new HashMap<>();
      sliceProps.put(Slice.RANGE, shardRange);
      sliceProps.put(ZkStateReader.STATE_PROP, shardState);
      sliceProps.put(Slice.PARENT, shardParent);
    }
    replicas.put(replica.getName(), replica);
    slice = new Slice(sliceName, replicas, sliceProps, collectionName);

    DocCollection newCollection = CollectionMutator.updateSlice(collectionName, collection, slice);
    log.debug("Collection is now: {}", newCollection);
    return new ZkWriteCommand(collectionName, newCollection);
  }

  /**
   * Handles non-legacy state updates
   */
  protected ZkWriteCommand updateStateNew(ClusterState clusterState, final ZkNodeProps message) {
    String collectionName = message.getStr(ZkStateReader.COLLECTION_PROP);
    if (!checkCollectionKeyExistence(message)) return ZkStateWriter.NO_OP;
    String sliceName = message.getStr(ZkStateReader.SHARD_ID_PROP);

    if (collectionName == null || sliceName == null) {
      log.error("Invalid collection and slice {}", message);
      return ZkStateWriter.NO_OP;
    }
    DocCollection collection = clusterState.getCollectionOrNull(collectionName);
    Slice slice = collection != null ? collection.getSlice(sliceName) : null;
    if (slice == null) {
      log.error("No such slice exists {}", message);
      return ZkStateWriter.NO_OP;
    }

    return updateState(clusterState, message);
  }

  private DocCollection checkAndCompleteShardSplit(ClusterState prevState, DocCollection collection, String coreNodeName, String sliceName, Replica replica) {
    Slice slice = collection.getSlice(sliceName);
    Map<String, Object> sliceProps = slice.getProperties();
    if (slice.getState() == Slice.State.RECOVERY) {
      log.info("Shard: {} is in recovery state", sliceName);
      // is this replica active?
      if (replica.getState() == Replica.State.ACTIVE) {
        log.info("Shard: {} is in recovery state and coreNodeName: {} is active", sliceName, coreNodeName);
        // are all other replicas also active?
        boolean allActive = true;
        for (Map.Entry<String, Replica> entry : slice.getReplicasMap().entrySet()) {
          if (coreNodeName.equals(entry.getKey())) continue;
          if (entry.getValue().getState() != Replica.State.ACTIVE) {
            allActive = false;
            break;
          }
        }
        if (allActive) {
          log.info("Shard: {} - all {} replicas are active. Finding status of fellow sub-shards", sliceName, slice.getReplicasMap().size());
          // find out about other sub shards
          Map<String, Slice> allSlicesCopy = new HashMap<>(collection.getSlicesMap());
          List<Slice> subShardSlices = new ArrayList<>();
          outer:
          for (Map.Entry<String, Slice> entry : allSlicesCopy.entrySet()) {
            if (sliceName.equals(entry.getKey()))
              continue;
            Slice otherSlice = entry.getValue();
            if (otherSlice.getState() == Slice.State.RECOVERY) {
              if (slice.getParent() != null && slice.getParent().equals(otherSlice.getParent())) {
                log.info("Shard: {} - Fellow sub-shard: {} found", sliceName, otherSlice.getName());
                // this is a fellow sub shard so check if all replicas are active
                for (Map.Entry<String, Replica> sliceEntry : otherSlice.getReplicasMap().entrySet()) {
                  if (sliceEntry.getValue().getState() != Replica.State.ACTIVE) {
                    allActive = false;
                    break outer;
                  }
                }
                log.info("Shard: {} - Fellow sub-shard: {} has all {} replicas active", sliceName, otherSlice.getName(), otherSlice.getReplicasMap().size());
                subShardSlices.add(otherSlice);
              }
            }
          }
          if (allActive) {
            // hurray, all sub shard replicas are active
            log.info("Shard: {} - All replicas across all fellow sub-shards are now ACTIVE.", sliceName);
            String parentSliceName = (String) sliceProps.remove(Slice.PARENT);
            // now lets see if the parent leader is still the same or else there's a chance of data loss
            // see SOLR-9438 for details
            String shardParentZkSession = (String) sliceProps.remove("shard_parent_zk_session");
            String shardParentNode = (String) sliceProps.remove("shard_parent_node");
            boolean isLeaderSame = true;
            if (shardParentNode != null && shardParentZkSession != null) {
              log.info("Checking whether sub-shard leader node is still the same one at {} with ZK session id {}", shardParentNode, shardParentZkSession);
              try {
                VersionedData leaderZnode = null;
                try {
                  leaderZnode = stateManager.getData(ZkStateReader.LIVE_NODES_ZKNODE
                      + "/" + shardParentNode, null);
                } catch (NoSuchElementException e) {
                  // ignore
                }
                if (leaderZnode == null) {
                  log.error("The shard leader node: {} is not live anymore!", shardParentNode);
                  isLeaderSame = false;
                } else if (!shardParentZkSession.equals(leaderZnode.getOwner())) {
                  log.error("The zk session id for shard leader node: {} has changed from {} to {}",
                      shardParentNode, shardParentZkSession, leaderZnode.getOwner());
                  isLeaderSame = false;
                }
              } catch (Exception e) {
                log.warn("Error occurred while checking if parent shard node is still live with the same zk session id. " +
                    "We cannot switch shard states at this time.", e);
                return collection; // we aren't going to make any changes right now
              }
            }

            Map<String, Object> propMap = new HashMap<>();
            propMap.put(Overseer.QUEUE_OPERATION, OverseerAction.UPDATESHARDSTATE.toLower());
            propMap.put(ZkStateReader.COLLECTION_PROP, collection.getName());
            if (isLeaderSame) {
              log.info("Sub-shard leader node is still the same one at {} with ZK session id {}. Preparing to switch shard states.", shardParentNode, shardParentZkSession);
              propMap.put(parentSliceName, Slice.State.INACTIVE.toString());
              propMap.put(sliceName, Slice.State.ACTIVE.toString());
              long now = cloudManager.getTimeSource().getEpochTimeNs();
              for (Slice subShardSlice : subShardSlices) {
                propMap.put(subShardSlice.getName(), Slice.State.ACTIVE.toString());
                String lastTimeStr = subShardSlice.getStr(ZkStateReader.STATE_TIMESTAMP_PROP);
                if (lastTimeStr != null) {
                  long start = Long.parseLong(lastTimeStr);
                  log.info("TIMINGS: Sub-shard " + subShardSlice.getName() + " recovered in " +
                      TimeUnit.MILLISECONDS.convert(now - start, TimeUnit.NANOSECONDS) + " ms");
                } else {
                  log.info("TIMINGS Sub-shard " + subShardSlice.getName() + " not available: " + subShardSlice);
                }
              }
            } else {
              // we must mark the shard split as failed by switching sub-shards to recovery_failed state
              propMap.put(sliceName, Slice.State.RECOVERY_FAILED.toString());
              for (Slice subShardSlice : subShardSlices) {
                propMap.put(subShardSlice.getName(), Slice.State.RECOVERY_FAILED.toString());
              }
            }
            TestInjection.injectSplitLatch();
            try {
              SplitShardCmd.unlockForSplit(cloudManager, collection.getName(), parentSliceName);
            } catch (Exception e) {
              log.warn("Failed to unlock shard after " + (isLeaderSame ? "" : "un") + "successful split: {} / {}",
                  collection.getName(), parentSliceName);
            }
            ZkNodeProps m = new ZkNodeProps(propMap);
            return new SliceMutator(cloudManager).updateShardState(prevState, m).collection;
          }
        }
      }
    }
    return collection;
  }
}

