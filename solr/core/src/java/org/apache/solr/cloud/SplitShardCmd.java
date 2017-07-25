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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.cloud.OverseerCollectionMessageHandler.Cmd;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.common.cloud.ReplicaPosition;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.CompositeIdRouter;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.PlainIdRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.solr.util.TestInjection;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.OverseerCollectionMessageHandler.COLL_PROP_PREFIX;
import static org.apache.solr.cloud.OverseerCollectionMessageHandler.SKIP_CREATE_REPLICA_IN_CLUSTER_STATE;
import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.ADDREPLICA;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.CREATESHARD;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETESHARD;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;


public class SplitShardCmd implements Cmd {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final OverseerCollectionMessageHandler ocmh;

  public SplitShardCmd(OverseerCollectionMessageHandler ocmh) {
    this.ocmh = ocmh;
  }

  @Override
  public void call(ClusterState state, ZkNodeProps message, NamedList results) throws Exception {
    split(state, message, results);
  }

  public boolean split(ClusterState clusterState, ZkNodeProps message, NamedList results) throws Exception {
    String collectionName = message.getStr("collection");
    String slice = message.getStr(ZkStateReader.SHARD_ID_PROP);

    log.info("Split shard invoked");
    ZkStateReader zkStateReader = ocmh.zkStateReader;
    zkStateReader.forceUpdateCollection(collectionName);

    String splitKey = message.getStr("split.key");
    ShardHandler shardHandler = ocmh.shardHandlerFactory.getShardHandler();

    DocCollection collection = clusterState.getCollection(collectionName);
    DocRouter router = collection.getRouter() != null ? collection.getRouter() : DocRouter.DEFAULT;

    Slice parentSlice;

    if (slice == null) {
      if (router instanceof CompositeIdRouter) {
        Collection<Slice> searchSlices = router.getSearchSlicesSingle(splitKey, new ModifiableSolrParams(), collection);
        if (searchSlices.isEmpty()) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unable to find an active shard for split.key: " + splitKey);
        }
        if (searchSlices.size() > 1) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
              "Splitting a split.key: " + splitKey + " which spans multiple shards is not supported");
        }
        parentSlice = searchSlices.iterator().next();
        slice = parentSlice.getName();
        log.info("Split by route.key: {}, parent shard is: {} ", splitKey, slice);
      } else {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "Split by route key can only be used with CompositeIdRouter or subclass. Found router: "
                + router.getClass().getName());
      }
    } else {
      parentSlice = collection.getSlice(slice);
    }

    if (parentSlice == null) {
      // no chance of the collection being null because ClusterState#getCollection(String) would have thrown
      // an exception already
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No shard with the specified name exists: " + slice);
    }

    // find the leader for the shard
    Replica parentShardLeader = null;
    try {
      parentShardLeader = zkStateReader.getLeaderRetry(collectionName, slice, 10000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // let's record the ephemeralOwner of the parent leader node
    Stat leaderZnodeStat = zkStateReader.getZkClient().exists(ZkStateReader.LIVE_NODES_ZKNODE + "/" + parentShardLeader.getNodeName(), null, true);
    if (leaderZnodeStat == null)  {
      // we just got to know the leader but its live node is gone already!
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "The shard leader node: " + parentShardLeader.getNodeName() + " is not live anymore!");
    }

    DocRouter.Range range = parentSlice.getRange();
    if (range == null) {
      range = new PlainIdRouter().fullRange();
    }

    List<DocRouter.Range> subRanges = null;
    String rangesStr = message.getStr(CoreAdminParams.RANGES);
    if (rangesStr != null) {
      String[] ranges = rangesStr.split(",");
      if (ranges.length == 0 || ranges.length == 1) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "There must be at least two ranges specified to split a shard");
      } else {
        subRanges = new ArrayList<>(ranges.length);
        for (int i = 0; i < ranges.length; i++) {
          String r = ranges[i];
          try {
            subRanges.add(DocRouter.DEFAULT.fromString(r));
          } catch (Exception e) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Exception in parsing hexadecimal hash range: " + r, e);
          }
          if (!subRanges.get(i).isSubsetOf(range)) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                "Specified hash range: " + r + " is not a subset of parent shard's range: " + range.toString());
          }
        }
        List<DocRouter.Range> temp = new ArrayList<>(subRanges); // copy to preserve original order
        Collections.sort(temp);
        if (!range.equals(new DocRouter.Range(temp.get(0).min, temp.get(temp.size() - 1).max))) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
              "Specified hash ranges: " + rangesStr + " do not cover the entire range of parent shard: " + range);
        }
        for (int i = 1; i < temp.size(); i++) {
          if (temp.get(i - 1).max + 1 != temp.get(i).min) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Specified hash ranges: " + rangesStr
                + " either overlap with each other or " + "do not cover the entire range of parent shard: " + range);
          }
        }
      }
    } else if (splitKey != null) {
      if (router instanceof CompositeIdRouter) {
        CompositeIdRouter compositeIdRouter = (CompositeIdRouter) router;
        subRanges = compositeIdRouter.partitionRangeByKey(splitKey, range);
        if (subRanges.size() == 1) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "The split.key: " + splitKey
              + " has a hash range that is exactly equal to hash range of shard: " + slice);
        }
        for (DocRouter.Range subRange : subRanges) {
          if (subRange.min == subRange.max) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "The split.key: " + splitKey + " must be a compositeId");
          }
        }
        log.info("Partitioning parent shard " + slice + " range: " + parentSlice.getRange() + " yields: " + subRanges);
        rangesStr = "";
        for (int i = 0; i < subRanges.size(); i++) {
          DocRouter.Range subRange = subRanges.get(i);
          rangesStr += subRange.toString();
          if (i < subRanges.size() - 1) rangesStr += ',';
        }
      }
    } else {
      // todo: fixed to two partitions?
      subRanges = router.partitionRange(2, range);
    }

    try {
      List<String> subSlices = new ArrayList<>(subRanges.size());
      List<String> subShardNames = new ArrayList<>(subRanges.size());
      String nodeName = parentShardLeader.getNodeName();
      for (int i = 0; i < subRanges.size(); i++) {
        String subSlice = slice + "_" + i;
        subSlices.add(subSlice);
        String subShardName = Assign.buildCoreName(ocmh.zkStateReader.getZkClient(), collection, subSlice, Replica.Type.NRT);
        subShardNames.add(subShardName);
      }

      boolean oldShardsDeleted = false;
      for (String subSlice : subSlices) {
        Slice oSlice = collection.getSlice(subSlice);
        if (oSlice != null) {
          final Slice.State state = oSlice.getState();
          if (state == Slice.State.ACTIVE) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                "Sub-shard: " + subSlice + " exists in active state. Aborting split shard.");
          } else if (state == Slice.State.CONSTRUCTION || state == Slice.State.RECOVERY) {
            // delete the shards
            log.info("Sub-shard: {} already exists therefore requesting its deletion", subSlice);
            Map<String, Object> propMap = new HashMap<>();
            propMap.put(Overseer.QUEUE_OPERATION, "deleteshard");
            propMap.put(COLLECTION_PROP, collectionName);
            propMap.put(SHARD_ID_PROP, subSlice);
            ZkNodeProps m = new ZkNodeProps(propMap);
            try {
              ocmh.commandMap.get(DELETESHARD).call(clusterState, m, new NamedList());
            } catch (Exception e) {
              throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unable to delete already existing sub shard: " + subSlice,
                  e);
            }

            oldShardsDeleted = true;
          }
        }
      }

      if (oldShardsDeleted) {
        // refresh the locally cached cluster state
        // we know we have the latest because otherwise deleteshard would have failed
        clusterState = zkStateReader.getClusterState();
        collection = clusterState.getCollection(collectionName);
      }

      final String asyncId = message.getStr(ASYNC);
      Map<String, String> requestMap = new HashMap<>();

      for (int i = 0; i < subRanges.size(); i++) {
        String subSlice = subSlices.get(i);
        String subShardName = subShardNames.get(i);
        DocRouter.Range subRange = subRanges.get(i);

        log.info("Creating slice " + subSlice + " of collection " + collectionName + " on " + nodeName);

        Map<String, Object> propMap = new HashMap<>();
        propMap.put(Overseer.QUEUE_OPERATION, CREATESHARD.toLower());
        propMap.put(ZkStateReader.SHARD_ID_PROP, subSlice);
        propMap.put(ZkStateReader.COLLECTION_PROP, collectionName);
        propMap.put(ZkStateReader.SHARD_RANGE_PROP, subRange.toString());
        propMap.put(ZkStateReader.SHARD_STATE_PROP, Slice.State.CONSTRUCTION.toString());
        propMap.put(ZkStateReader.SHARD_PARENT_PROP, parentSlice.getName());
        propMap.put("shard_parent_node", parentShardLeader.getNodeName());
        propMap.put("shard_parent_zk_session", leaderZnodeStat.getEphemeralOwner());
        DistributedQueue inQueue = Overseer.getStateUpdateQueue(zkStateReader.getZkClient());
        inQueue.offer(Utils.toJSON(new ZkNodeProps(propMap)));

        // wait until we are able to see the new shard in cluster state
        ocmh.waitForNewShard(collectionName, subSlice);

        // refresh cluster state
        clusterState = zkStateReader.getClusterState();

        log.info("Adding replica " + subShardName + " as part of slice " + subSlice + " of collection " + collectionName
            + " on " + nodeName);
        propMap = new HashMap<>();
        propMap.put(Overseer.QUEUE_OPERATION, ADDREPLICA.toLower());
        propMap.put(COLLECTION_PROP, collectionName);
        propMap.put(SHARD_ID_PROP, subSlice);
        propMap.put("node", nodeName);
        propMap.put(CoreAdminParams.NAME, subShardName);
        // copy over property params:
        for (String key : message.keySet()) {
          if (key.startsWith(COLL_PROP_PREFIX)) {
            propMap.put(key, message.getStr(key));
          }
        }
        // add async param
        if (asyncId != null) {
          propMap.put(ASYNC, asyncId);
        }
        ocmh.addReplica(clusterState, new ZkNodeProps(propMap), results, null);
      }

      ocmh.processResponses(results, shardHandler, true, "SPLITSHARD failed to create subshard leaders", asyncId, requestMap);

      for (String subShardName : subShardNames) {
        // wait for parent leader to acknowledge the sub-shard core
        log.info("Asking parent leader to wait for: " + subShardName + " to be alive on: " + nodeName);
        String coreNodeName = ocmh.waitForCoreNodeName(collectionName, nodeName, subShardName);
        CoreAdminRequest.WaitForState cmd = new CoreAdminRequest.WaitForState();
        cmd.setCoreName(subShardName);
        cmd.setNodeName(nodeName);
        cmd.setCoreNodeName(coreNodeName);
        cmd.setState(Replica.State.ACTIVE);
        cmd.setCheckLive(true);
        cmd.setOnlyIfLeader(true);

        ModifiableSolrParams p = new ModifiableSolrParams(cmd.getParams());
        ocmh.sendShardRequest(nodeName, p, shardHandler, asyncId, requestMap);
      }

      ocmh.processResponses(results, shardHandler, true, "SPLITSHARD timed out waiting for subshard leaders to come up",
          asyncId, requestMap);

      log.info("Successfully created all sub-shards for collection " + collectionName + " parent shard: " + slice
          + " on: " + parentShardLeader);

      log.info("Splitting shard " + parentShardLeader.getName() + " as part of slice " + slice + " of collection "
          + collectionName + " on " + parentShardLeader);

      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.SPLIT.toString());
      params.set(CoreAdminParams.CORE, parentShardLeader.getStr("core"));
      for (int i = 0; i < subShardNames.size(); i++) {
        String subShardName = subShardNames.get(i);
        params.add(CoreAdminParams.TARGET_CORE, subShardName);
      }
      params.set(CoreAdminParams.RANGES, rangesStr);

      ocmh.sendShardRequest(parentShardLeader.getNodeName(), params, shardHandler, asyncId, requestMap);

      ocmh.processResponses(results, shardHandler, true, "SPLITSHARD failed to invoke SPLIT core admin command", asyncId,
          requestMap);

      log.info("Index on shard: " + nodeName + " split into two successfully");

      // apply buffered updates on sub-shards
      for (int i = 0; i < subShardNames.size(); i++) {
        String subShardName = subShardNames.get(i);

        log.info("Applying buffered updates on : " + subShardName);

        params = new ModifiableSolrParams();
        params.set(CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.REQUESTAPPLYUPDATES.toString());
        params.set(CoreAdminParams.NAME, subShardName);

        ocmh.sendShardRequest(nodeName, params, shardHandler, asyncId, requestMap);
      }

      ocmh.processResponses(results, shardHandler, true, "SPLITSHARD failed while asking sub shard leaders" +
          " to apply buffered updates", asyncId, requestMap);

      log.info("Successfully applied buffered updates on : " + subShardNames);

      // Replica creation for the new Slices

      // look at the replication factor and see if it matches reality
      // if it does not, find best nodes to create more cores

      // TODO: Have replication factor decided in some other way instead of numShards for the parent

      int repFactor = parentSlice.getReplicas().size();

      // we need to look at every node and see how many cores it serves
      // add our new cores to existing nodes serving the least number of cores
      // but (for now) require that each core goes on a distinct node.

      // TODO: add smarter options that look at the current number of cores per
      // node?
      // for now we just go random
      Set<String> nodes = clusterState.getLiveNodes();
      List<String> nodeList = new ArrayList<>(nodes.size());
      nodeList.addAll(nodes);

      // TODO: Have maxShardsPerNode param for this operation?

      // Remove the node that hosts the parent shard for replica creation.
      nodeList.remove(nodeName);

      // TODO: change this to handle sharding a slice into > 2 sub-shards.

      List<ReplicaPosition> replicaPositions = Assign.identifyNodes(() -> ocmh.overseer.getZkController().getCoreContainer(),
          ocmh.zkStateReader, clusterState,
          new ArrayList<>(clusterState.getLiveNodes()),
          collectionName,
          new ZkNodeProps(collection.getProperties()),
          subSlices, repFactor - 1, 0, 0);

      List<Map<String, Object>> replicas = new ArrayList<>((repFactor - 1) * 2);

      for (ReplicaPosition replicaPosition : replicaPositions) {
        String sliceName = replicaPosition.shard;
        String subShardNodeName = replicaPosition.node;
        String shardName = collectionName + "_" + sliceName + "_replica" + (replicaPosition.index);

        log.info("Creating replica shard " + shardName + " as part of slice " + sliceName + " of collection "
            + collectionName + " on " + subShardNodeName);

        ZkNodeProps props = new ZkNodeProps(Overseer.QUEUE_OPERATION, ADDREPLICA.toLower(),
            ZkStateReader.COLLECTION_PROP, collectionName,
            ZkStateReader.SHARD_ID_PROP, sliceName,
            ZkStateReader.CORE_NAME_PROP, shardName,
            ZkStateReader.STATE_PROP, Replica.State.DOWN.toString(),
            ZkStateReader.BASE_URL_PROP, zkStateReader.getBaseUrlForNodeName(subShardNodeName),
            ZkStateReader.NODE_NAME_PROP, subShardNodeName);
        Overseer.getStateUpdateQueue(zkStateReader.getZkClient()).offer(Utils.toJSON(props));

        HashMap<String, Object> propMap = new HashMap<>();
        propMap.put(Overseer.QUEUE_OPERATION, ADDREPLICA.toLower());
        propMap.put(COLLECTION_PROP, collectionName);
        propMap.put(SHARD_ID_PROP, sliceName);
        propMap.put("node", subShardNodeName);
        propMap.put(CoreAdminParams.NAME, shardName);
        // copy over property params:
        for (String key : message.keySet()) {
          if (key.startsWith(COLL_PROP_PREFIX)) {
            propMap.put(key, message.getStr(key));
          }
        }
        // add async param
        if (asyncId != null) {
          propMap.put(ASYNC, asyncId);
        }
        // special flag param to instruct addReplica not to create the replica in cluster state again
        propMap.put(SKIP_CREATE_REPLICA_IN_CLUSTER_STATE, "true");

        replicas.add(propMap);
      }

      assert TestInjection.injectSplitFailureBeforeReplicaCreation();

      long ephemeralOwner = leaderZnodeStat.getEphemeralOwner();
      // compare against the ephemeralOwner of the parent leader node
      leaderZnodeStat = zkStateReader.getZkClient().exists(ZkStateReader.LIVE_NODES_ZKNODE + "/" + parentShardLeader.getNodeName(), null, true);
      if (leaderZnodeStat == null || ephemeralOwner != leaderZnodeStat.getEphemeralOwner()) {
        // put sub-shards in recovery_failed state
        DistributedQueue inQueue = Overseer.getStateUpdateQueue(zkStateReader.getZkClient());
        Map<String, Object> propMap = new HashMap<>();
        propMap.put(Overseer.QUEUE_OPERATION, OverseerAction.UPDATESHARDSTATE.toLower());
        for (String subSlice : subSlices) {
          propMap.put(subSlice, Slice.State.RECOVERY_FAILED.toString());
        }
        propMap.put(ZkStateReader.COLLECTION_PROP, collectionName);
        ZkNodeProps m = new ZkNodeProps(propMap);
        inQueue.offer(Utils.toJSON(m));

        if (leaderZnodeStat == null)  {
          // the leader is not live anymore, fail the split!
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "The shard leader node: " + parentShardLeader.getNodeName() + " is not live anymore!");
        } else if (ephemeralOwner != leaderZnodeStat.getEphemeralOwner()) {
          // there's a new leader, fail the split!
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
              "The zk session id for the shard leader node: " + parentShardLeader.getNodeName() + " has changed from "
                  + ephemeralOwner + " to " + leaderZnodeStat.getEphemeralOwner() + ". This can cause data loss so we must abort the split");
        }
      }

      // we must set the slice state into recovery before actually creating the replica cores
      // this ensures that the logic inside Overseer to update sub-shard state to 'active'
      // always gets a chance to execute. See SOLR-7673

      if (repFactor == 1) {
        // switch sub shard states to 'active'
        log.info("Replication factor is 1 so switching shard states");
        DistributedQueue inQueue = Overseer.getStateUpdateQueue(zkStateReader.getZkClient());
        Map<String, Object> propMap = new HashMap<>();
        propMap.put(Overseer.QUEUE_OPERATION, OverseerAction.UPDATESHARDSTATE.toLower());
        propMap.put(slice, Slice.State.INACTIVE.toString());
        for (String subSlice : subSlices) {
          propMap.put(subSlice, Slice.State.ACTIVE.toString());
        }
        propMap.put(ZkStateReader.COLLECTION_PROP, collectionName);
        ZkNodeProps m = new ZkNodeProps(propMap);
        inQueue.offer(Utils.toJSON(m));
      } else {
        log.info("Requesting shard state be set to 'recovery'");
        DistributedQueue inQueue = Overseer.getStateUpdateQueue(zkStateReader.getZkClient());
        Map<String, Object> propMap = new HashMap<>();
        propMap.put(Overseer.QUEUE_OPERATION, OverseerAction.UPDATESHARDSTATE.toLower());
        for (String subSlice : subSlices) {
          propMap.put(subSlice, Slice.State.RECOVERY.toString());
        }
        propMap.put(ZkStateReader.COLLECTION_PROP, collectionName);
        ZkNodeProps m = new ZkNodeProps(propMap);
        inQueue.offer(Utils.toJSON(m));
      }

      // now actually create replica cores on sub shard nodes
      for (Map<String, Object> replica : replicas) {
        ocmh.addReplica(clusterState, new ZkNodeProps(replica), results, null);
      }

      ocmh.processResponses(results, shardHandler, true, "SPLITSHARD failed to create subshard replicas", asyncId, requestMap);

      log.info("Successfully created all replica shards for all sub-slices " + subSlices);

      ocmh.commit(results, slice, parentShardLeader);

      return true;
    } catch (SolrException e) {
      throw e;
    } catch (Exception e) {
      log.error("Error executing split operation for collection: " + collectionName + " parent shard: " + slice, e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, null, e);
    }
  }
}
