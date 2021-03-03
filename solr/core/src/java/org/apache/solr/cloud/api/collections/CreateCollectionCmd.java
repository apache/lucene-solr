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

import org.apache.solr.client.solrj.cloud.AlreadyExistsException;
import org.apache.solr.client.solrj.cloud.DistribStateManager;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.VersionedData;
import org.apache.solr.client.solrj.impl.BaseCloudSolrClient;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.api.collections.OverseerCollectionMessageHandler.ShardRequestTracker;
import org.apache.solr.cloud.overseer.CollectionMutator;
import org.apache.solr.cloud.overseer.SliceMutator;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.CollectionStatePredicate;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.ImplicitDocRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ReplicaPosition;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkConfigManager;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CommonAdminParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.ConfigSetsHandlerApi;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.Overseer.QUEUE_OPERATION;
import static org.apache.solr.common.cloud.ZkStateReader.MAX_SHARDS_PER_NODE;
import static org.apache.solr.common.cloud.ZkStateReader.NRT_REPLICAS;
import static org.apache.solr.common.cloud.ZkStateReader.PULL_REPLICAS;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICATION_FACTOR;
import static org.apache.solr.common.cloud.ZkStateReader.TLOG_REPLICAS;
import static org.apache.solr.common.cloud.ZkStateReader.getCollectionSCNPath;
import static org.apache.solr.common.params.CollectionAdminParams.ALIAS;
import static org.apache.solr.common.params.CollectionAdminParams.COLL_CONF;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.ADDREPLICA;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.MODIFYCOLLECTION;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.common.util.StrUtils.formatString;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class CreateCollectionCmd implements OverseerCollectionMessageHandler.Cmd {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public final int CREATE_COLLECTION_TIMEOUT = Integer.getInteger("solr.createCollectionTimeout",60000);
  private final OverseerCollectionMessageHandler ocmh;
  private final TimeSource timeSource;
  private final ZkStateReader zkStateReader;
  private final SolrCloudManager cloudManager;

  public CreateCollectionCmd(OverseerCollectionMessageHandler ocmh, CoreContainer cc, SolrCloudManager cloudManager) {
    this.ocmh = ocmh;
    this.timeSource = ocmh.cloudManager.getTimeSource();
    this.zkStateReader = ocmh.zkStateReader;
    this.cloudManager = cloudManager;
  }

  @Override
  public boolean cleanup(ZkNodeProps message) {
    final String collectionName = message.getStr(NAME);
    boolean activeAndLive = false;
    DocCollection collection = zkStateReader.getClusterState().getCollectionOrNull(collectionName);
    if (collection != null) {
      Collection<Slice> slices = collection.getSlices();
      for (Slice slice : slices) {

        if (slice.getLeader() != null && slice.getLeader().isActive(zkStateReader.getLiveNodes())) {
          activeAndLive = true;
        }
      }
      if (!activeAndLive) {
        ZkNodeProps m = new ZkNodeProps();
        try {
          m.getProperties().put(QUEUE_OPERATION, "delete");
          m.getProperties().put(NAME, collectionName);
          ocmh.overseer.getCoreContainer().getZkController().getOverseerCollectionQueue().offer(Utils.toJSON(m), 15000);
          return false;
        } catch (KeeperException e) {
          log.error("", e);
        } catch (InterruptedException e) {
          log.error("", e);
        }
      }
    }
    return true;
  }

  @Override
  @SuppressWarnings({"unchecked"})
  public AddReplicaCmd.Response call(ClusterState clusterState, ZkNodeProps message, @SuppressWarnings({"rawtypes"})NamedList results) throws Exception {
    log.info("CreateCollectionCmd {}", message);
    if (ocmh.zkStateReader.aliasesManager != null) { // not a mock ZkStateReader
      ocmh.zkStateReader.aliasesManager.update(); // MRM TODO: - check into this
    }
    final String async = message.getStr(ASYNC);
    Map<String,ShardRequest> coresToCreate = new LinkedHashMap<>();
    List<ReplicaPosition> replicaPositions = null;
    final Aliases aliases = ocmh.zkStateReader.getAliases();
    final String collectionName = message.getStr(NAME);
    final boolean waitForFinalState = false;
    final String alias = message.getStr(ALIAS, collectionName);
    if (log.isDebugEnabled()) log.debug("Create collection {}", collectionName);
    CountDownLatch latch = new CountDownLatch(1);
    zkStateReader.getZkClient().getSolrZooKeeper().sync(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName, (rc, path, ctx) -> {
      latch.countDown();
    }, null);
    latch.await(5, TimeUnit.SECONDS);

    if (zkStateReader.getZkClient().exists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "collection already exists: " + collectionName);
    }

    if (aliases.hasAlias(collectionName)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "collection alias already exists: " + collectionName);
    }

    final ShardRequestTracker shardRequestTracker = ocmh.asyncRequestTracker(async, message.getStr(Overseer.QUEUE_OPERATION));
    ShardHandler shardHandler = ocmh.shardHandlerFactory.getShardHandler(ocmh.overseerLbClient);

    String withCollection = message.getStr(CollectionAdminParams.WITH_COLLECTION);
    String withCollectionShard = null;
    if (withCollection != null) {
      String realWithCollection = aliases.resolveSimpleAlias(withCollection);
      if (!clusterState.hasCollection(realWithCollection)) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "The 'withCollection' does not exist: " + realWithCollection);
      } else {
        DocCollection collection = clusterState.getCollection(realWithCollection);
        if (collection.getActiveSlices().size() > 1) {
          throw new SolrException(ErrorCode.BAD_REQUEST, "The `withCollection` must have only one shard, found: " + collection.getActiveSlices().size());
        }
        withCollectionShard = collection.getActiveSlices().iterator().next().getName();
      }
    }

    String configName = getConfigName(collectionName, message);
    if (log.isDebugEnabled()) log.debug("configName={} collection={}", configName, collectionName);
    if (configName == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No config set found to associate with the collection.");
    }

    ocmh.validateConfigOrThrowSolrException(configName);

    String router = message.getStr("router.name", DocRouter.DEFAULT_NAME);

    // fail fast if parameters are wrong or incomplete
    List<String> shardNames = BaseCloudSolrClient.populateShardNames(message, router);
    checkReplicaTypes(message);

    try {

      Map<String,String> collectionParams = new HashMap<>();
      Map<String,Object> collectionProps = message.getProperties();
      for (Map.Entry<String,Object> entry : collectionProps.entrySet()) {
        String propName = entry.getKey();
        if (propName.startsWith(ZkController.COLLECTION_PARAM_PREFIX)) {
          collectionParams.put(propName.substring(ZkController.COLLECTION_PARAM_PREFIX.length()), (String) entry.getValue());
        }
      }

      //      if (zkStateReader.getClusterState().getCollectionOrNull(collectionName) != null) {
      //        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Collection '"+collectionName+"' already exists!");
      //      }

      createCollectionZkNode(cloudManager.getDistribStateManager(), collectionName, collectionParams, configName);

      OverseerCollectionMessageHandler.createConfNode(cloudManager.getDistribStateManager(), configName, collectionName);

      long id = ocmh.overseer.getZkStateWriter().getHighestId();
      DocCollection docCollection = buildDocCollection(cloudManager, id, message, true);
      clusterState = clusterState.copyWith(collectionName, docCollection);
      try {
        replicaPositions = buildReplicaPositions(cloudManager, message, shardNames);
      } catch (Exception e) {
        log.error("Exception building replica positions", e);
        // unwrap the exception
        throw new SolrException(ErrorCode.BAD_REQUEST, e.getMessage(), e.getCause());
      }

      //      if (replicaPositions.isEmpty()) {
      //        if (log.isDebugEnabled()) log.debug("Finished create command for collection: {}", collectionName);
      //        throw new SolrException(ErrorCode.SERVER_ERROR, "No positions found to place replicas " + replicaPositions);
      //      }

      if (log.isDebugEnabled()) {
        log.debug(formatString("Creating SolrCores for new collection {0}, shardNames {1} replicaCount {2}, message : {3}", collectionName, shardNames, replicaPositions.size(), message));
      }

      for (ReplicaPosition replicaPosition : replicaPositions) {
        String nodeName = replicaPosition.node;

        if (withCollection != null) {
          // check that we have a replica of `withCollection` on this node and if not, create one
          DocCollection wcollection = clusterState.getCollection(withCollection);
          List<Replica> replicas = wcollection.getReplicas(nodeName);
          if (replicas == null || replicas.isEmpty()) {
            ZkNodeProps props = new ZkNodeProps(Overseer.QUEUE_OPERATION, ADDREPLICA.toString(), ZkStateReader.COLLECTION_PROP, withCollection, ZkStateReader.SHARD_ID_PROP, withCollectionShard,
                "node", nodeName, CommonAdminParams.WAIT_FOR_FINAL_STATE, Boolean.TRUE.toString()); // set to true because we want `withCollection` to be ready after this collection is created

            new AddReplicaCmd(ocmh, true).call(clusterState, props, results);
            clusterState = new SliceMutator(cloudManager).addReplica(clusterState, props);
          }
        }
        DocCollection coll = clusterState.getCollectionOrNull(collectionName);
        String coreName = Assign.buildSolrCoreName(coll, replicaPosition.shard, replicaPosition.type);
        if (log.isDebugEnabled()) log.debug(formatString("Creating core {0} as part of shard {1} of collection {2} on {3}", coreName, replicaPosition.shard, collectionName, nodeName));

        String baseUrl = zkStateReader.getBaseUrlForNodeName(nodeName);
        // create the replica in the collection's state.json in ZK prior to creating the core.
        // Otherwise the core creation fails

        if (log.isDebugEnabled()) log.debug("Base url for replica={}", baseUrl);

        ZkNodeProps props = new ZkNodeProps();
        //props.getProperties().putAll(message.getProperties());
        ZkNodeProps addReplicaProps = new ZkNodeProps(Overseer.QUEUE_OPERATION, ADDREPLICA.toString(), ZkStateReader.COLLECTION_PROP, collectionName, ZkStateReader.SHARD_ID_PROP,
            replicaPosition.shard, ZkStateReader.CORE_NAME_PROP, coreName, ZkStateReader.STATE_PROP, Replica.State.RECOVERING.toString(), ZkStateReader.NODE_NAME_PROP, nodeName, "node", nodeName,
            ZkStateReader.REPLICA_TYPE, replicaPosition.type.name(), ZkStateReader.NUM_SHARDS_PROP, message.getStr(ZkStateReader.NUM_SHARDS_PROP), "shards", message.getStr("shards"),
            CommonAdminParams.WAIT_FOR_FINAL_STATE, Boolean.toString(waitForFinalState));
        props.getProperties().putAll(addReplicaProps.getProperties());
        if (log.isDebugEnabled()) log.debug("Sending state update to populate clusterstate with new replica {}", props);

        clusterState = new AddReplicaCmd(ocmh, true).call(clusterState, props, results).clusterState;
        // log.info("CreateCollectionCmd after add replica clusterstate={}", clusterState);

        //clusterState = new SliceMutator(cloudManager).addReplica(clusterState, props);

        // Need to create new params for each request
        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set(CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.CREATE.toString());

        params.set(CoreAdminParams.NAME, coreName);
        params.set(CoreAdminParams.PROPERTY_PREFIX + "id",  Long.toString(docCollection.getHighestReplicaId()));
        params.set(CoreAdminParams.PROPERTY_PREFIX + "collId", Long.toString(id));
        params.set(COLL_CONF, configName);
        params.set(CoreAdminParams.COLLECTION, collectionName);
        params.set(CoreAdminParams.SHARD, replicaPosition.shard);
        params.set(ZkStateReader.NUM_SHARDS_PROP, shardNames.size());
        params.set(ZkStateReader.NODE_NAME_PROP, nodeName);
        params.set(CoreAdminParams.NEW_COLLECTION, "true");
        params.set(CoreAdminParams.REPLICA_TYPE, replicaPosition.type.name());

        if (async != null) {
          String coreAdminAsyncId = async + Math.abs(System.nanoTime());
          params.add(ASYNC, coreAdminAsyncId);
          shardRequestTracker.track(nodeName, coreAdminAsyncId);
        }
        ocmh.addPropertyParams(message, params);

        ShardRequest sreq = new ShardRequest();
        sreq.nodeName = nodeName;
        params.set("qt", ocmh.adminPath);
        sreq.purpose = 1;
        sreq.shards = new String[] {baseUrl};
        sreq.actualShards = sreq.shards;
        sreq.params = params;

        coresToCreate.put(coreName, sreq);
      }

      ocmh.overseer.getZkStateWriter().enqueueUpdate(clusterState, null, false);
      ocmh.overseer.getZkStateWriter().writePendingUpdates();

      if (log.isDebugEnabled()) log.debug("Sending create call for {} replicas for {}", coresToCreate.size(), collectionName);
      for (Map.Entry<String,ShardRequest> e : coresToCreate.entrySet()) {
        ShardRequest sreq = e.getValue();
        if (log.isDebugEnabled()) log.debug("Submit request to shard for for replica coreName={} total requests={} shards={}", e.getKey(), coresToCreate.size(),
            sreq.actualShards != null ? Arrays.asList(sreq.actualShards) : "null");
        shardHandler.submit(sreq, sreq.shards[0], sreq.params);
      }

      // modify the `withCollection` and store this new collection's name with it
      if (withCollection != null) {
        ZkNodeProps props = new ZkNodeProps(Overseer.QUEUE_OPERATION, MODIFYCOLLECTION.toString(), ZkStateReader.COLLECTION_PROP, withCollection, CollectionAdminParams.COLOCATED_WITH, collectionName);
        clusterState = new CollectionMutator(cloudManager).modifyCollection(clusterState, props);
      }

      // create an alias pointing to the new collection, if different from the collectionName
      if (!alias.equals(collectionName)) {
        ocmh.zkStateReader.aliasesManager.applyModificationAndExportToZk(a -> a.cloneWithCollectionAlias(alias, collectionName));
      }

    } catch (InterruptedException ex) {
      ParWork.propagateInterrupt(ex);
      throw ex;
    } catch (SolrException ex) {
      log.error("Exception creating collections", ex);
      throw ex;
    } catch (Exception ex) {
      log.error("Exception creating collection", ex);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, null, ex);
    }

    if (log.isDebugEnabled()) log.debug("CreateCollectionCmd clusterstate={}", clusterState);
    AddReplicaCmd.Response response = new AddReplicaCmd.Response();

    List<ReplicaPosition> finalReplicaPositions = replicaPositions;
    response.asyncFinalRunner = new OverseerCollectionMessageHandler.Finalize() {
      @Override
      public AddReplicaCmd.Response call() {
        try {
          shardRequestTracker.processResponses(results, shardHandler, false, null, Collections.emptySet());
        } catch (KeeperException e) {
          log.error("", e);
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
        } catch (InterruptedException e) {
          ParWork.propagateInterrupt(e);
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
        }
        //  MRM TODO: - put this in finalizer and finalizer after all calls to allow parallel and forward momentum ... MRM later on, huh?

        AddReplicaCmd.Response response = new AddReplicaCmd.Response();

        @SuppressWarnings({"rawtypes"}) boolean failure = results.get("failure") != null && ((SimpleOrderedMap) results.get("failure")).size() > 0;
        if (failure) {
          log.error("Failure creating collection {}", results.get("failure"));
          //        // Let's cleanup as we hit an exception
          //        // We shouldn't be passing 'results' here for the cleanup as the response would then contain 'success'
          //        // element, which may be interpreted by the user as a positive ack
          //        // MRM TODO: review
          try {
            AddReplicaCmd.Response rsp = ocmh.cleanupCollection(collectionName, new NamedList<Object>());

            response.clusterState = rsp.clusterState;
            if (rsp.asyncFinalRunner != null) {
              rsp.asyncFinalRunner.call();
            }
          } catch (Exception e) {
            log.error("Exception trying to clean up collection after fail {}", collectionName);
          }
          if (log.isDebugEnabled()) log.debug("Cleaned up artifacts for failed create collection for [{}]", collectionName);
          throw new SolrException(ErrorCode.BAD_REQUEST, "Underlying core creation failed while creating collection: " + collectionName + "\n" + results);
        } else {
          Object createNodeSet = message.get(ZkStateReader.CREATE_NODE_SET);
          if (log.isDebugEnabled()) log.debug("createNodeSet={}", createNodeSet);
          if (createNodeSet == null || !createNodeSet.equals(ZkStateReader.CREATE_NODE_SET_EMPTY)) {
            try {
              zkStateReader.waitForState(collectionName, CREATE_COLLECTION_TIMEOUT, TimeUnit.SECONDS, (l, c) -> {
                if (c == null) {
                  return false;
                }
                for (String name : coresToCreate.keySet()) {
                  if (log.isTraceEnabled()) log.trace("look for core {}", name);
                  if (c.getReplica(name) == null || c.getReplica(name).getState() != Replica.State.ACTIVE) {
                    if (log.isTraceEnabled()) log.trace("not the right replica or state {}", c.getReplica(name));
                    return false;
                  }
                }
                Collection<Slice> slices = c.getSlices();
                if (slices.size() < shardNames.size()) {
                  if (log.isTraceEnabled()) log.trace("wrong number slices {} vs {}", slices.size(), shardNames.size());
                  return false;
                }
                for (Slice slice : slices) {
                  if (log.isTraceEnabled()) log.trace("slice {} leader={}", slice, slice.getLeader());
                  if (slice.getLeader() == null || (slice.getLeader() != null && slice.getLeader().getState() != Replica.State.ACTIVE)) {
                    if (log.isTraceEnabled()) log.trace("no leader found for slice {}", slice.getName());
                    return false;
                  }
                }
                if (log.isTraceEnabled()) log.trace("return true, everything active");
                return true;
              });
            } catch (InterruptedException e) {
              log.warn("Interrupted waiting for active replicas on collection creation collection={}", collectionName);
              throw new SolrException(ErrorCode.SERVER_ERROR, e);
            } catch (TimeoutException e) {
              log.error("Timeout waiting for active replicas on collection creation collection={}", collectionName);
              throw new SolrException(ErrorCode.SERVER_ERROR, e);
            }
          }

          if (log.isDebugEnabled()) log.debug("Finished create command on all shards for collection: {}", collectionName);

          // Emit a warning about production use of data driven functionality
          boolean defaultConfigSetUsed = message.getStr(COLL_CONF) == null || message.getStr(COLL_CONF).equals(ConfigSetsHandlerApi.DEFAULT_CONFIGSET_NAME);
          if (defaultConfigSetUsed) {
            results.add("warning",
                "Using _default configset. Data driven schema functionality" + " is enabled by default, which is NOT RECOMMENDED for production use. To turn it off:" + " curl http://{host:port}/solr/"
                    + collectionName + "/config -d '{\"set-user-property\": {\"update.autoCreateFields\":\"false\"}}'");
          }

        }

        return response;
      }
    };

    if (log.isDebugEnabled()) log.debug("return cs from create collection cmd {}", clusterState);
    //response.clusterState = clusterState;
    return response;
  }

  public static List<ReplicaPosition> buildReplicaPositions(SolrCloudManager cloudManager,
                                                            ZkNodeProps message,
                                                            List<String> shardNames) throws IOException, InterruptedException, Assign.AssignmentException {
    if (log.isDebugEnabled()) {
      log.debug("buildReplicaPositions(SolrCloudManager cloudManager={}, ZkNodeProps message={}, List<String> shardNames={}) - start",
          cloudManager, message, shardNames);
    }

    final String collectionName = message.getStr(NAME);
    // look at the replication factor and see if it matches reality
    // if it does not, find best nodes to create more cores
    int numTlogReplicas = message.getInt(TLOG_REPLICAS, 0);
    int numNrtReplicas = message.getInt(NRT_REPLICAS, message.getInt(REPLICATION_FACTOR, numTlogReplicas>0?0:1));
    int numPullReplicas = message.getInt(PULL_REPLICAS, 0);

    int numSlices = shardNames.size();
    int maxShardsPerNode = message.getInt(MAX_SHARDS_PER_NODE, 1);
    if (maxShardsPerNode == -1) maxShardsPerNode = Integer.MAX_VALUE;
    int totalNumReplicas = numNrtReplicas + numTlogReplicas + numPullReplicas;
    // we need to look at every node and see how many cores it serves
    // add our new cores to existing nodes serving the least number of cores
    // but (for now) require that each core goes on a distinct node.

    List<ReplicaPosition> replicaPositions;
    List<String> nodeList = null;
    try {
      nodeList = Assign.getLiveOrLiveAndCreateNodeSetList(cloudManager.getDistribStateManager().listData(ZkStateReader.LIVE_NODES_ZKNODE), message, OverseerCollectionMessageHandler.RANDOM);
    } catch (KeeperException e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    }
    if (nodeList.isEmpty()) {
      log.warn("It is unusual to create a collection ("+collectionName+") without cores. message={}",  message);

      replicaPositions = new ArrayList<>();
    } else {

      if (totalNumReplicas > nodeList.size()) {
        log.warn("Specified number of replicas of "
                + totalNumReplicas
                + " on collection "
                + collectionName
                + " is higher than the number of Solr instances currently live or live and part of your " + ZkStateReader.CREATE_NODE_SET + "("
                + nodeList.size()
                + "). It's unusual to run two replica of the same slice on the same Solr-instance.");
      }

      Assign.AssignRequest assignRequest = new Assign.AssignRequestBuilder()
              .forCollection(collectionName)
              .forShard(shardNames)
              .assignNrtReplicas(numNrtReplicas)
              .assignTlogReplicas(numTlogReplicas)
              .assignPullReplicas(numPullReplicas)
              .onNodes(nodeList)
              .build();
      Assign.AssignStrategyFactory assignStrategyFactory = new Assign.AssignStrategyFactory(cloudManager);
      Assign.AssignStrategy assignStrategy = assignStrategyFactory.create();
      replicaPositions = assignStrategy.assign(cloudManager, assignRequest);
    }

    if (log.isDebugEnabled()) {
      log.debug("buildReplicaPositions(SolrCloudManager, ClusterState, DocCollection, ZkNodeProps, List<String>, AtomicReference<PolicyHelper.SessionWrapper>) - end");
    }
    if (nodeList.size() > 0 && replicaPositions.size() != (totalNumReplicas * numSlices)) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Did not get a position assigned for every replica " + replicaPositions.size() + "/" + (totalNumReplicas * numSlices));
    }
    return replicaPositions;
  }

  public static void checkReplicaTypes(ZkNodeProps message) {
    int numTlogReplicas = message.getInt(TLOG_REPLICAS, 0);
    int numNrtReplicas = message.getInt(NRT_REPLICAS, message.getInt(REPLICATION_FACTOR, numTlogReplicas > 0 ? 0 : 1));

    if (numNrtReplicas + numTlogReplicas <= 0) {
      throw new SolrException(ErrorCode.BAD_REQUEST, NRT_REPLICAS + " + " + TLOG_REPLICAS + " must be greater than 0");
    }
  }

  public static DocCollection buildDocCollection(SolrCloudManager cloudManager, Long id, ZkNodeProps message, boolean withDocRouter) {
    if (log.isDebugEnabled()) log.debug("buildDocCollection {}", message);
    String cName = message.getStr(NAME);
    DocRouter router = null;
    Map<String,Object> routerSpec = null;
    if (withDocRouter) {
      routerSpec = DocRouter.getRouterSpec(message);
      String routerName = routerSpec.get(NAME) == null ? DocRouter.DEFAULT_NAME : (String) routerSpec.get(NAME);
      router = DocRouter.getDocRouter(routerName);
    }
    Object messageShardsObj = message.get("shards");

    Map<String,Slice> slices;
    if (messageShardsObj instanceof Map) { // we are being explicitly told the slice data (e.g. coll restore)
      slices = Slice.loadAllFromMap((Replica.NodeNameToBaseUrl) cloudManager.getClusterStateProvider(), message.getStr(ZkStateReader.COLLECTION_PROP), id, (Map<String,Object>) messageShardsObj);
    } else {
      List<String> shardNames = new ArrayList<>();
      if (withDocRouter) {
        if (router instanceof ImplicitDocRouter) {
          getShardNames(shardNames, message.getStr("shards", DocRouter.DEFAULT_NAME));
        } else {
          int numShards = message.getInt(ZkStateReader.NUM_SHARDS_PROP, -1);
          if (numShards < 1)
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                    "numShards is a required parameter for 'compositeId' router {}" + message);
          getShardNames(numShards, shardNames);
        }
      }

      List<DocRouter.Range> ranges = null;
      if (withDocRouter) {
        ranges = router.partitionRange(shardNames.size(), router.fullRange());// maybe null
      }
      slices = new LinkedHashMap<>();
      for (int i = 0; i < shardNames.size(); i++) {
        String sliceName = shardNames.get(i);

        Map<String,Object> sliceProps = new LinkedHashMap<>(1);

        if (withDocRouter) {
          sliceProps.put(Slice.RANGE, ranges == null ? null : ranges.get(i));
        }

        slices.put(sliceName, new Slice(sliceName, null, sliceProps, message.getStr(ZkStateReader.COLLECTION_PROP), id, (Replica.NodeNameToBaseUrl) cloudManager.getClusterStateProvider()));

      }
    }

    Map<String,Object> collectionProps = new HashMap<>();

    collectionProps.put("id", id);

    for (Map.Entry<String,Object> e : OverseerCollectionMessageHandler.COLLECTION_PROPS_AND_DEFAULTS.entrySet()) {
      Object val = message.get(e.getKey());
      if (val == null) {
        val = OverseerCollectionMessageHandler.COLLECTION_PROPS_AND_DEFAULTS.get(e.getKey());
      }
      if (val != null) collectionProps.put(e.getKey(), val);
    }
    if (withDocRouter) {
      collectionProps.put(DocCollection.DOC_ROUTER, routerSpec);
    }
    if (withDocRouter) {

      if (message.getStr("fromApi") == null) {
        collectionProps.put("autoCreated", "true");
      }
    }
    DistribStateManager stateManager = cloudManager.getDistribStateManager();
    // TODO need to make this makePath calls efficient and not use zkSolrClient#makePath
    for (String shardName : slices.keySet()) {
      try {
        //stateManager.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/" + cName + "/" + shardName, null, CreateMode.PERSISTENT, false);
        stateManager.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/" + cName + "/leader_elect", null, CreateMode.PERSISTENT, false);
        stateManager.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/" + cName + "/leader_elect/" + shardName, null, CreateMode.PERSISTENT, false);
        stateManager.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/" + cName + "/leader_elect/" + shardName + "/election", null, CreateMode.PERSISTENT, false);
        stateManager.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/" + cName + "/leaders/" + shardName, null, CreateMode.PERSISTENT, false);
        stateManager.makePath(ZkStateReader.COLLECTIONS_ZKNODE  + "/" + cName + "/terms", null, CreateMode.PERSISTENT, false);
        stateManager.makePath(ZkStateReader.COLLECTIONS_ZKNODE  + "/" + cName + "/terms/" + shardName, ZkStateReader.emptyJson, CreateMode.PERSISTENT, false);
      } catch (AlreadyExistsException e) {
        // okay
      } catch (InterruptedException e) {
        ParWork.propagateInterrupt(e);
        throw new SolrException(ErrorCode.SERVER_ERROR, e);
      } catch (KeeperException e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, e);
      } catch (IOException e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, e);
      }
    }
    DocCollection newCollection = new DocCollection(cName,
            slices, collectionProps, router, 0, false);

    return newCollection;
  }

  public static void getShardNames(List<String> shardNames, String shards) {
    if (shards == null)
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "shards" + " is a required param");
    for (String s : shards.split(",")) {
      if (s == null || s.trim().isEmpty()) continue;
      shardNames.add(s.trim());
    }
    if (shardNames.isEmpty())
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "shards" + " is a required param");
  }

  String getConfigName(String coll, ZkNodeProps message) throws KeeperException, InterruptedException {
    String configName = message.getStr(COLL_CONF);

    if (configName == null) {
      // if there is only one conf, use that
      List<String> configNames = null;
      try {
        configNames = ocmh.zkStateReader.getZkClient().getChildren(ZkConfigManager.CONFIGS_ZKNODE, null, true);
        if (configNames.contains(ConfigSetsHandlerApi.DEFAULT_CONFIGSET_NAME)) {
          if (CollectionAdminParams.SYSTEM_COLL.equals(coll)) {
            return coll;
          } else {
            String intendedConfigSetName = ConfigSetsHandlerApi.getSuffixedNameForAutoGeneratedConfigSet(coll);
            copyDefaultConfigSetTo(configNames, intendedConfigSetName);
            return intendedConfigSetName;
          }
        } else if (configNames != null && configNames.size() == 1) {
          configName = configNames.get(0);
          // no config set named, but there is only 1 - use it
          if (log.isDebugEnabled()) log.debug("Only one config set found in zk - using it: {}", configName);
        }
      } catch (KeeperException.NoNodeException e) {

      }
    }
    return "".equals(configName)? null: configName;
  }

  /**
   * Copies the _default configset to the specified configset name (overwrites if pre-existing)
   */
  private void copyDefaultConfigSetTo(List<String> configNames, String targetConfig) {
    ZkConfigManager configManager = new ZkConfigManager(ocmh.zkStateReader.getZkClient());

    // if a configset named collection exists, re-use it
    if (configNames.contains(targetConfig)) {
      log.info("There exists a configset by the same name as the collection we're trying to create: {}, re-using it.", targetConfig);
      return;
    }
    // Copy _default into targetConfig
    try {
      configManager.copyConfigDir(ConfigSetsHandlerApi.DEFAULT_CONFIGSET_NAME, targetConfig, new HashSet<>());
    } catch (Exception e) {
      ParWork.propagateInterrupt(e);
      throw new SolrException(ErrorCode.INVALID_STATE, "Error while copying _default to " + targetConfig, e);
    }
  }

  public static void createCollectionZkNode(DistribStateManager stateManager, String collection, Map<String,String> params, String configName) {
    if (log.isDebugEnabled()) {
      log.debug("createCollectionZkNode(DistribStateManager stateManager={}, String collection={}, Map<String,String> params={}) - start", stateManager, collection, params);
    }

    String collectionPath = ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection;
//    // clean up old terms node
//    String termsPath = ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection + "/terms";
//    try {
//      stateManager.removeRecursively(termsPath, true, true);
//    } catch (Exception e) {
//      ParWork.propegateInterrupt(e);
//      throw new SolrException(ErrorCode.SERVER_ERROR, "createCollectionZkNode(DistribStateManager=" + stateManager + ", String=" + collection + ", Map<String,String>=" + params + ")", e);
   // }
    try {
      if (log.isDebugEnabled()) log.debug("Creating collection in ZooKeeper:" + collection);

      Map<String,Object> collectionProps = new HashMap<>();

      if (params.size() > 0) {
        collectionProps.putAll(params);
        // if the config name wasn't passed in, use the default
        if (!collectionProps.containsKey(ZkController.CONFIGNAME_PROP)) {
          // users can create the collection node and conf link ahead of time, or this may return another option
          getConfName(stateManager, collection, collectionPath, collectionProps);
        }

      } else if (System.getProperty("bootstrap_confdir") != null) {
        String defaultConfigName = System
                .getProperty(ZkController.COLLECTION_PARAM_PREFIX + ZkController.CONFIGNAME_PROP, collection);

        // if we are bootstrapping a collection, default the config for
        // a new collection to the collection we are bootstrapping
        if (log.isDebugEnabled()) log.debug("Setting config for collection:" + collection + " to " + defaultConfigName);

        Properties sysProps = System.getProperties();
        for (String sprop : System.getProperties().stringPropertyNames()) {
          if (sprop.startsWith(ZkController.COLLECTION_PARAM_PREFIX)) {
            collectionProps.put(sprop.substring(ZkController.COLLECTION_PARAM_PREFIX.length()),
                    sysProps.getProperty(sprop));
          }
        }

        // if the config name wasn't passed in, use the default
        if (!collectionProps.containsKey(ZkController.CONFIGNAME_PROP))
          collectionProps.put(ZkController.CONFIGNAME_PROP, defaultConfigName);

      } else if (Boolean.getBoolean("bootstrap_conf")) {
        // the conf name should should be the collection name of this core
        collectionProps.put(ZkController.CONFIGNAME_PROP, collection);
      } else {
        getConfName(stateManager, collection, collectionPath, collectionProps);
      }

      collectionProps.remove(ZkStateReader.NUM_SHARDS_PROP); // we don't put numShards in the collections properties

      // TODO - fix, no makePath (ensure every path part exists), async, single node

      try {
        stateManager.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection);
      } catch (AlreadyExistsException e) {
        // sadly, can be created, say to point to a specific config set
      }

      collectionProps.put(ZkController.CONFIGNAME_PROP, configName);
      ZkNodeProps zkProps = new ZkNodeProps(collectionProps);
      stateManager.setData(collectionPath, Utils.toJSON(zkProps), -1);

      stateManager.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection
              + "/leader_elect", null, CreateMode.PERSISTENT, false);
      stateManager.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection
              + "/terms", null, CreateMode.PERSISTENT, false);
      stateManager.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection + "/"
              + ZkStateReader.SHARD_LEADERS_ZKNODE, null, CreateMode.PERSISTENT, false);

      stateManager.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection + ZkStateReader.STATE_JSON,
              ZkStateReader.emptyJson, CreateMode.PERSISTENT, false);
      stateManager.makePath(getCollectionSCNPath(collection),
          ZkStateReader.emptyJson, CreateMode.PERSISTENT, false);
      stateManager.makePath(ZkStateReader.getCollectionStateUpdatesPath(collection),
          ZkStateReader.emptyJson, CreateMode.PERSISTENT, false);
      stateManager.makePath(ZkStateReader.getCollectionPropsPath(collection),
              ZkStateReader.emptyJson, CreateMode.PERSISTENT, false);
      stateManager.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection + "/schema_lock", null, CreateMode.PERSISTENT, false);

    } catch (Exception e) {
      ParWork.propagateInterrupt(e);
      throw new SolrException(ErrorCode.SERVER_ERROR, "createCollectionZkNode(DistribStateManager=" + stateManager + ", String=" + collection + ", Map<String,String>=" + params + ")", e);
    }


    if (log.isDebugEnabled()) {
      log.debug("createCollectionZkNode(DistribStateManager, String, Map<String,String>) - end");
    }
  }

  public static void getShardNames(Integer numShards, List<String> shardNames) {
    if (numShards == null)
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "numShards" + " is a required param");
    for (int i = 0; i < numShards; i++) {
      final String sliceName = "s" + (i + 1);
      shardNames.add(sliceName);
    }
  }

  private static void getConfName(DistribStateManager stateManager, String collection, String collectionPath, Map<String,Object> collectionProps) throws IOException,
      KeeperException, InterruptedException {
    // check for configName
    log.debug("Looking for collection configName");
    if (collectionProps.containsKey("configName")) {
      if (log.isInfoEnabled()) {
        if (log.isDebugEnabled()) log.debug("configName was passed as a param {}", collectionProps.get("configName"));
      }
      return;
    }

    List<String> configNames = null;

    if (stateManager.hasData(collectionPath)) {
      VersionedData data = stateManager.getData(collectionPath);
      if (data != null && data.getData() != null) {
        ZkNodeProps cProps = ZkNodeProps.load(data.getData());
        if (cProps.containsKey(ZkController.CONFIGNAME_PROP)) {
          return;
        }
      }
    }

    try {
      configNames = stateManager.listData(ZkConfigManager.CONFIGS_ZKNODE);
    } catch (NoSuchElementException | NoNodeException e) {
      // just keep trying
    }

    // check if there's a config set with the same name as the collection
    if (configNames != null && configNames.contains(collection)) {
      if (log.isDebugEnabled()) log.info("Could not find explicit collection configName, but found config name matching collection name - using that set.");
      collectionProps.put(ZkController.CONFIGNAME_PROP, collection);
      return;
    }
    // if _default exists, use that
    if (configNames != null && configNames.contains(ConfigSetsHandlerApi.DEFAULT_CONFIGSET_NAME)) {
      log.info("Could not find explicit collection configName, but found _default config set - using that set.");
      collectionProps.put(ZkController.CONFIGNAME_PROP, ConfigSetsHandlerApi.DEFAULT_CONFIGSET_NAME);
      return;
    }
    // if there is only one conf, use that
    if (configNames != null && configNames.size() == 1) {
      // no config set named, but there is only 1 - use it
      if (log.isInfoEnabled()) {
        log.info("Only one config set found in zk - using it: {}", configNames.get(0));
      }
      collectionProps.put(ZkController.CONFIGNAME_PROP, configNames.get(0));
      return;
    }

    if (configNames == null) {
      log.error("Could not find configName for collection {}", collection);
      throw new ZooKeeperException(
              SolrException.ErrorCode.SERVER_ERROR,
              "Could not find configName for collection " + collection + " found:" + configNames);
    }
  }

  public static CollectionStatePredicate expectedReplicas(int expectedReplicas, Map<String,Replica> replicaMap) {
    if (log.isDebugEnabled()) log.debug("Wait for expectedReplicas={}", expectedReplicas);

    return (liveNodes, collectionState) -> {
    //  log.info("Updated state {}", collectionState);
      if (collectionState == null) {
        return false;
      }
      if (collectionState.getSlices() == null) {
        return false;
      }

      int replicas = 0;
      for (Slice slice : collectionState) {
        for (Replica replica : slice) {
            replicaMap.put(replica.getName(), replica);
            replicas++;
        }
      }
      if (replicas >= expectedReplicas) {
        if (log.isDebugEnabled()) log.debug("Found expected replicas={} {}", expectedReplicas, replicaMap);
        return true;
      }
      return false;
    };
  }

}
