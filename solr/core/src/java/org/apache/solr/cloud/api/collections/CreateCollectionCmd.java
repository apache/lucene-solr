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

import org.apache.solr.client.solrj.cloud.DistribStateManager;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.AlreadyExistsException;
import org.apache.solr.client.solrj.cloud.autoscaling.PolicyHelper;
import org.apache.solr.client.solrj.cloud.autoscaling.VersionedData;
import org.apache.solr.client.solrj.impl.BaseCloudSolrClient;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.api.collections.OverseerCollectionMessageHandler.ShardRequestTracker;
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

import static org.apache.solr.common.cloud.ZkStateReader.MAX_SHARDS_PER_NODE;
import static org.apache.solr.common.cloud.ZkStateReader.NRT_REPLICAS;
import static org.apache.solr.common.cloud.ZkStateReader.PULL_REPLICAS;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICATION_FACTOR;
import static org.apache.solr.common.cloud.ZkStateReader.TLOG_REPLICAS;
import static org.apache.solr.common.params.CollectionAdminParams.ALIAS;
import static org.apache.solr.common.params.CollectionAdminParams.COLL_CONF;
import static org.apache.solr.common.params.CollectionAdminParams.COLOCATED_WITH;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.ADDREPLICA;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.MODIFYCOLLECTION;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonAdminParams.WAIT_FOR_FINAL_STATE;
import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.common.util.StrUtils.formatString;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

public class CreateCollectionCmd implements OverseerCollectionMessageHandler.Cmd {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final OverseerCollectionMessageHandler ocmh;
  private final TimeSource timeSource;
  private final DistribStateManager stateManager;
  private final ZkStateReader zkStateReader;
  private final SolrCloudManager cloudManager;

  public CreateCollectionCmd(OverseerCollectionMessageHandler ocmh, CoreContainer cc, SolrCloudManager cloudManager, ZkStateReader zkStateReader) {
    this.ocmh = ocmh;
    this.stateManager = ocmh.cloudManager.getDistribStateManager();
    this.timeSource = ocmh.cloudManager.getTimeSource();
    this.zkStateReader = zkStateReader;
    this.cloudManager = cloudManager;
  }

  @Override
  @SuppressWarnings({"unchecked"})
  public void call(ClusterState clusterState, ZkNodeProps message, @SuppressWarnings({"rawtypes"})NamedList results) throws Exception {
    if (ocmh.zkStateReader.aliasesManager != null) { // not a mock ZkStateReader
      ocmh.zkStateReader.aliasesManager.update();
    }
    final Aliases aliases = ocmh.zkStateReader.getAliases();
    final String collectionName = message.getStr(NAME);
    final boolean waitForFinalState = message.getBool(WAIT_FOR_FINAL_STATE, false);
    final String alias = message.getStr(ALIAS, collectionName);
    log.info("Create collection {}", collectionName);
//    if (clusterState.hasCollection(collectionName)) {
//      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "collection already exists: " + collectionName);
//    }
    if (aliases.hasAlias(collectionName)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "collection alias already exists: " + collectionName);
    }

    String withCollection = message.getStr(CollectionAdminParams.WITH_COLLECTION);
    String withCollectionShard = null;
    if (withCollection != null) {
      String realWithCollection = aliases.resolveSimpleAlias(withCollection);
      if (!clusterState.hasCollection(realWithCollection)) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "The 'withCollection' does not exist: " + realWithCollection);
      } else  {
        DocCollection collection = clusterState.getCollection(realWithCollection);
        if (collection.getActiveSlices().size() > 1)  {
          throw new SolrException(ErrorCode.BAD_REQUEST, "The `withCollection` must have only one shard, found: " + collection.getActiveSlices().size());
        }
        withCollectionShard = collection.getActiveSlices().iterator().next().getName();
      }
    }

    String configName = getConfigName(collectionName, message);
    log.info("configName={} colleciton={}", configName, collectionName);
    if (configName == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No config set found to associate with the collection.");
    }

    ocmh.validateConfigOrThrowSolrException(configName);

    String router = message.getStr("router.name", DocRouter.DEFAULT_NAME);

    // fail fast if parameters are wrong or incomplete
    List<String> shardNames = BaseCloudSolrClient.populateShardNames(message, router);
    checkReplicaTypes(message);

    AtomicReference<PolicyHelper.SessionWrapper> sessionWrapper = new AtomicReference<>();

    try {

      final String async = message.getStr(ASYNC);

      boolean isLegacyCloud = Overseer.isLegacy(zkStateReader);

      Map<String,String> collectionParams = new HashMap<>();
      Map<String,Object> collectionProps = message.getProperties();
      for (Map.Entry<String, Object> entry : collectionProps.entrySet()) {
        String propName = entry.getKey();
        if (propName.startsWith(ZkController.COLLECTION_PARAM_PREFIX)) {
          collectionParams.put(propName.substring(ZkController.COLLECTION_PARAM_PREFIX.length()), (String) entry.getValue());
        }
      }
      createCollectionZkNode(stateManager, collectionName, collectionParams, configName);

      OverseerCollectionMessageHandler.createConfNode(stateManager, configName, collectionName, isLegacyCloud);

      // nocommit
      for (String shardName : shardNames) {
        stateManager.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName + "/" + shardName, null, CreateMode.PERSISTENT, false);
       // stateManager.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName + "/leader_elect", null, CreateMode.PERSISTENT, false);
        stateManager.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName + "/leader_elect/" + shardName, null, CreateMode.PERSISTENT, false);
        stateManager.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName + "/leader_elect/" + shardName + "/election", null, CreateMode.PERSISTENT, false);
        stateManager.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName + "/leaders/" + shardName, null, CreateMode.PERSISTENT, false);
        stateManager.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName + "/terms/" + shardName, ZkStateReader.emptyJson, CreateMode.PERSISTENT, false);

      }

      if (log.isDebugEnabled()) log.debug("Offer create operation to Overseer queue");
      ocmh.overseer.offerStateUpdate(Utils.toJSON(message));


      // nocommit
      // wait for a while until we see the collection

      ocmh.zkStateReader.waitForState(collectionName, 10, TimeUnit.SECONDS, (n, c) -> c != null);

      // refresh cluster state
      clusterState = ocmh.zkStateReader.getClusterState();

      List<ReplicaPosition> replicaPositions = null;
//      try {
//        replicaPositions = buildReplicaPositions(ocmh.cloudManager, clusterState,
//                clusterState.getCollection(collectionName), message, shardNames, sessionWrapper);
//      } catch (Exception e) {
//        ParWork.propegateInterrupt(e);
//        SolrException exp = new SolrException(ErrorCode.SERVER_ERROR, "call(ClusterState=" + clusterState + ", ZkNodeProps=" + message + ", NamedList=" + results + ")", e);
//        try {
//          ZkNodeProps deleteMessage = new ZkNodeProps("name", collectionName);
//          new DeleteCollectionCmd(ocmh).call(clusterState, deleteMessage, results);
//          // unwrap the exception
//        } catch (Exception e1) {
//          ParWork.propegateInterrupt(e1);
//          exp.addSuppressed(e1);
//        }
//        throw exp;
//      }

      DocCollection docCollection = clusterState.getCollection(collectionName);
      try {
        replicaPositions = buildReplicaPositions(cloudManager, clusterState,
                docCollection, message, shardNames, sessionWrapper);
      } catch (Exception e) {
        ZkNodeProps deleteMessage = new ZkNodeProps("name", collectionName);
        new DeleteCollectionCmd(ocmh).call(clusterState, deleteMessage, results);
        // unwrap the exception
        throw new SolrException(ErrorCode.BAD_REQUEST, e.getMessage(), e.getCause());
      }

//      if (replicaPositions.isEmpty()) {
//        if (log.isDebugEnabled()) log.debug("Finished create command for collection: {}", collectionName);
//        throw new SolrException(ErrorCode.SERVER_ERROR, "No positions found to place replicas " + replicaPositions);
//      }

      final ShardRequestTracker shardRequestTracker = ocmh.asyncRequestTracker(async);
      if (log.isDebugEnabled()) {
        log.debug(formatString("Creating SolrCores for new collection {0}, shardNames {1} , message : {2}",
            collectionName, shardNames, message));
      }
      Map<String,ShardRequest> coresToCreate = new LinkedHashMap<>();
      ShardHandler shardHandler = ocmh.shardHandlerFactory.getShardHandler(ocmh.overseer.getCoreContainer().getUpdateShardHandler().getTheSharedHttpClient());
      for (ReplicaPosition replicaPosition : replicaPositions) {
        String nodeName = replicaPosition.node;

        if (withCollection != null) {
          // check that we have a replica of `withCollection` on this node and if not, create one
          DocCollection collection = clusterState.getCollection(withCollection);
          List<Replica> replicas = collection.getReplicas(nodeName);
          if (replicas == null || replicas.isEmpty()) {
            ZkNodeProps props = new ZkNodeProps(
                Overseer.QUEUE_OPERATION, ADDREPLICA.toString(),
                ZkStateReader.COLLECTION_PROP, withCollection,
                ZkStateReader.SHARD_ID_PROP, withCollectionShard,
                "node", nodeName,
                CommonAdminParams.WAIT_FOR_FINAL_STATE, Boolean.TRUE.toString()); // set to true because we want `withCollection` to be ready after this collection is created
            new AddReplicaCmd(ocmh).call(clusterState, props, results);
            clusterState = zkStateReader.getClusterState(); // refresh
          }
        }

        String coreName = Assign.buildSolrCoreName(cloudManager.getDistribStateManager(),
                docCollection,
                replicaPosition.shard, replicaPosition.type, true);
        log.info(formatString("Creating core {0} as part of shard {1} of collection {2} on {3}"
                , coreName, replicaPosition.shard, collectionName, nodeName));


        String baseUrl = zkStateReader.getBaseUrlForNodeName(nodeName);
        //in the new mode, create the replica in clusterstate prior to creating the core.
        // Otherwise the core creation fails

        log.info("Base url for replica={}", baseUrl);

        if (!isLegacyCloud) {

          ZkNodeProps props = new ZkNodeProps();
          props.getProperties().putAll(message.getProperties());
          ZkNodeProps addReplicaProps = new ZkNodeProps(
                  Overseer.QUEUE_OPERATION, ADDREPLICA.toString(),
                  ZkStateReader.COLLECTION_PROP, collectionName,
                  ZkStateReader.SHARD_ID_PROP, replicaPosition.shard,
                  ZkStateReader.CORE_NAME_PROP, coreName,
                  ZkStateReader.STATE_PROP, Replica.State.DOWN.toString(),
                  ZkStateReader.BASE_URL_PROP, baseUrl,
                  ZkStateReader.NODE_NAME_PROP, nodeName,
                  ZkStateReader.REPLICA_TYPE, replicaPosition.type.name(),
                  ZkStateReader.NUM_SHARDS_PROP, message.getStr(ZkStateReader.NUM_SHARDS_PROP),
                      "shards", message.getStr("shards"),
                  CommonAdminParams.WAIT_FOR_FINAL_STATE, Boolean.toString(waitForFinalState));
          props.getProperties().putAll(addReplicaProps.getProperties());
          if (log.isDebugEnabled()) log.debug("Sending state update to populate clusterstate with new replica {}", props);
          ocmh.overseer.offerStateUpdate(Utils.toJSON(props));
        }

        // Need to create new params for each request
        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set(CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.CREATE.toString());

        params.set(CoreAdminParams.NAME, coreName);
        params.set(COLL_CONF, configName);
        params.set(CoreAdminParams.COLLECTION, collectionName);
        params.set(CoreAdminParams.SHARD, replicaPosition.shard);
        params.set(ZkStateReader.NUM_SHARDS_PROP, shardNames.size());
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
        sreq.shards = new String[]{baseUrl};
        sreq.actualShards = sreq.shards;
        sreq.params = params;

        if (isLegacyCloud) {
          log.info("Submit request to shard for legacyCloud for replica={}", baseUrl);
          shardHandler.submit(sreq, sreq.shards[0], sreq.params);
        } else {
          coresToCreate.put(coreName, sreq);
        }
      }

      if(!isLegacyCloud) {
        // wait for all replica entries to be created
        Map<String,Replica> replicas = new HashMap<>();
        zkStateReader.waitForState(collectionName, 5, TimeUnit.SECONDS, expectedReplicas(coresToCreate.size(), replicas)); // nocommit - timeout - keep this below containing timeouts - need central timeout stuff
       // nocommit, what if replicas comes back wrong?
        if (replicas.size() > 0) {
          for (Map.Entry<String, ShardRequest> e : coresToCreate.entrySet()) {
            ShardRequest sreq = e.getValue();
            for (Replica rep : replicas.values()) {
              if (rep.getCoreName().equals(sreq.params.get(CoreAdminParams.NAME)) && rep.getBaseUrl().equals(sreq.shards[0])) {
                sreq.params.set(CoreAdminParams.CORE_NODE_NAME, rep.getName());
              }
            }
            Replica replica = replicas.get(e.getKey());

            if (replica != null) {
              String coreNodeName = replica.getName();
              sreq.params.set(CoreAdminParams.CORE_NODE_NAME, coreNodeName);
              log.info("Set the {} for replica {} to {}", CoreAdminParams.CORE_NODE_NAME, replica, coreNodeName);
            }
           
            log.info("Submit request to shard for for replica={}", sreq.actualShards != null ? Arrays.asList(sreq.actualShards) : "null");
            shardHandler.submit(sreq, sreq.shards[0], sreq.params);
          }
        }
      }

      shardRequestTracker.processResponses(results, shardHandler, false, null, Collections.emptySet());
      @SuppressWarnings({"rawtypes"})
      boolean failure = results.get("failure") != null && ((SimpleOrderedMap)results.get("failure")).size() > 0;
      if (failure) {
        // Let's cleanup as we hit an exception
        // We shouldn't be passing 'results' here for the cleanup as the response would then contain 'success'
        // element, which may be interpreted by the user as a positive ack
        ocmh.cleanupCollection(collectionName, new NamedList<Object>());
        log.info("Cleaned up artifacts for failed create collection for [{}]", collectionName);
        throw new SolrException(ErrorCode.BAD_REQUEST, "Underlying core creation failed while creating collection: " + collectionName + "\n" + results);
      } else {
        log.debug("Finished create command on all shards for collection: {}", collectionName);

        // Emit a warning about production use of data driven functionality
        boolean defaultConfigSetUsed = message.getStr(COLL_CONF) == null ||
            message.getStr(COLL_CONF).equals(ConfigSetsHandlerApi.DEFAULT_CONFIGSET_NAME);
        if (defaultConfigSetUsed) {
          results.add("warning", "Using _default configset. Data driven schema functionality"
              + " is enabled by default, which is NOT RECOMMENDED for production use. To turn it off:"
              + " curl http://{host:port}/solr/" + collectionName + "/config -d '{\"set-user-property\": {\"update.autoCreateFields\":\"false\"}}'");
        }
        if (async != null) {
          zkStateReader.waitForState(collectionName, 30, TimeUnit.SECONDS, BaseCloudSolrClient.expectedShardsAndActiveReplicas(shardNames.size(), replicaPositions.size()));
        }
      }

      // modify the `withCollection` and store this new collection's name with it
      if (withCollection != null) {
        ZkNodeProps props = new ZkNodeProps(
            Overseer.QUEUE_OPERATION, MODIFYCOLLECTION.toString(),
            ZkStateReader.COLLECTION_PROP, withCollection,
            CollectionAdminParams.COLOCATED_WITH, collectionName);
        ocmh.overseer.offerStateUpdate(Utils.toJSON(props));
        try {
          zkStateReader.waitForState(withCollection, 30, TimeUnit.SECONDS, (collectionState) -> collectionName.equals(collectionState.getStr(COLOCATED_WITH)));
        } catch (TimeoutException e) {
          log.warn("Timed out waiting to see the {} property set on collection: {}", COLOCATED_WITH, withCollection);
          // maybe the overseer queue is backed up, we don't want to fail the create request
          // because of this time out, continue
        }
      }

      // create an alias pointing to the new collection, if different from the collectionName
      if (!alias.equals(collectionName)) {
        ocmh.zkStateReader.aliasesManager.applyModificationAndExportToZk(a -> a.cloneWithCollectionAlias(alias, collectionName));
      }

    } catch (InterruptedException ex) {
      ParWork.propegateInterrupt(ex);
      throw ex;
    } catch (SolrException ex) {
      log.error("Exception creating collections", ex);
      throw ex;
    } catch (Exception ex) {
      log.error("Exception creating collection", ex);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, null, ex);
    } finally {
      if (sessionWrapper.get() != null) sessionWrapper.get().release();
    }
  }

  public static List<ReplicaPosition> buildReplicaPositions(SolrCloudManager cloudManager, ClusterState clusterState,
                                                            DocCollection docCollection,
                                                            ZkNodeProps message,
                                                            List<String> shardNames,
                                                            AtomicReference<PolicyHelper.SessionWrapper> sessionWrapper) throws IOException, InterruptedException, Assign.AssignmentException {
    if (log.isDebugEnabled()) {
      log.debug("buildReplicaPositions(SolrCloudManager cloudManager={}, ClusterState clusterState={}, DocCollection docCollection={}, ZkNodeProps message={}, List<String> shardNames={}, AtomicReference<PolicyHelper.SessionWrapper> sessionWrapper={}) - start", cloudManager, clusterState, docCollection, message, shardNames, sessionWrapper);
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
    List<String> nodeList = Assign.getLiveOrLiveAndCreateNodeSetList(clusterState.getLiveNodes(), message, OverseerCollectionMessageHandler.RANDOM);
    if (nodeList.isEmpty()) {
      log.warn("It is unusual to create a collection ("+collectionName+") without cores. liveNodes={} message={}", clusterState.getLiveNodes(), message);

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

      int maxShardsAllowedToCreate = maxShardsPerNode == Integer.MAX_VALUE ?
              Integer.MAX_VALUE :
              maxShardsPerNode * nodeList.size();
      int requestedShardsToCreate = numSlices * totalNumReplicas;
      if (maxShardsAllowedToCreate < requestedShardsToCreate) {
        String msg = "Cannot create collection " + collectionName + ". Value of "
                + MAX_SHARDS_PER_NODE + " is " + maxShardsPerNode
                + ", and the number of nodes currently live or live and part of your "+ZkStateReader.CREATE_NODE_SET+" is " + nodeList.size()
                + ". This allows a maximum of " + maxShardsAllowedToCreate
                + " to be created. Value of " + ZkStateReader.NUM_SHARDS_PROP + " is " + numSlices
                + ", value of " + NRT_REPLICAS + " is " + numNrtReplicas
                + ", value of " + TLOG_REPLICAS + " is " + numTlogReplicas
                + " and value of " + PULL_REPLICAS + " is " + numPullReplicas
                + ". This requires " + requestedShardsToCreate
                + " shards to be created (higher than the allowed number)";

        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, msg);
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
      Assign.AssignStrategy assignStrategy = assignStrategyFactory.create(clusterState, docCollection);
      replicaPositions = assignStrategy.assign(cloudManager, assignRequest);
      sessionWrapper.set(PolicyHelper.getLastSessionWrapper(true));
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

  public static DocCollection buildDocCollection(ZkNodeProps message, boolean withDocRouter) {
    log.info("buildDocCollection {}", message);
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
      slices = Slice.loadAllFromMap(message.getStr(ZkStateReader.COLLECTION_PROP), (Map<String,Object>) messageShardsObj);
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

        slices.put(sliceName, new Slice(sliceName, null, sliceProps, message.getStr(ZkStateReader.COLLECTION_PROP)));

      }
    }

    Map<String,Object> collectionProps = new HashMap<>();

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

    // TODO default to 2; but need to debug why BasicDistributedZk2Test fails early on
    String znode = message.getInt(DocCollection.STATE_FORMAT, 1) == 1 ? ZkStateReader.CLUSTER_STATE
            : ZkStateReader.getCollectionPath(cName);

    DocCollection newCollection = new DocCollection(cName,
            slices, collectionProps, router, -1, znode);

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
          log.info("Only one config set found in zk - using it: {}", configName);
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
      ParWork.propegateInterrupt(e);
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
      log.info("Creating collection in ZooKeeper:" + collection);

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
        log.info("Setting config for collection:" + collection + " to " + defaultConfigName);

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

      // nocommit make efficient

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
      stateManager.makePath(ZkStateReader.getCollectionPropsPath(collection),
              ZkStateReader.emptyJson, CreateMode.PERSISTENT, false);

    } catch (Exception e) {
      ParWork.propegateInterrupt(e);
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
      final String sliceName = "shard" + (i + 1);
      shardNames.add(sliceName);
    }
  }

  private static void getConfName(DistribStateManager stateManager, String collection, String collectionPath, Map<String,Object> collectionProps) throws IOException,
      KeeperException, InterruptedException {
    // check for configName
    log.debug("Looking for collection configName");
    if (collectionProps.containsKey("configName")) {
      if (log.isInfoEnabled()) {
        log.info("configName was passed as a param {}", collectionProps.get("configName"));
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
      log.info("Could not find explicit collection configName, but found config name matching collection name - using that set.");
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
    log.info("Wait for expectedReplicas={}", expectedReplicas);

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
            replicaMap.put(replica.getCoreName(), replica);
            replicas++;
        }
      }
      if (replicas == expectedReplicas) {
        return true;
      }
      return false;
    };
  }

}
