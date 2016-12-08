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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.solr.cloud.OverseerCollectionMessageHandler.Cmd;
import org.apache.solr.cloud.overseer.ClusterStateMutator;
import org.apache.solr.cloud.rule.ReplicaAssigner;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.ImplicitDocRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkConfigManager;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.Utils;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.OverseerCollectionMessageHandler.COLL_CONF;
import static org.apache.solr.cloud.OverseerCollectionMessageHandler.CREATE_NODE_SET;
import static org.apache.solr.cloud.OverseerCollectionMessageHandler.NUM_SLICES;
import static org.apache.solr.cloud.OverseerCollectionMessageHandler.RANDOM;
import static org.apache.solr.common.cloud.ZkStateReader.MAX_SHARDS_PER_NODE;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICATION_FACTOR;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.ADDREPLICA;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.common.util.StrUtils.formatString;

public class CreateCollectionCmd implements Cmd {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final OverseerCollectionMessageHandler ocmh;
  private SolrZkClient zkClient;

  public CreateCollectionCmd(OverseerCollectionMessageHandler ocmh) {
    this.ocmh = ocmh;
    this.zkClient = ocmh.zkStateReader.getZkClient();
  }

  @Override
  public void call(ClusterState clusterState, ZkNodeProps message, NamedList results) throws Exception {
    final String collectionName = message.getStr(NAME);
    log.info("Create collection {}", collectionName);
    if (clusterState.hasCollection(collectionName)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "collection already exists: " + collectionName);
    }

    String configName = getConfigName(collectionName, message);
    if (configName == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No config set found to associate with the collection.");
    }

    ocmh.validateConfigOrThrowSolrException(configName);

    try {
      // look at the replication factor and see if it matches reality
      // if it does not, find best nodes to create more cores

      int repFactor = message.getInt(REPLICATION_FACTOR, 1);

      ShardHandler shardHandler = ocmh.shardHandlerFactory.getShardHandler();
      final String async = message.getStr(ASYNC);

      Integer numSlices = message.getInt(NUM_SLICES, null);
      String router = message.getStr("router.name", DocRouter.DEFAULT_NAME);
      List<String> shardNames = new ArrayList<>();
      if(ImplicitDocRouter.NAME.equals(router)){
        ClusterStateMutator.getShardNames(shardNames, message.getStr("shards", null));
        numSlices = shardNames.size();
      } else {
        if (numSlices == null ) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, NUM_SLICES + " is a required param (when using CompositeId router).");
        }
        ClusterStateMutator.getShardNames(numSlices, shardNames);
      }

      int maxShardsPerNode = message.getInt(MAX_SHARDS_PER_NODE, 1);

      if (repFactor <= 0) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, REPLICATION_FACTOR + " must be greater than 0");
      }

      if (numSlices <= 0) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, NUM_SLICES + " must be > 0");
      }

      // we need to look at every node and see how many cores it serves
      // add our new cores to existing nodes serving the least number of cores
      // but (for now) require that each core goes on a distinct node.

      final List<String> nodeList = OverseerCollectionMessageHandler.getLiveOrLiveAndCreateNodeSetList(clusterState.getLiveNodes(), message, RANDOM);
      Map<ReplicaAssigner.Position, String> positionVsNodes;
      if (nodeList.isEmpty()) {
        log.warn("It is unusual to create a collection ("+collectionName+") without cores.");

        positionVsNodes = new HashMap<>();
      } else {
        if (repFactor > nodeList.size()) {
          log.warn("Specified "
              + REPLICATION_FACTOR
              + " of "
              + repFactor
              + " on collection "
              + collectionName
              + " is higher than or equal to the number of Solr instances currently live or live and part of your " + CREATE_NODE_SET + "("
              + nodeList.size()
              + "). It's unusual to run two replica of the same slice on the same Solr-instance.");
        }

        int maxShardsAllowedToCreate = maxShardsPerNode * nodeList.size();
        int requestedShardsToCreate = numSlices * repFactor;
        if (maxShardsAllowedToCreate < requestedShardsToCreate) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Cannot create collection " + collectionName + ". Value of "
              + MAX_SHARDS_PER_NODE + " is " + maxShardsPerNode
              + ", and the number of nodes currently live or live and part of your "+CREATE_NODE_SET+" is " + nodeList.size()
              + ". This allows a maximum of " + maxShardsAllowedToCreate
              + " to be created. Value of " + NUM_SLICES + " is " + numSlices
              + " and value of " + REPLICATION_FACTOR + " is " + repFactor
              + ". This requires " + requestedShardsToCreate
              + " shards to be created (higher than the allowed number)");
        }

        positionVsNodes = ocmh.identifyNodes(clusterState, nodeList, message, shardNames, repFactor);
      }

      ZkStateReader zkStateReader = ocmh.zkStateReader;
      boolean isLegacyCloud = Overseer.isLegacy(zkStateReader);

      ocmh.createConfNode(configName, collectionName, isLegacyCloud);

      Map<String,String> collectionParams = new HashMap<>();
      Map<String,Object> collectionProps = message.getProperties();
      for (String propName : collectionProps.keySet()) {
        if (propName.startsWith(ZkController.COLLECTION_PARAM_PREFIX)) {
          collectionParams.put(propName.substring(ZkController.COLLECTION_PARAM_PREFIX.length()), (String) collectionProps.get(propName));
        }
      }
      
      createCollectionZkNode(zkClient, collectionName, collectionParams);
      
      Overseer.getStateUpdateQueue(zkStateReader.getZkClient()).offer(Utils.toJSON(message));

      // wait for a while until we don't see the collection
      TimeOut waitUntil = new TimeOut(30, TimeUnit.SECONDS);
      boolean created = false;
      while (! waitUntil.hasTimedOut()) {
        Thread.sleep(100);
        created = zkStateReader.getClusterState().hasCollection(collectionName);
        if(created) break;
      }
      if (!created)
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Could not fully create collection: " + collectionName);

      if (nodeList.isEmpty()) {
        log.debug("Finished create command for collection: {}", collectionName);
        return;
      }

      // For tracking async calls.
      Map<String, String> requestMap = new HashMap<>();


      log.debug(formatString("Creating SolrCores for new collection {0}, shardNames {1} , replicationFactor : {2}",
          collectionName, shardNames, repFactor));
      Map<String,ShardRequest> coresToCreate = new LinkedHashMap<>();
      for (Map.Entry<ReplicaAssigner.Position, String> e : positionVsNodes.entrySet()) {
        ReplicaAssigner.Position position = e.getKey();
        String nodeName = e.getValue();
        String coreName = collectionName + "_" + position.shard + "_replica" + (position.index + 1);
        log.debug(formatString("Creating core {0} as part of shard {1} of collection {2} on {3}"
            , coreName, position.shard, collectionName, nodeName));


        String baseUrl = zkStateReader.getBaseUrlForNodeName(nodeName);
        //in the new mode, create the replica in clusterstate prior to creating the core.
        // Otherwise the core creation fails
        if (!isLegacyCloud) {
          ZkNodeProps props = new ZkNodeProps(
              Overseer.QUEUE_OPERATION, ADDREPLICA.toString(),
              ZkStateReader.COLLECTION_PROP, collectionName,
              ZkStateReader.SHARD_ID_PROP, position.shard,
              ZkStateReader.CORE_NAME_PROP, coreName,
              ZkStateReader.STATE_PROP, Replica.State.DOWN.toString(),
              ZkStateReader.BASE_URL_PROP, baseUrl);
          Overseer.getStateUpdateQueue(zkStateReader.getZkClient()).offer(Utils.toJSON(props));
        }

        // Need to create new params for each request
        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set(CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.CREATE.toString());

        params.set(CoreAdminParams.NAME, coreName);
        params.set(COLL_CONF, configName);
        params.set(CoreAdminParams.COLLECTION, collectionName);
        params.set(CoreAdminParams.SHARD, position.shard);
        params.set(ZkStateReader.NUM_SHARDS_PROP, numSlices);
        params.set(CoreAdminParams.NEW_COLLECTION, "true");

        if (async != null) {
          String coreAdminAsyncId = async + Math.abs(System.nanoTime());
          params.add(ASYNC, coreAdminAsyncId);
          requestMap.put(nodeName, coreAdminAsyncId);
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
          shardHandler.submit(sreq, sreq.shards[0], sreq.params);
        } else {
          coresToCreate.put(coreName, sreq);
        }
      }

      if(!isLegacyCloud) {
        // wait for all replica entries to be created
        Map<String, Replica> replicas = ocmh.waitToSeeReplicasInState(collectionName, coresToCreate.keySet());
        for (Map.Entry<String, ShardRequest> e : coresToCreate.entrySet()) {
          ShardRequest sreq = e.getValue();
          sreq.params.set(CoreAdminParams.CORE_NODE_NAME, replicas.get(e.getKey()).getName());
          shardHandler.submit(sreq, sreq.shards[0], sreq.params);
        }
      }

      ocmh.processResponses(results, shardHandler, false, null, async, requestMap, Collections.emptySet());
      if(results.get("failure") != null && ((SimpleOrderedMap)results.get("failure")).size() > 0) {
        // Let's cleanup as we hit an exception
        // We shouldn't be passing 'results' here for the cleanup as the response would then contain 'success'
        // element, which may be interpreted by the user as a positive ack
        ocmh.cleanupCollection(collectionName, new NamedList());
        log.info("Cleaned up artifacts for failed create collection for [{}]", collectionName);
      } else {
        log.debug("Finished create command on all shards for collection: {}", collectionName);
      }
    } catch (SolrException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, null, ex);
    }
  }
  String getConfigName(String coll, ZkNodeProps message) throws KeeperException, InterruptedException {
    String configName = message.getStr(COLL_CONF);

    if (configName == null) {
      // if there is only one conf, use that
      List<String> configNames = null;
      try {
        configNames = ocmh.zkStateReader.getZkClient().getChildren(ZkConfigManager.CONFIGS_ZKNODE, null, true);
        if (configNames != null && configNames.size() == 1) {
          configName = configNames.get(0);
          // no config set named, but there is only 1 - use it
          log.info("Only one config set found in zk - using it:" + configName);
        } else if (configNames.contains(coll)) {
          configName = coll;
        }
      } catch (KeeperException.NoNodeException e) {

      }
    }
    return configName;
  }
  
  public static void createCollectionZkNode(SolrZkClient zkClient, String collection, Map<String,String> params) {
    log.debug("Check for collection zkNode:" + collection);
    String collectionPath = ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection;

    try {
      if (!zkClient.exists(collectionPath, true)) {
        log.debug("Creating collection in ZooKeeper:" + collection);

        try {
          Map<String,Object> collectionProps = new HashMap<>();

          // TODO: if collection.configName isn't set, and there isn't already a conf in zk, just use that?
          String defaultConfigName = System.getProperty(ZkController.COLLECTION_PARAM_PREFIX + ZkController.CONFIGNAME_PROP, collection);

          if (params.size() > 0) {
            collectionProps.putAll(params);
            // if the config name wasn't passed in, use the default
            if (!collectionProps.containsKey(ZkController.CONFIGNAME_PROP)) {
              // users can create the collection node and conf link ahead of time, or this may return another option
              getConfName(zkClient, collection, collectionPath, collectionProps);
            }

          } else if (System.getProperty("bootstrap_confdir") != null) {
            // if we are bootstrapping a collection, default the config for
            // a new collection to the collection we are bootstrapping
            log.info("Setting config for collection:" + collection + " to " + defaultConfigName);

            Properties sysProps = System.getProperties();
            for (String sprop : System.getProperties().stringPropertyNames()) {
              if (sprop.startsWith(ZkController.COLLECTION_PARAM_PREFIX)) {
                collectionProps.put(sprop.substring(ZkController.COLLECTION_PARAM_PREFIX.length()), sysProps.getProperty(sprop));
              }
            }

            // if the config name wasn't passed in, use the default
            if (!collectionProps.containsKey(ZkController.CONFIGNAME_PROP))
              collectionProps.put(ZkController.CONFIGNAME_PROP, defaultConfigName);

          } else if (Boolean.getBoolean("bootstrap_conf")) {
            // the conf name should should be the collection name of this core
            collectionProps.put(ZkController.CONFIGNAME_PROP, collection);
          } else {
            getConfName(zkClient, collection, collectionPath, collectionProps);
          }

          collectionProps.remove(ZkStateReader.NUM_SHARDS_PROP);  // we don't put numShards in the collections properties

          ZkNodeProps zkProps = new ZkNodeProps(collectionProps);
          zkClient.makePath(collectionPath, Utils.toJSON(zkProps), CreateMode.PERSISTENT, null, true);

        } catch (KeeperException e) {
          // it's okay if the node already exists
          if (e.code() != KeeperException.Code.NODEEXISTS) {
            throw e;
          }
        }
      } else {
        log.debug("Collection zkNode exists");
      }

    } catch (KeeperException e) {
      // it's okay if another beats us creating the node
      if (e.code() == KeeperException.Code.NODEEXISTS) {
        return;
      }
      throw new SolrException(ErrorCode.SERVER_ERROR, "Error creating collection node in Zookeeper", e);
    } catch (InterruptedException e) {
      Thread.interrupted();
      throw new SolrException(ErrorCode.SERVER_ERROR, "Error creating collection node in Zookeeper", e);
    }

  }
  
  private static void getConfName(SolrZkClient zkClient, String collection, String collectionPath, Map<String,Object> collectionProps) throws KeeperException,
      InterruptedException {
    // check for configName
    log.debug("Looking for collection configName");
    if (collectionProps.containsKey("configName")) {
      log.info("configName was passed as a param {}", collectionProps.get("configName"));
      return;
    }
    
    List<String> configNames = null;
    int retry = 1;
    int retryLimt = 6;
    for (; retry < retryLimt; retry++) {
      if (zkClient.exists(collectionPath, true)) {
        ZkNodeProps cProps = ZkNodeProps.load(zkClient.getData(collectionPath, null, null, true));
        if (cProps.containsKey(ZkController.CONFIGNAME_PROP)) {
          break;
        }
      }

      // if there is only one conf, use that
      try {
        configNames = zkClient.getChildren(ZkConfigManager.CONFIGS_ZKNODE, null,
            true);
      } catch (NoNodeException e) {
        // just keep trying
      }
      if (configNames != null && configNames.size() == 1) {
        // no config set named, but there is only 1 - use it
        log.info("Only one config set found in zk - using it:" + configNames.get(0));
        collectionProps.put(ZkController.CONFIGNAME_PROP, configNames.get(0));
        break;
      }

      if (configNames != null && configNames.contains(collection)) {
        log.info(
            "Could not find explicit collection configName, but found config name matching collection name - using that set.");
        collectionProps.put(ZkController.CONFIGNAME_PROP, collection);
        break;
      }

      log.info("Could not find collection configName - pausing for 3 seconds and trying again - try: " + retry);
      Thread.sleep(3000);
    }
    if (retry == retryLimt) {
      log.error("Could not find configName for collection " + collection);
      throw new ZooKeeperException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Could not find configName for collection " + collection + " found:" + configNames);
    }
  }
}
