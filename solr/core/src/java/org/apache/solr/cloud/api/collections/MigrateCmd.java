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

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.api.collections.OverseerCollectionMessageHandler.ShardRequestTracker;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.CompositeIdRouter;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.RoutingRule;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.handler.component.HttpShardHandlerFactory;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.solr.handler.component.ShardHandlerFactory;
import org.apache.solr.update.SolrIndexSplitter;
import org.apache.solr.util.TimeOut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.NRT_REPLICAS;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.params.CollectionAdminParams.FOLLOW_ALIASES;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.ADDREPLICA;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.CREATE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETE;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.common.util.Utils.makeMap;

public class MigrateCmd implements OverseerCollectionMessageHandler.Cmd {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final OverseerCollectionMessageHandler ocmh;
  private final TimeSource timeSource;

  public MigrateCmd(OverseerCollectionMessageHandler ocmh) {
    this.ocmh = ocmh;
    this.timeSource = ocmh.cloudManager.getTimeSource();
  }


  @Override
  public void call(ClusterState clusterState, ZkNodeProps message, @SuppressWarnings({"rawtypes"})NamedList results) throws Exception {
    String extSourceCollectionName = message.getStr("collection");
    String splitKey = message.getStr("split.key");
    String extTargetCollectionName = message.getStr("target.collection");
    int timeout = message.getInt("forward.timeout", 10 * 60) * 1000;

    boolean followAliases = message.getBool(FOLLOW_ALIASES, false);
    String sourceCollectionName;
    String targetCollectionName;
    if (followAliases) {
      sourceCollectionName = ocmh.cloudManager.getClusterStateProvider().resolveSimpleAlias(extSourceCollectionName);
      targetCollectionName = ocmh.cloudManager.getClusterStateProvider().resolveSimpleAlias(extTargetCollectionName);
    } else {
      sourceCollectionName = extSourceCollectionName;
      targetCollectionName = extTargetCollectionName;
    }

    DocCollection sourceCollection = clusterState.getCollection(sourceCollectionName);
    if (sourceCollection == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unknown source collection: " + sourceCollectionName);
    }
    DocCollection targetCollection = clusterState.getCollection(targetCollectionName);
    if (targetCollection == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unknown target collection: " + sourceCollectionName);
    }
    if (!(sourceCollection.getRouter() instanceof CompositeIdRouter)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Source collection must use a compositeId router");
    }
    if (!(targetCollection.getRouter() instanceof CompositeIdRouter)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Target collection must use a compositeId router");
    }

    if (splitKey == null || splitKey.trim().length() == 0)  {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "The split.key cannot be null or empty");
    }

    CompositeIdRouter sourceRouter = (CompositeIdRouter) sourceCollection.getRouter();
    CompositeIdRouter targetRouter = (CompositeIdRouter) targetCollection.getRouter();
    Collection<Slice> sourceSlices = sourceRouter.getSearchSlicesSingle(splitKey, null, sourceCollection);
    if (sourceSlices.isEmpty()) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "No active slices available in source collection: " + sourceCollection + "for given split.key: " + splitKey);
    }
    Collection<Slice> targetSlices = targetRouter.getSearchSlicesSingle(splitKey, null, targetCollection);
    if (targetSlices.isEmpty()) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "No active slices available in target collection: " + targetCollection + "for given split.key: " + splitKey);
    }

    String asyncId = null;
    if (message.containsKey(ASYNC) && message.get(ASYNC) != null)
      asyncId = message.getStr(ASYNC);

    for (Slice sourceSlice : sourceSlices) {
      for (Slice targetSlice : targetSlices) {
        log.info("Migrating source shard: {} to target shard: {} for split.key = {}", sourceSlice, targetSlice, splitKey);
        migrateKey(clusterState, sourceCollection, sourceSlice, targetCollection, targetSlice, splitKey,
            timeout, results, asyncId, message);
      }
    }
  }

  @SuppressWarnings({"unchecked"})
  private void migrateKey(ClusterState clusterState, DocCollection sourceCollection, Slice sourceSlice,
                          DocCollection targetCollection, Slice targetSlice,
                          String splitKey, int timeout,
                          @SuppressWarnings({"rawtypes"})NamedList results, String asyncId, ZkNodeProps message) throws Exception {
    String tempSourceCollectionName = "split_" + sourceSlice.getName() + "_temp_" + targetSlice.getName();
    ZkStateReader zkStateReader = ocmh.zkStateReader;
    if (clusterState.hasCollection(tempSourceCollectionName)) {
      log.info("Deleting temporary collection: {}", tempSourceCollectionName);
      Map<String, Object> props = makeMap(
          Overseer.QUEUE_OPERATION, DELETE.toLower(),
          NAME, tempSourceCollectionName);

      try {
        ocmh.commandMap.get(DELETE).call(zkStateReader.getClusterState(), new ZkNodeProps(props), results);
        clusterState = zkStateReader.getClusterState();
      } catch (Exception e) {
        log.warn("Unable to clean up existing temporary collection: {}", tempSourceCollectionName, e);
      }
    }

    CompositeIdRouter sourceRouter = (CompositeIdRouter) sourceCollection.getRouter();
    DocRouter.Range keyHashRange = sourceRouter.keyHashRange(splitKey);

    ShardHandlerFactory shardHandlerFactory = ocmh.shardHandlerFactory;
    ShardHandler shardHandler = ((HttpShardHandlerFactory)shardHandlerFactory).getShardHandler(ocmh.overseer.getCoreContainer().getUpdateShardHandler().getDefaultHttpClient());

    log.info("Hash range for split.key: {} is: {}", splitKey, keyHashRange);
    // intersect source range, keyHashRange and target range
    // this is the range that has to be split from source and transferred to target
    DocRouter.Range splitRange = ocmh.intersect(targetSlice.getRange(), ocmh.intersect(sourceSlice.getRange(), keyHashRange));
    if (splitRange == null) {
      if (log.isInfoEnabled()) {
        log.info("No common hashes between source shard: {} and target shard: {}", sourceSlice.getName(), targetSlice.getName());
      }
      return;
    }
    if (log.isInfoEnabled()) {
      log.info("Common hash range between source shard: {} and target shard: {} = {}", sourceSlice.getName(), targetSlice.getName(), splitRange);
    }

    Replica targetLeader = zkStateReader.getLeaderRetry(targetCollection.getName(), targetSlice.getName(), 10000);

    if (log.isInfoEnabled()) {
      log.info("Asking target leader node: {} core: {} to buffer updates"
          , targetLeader.getNodeName(), targetLeader.getStr("core"));
    }
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.REQUESTBUFFERUPDATES.toString());
    params.set(CoreAdminParams.NAME, targetLeader.getStr("core"));

    {
      final ShardRequestTracker shardRequestTracker = ocmh.asyncRequestTracker(asyncId);
      shardRequestTracker.sendShardRequest(targetLeader.getNodeName(), params, shardHandler);

      shardRequestTracker.processResponses(results, shardHandler, true, "MIGRATE failed to request node to buffer updates");
    }
    ZkNodeProps m = new ZkNodeProps(
        Overseer.QUEUE_OPERATION, OverseerAction.ADDROUTINGRULE.toLower(),
        COLLECTION_PROP, sourceCollection.getName(),
        SHARD_ID_PROP, sourceSlice.getName(),
        "routeKey", SolrIndexSplitter.getRouteKey(splitKey) + "!",
        "range", splitRange.toString(),
        "targetCollection", targetCollection.getName(),
        "expireAt", RoutingRule.makeExpiryAt(timeout));
    log.info("Adding routing rule: {}", m);
    ocmh.overseer.offerStateUpdate(Utils.toJSON(m));

    // wait for a while until we see the new rule
    log.info("Waiting to see routing rule updated in clusterstate");
    TimeOut waitUntil = new TimeOut(60, TimeUnit.SECONDS, timeSource);
    boolean added = false;
    while (!waitUntil.hasTimedOut()) {
      waitUntil.sleep(100);
      sourceCollection = zkStateReader.getClusterState().getCollection(sourceCollection.getName());
      sourceSlice = sourceCollection.getSlice(sourceSlice.getName());
      Map<String, RoutingRule> rules = sourceSlice.getRoutingRules();
      if (rules != null) {
        RoutingRule rule = rules.get(SolrIndexSplitter.getRouteKey(splitKey) + "!");
        if (rule != null && rule.getRouteRanges().contains(splitRange)) {
          added = true;
          break;
        }
      }
    }
    if (!added) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Could not add routing rule: " + m);
    }

    log.info("Routing rule added successfully");

    // Create temp core on source shard
    Replica sourceLeader = zkStateReader.getLeaderRetry(sourceCollection.getName(), sourceSlice.getName(), 10000);

    // create a temporary collection with just one node on the shard leader
    String configName = zkStateReader.readConfigName(sourceCollection.getName());
    Map<String, Object> props = makeMap(
        Overseer.QUEUE_OPERATION, CREATE.toLower(),
        NAME, tempSourceCollectionName,
        NRT_REPLICAS, 1,
        OverseerCollectionMessageHandler.NUM_SLICES, 1,
        CollectionAdminParams.COLL_CONF, configName,
        OverseerCollectionMessageHandler.CREATE_NODE_SET, sourceLeader.getNodeName());
    if (asyncId != null) {
      String internalAsyncId = asyncId + Math.abs(System.nanoTime());
      props.put(ASYNC, internalAsyncId);
    }

    log.info("Creating temporary collection: {}", props);
    ocmh.commandMap.get(CREATE).call(clusterState, new ZkNodeProps(props), results);
    // refresh cluster state
    clusterState = zkStateReader.getClusterState();
    Slice tempSourceSlice = clusterState.getCollection(tempSourceCollectionName).getSlices().iterator().next();
    Replica tempSourceLeader = zkStateReader.getLeaderRetry(tempSourceCollectionName, tempSourceSlice.getName(), 120000);

    String tempCollectionReplica1 = tempSourceLeader.getCoreName();
    String coreNodeName = ocmh.waitForCoreNodeName(tempSourceCollectionName,
        sourceLeader.getNodeName(), tempCollectionReplica1);
    // wait for the replicas to be seen as active on temp source leader
    if (log.isInfoEnabled()) {
      log.info("Asking source leader to wait for: {} to be alive on: {}", tempCollectionReplica1, sourceLeader.getNodeName());
    }
    CoreAdminRequest.WaitForState cmd = new CoreAdminRequest.WaitForState();
    cmd.setCoreName(tempCollectionReplica1);
    cmd.setNodeName(sourceLeader.getNodeName());
    cmd.setCoreNodeName(coreNodeName);
    cmd.setState(Replica.State.ACTIVE);
    cmd.setCheckLive(true);
    cmd.setOnlyIfLeader(true);
    {
      final ShardRequestTracker syncRequestTracker = ocmh.syncRequestTracker();
      // we don't want this to happen asynchronously
      syncRequestTracker.sendShardRequest(tempSourceLeader.getNodeName(), new ModifiableSolrParams(cmd.getParams()),
          shardHandler);

      syncRequestTracker.processResponses(results, shardHandler, true,
          "MIGRATE failed to create temp collection leader" +
              " or timed out waiting for it to come up");
    }
    
    log.info("Asking source leader to split index");
    params = new ModifiableSolrParams();
    params.set(CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.SPLIT.toString());
    params.set(CoreAdminParams.CORE, sourceLeader.getStr("core"));
    params.add(CoreAdminParams.TARGET_CORE, tempSourceLeader.getStr("core"));
    params.set(CoreAdminParams.RANGES, splitRange.toString());
    params.set("split.key", splitKey);

    String tempNodeName = sourceLeader.getNodeName();

    {
      final ShardRequestTracker shardRequestTracker = ocmh.asyncRequestTracker(asyncId);
      shardRequestTracker.sendShardRequest(tempNodeName, params, shardHandler);
      shardRequestTracker.processResponses(results, shardHandler, true, "MIGRATE failed to invoke SPLIT core admin command");
    }
    if (log.isInfoEnabled()) {
      log.info("Creating a replica of temporary collection: {} on the target leader node: {}",
          tempSourceCollectionName, targetLeader.getNodeName());
    }
    String tempCollectionReplica2 = Assign.buildSolrCoreName(ocmh.overseer.getSolrCloudManager().getDistribStateManager(),
        zkStateReader.getClusterState().getCollection(tempSourceCollectionName), tempSourceSlice.getName(), Replica.Type.NRT);
    props = new HashMap<>();
    props.put(Overseer.QUEUE_OPERATION, ADDREPLICA.toLower());
    props.put(COLLECTION_PROP, tempSourceCollectionName);
    props.put(SHARD_ID_PROP, tempSourceSlice.getName());
    props.put("node", targetLeader.getNodeName());
    props.put(CoreAdminParams.NAME, tempCollectionReplica2);
    // copy over property params:
    for (String key : message.keySet()) {
      if (key.startsWith(OverseerCollectionMessageHandler.COLL_PROP_PREFIX)) {
        props.put(key, message.getStr(key));
      }
    }
    // add async param
    if (asyncId != null) {
      props.put(ASYNC, asyncId);
    }
    ((AddReplicaCmd)ocmh.commandMap.get(ADDREPLICA)).addReplica(clusterState, new ZkNodeProps(props), results, null);

    {
      final ShardRequestTracker syncRequestTracker = ocmh.syncRequestTracker();
      syncRequestTracker.processResponses(results, shardHandler, true, "MIGRATE failed to create replica of " +
        "temporary collection in target leader node.");
    }
    coreNodeName = ocmh.waitForCoreNodeName(tempSourceCollectionName,
        targetLeader.getNodeName(), tempCollectionReplica2);
    // wait for the replicas to be seen as active on temp source leader
    if (log.isInfoEnabled()) {
      log.info("Asking temp source leader to wait for: {} to be alive on: {}", tempCollectionReplica2, targetLeader.getNodeName());
    }
    cmd = new CoreAdminRequest.WaitForState();
    cmd.setCoreName(tempSourceLeader.getStr("core"));
    cmd.setNodeName(targetLeader.getNodeName());
    cmd.setCoreNodeName(coreNodeName);
    cmd.setState(Replica.State.ACTIVE);
    cmd.setCheckLive(true);
    cmd.setOnlyIfLeader(true);
    params = new ModifiableSolrParams(cmd.getParams());

    {
      final ShardRequestTracker shardRequestTracker = ocmh.asyncRequestTracker(asyncId);
      shardRequestTracker.sendShardRequest(tempSourceLeader.getNodeName(), params, shardHandler);

      shardRequestTracker.processResponses(results, shardHandler, true, "MIGRATE failed to create temp collection" +
        " replica or timed out waiting for them to come up");
    }
    log.info("Successfully created replica of temp source collection on target leader node");

    log.info("Requesting merge of temp source collection replica to target leader");
    params = new ModifiableSolrParams();
    params.set(CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.MERGEINDEXES.toString());
    params.set(CoreAdminParams.CORE, targetLeader.getStr("core"));
    params.set(CoreAdminParams.SRC_CORE, tempCollectionReplica2);

    {
      final ShardRequestTracker shardRequestTracker = ocmh.asyncRequestTracker(asyncId);
    
      shardRequestTracker.sendShardRequest(targetLeader.getNodeName(), params, shardHandler);
    String msg = "MIGRATE failed to merge " + tempCollectionReplica2 + " to "
        + targetLeader.getStr("core") + " on node: " + targetLeader.getNodeName();
    shardRequestTracker.processResponses(results, shardHandler, true, msg);
    }
    log.info("Asking target leader to apply buffered updates");
    params = new ModifiableSolrParams();
    params.set(CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.REQUESTAPPLYUPDATES.toString());
    params.set(CoreAdminParams.NAME, targetLeader.getStr("core"));

    {
      final ShardRequestTracker shardRequestTracker = ocmh.asyncRequestTracker(asyncId);
      shardRequestTracker.sendShardRequest(targetLeader.getNodeName(), params, shardHandler);
      shardRequestTracker.processResponses(results, shardHandler, true, "MIGRATE failed to request node to apply buffered updates");
    }
    try {
      log.info("Deleting temporary collection: {}", tempSourceCollectionName);
      props = makeMap(
          Overseer.QUEUE_OPERATION, DELETE.toLower(),
          NAME, tempSourceCollectionName);
      ocmh.commandMap.get(DELETE). call(zkStateReader.getClusterState(), new ZkNodeProps(props), results);
    } catch (Exception e) {
      log.error("Unable to delete temporary collection: {}. Please remove it manually", tempSourceCollectionName, e);
    }
  }
}
