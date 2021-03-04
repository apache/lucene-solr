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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.cloud.AlreadyExistsException;
import org.apache.solr.client.solrj.cloud.BadVersionException;
import org.apache.solr.client.solrj.cloud.DistribStateManager;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.cloud.DistributedClusterStateUpdater;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.overseer.ClusterStateMutator;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.*;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.backup.BackupId;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.solr.handler.component.ShardHandlerFactory;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.component.ShardResponse;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETE;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.common.util.Utils.makeMap;

/**
 * This class contains helper methods used by commands of the Collection API. Previously these methods were in
 * {@link OverseerCollectionMessageHandler} and were refactored out to (eventually) allow Collection API commands to be
 * executed outside the context of the Overseer.
 */
public class CollectionHandlingUtils {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String NUM_SLICES = "numShards";

  public static final boolean CREATE_NODE_SET_SHUFFLE_DEFAULT = true;
  public static final String CREATE_NODE_SET_SHUFFLE = CollectionAdminParams.CREATE_NODE_SET_SHUFFLE_PARAM;
  public static final String CREATE_NODE_SET_EMPTY = "EMPTY";
  public static final String CREATE_NODE_SET = CollectionAdminParams.CREATE_NODE_SET_PARAM;

  public static final String ROUTER = "router";

  public static final String SHARDS_PROP = "shards";

  public static final String REQUESTID = "requestid";

  public static final String ONLY_IF_DOWN = "onlyIfDown";

  public static final String SHARD_UNIQUE = "shardUnique";

  public static final String ONLY_ACTIVE_NODES = "onlyactivenodes";

  static final String SKIP_CREATE_REPLICA_IN_CLUSTER_STATE = "skipCreateReplicaInClusterState";

  public static final Map<String, Object> COLLECTION_PROPS_AND_DEFAULTS = Collections.unmodifiableMap(makeMap(
      ROUTER, DocRouter.DEFAULT_NAME,
      ZkStateReader.REPLICATION_FACTOR, "1",
      ZkStateReader.NRT_REPLICAS, "1",
      ZkStateReader.TLOG_REPLICAS, "0",
      DocCollection.PER_REPLICA_STATE, null,
      ZkStateReader.PULL_REPLICAS, "0"));

  public static final Random RANDOM;
  static {
    // We try to make things reproducible in the context of our tests by initializing the random instance
    // based on the current seed
    String seed = System.getProperty("tests.seed");
    if (seed == null) {
      RANDOM = new Random();
    } else {
      RANDOM = new Random(seed.hashCode());
    }
  }

  static boolean waitForCoreNodeGone(String collectionName, String shard, String replicaName, int timeoutms, ZkStateReader zkStateReader) throws InterruptedException {
    try {
      zkStateReader.waitForState(collectionName, timeoutms, TimeUnit.MILLISECONDS, (c) -> {
        if (c == null)
          return true;
        Slice slice = c.getSlice(shard);
        if(slice == null || slice.getReplica(replicaName) == null) {
          return true;
        }
        return false;
      });
    } catch (TimeoutException e) {
      return false;
    }

    return true;
  }

  static void deleteCoreNode(String collectionName, String replicaName, Replica replica, String core, CollectionCommandContext ccc) throws Exception {
    ZkNodeProps m = new ZkNodeProps(
        Overseer.QUEUE_OPERATION, OverseerAction.DELETECORE.toLower(),
        ZkStateReader.CORE_NAME_PROP, core,
        ZkStateReader.NODE_NAME_PROP, replica.getNodeName(),
        ZkStateReader.COLLECTION_PROP, collectionName,
        ZkStateReader.CORE_NODE_NAME_PROP, replicaName);
    if (ccc.getDistributedClusterStateUpdater().isDistributedStateUpdate()) {
      ccc.getDistributedClusterStateUpdater().doSingleStateUpdate(DistributedClusterStateUpdater.MutatingCommand.SliceRemoveReplica, m,
          ccc.getSolrCloudManager(), ccc.getZkStateReader());
    } else {
      ccc.offerStateUpdate(Utils.toJSON(m));
    }
  }

  static void checkRequired(ZkNodeProps message, String... props) {
    for (String prop : props) {
      if (message.get(prop) == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, StrUtils.join(Arrays.asList(props), ',') + " are required params");
      }
    }
  }

  static void checkResults(String label, NamedList<Object> results, boolean failureIsFatal) throws SolrException {
    Object failure = results.get("failure");
    if (failure == null) {
      failure = results.get("error");
    }
    if (failure != null) {
      String msg = "Error: " + label + ": " + Utils.toJSONString(results);
      if (failureIsFatal) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, msg);
      } else {
        log.error(msg);
      }
    }
  }

  @SuppressWarnings({"unchecked"})
  static void commit(@SuppressWarnings({"rawtypes"}) NamedList results, String slice, Replica parentShardLeader) {
    log.debug("Calling soft commit to make sub shard updates visible");
    String coreUrl = new ZkCoreNodeProps(parentShardLeader).getCoreUrl();
    // HttpShardHandler is hard coded to send a QueryRequest hence we go direct
    // and we force open a searcher so that we have documents to show upon switching states
    UpdateResponse updateResponse = null;
    try {
      updateResponse = softCommit(coreUrl);
      CollectionHandlingUtils.processResponse(results, null, coreUrl, updateResponse, slice, Collections.emptySet());
    } catch (Exception e) {
      CollectionHandlingUtils.processResponse(results, e, coreUrl, updateResponse, slice, Collections.emptySet());
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unable to call distrib softCommit on: " + coreUrl, e);
    }
  }

  static UpdateResponse softCommit(String url) throws SolrServerException, IOException {

    try (HttpSolrClient client = new HttpSolrClient.Builder(url)
        .withConnectionTimeout(30000)
        .withSocketTimeout(120000)
        .build()) {
      UpdateRequest ureq = new UpdateRequest();
      ureq.setParams(new ModifiableSolrParams());
      ureq.setAction(AbstractUpdateRequest.ACTION.COMMIT, false, true, true);
      return ureq.process(client);
    }
  }

  public static String waitForCoreNodeName(String collectionName, String msgNodeName, String msgCore, ZkStateReader zkStateReader) {
    try {
      DocCollection collection = zkStateReader.waitForState(collectionName, 320, TimeUnit.SECONDS, c ->
          ClusterStateMutator.getAssignedCoreNodeName(c, msgNodeName, msgCore) != null
      );
      return ClusterStateMutator.getAssignedCoreNodeName(collection, msgNodeName, msgCore);
    } catch (TimeoutException | InterruptedException e) {
      SolrZkClient.checkInterrupted(e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Failed waiting for coreNodeName", e);
    }
  }

  static ClusterState waitForNewShard(String collectionName, String sliceName, ZkStateReader zkStateReader) throws KeeperException, InterruptedException {
    log.debug("Waiting for slice {} of collection {} to be available", sliceName, collectionName);
    try {
      zkStateReader.waitForState(collectionName, 320, TimeUnit.SECONDS, c -> {
        return c != null && c.getSlice(sliceName) != null;
      });
    } catch (TimeoutException | InterruptedException e) {
      SolrZkClient.checkInterrupted(e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Failed waiting for new slice", e);
    }
    return zkStateReader.getClusterState();
  }

  static void addPropertyParams(ZkNodeProps message, ModifiableSolrParams params) {
    // Now add the property.key=value pairs
    for (String key : message.keySet()) {
      if (key.startsWith(CollectionAdminParams.PROPERTY_PREFIX)) {
        params.set(key, message.getStr(key));
      }
    }
  }

  static void addPropertyParams(ZkNodeProps message, Map<String, Object> map) {
    // Now add the property.key=value pairs
    for (String key : message.keySet()) {
      if (key.startsWith(CollectionAdminParams.PROPERTY_PREFIX)) {
        map.put(key, message.getStr(key));
      }
    }
  }

  static void cleanupCollection(String collectionName, @SuppressWarnings({"rawtypes"})NamedList results, CollectionCommandContext ccc) throws Exception {
    log.error("Cleaning up collection [{}].", collectionName);
    Map<String, Object> props = makeMap(
        Overseer.QUEUE_OPERATION, DELETE.toLower(),
        NAME, collectionName);
    new DeleteCollectionCmd(ccc).call(ccc.getZkStateReader().getClusterState(), new ZkNodeProps(props), results);
  }

  static Map<String, Replica> waitToSeeReplicasInState(ZkStateReader zkStateReader, TimeSource timeSource, String collectionName, Collection<String> coreNames) throws InterruptedException {
    assert coreNames.size() > 0;
    Map<String, Replica> results = new ConcurrentHashMap<>();

    long maxWait = Long.getLong("solr.waitToSeeReplicasInStateTimeoutSeconds", 120); // could be a big cluster
    try {
      zkStateReader.waitForState(collectionName, maxWait, TimeUnit.SECONDS, c -> {
        if (c == null) return false;

        // We write into a ConcurrentHashMap, which will be ok if called multiple times by multiple threads
        c.getSlices().stream().flatMap(slice -> slice.getReplicas().stream())
            .filter(r -> coreNames.contains(r.getCoreName()))       // Only the elements that were asked for...
            .forEach(r -> results.putIfAbsent(r.getCoreName(), r)); // ...get added to the map

        log.debug("Expecting {} cores, found {}", coreNames, results);
        return results.size() == coreNames.size();
      });
    } catch (TimeoutException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e.getMessage(), e);
    }

    return results;
  }

  @SuppressWarnings({"rawtypes"})
  static void cleanBackup(BackupRepository repository, URI backupUri, BackupId backupId, CollectionCommandContext ccc) throws Exception {
    new DeleteBackupCmd(ccc).deleteBackupIds(backupUri, repository, Collections.singleton(backupId), new NamedList());
  }

  static void deleteBackup(BackupRepository repository, URI backupPath, int maxNumBackup,
                    @SuppressWarnings({"rawtypes"}) NamedList results, CollectionCommandContext ccc) throws Exception {
    new DeleteBackupCmd(ccc).keepNumberOfBackup(repository, backupPath, maxNumBackup, results);
  }

  static List<ZkNodeProps> addReplica(ClusterState clusterState, ZkNodeProps message,
                                      @SuppressWarnings({"rawtypes"})NamedList results, Runnable onComplete, CollectionCommandContext ccc)
      throws Exception {

    return new AddReplicaCmd(ccc).addReplica(clusterState, message, results, onComplete);
  }

  static void validateConfigOrThrowSolrException(SolrCloudManager cloudManager, String configName) throws IOException, KeeperException, InterruptedException {
    boolean isValid = cloudManager.getDistribStateManager().hasData(ZkConfigManager.CONFIGS_ZKNODE + "/" + configName);
    if (!isValid) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Can not find the specified config set: " + configName);
    }
  }

  /**
   * This doesn't validate the config (path) itself and is just responsible for creating the confNode.
   * That check should be done before the config node is created.
   */
  public static void createConfNode(DistribStateManager stateManager, String configName, String coll) throws IOException, AlreadyExistsException, BadVersionException, KeeperException, InterruptedException {

    if (configName != null) {
      String collDir = ZkStateReader.COLLECTIONS_ZKNODE + "/" + coll;
      log.debug("creating collections conf node {} ", collDir);
      byte[] data = Utils.toJSON(makeMap(ZkController.CONFIGNAME_PROP, configName));
      if (stateManager.hasData(collDir)) {
        stateManager.setData(collDir, data, -1);
      } else {
        stateManager.makePath(collDir, data, CreateMode.PERSISTENT, false);
      }
    } else {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,"Unable to get config name");
    }
  }

  /**
   * Send request to all replicas of a collection
   * @return List of replicas which is not live for receiving the request
   */
  static List<Replica> collectionCmd(ZkNodeProps message, ModifiableSolrParams params,
                                     NamedList<Object> results, Replica.State stateMatcher, String asyncId, Set<String> okayExceptions,
                                     CollectionCommandContext ccc, ClusterState clusterState) {
    log.info("Executing Collection Cmd={}, asyncId={}", params, asyncId);
    String collectionName = message.getStr(NAME);
    ShardHandler shardHandler = ccc.getShardHandler();
    DocCollection coll = clusterState.getCollection(collectionName);
    List<Replica> notLivesReplicas = new ArrayList<>();
    final CollectionHandlingUtils.ShardRequestTracker shardRequestTracker = asyncRequestTracker(asyncId, ccc);
    for (Slice slice : coll.getSlices()) {
      notLivesReplicas.addAll(shardRequestTracker.sliceCmd(clusterState, params, stateMatcher, slice, shardHandler));
    }

    shardRequestTracker.processResponses(results, shardHandler, false, null, okayExceptions);
    return notLivesReplicas;
  }

  static void processResponse(NamedList<Object> results, ShardResponse srsp, Set<String> okayExceptions) {
    Throwable e = srsp.getException();
    String nodeName = srsp.getNodeName();
    SolrResponse solrResponse = srsp.getSolrResponse();
    String shard = srsp.getShard();

    processResponse(results, e, nodeName, solrResponse, shard, okayExceptions);
  }

  @SuppressWarnings("deprecation")
  static void processResponse(NamedList<Object> results, Throwable e, String nodeName, SolrResponse solrResponse, String shard, Set<String> okayExceptions) {
    String rootThrowable = null;
    if (e instanceof BaseHttpSolrClient.RemoteSolrException) {
      rootThrowable = ((BaseHttpSolrClient.RemoteSolrException) e).getRootThrowable();
    }

    if (e != null && (rootThrowable == null || !okayExceptions.contains(rootThrowable))) {
      log.error("Error from shard: {}", shard, e);
      addFailure(results, nodeName, e.getClass().getName() + ":" + e.getMessage());
    } else {
      addSuccess(results, nodeName, solrResponse.getResponse());
    }
  }

  @SuppressWarnings("unchecked")
  private static void addFailure(NamedList<Object> results, String key, Object value) {
    SimpleOrderedMap<Object> failure = (SimpleOrderedMap<Object>) results.get("failure");
    if (failure == null) {
      failure = new SimpleOrderedMap<>();
      results.add("failure", failure);
    }
    failure.add(key, value);
  }

  @SuppressWarnings("unchecked")
  private static void addSuccess(NamedList<Object> results, String key, Object value) {
    SimpleOrderedMap<Object> success = (SimpleOrderedMap<Object>) results.get("success");
    if (success == null) {
      success = new SimpleOrderedMap<>();
      results.add("success", success);
    }
    success.add(key, value);
  }

  private static NamedList<Object> waitForCoreAdminAsyncCallToComplete(ShardHandlerFactory shardHandlerFactory, String adminPath, ZkStateReader zkStateReader, String nodeName, String requestId) {
    ShardHandler shardHandler = shardHandlerFactory.getShardHandler();
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.REQUESTSTATUS.toString());
    params.set(CoreAdminParams.REQUESTID, requestId);
    int counter = 0;
    ShardRequest sreq;
    do {
      sreq = new ShardRequest();
      params.set("qt", adminPath);
      sreq.purpose = 1;
      String replica = zkStateReader.getBaseUrlForNodeName(nodeName);
      sreq.shards = new String[] {replica};
      sreq.actualShards = sreq.shards;
      sreq.params = params;

      shardHandler.submit(sreq, replica, sreq.params);

      ShardResponse srsp;
      do {
        srsp = shardHandler.takeCompletedOrError();
        if (srsp != null) {
          NamedList<Object> results = new NamedList<>();
          processResponse(results, srsp, Collections.emptySet());
          if (srsp.getSolrResponse().getResponse() == null) {
            NamedList<Object> response = new NamedList<>();
            response.add("STATUS", "failed");
            return response;
          }

          String r = (String) srsp.getSolrResponse().getResponse().get("STATUS");
          if (r.equals("running")) {
            log.debug("The task is still RUNNING, continuing to wait.");
            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
            continue;

          } else if (r.equals("completed")) {
            log.debug("The task is COMPLETED, returning");
            return srsp.getSolrResponse().getResponse();
          } else if (r.equals("failed")) {
            // TODO: Improve this. Get more information.
            log.debug("The task is FAILED, returning");
            return srsp.getSolrResponse().getResponse();
          } else if (r.equals("notfound")) {
            log.debug("The task is notfound, retry");
            if (counter++ < 5) {
              try {
                Thread.sleep(1000);
              } catch (InterruptedException e) {
              }
              break;
            }
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Invalid status request for requestId: " + requestId + "" + srsp.getSolrResponse().getResponse().get("STATUS") +
                "retried " + counter + "times");
          } else {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Invalid status request " + srsp.getSolrResponse().getResponse().get("STATUS"));
          }
        }
      } while (srsp != null);
    } while(true);
  }

  public static ShardRequestTracker syncRequestTracker(CollectionCommandContext ccc) {
    return asyncRequestTracker(null, ccc);
  }

  public static ShardRequestTracker asyncRequestTracker(String asyncId, CollectionCommandContext ccc) {
    return new ShardRequestTracker(asyncId, ccc.getAdminPath(), ccc.getZkStateReader(), ccc.getShardHandler().getShardHandlerFactory());
  }

  public static class ShardRequestTracker {
    /*
     * backward compatibility reasons, add the response with the async ID as top level.
     * This can be removed in Solr 9
     */
    @Deprecated
    static boolean INCLUDE_TOP_LEVEL_RESPONSE = true;

    private final String asyncId;
    private final String adminPath;
    private final ZkStateReader zkStateReader;
    private final ShardHandlerFactory shardHandlerFactory;
    private final NamedList<String> shardAsyncIdByNode = new NamedList<String>();

    public ShardRequestTracker(String asyncId, String adminPath, ZkStateReader zkStateReader, ShardHandlerFactory shardHandlerFactory) {
      this.asyncId = asyncId;
      this.adminPath = adminPath;
      this.zkStateReader = zkStateReader;
      this.shardHandlerFactory = shardHandlerFactory;
    }

    /**
     * Send request to all replicas of a slice
     * @return List of replicas which is not live for receiving the request
     */
    public List<Replica> sliceCmd(ClusterState clusterState, ModifiableSolrParams params, Replica.State stateMatcher,
                                  Slice slice, ShardHandler shardHandler) {
      List<Replica> notLiveReplicas = new ArrayList<>();
      for (Replica replica : slice.getReplicas()) {
        if ((stateMatcher == null || Replica.State.getState(replica.getStr(ZkStateReader.STATE_PROP)) == stateMatcher)) {
          if (clusterState.liveNodesContain(replica.getStr(ZkStateReader.NODE_NAME_PROP))) {
            // For thread safety, only simple clone the ModifiableSolrParams
            ModifiableSolrParams cloneParams = new ModifiableSolrParams();
            cloneParams.add(params);
            cloneParams.set(CoreAdminParams.CORE, replica.getStr(ZkStateReader.CORE_NAME_PROP));

            sendShardRequest(replica.getStr(ZkStateReader.NODE_NAME_PROP), cloneParams, shardHandler);
          } else {
            notLiveReplicas.add(replica);
          }
        }
      }
      return notLiveReplicas;
    }

    public void sendShardRequest(String nodeName, ModifiableSolrParams params,
                                 ShardHandler shardHandler) {
      sendShardRequest(nodeName, params, shardHandler, adminPath, zkStateReader);
    }

    public void sendShardRequest(String nodeName, ModifiableSolrParams params, ShardHandler shardHandler,
                                 String adminPath, ZkStateReader zkStateReader) {
      if (asyncId != null) {
        String coreAdminAsyncId = asyncId + Math.abs(System.nanoTime());
        params.set(ASYNC, coreAdminAsyncId);
        track(nodeName, coreAdminAsyncId);
      }

      ShardRequest sreq = new ShardRequest();
      params.set("qt", adminPath);
      sreq.purpose = 1;
      String replica = zkStateReader.getBaseUrlForNodeName(nodeName);
      sreq.shards = new String[] {replica};
      sreq.actualShards = sreq.shards;
      sreq.nodeName = nodeName;
      sreq.params = params;

      shardHandler.submit(sreq, replica, sreq.params);
    }

    void processResponses(NamedList<Object> results, ShardHandler shardHandler, boolean abortOnError, String msgOnError) {
      processResponses(results, shardHandler, abortOnError, msgOnError, Collections.emptySet());
    }

    void processResponses(NamedList<Object> results, ShardHandler shardHandler, boolean abortOnError, String msgOnError,
                          Set<String> okayExceptions) {
      // Processes all shard responses
      ShardResponse srsp;
      do {
        srsp = shardHandler.takeCompletedOrError();
        if (srsp != null) {
          processResponse(results, srsp, okayExceptions);
          Throwable exception = srsp.getException();
          if (abortOnError && exception != null) {
            // drain pending requests
            while (srsp != null) {
              srsp = shardHandler.takeCompletedOrError();
            }
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, msgOnError, exception);
          }
        }
      } while (srsp != null);

      // If request is async wait for the core admin to complete before returning
      if (asyncId != null) {
        waitForAsyncCallsToComplete(results); // TODO: Shouldn't we abort with msgOnError exception when failure?
        shardAsyncIdByNode.clear();
      }
    }

    private void waitForAsyncCallsToComplete(NamedList<Object> results) {
      for (Map.Entry<String,String> nodeToAsync:shardAsyncIdByNode) {
        final String node = nodeToAsync.getKey();
        final String shardAsyncId = nodeToAsync.getValue();
        log.debug("I am Waiting for :{}/{}", node, shardAsyncId);
        NamedList<Object> reqResult = waitForCoreAdminAsyncCallToComplete(shardHandlerFactory, adminPath, zkStateReader, node, shardAsyncId);
        if (INCLUDE_TOP_LEVEL_RESPONSE) {
          results.add(shardAsyncId, reqResult);
        }
        if ("failed".equalsIgnoreCase(((String)reqResult.get("STATUS")))) {
          log.error("Error from shard {}: {}", node,  reqResult);
          addFailure(results, node, reqResult);
        } else {
          addSuccess(results, node, reqResult);
        }
      }
    }

    /** @deprecated consider to make it private after {@link CreateCollectionCmd} refactoring*/
    @Deprecated void track(String nodeName, String coreAdminAsyncId) {
      shardAsyncIdByNode.add(nodeName, coreAdminAsyncId);
    }
  }
}
