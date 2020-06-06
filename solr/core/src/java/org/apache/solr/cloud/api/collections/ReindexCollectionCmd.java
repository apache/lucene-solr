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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.cloud.DistribStateManager;
import org.apache.solr.client.solrj.cloud.autoscaling.Policy;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CommonAdminParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.TestInjection;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CollectionAdminParams.FOLLOW_ALIASES;

/**
 * Reindex a collection, usually in order to change the index schema.
 * <p>WARNING: Reindexing is potentially a lossy operation - some indexed data that is not available as
 * stored fields may be irretrievably lost, so users should use this command with caution, evaluating
 * the potential impact by using different source and target collection names first, and preserving
 * the source collection until the evaluation is complete.</p>
 * <p>Reindexing follows these steps:</p>
 * <ol>
 *    <li>creates a temporary collection using the most recent schema of the source collection
 *    (or the one specified in the parameters, which must already exist), and the shape of the original
 *    collection, unless overridden by parameters.</li>
 *    <li>copy the source documents to the temporary collection, using their stored fields and
 *    reindexing them using the specified schema. NOTE: some data
 *    loss may occur if the original stored field data is not available!</li>
 *    <li>create the target collection from scratch with the specified name (or the same as source if not
 *    specified) and the specified parameters. NOTE: if the target name was not specified or is the same
 *    as the source collection then a unique sequential collection name will be used.</li>
 *    <li>copy the documents from the source collection to the target collection.</li>
 *    <li>if the source and target collection name was the same then set up an alias pointing from the source collection name to the actual
 *    (sequentially named) target collection</li>
 *    <li>optionally delete the source collection.</li>
 * </ol>
 */
public class ReindexCollectionCmd implements OverseerCollectionMessageHandler.Cmd {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String COMMAND = "cmd";
  public static final String REINDEX_STATUS = "reindexStatus";
  public static final String REMOVE_SOURCE = "removeSource";
  public static final String TARGET = "target";
  public static final String TARGET_COL_PREFIX = ".rx_";
  public static final String CHK_COL_PREFIX = ".rx_ck_";
  public static final String REINDEXING_STATE = CollectionAdminRequest.PROPERTY_PREFIX + "rx";

  public static final String STATE = "state";
  public static final String PHASE = "phase";

  private static final List<String> COLLECTION_PARAMS = Arrays.asList(
      ZkStateReader.CONFIGNAME_PROP,
      ZkStateReader.NUM_SHARDS_PROP,
      ZkStateReader.NRT_REPLICAS,
      ZkStateReader.PULL_REPLICAS,
      ZkStateReader.TLOG_REPLICAS,
      ZkStateReader.REPLICATION_FACTOR,
      ZkStateReader.MAX_SHARDS_PER_NODE,
      "shards",
      Policy.POLICY,
      CollectionAdminParams.CREATE_NODE_SET_PARAM,
      CollectionAdminParams.CREATE_NODE_SET_SHUFFLE_PARAM,
      ZkStateReader.AUTO_ADD_REPLICAS
  );

  private final OverseerCollectionMessageHandler ocmh;

  private static AtomicInteger tmpCollectionSeq = new AtomicInteger();

  public enum State {
    IDLE,
    RUNNING,
    ABORTED,
    FINISHED;

    public String toLower() {
      return toString().toLowerCase(Locale.ROOT);
    }

    public static State get(Object p) {
      if (p == null) {
        return null;
      }
      p = String.valueOf(p).toLowerCase(Locale.ROOT);
      return states.get(p);
    }
    static Map<String, State> states = Collections.unmodifiableMap(
        Stream.of(State.values()).collect(Collectors.toMap(State::toLower, Function.identity())));
  }

  public enum Cmd {
    START,
    ABORT,
    STATUS;

    public String toLower() {
      return toString().toLowerCase(Locale.ROOT);
    }

    public static Cmd get(String p) {
      if (p == null) {
        return null;
      }
      p = p.toLowerCase(Locale.ROOT);
      return cmds.get(p);
    }
    static Map<String, Cmd> cmds = Collections.unmodifiableMap(
        Stream.of(Cmd.values()).collect(Collectors.toMap(Cmd::toLower, Function.identity())));
  }

  private String zkHost;

  public ReindexCollectionCmd(OverseerCollectionMessageHandler ocmh) {
    this.ocmh = ocmh;
  }

  @Override
  @SuppressWarnings({"unchecked"})
  public void call(ClusterState clusterState, ZkNodeProps message, @SuppressWarnings({"rawtypes"})NamedList results) throws Exception {

    log.debug("*** called: {}", message);

    String extCollection = message.getStr(CommonParams.NAME);

    if (extCollection == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Source collection name must be specified");
    }
    boolean followAliases = message.getBool(FOLLOW_ALIASES, false);
    String collection;
    if (followAliases) {
      collection = ocmh.cloudManager.getClusterStateProvider().resolveSimpleAlias(extCollection);
    } else {
      collection = extCollection;
    }
    if (!clusterState.hasCollection(collection)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Source collection name must exist");
    }
    String target = message.getStr(TARGET);
    if (target == null) {
      target = collection;
    } else {
      if (followAliases) {
        target = ocmh.cloudManager.getClusterStateProvider().resolveSimpleAlias(target);
      }
    }
    boolean sameTarget = target.equals(collection) || target.equals(extCollection);
    boolean removeSource = message.getBool(REMOVE_SOURCE, false);
    Cmd command = Cmd.get(message.getStr(COMMAND, Cmd.START.toLower()));
    if (command == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unknown command: " + message.getStr(COMMAND));
    }
    Map<String, Object> reindexingState = getReindexingState(ocmh.cloudManager.getDistribStateManager(), collection);
    if (!reindexingState.containsKey(STATE)) {
      reindexingState.put(STATE, State.IDLE.toLower());
    }
    State state = State.get(reindexingState.get(STATE));
    if (command == Cmd.ABORT) {
      log.info("Abort requested for collection {}, setting the state to ABORTED.", collection);
      // check that it's running
      if (state != State.RUNNING) {
        log.debug("Abort requested for collection {} but command is not running: {}", collection, state);
        return;
      }
      setReindexingState(collection, State.ABORTED, null);
      reindexingState.put(STATE, "aborting");
      results.add(REINDEX_STATUS, reindexingState);
      // if needed the cleanup will be performed by the running instance of the command
      return;
    } else if (command == Cmd.STATUS) {
      results.add(REINDEX_STATUS, reindexingState);
      return;
    }
    // command == Cmd.START

    // check it's not already running
    if (state == State.RUNNING) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Reindex is already running for collection " + collection +
          ". If you are sure this is not the case you can issue &cmd=abort to clean up this state.");
    }

    DocCollection coll = clusterState.getCollection(collection);
    boolean aborted = false;
    int batchSize = message.getInt(CommonParams.ROWS, 100);
    String query = message.getStr(CommonParams.Q, "*:*");
    String fl = message.getStr(CommonParams.FL, "*");
    Integer rf = message.getInt(ZkStateReader.REPLICATION_FACTOR, coll.getReplicationFactor());
    Integer numNrt = message.getInt(ZkStateReader.NRT_REPLICAS, coll.getNumNrtReplicas());
    Integer numTlog = message.getInt(ZkStateReader.TLOG_REPLICAS, coll.getNumTlogReplicas());
    Integer numPull = message.getInt(ZkStateReader.PULL_REPLICAS, coll.getNumPullReplicas());
    int numShards = message.getInt(ZkStateReader.NUM_SHARDS_PROP, coll.getActiveSlices().size());
    int maxShardsPerNode = message.getInt(ZkStateReader.MAX_SHARDS_PER_NODE, coll.getMaxShardsPerNode());
    DocRouter router = coll.getRouter();
    if (router == null) {
      router = DocRouter.DEFAULT;
    }

    String configName = message.getStr(ZkStateReader.CONFIGNAME_PROP, ocmh.zkStateReader.readConfigName(collection));
    String targetCollection;
    int seq = tmpCollectionSeq.getAndIncrement();
    if (sameTarget) {
      do {
        targetCollection = TARGET_COL_PREFIX + extCollection + "_" + seq;
        if (!clusterState.hasCollection(targetCollection)) {
          break;
        }
        seq = tmpCollectionSeq.getAndIncrement();
      } while (clusterState.hasCollection(targetCollection));
    } else {
      targetCollection = target;
    }
    String chkCollection = CHK_COL_PREFIX + extCollection;
    String daemonUrl = null;
    Exception exc = null;
    boolean createdTarget = false;
    try {
      zkHost = ocmh.zkStateReader.getZkClient().getZkServerAddress();
      // set the running flag
      reindexingState.clear();
      reindexingState.put("actualSourceCollection", collection);
      reindexingState.put("actualTargetCollection", targetCollection);
      reindexingState.put("checkpointCollection", chkCollection);
      reindexingState.put("inputDocs", getNumberOfDocs(collection));
      reindexingState.put(PHASE, "creating target and checkpoint collections");
      setReindexingState(collection, State.RUNNING, reindexingState);

      // 0. set up target and checkpoint collections
      NamedList<Object> cmdResults = new NamedList<>();
      ZkNodeProps cmd;
      if (clusterState.hasCollection(targetCollection)) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Target collection " + targetCollection + " already exists! Delete it first.");
      }
      if (clusterState.hasCollection(chkCollection)) {
        // delete the checkpoint collection
        cmd = new ZkNodeProps(
            Overseer.QUEUE_OPERATION, CollectionParams.CollectionAction.DELETE.toLower(),
            CommonParams.NAME, chkCollection,
            CoreAdminParams.DELETE_METRICS_HISTORY, "true"
        );
        ocmh.commandMap.get(CollectionParams.CollectionAction.DELETE).call(clusterState, cmd, cmdResults);
        ocmh.checkResults("deleting old checkpoint collection " + chkCollection, cmdResults, true);
      }

      if (maybeAbort(collection)) {
        aborted = true;
        return;
      }

      Map<String, Object> propMap = new HashMap<>();
      propMap.put(Overseer.QUEUE_OPERATION, CollectionParams.CollectionAction.CREATE.toLower());
      propMap.put(CommonParams.NAME, targetCollection);
      propMap.put(ZkStateReader.NUM_SHARDS_PROP, numShards);
      propMap.put(CollectionAdminParams.COLL_CONF, configName);
      // init first from the same router
      propMap.put("router.name", router.getName());
      for (String key : coll.keySet()) {
        if (key.startsWith("router.")) {
          propMap.put(key, coll.get(key));
        }
      }
      // then apply overrides if present
      for (String key : message.keySet()) {
        if (key.startsWith("router.")) {
          propMap.put(key, message.getStr(key));
        } else if (COLLECTION_PARAMS.contains(key)) {
          propMap.put(key, message.get(key));
        }
      }

      propMap.put(ZkStateReader.MAX_SHARDS_PER_NODE, maxShardsPerNode);
      propMap.put(CommonAdminParams.WAIT_FOR_FINAL_STATE, true);
      propMap.put(DocCollection.STATE_FORMAT, message.getInt(DocCollection.STATE_FORMAT, coll.getStateFormat()));
      if (rf != null) {
        propMap.put(ZkStateReader.REPLICATION_FACTOR, rf);
      }
      if (numNrt != null) {
        propMap.put(ZkStateReader.NRT_REPLICAS, numNrt);
      }
      if (numTlog != null) {
        propMap.put(ZkStateReader.TLOG_REPLICAS, numTlog);
      }
      if (numPull != null) {
        propMap.put(ZkStateReader.PULL_REPLICAS, numPull);
      }
      // create the target collection
      cmd = new ZkNodeProps(propMap);
      cmdResults = new NamedList<>();
      ocmh.commandMap.get(CollectionParams.CollectionAction.CREATE).call(clusterState, cmd, cmdResults);
      createdTarget = true;
      ocmh.checkResults("creating target collection " + targetCollection, cmdResults, true);

      // create the checkpoint collection - use RF=1 and 1 shard
      cmd = new ZkNodeProps(
          Overseer.QUEUE_OPERATION, CollectionParams.CollectionAction.CREATE.toLower(),
          CommonParams.NAME, chkCollection,
          ZkStateReader.NUM_SHARDS_PROP, "1",
          ZkStateReader.REPLICATION_FACTOR, "1",
          DocCollection.STATE_FORMAT, "2",
          CollectionAdminParams.COLL_CONF, "_default",
          CommonAdminParams.WAIT_FOR_FINAL_STATE, "true"
      );
      cmdResults = new NamedList<>();
      ocmh.commandMap.get(CollectionParams.CollectionAction.CREATE).call(clusterState, cmd, cmdResults);
      ocmh.checkResults("creating checkpoint collection " + chkCollection, cmdResults, true);
      // wait for a while until we see both collections
      TimeOut waitUntil = new TimeOut(30, TimeUnit.SECONDS, ocmh.timeSource);
      boolean created = false;
      while (!waitUntil.hasTimedOut()) {
        waitUntil.sleep(100);
        // this also refreshes our local var clusterState
        clusterState = ocmh.cloudManager.getClusterStateProvider().getClusterState();
        created = clusterState.hasCollection(targetCollection) && clusterState.hasCollection(chkCollection);
        if (created) break;
      }
      if (!created) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Could not fully create temporary collection(s)");
      }
      if (maybeAbort(collection)) {
        aborted = true;
        return;
      }

      // 1. put the source collection in read-only mode
      cmd = new ZkNodeProps(
          Overseer.QUEUE_OPERATION, CollectionParams.CollectionAction.MODIFYCOLLECTION.toLower(),
          ZkStateReader.COLLECTION_PROP, collection,
          ZkStateReader.READ_ONLY, "true");
      ocmh.overseer.offerStateUpdate(Utils.toJSON(cmd));

      TestInjection.injectReindexLatch();

      if (maybeAbort(collection)) {
        aborted = true;
        return;
      }

      // 2. copy the documents to target
      // Recipe taken from: http://joelsolr.blogspot.com/2016/10/solr-63-batch-jobs-parallel-etl-and.html
      ModifiableSolrParams q = new ModifiableSolrParams();
      q.set(CommonParams.QT, "/stream");
      q.set("collection", collection);
      q.set("expr",
          "daemon(id=\"" + targetCollection + "\"," +
              "terminate=\"true\"," +
              "commit(" + targetCollection + "," +
                "update(" + targetCollection + "," +
                  "batchSize=" + batchSize + "," +
                  "topic(" + chkCollection + "," +
                    collection + "," +
                    "q=\"" + query + "\"," +
                    "fl=\"" + fl + "\"," +
                    "id=\"topic_" + targetCollection + "\"," +
                    // some of the documents eg. in .system contain large blobs
                    "rows=\"" + batchSize + "\"," +
                    "initialCheckpoint=\"0\"))))");
      log.debug("- starting copying documents from {} to {}", collection, targetCollection);
      SolrResponse rsp = null;
      try {
        rsp = ocmh.cloudManager.request(new QueryRequest(q));
      } catch (Exception e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unable to copy documents from " +
            collection + " to " + targetCollection, e);
      }
      daemonUrl = getDaemonUrl(rsp, coll);
      if (daemonUrl == null) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unable to copy documents from " +
            collection + " to " + targetCollection + ": " + Utils.toJSONString(rsp));
      }
      reindexingState.put("daemonUrl", daemonUrl);
      reindexingState.put("daemonName", targetCollection);
      reindexingState.put(PHASE, "copying documents");
      setReindexingState(collection, State.RUNNING, reindexingState);

      // wait for the daemon to finish
      waitForDaemon(targetCollection, daemonUrl, collection, targetCollection, reindexingState);
      if (maybeAbort(collection)) {
        aborted = true;
        return;
      }
      log.debug("- finished copying from {} to {}", collection, targetCollection);
      // fail here or earlier during daemon run
      TestInjection.injectReindexFailure();

      // 5. if (sameTarget) set up an alias to use targetCollection as the source name
      if (sameTarget) {
        log.debug("- setting up alias from {} to {}", extCollection, targetCollection);
        cmd = new ZkNodeProps(
            CommonParams.NAME, extCollection,
            "collections", targetCollection);
        cmdResults = new NamedList<>();
        ocmh.commandMap.get(CollectionParams.CollectionAction.CREATEALIAS).call(clusterState, cmd, cmdResults);
        ocmh.checkResults("setting up alias " + extCollection + " -> " + targetCollection, cmdResults, true);
        reindexingState.put("alias", extCollection + " -> " + targetCollection);
      }

      reindexingState.remove("daemonUrl");
      reindexingState.remove("daemonName");
      reindexingState.put("processedDocs", getNumberOfDocs(targetCollection));
      reindexingState.put(PHASE, "copying done, finalizing");
      setReindexingState(collection, State.RUNNING, reindexingState);

      if (maybeAbort(collection)) {
        aborted = true;
        return;
      }
      // 6. delete the checkpoint collection
      log.debug("- deleting {}", chkCollection);
      cmd = new ZkNodeProps(
          Overseer.QUEUE_OPERATION, CollectionParams.CollectionAction.DELETE.toLower(),
          CommonParams.NAME, chkCollection,
          CoreAdminParams.DELETE_METRICS_HISTORY, "true"
      );
      cmdResults = new NamedList<>();
      ocmh.commandMap.get(CollectionParams.CollectionAction.DELETE).call(clusterState, cmd, cmdResults);
      ocmh.checkResults("deleting checkpoint collection " + chkCollection, cmdResults, true);

      // 7. optionally delete the source collection
      if (removeSource) {
        log.debug("- deleting source collection");
        cmd = new ZkNodeProps(
            Overseer.QUEUE_OPERATION, CollectionParams.CollectionAction.DELETE.toLower(),
            CommonParams.NAME, collection,
            FOLLOW_ALIASES, "false",
            CoreAdminParams.DELETE_METRICS_HISTORY, "true"
        );
        cmdResults = new NamedList<>();
        ocmh.commandMap.get(CollectionParams.CollectionAction.DELETE).call(clusterState, cmd, cmdResults);
        ocmh.checkResults("deleting source collection " + collection, cmdResults, true);
      } else {
        // 8. clear readOnly on source
        ZkNodeProps props = new ZkNodeProps(
            Overseer.QUEUE_OPERATION, CollectionParams.CollectionAction.MODIFYCOLLECTION.toLower(),
            ZkStateReader.COLLECTION_PROP, collection,
            ZkStateReader.READ_ONLY, null);
        ocmh.overseer.offerStateUpdate(Utils.toJSON(props));
      }
      // 9. set FINISHED state on the target and clear the state on the source
      ZkNodeProps props = new ZkNodeProps(
          Overseer.QUEUE_OPERATION, CollectionParams.CollectionAction.MODIFYCOLLECTION.toLower(),
          ZkStateReader.COLLECTION_PROP, targetCollection,
          REINDEXING_STATE, State.FINISHED.toLower());
      ocmh.overseer.offerStateUpdate(Utils.toJSON(props));

      reindexingState.put(STATE, State.FINISHED.toLower());
      reindexingState.put(PHASE, "done");
      removeReindexingState(collection);
    } catch (Exception e) {
      log.warn("Error during reindexing of {}", extCollection, e);
      exc = e;
      aborted = true;
    } finally {
      if (aborted) {
        cleanup(collection, targetCollection, chkCollection, daemonUrl, targetCollection, createdTarget);
        if (exc != null) {
          results.add("error", exc.toString());
        }
        reindexingState.put(STATE, State.ABORTED.toLower());
      }
      results.add(REINDEX_STATUS, reindexingState);
    }
  }

  private static final String REINDEXING_STATE_PATH = "/.reindexing";

  private Map<String, Object> setReindexingState(String collection, State state, Map<String, Object> props) throws Exception {
    String path = ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection + REINDEXING_STATE_PATH;
    DistribStateManager stateManager = ocmh.cloudManager.getDistribStateManager();
    if (props == null) { // retrieve existing props, if any
      props = Utils.getJson(stateManager, path);
    }
    Map<String, Object> copyProps = new HashMap<>(props);
    copyProps.put("state", state.toLower());
    if (stateManager.hasData(path)) {
      stateManager.setData(path, Utils.toJSON(copyProps), -1);
    } else {
      stateManager.makePath(path, Utils.toJSON(copyProps), CreateMode.PERSISTENT, false);
    }
    return copyProps;
  }

  private void removeReindexingState(String collection) throws Exception {
    String path = ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection + REINDEXING_STATE_PATH;
    DistribStateManager stateManager = ocmh.cloudManager.getDistribStateManager();
    if (stateManager.hasData(path)) {
      stateManager.removeData(path, -1);
    }
  }

  @VisibleForTesting
  public static Map<String, Object> getReindexingState(DistribStateManager stateManager, String collection) throws Exception {
    String path = ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection + REINDEXING_STATE_PATH;
    // make it modifiable
    return new TreeMap<>(Utils.getJson(stateManager, path));
  }

  private long getNumberOfDocs(String collection) {
    CloudSolrClient solrClient = ocmh.overseer.getCoreContainer().getSolrClientCache().getCloudSolrClient(zkHost);
    try {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.add(CommonParams.Q, "*:*");
      params.add(CommonParams.ROWS, "0");
      QueryResponse rsp = solrClient.query(collection, params);
      return rsp.getResults().getNumFound();
    } catch (Exception e) {
      return 0L;
    }
  }

  private boolean maybeAbort(String collection) throws Exception {
    DocCollection coll = ocmh.cloudManager.getClusterStateProvider().getClusterState().getCollectionOrNull(collection);
    if (coll == null) {
      // collection no longer present - abort
      log.info("## Aborting - collection {} no longer present.", collection);
      return true;
    }
    Map<String, Object> reindexingState = getReindexingState(ocmh.cloudManager.getDistribStateManager(), collection);
    State state = State.get(reindexingState.getOrDefault(STATE, State.RUNNING.toLower()));
    if (state != State.ABORTED) {
      return false;
    }
    log.info("## Aborting - collection {} state is {}", collection, state);
    return true;
  }

  // XXX see #waitForDaemon() for why we need this
  private String getDaemonUrl(SolrResponse rsp, DocCollection coll) {
    @SuppressWarnings({"unchecked"})
    Map<String, Object> rs = (Map<String, Object>) rsp.getResponse().get("result-set");
    if (rs == null || rs.isEmpty()) {
      if (log.isDebugEnabled()) {
        log.debug(" -- Missing daemon information in response: {}", Utils.toJSONString(rsp));
      }
    }
    @SuppressWarnings({"unchecked"})
    List<Object> list = (List<Object>) rs.get("docs");
    if (list == null) {
      if (log.isDebugEnabled()) {
        log.debug(" -- Missing daemon information in response: {}", Utils.toJSONString(rsp));
      }
      return null;
    }
    String replicaName = null;
    for (Object o : list) {
      @SuppressWarnings({"unchecked"})
      Map<String, Object> map = (Map<String, Object>) o;
      String op = (String) map.get("DaemonOp");
      if (op == null) {
        continue;
      }
      String[] parts = op.split("\\s+");
      if (parts.length != 4) {
        log.debug(" -- Invalid daemon location info, expected 4 tokens: {}", op);
        return null;
      }
      // check if it's plausible
      if (parts[3].contains("shard") && parts[3].contains("replica")) {
        replicaName = parts[3];
        break;
      } else {
        log.debug(" -- daemon location info likely invalid: {}", op);
        return null;
      }
    }
    if (replicaName == null) {
      return null;
    }
    // build a baseUrl of the replica
    for (Replica r : coll.getReplicas()) {
      if (replicaName.equals(r.getCoreName())) {
        return r.getBaseUrl() + "/" + r.getCoreName();
      }
    }
    return null;
  }

  // XXX currently this is complicated to due a bug in the way the daemon 'list'
  // XXX operation is implemented - see SOLR-13245. We need to query the actual
  // XXX SolrCore where the daemon is running
  @SuppressWarnings({"unchecked"})
  private void waitForDaemon(String daemonName, String daemonUrl, String sourceCollection, String targetCollection, Map<String, Object> reindexingState) throws Exception {
    HttpClient client = ocmh.overseer.getCoreContainer().getUpdateShardHandler().getDefaultHttpClient();
    try (HttpSolrClient solrClient = new HttpSolrClient.Builder()
        .withHttpClient(client)
        .withBaseSolrUrl(daemonUrl).build()) {
      ModifiableSolrParams q = new ModifiableSolrParams();
      q.set(CommonParams.QT, "/stream");
      q.set("action", "list");
      q.set(CommonParams.DISTRIB, false);
      QueryRequest req = new QueryRequest(q);
      boolean isRunning;
      int statusCheck = 0;
      do {
        isRunning = false;
        statusCheck++;
        try {
          NamedList<Object> rsp = solrClient.request(req);
          Map<String, Object> rs = (Map<String, Object>)rsp.get("result-set");
          if (rs == null || rs.isEmpty()) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Can't find daemon list: missing result-set: " + Utils.toJSONString(rsp));
          }
          List<Object> list = (List<Object>)rs.get("docs");
          if (list == null) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Can't find daemon list: missing result-set: " + Utils.toJSONString(rsp));
          }
          if (list.isEmpty()) { // finished?
            break;
          }
          for (Object o : list) {
            Map<String, Object> map = (Map<String, Object>)o;
            String id = (String)map.get("id");
            if (daemonName.equals(id)) {
              isRunning = true;
              // fail here
              TestInjection.injectReindexFailure();
              break;
            }
          }
        } catch (Exception e) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Exception waiting for daemon " +
              daemonName + " at " + daemonUrl, e);
        }
        if (statusCheck % 5 == 0) {
          reindexingState.put("processedDocs", getNumberOfDocs(targetCollection));
          setReindexingState(sourceCollection, State.RUNNING, reindexingState);
        }
        ocmh.cloudManager.getTimeSource().sleep(2000);
      } while (isRunning && !maybeAbort(sourceCollection));
    }
  }

  @SuppressWarnings({"unchecked"})
  private void killDaemon(String daemonName, String daemonUrl) throws Exception {
    log.debug("-- killing daemon {} at {}", daemonName, daemonUrl);
    HttpClient client = ocmh.overseer.getCoreContainer().getUpdateShardHandler().getDefaultHttpClient();
    try (HttpSolrClient solrClient = new HttpSolrClient.Builder()
        .withHttpClient(client)
        .withBaseSolrUrl(daemonUrl).build()) {
      ModifiableSolrParams q = new ModifiableSolrParams();
      q.set(CommonParams.QT, "/stream");
      // we should really use 'kill' here, but then we will never
      // know when the daemon actually finishes running - 'kill' only
      // sets a flag that may be noticed much later
      q.set("action", "stop");
      q.set(CommonParams.ID, daemonName);
      q.set(CommonParams.DISTRIB, false);
      QueryRequest req = new QueryRequest(q);
      NamedList<Object> rsp = solrClient.request(req);
      // /result-set/docs/[0]/DaemonOp : Deamon:id killed on coreName
      if (log.isDebugEnabled()) {
        log.debug(" -- stop daemon response: {}", Utils.toJSONString(rsp));
      }
      Map<String, Object> rs = (Map<String, Object>) rsp.get("result-set");
      if (rs == null || rs.isEmpty()) {
        log.warn("Problem killing daemon {}: missing result-set: {}", daemonName, Utils.toJSONString(rsp));
        return;
      }
      List<Object> list = (List<Object>) rs.get("docs");
      if (list == null) {
        log.warn("Problem killing daemon {}: missing result-set: {}", daemonName, Utils.toJSONString(rsp));
        return;
      }
      if (list.isEmpty()) { // already finished?
        return;
      }
      for (Object o : list) {
        Map<String, Object> map = (Map<String, Object>) o;
        String op = (String) map.get("DaemonOp");
        if (op == null) {
          continue;
        }
        if (op.contains(daemonName) && op.contains("stopped")) {
          // now wait for the daemon to really stop
          q.set("action", "list");
          req = new QueryRequest(q);
          TimeOut timeOut = new TimeOut(60, TimeUnit.SECONDS, ocmh.timeSource);
          while (!timeOut.hasTimedOut()) {
            rsp = solrClient.request(req);
            rs = (Map<String, Object>) rsp.get("result-set");
            if (rs == null || rs.isEmpty()) {
              log.warn("Problem killing daemon {}: missing result-set: {}", daemonName, Utils.toJSONString(rsp));
              break;
            }
            List<Object> list2 = (List<Object>) rs.get("docs");
            if (list2 == null) {
              log.warn("Problem killing daemon {}: missing result-set: {}", daemonName, Utils.toJSONString(rsp));
              break;
            }
            if (list2.isEmpty()) { // already finished?
              break;
            }
            Map<String, Object> status2 = null;
            for (Object o2 : list2) {
              Map<String, Object> map2 = (Map<String, Object>)o2;
              if (daemonName.equals(map2.get("id"))) {
                status2 = map2;
                break;
              }
            }
            if (status2 == null) { // finished?
              break;
            }
            Number stopTime = (Number)status2.get("stopTime");
            if (stopTime.longValue() > 0) {
              break;
            }
          }
          if (timeOut.hasTimedOut()) {
            log.warn("Problem killing daemon {}: timed out waiting for daemon to stop.", daemonName);
            // proceed anyway
          }
        }
      }
      // now kill it - it's already stopped, this simply removes its status
      q.set("action", "kill");
      req = new QueryRequest(q);
      solrClient.request(req);
    }
  }

  private void cleanup(String collection, String targetCollection, String chkCollection,
                       String daemonUrl, String daemonName, boolean createdTarget) throws Exception {
    log.info("## Cleaning up after abort or error");
    // 1. kill the daemon
    // 2. cleanup target / chk collections IFF the source collection still exists and is not empty
    // 3. cleanup collection state

    if (daemonUrl != null) {
      killDaemon(daemonName, daemonUrl);
    }
    ClusterState clusterState = ocmh.cloudManager.getClusterStateProvider().getClusterState();
    NamedList<Object> cmdResults = new NamedList<>();
    if (createdTarget && !collection.equals(targetCollection) && clusterState.hasCollection(targetCollection)) {
      log.debug(" -- removing {}", targetCollection);
      ZkNodeProps cmd = new ZkNodeProps(
          Overseer.QUEUE_OPERATION, CollectionParams.CollectionAction.DELETE.toLower(),
          CommonParams.NAME, targetCollection,
          FOLLOW_ALIASES, "false",
          CoreAdminParams.DELETE_METRICS_HISTORY, "true"
      );
      ocmh.commandMap.get(CollectionParams.CollectionAction.DELETE).call(clusterState, cmd, cmdResults);
      ocmh.checkResults("CLEANUP: deleting target collection " + targetCollection, cmdResults, false);

    }
    // remove chk collection
    if (clusterState.hasCollection(chkCollection)) {
      log.debug(" -- removing {}", chkCollection);
      ZkNodeProps cmd = new ZkNodeProps(
          Overseer.QUEUE_OPERATION, CollectionParams.CollectionAction.DELETE.toLower(),
          CommonParams.NAME, chkCollection,
          FOLLOW_ALIASES, "false",
          CoreAdminParams.DELETE_METRICS_HISTORY, "true"
      );
      cmdResults = new NamedList<>();
      ocmh.commandMap.get(CollectionParams.CollectionAction.DELETE).call(clusterState, cmd, cmdResults);
      ocmh.checkResults("CLEANUP: deleting checkpoint collection " + chkCollection, cmdResults, false);
    }
    log.debug(" -- turning readOnly mode off for {}", collection);
    ZkNodeProps props = new ZkNodeProps(
        Overseer.QUEUE_OPERATION, CollectionParams.CollectionAction.MODIFYCOLLECTION.toLower(),
        ZkStateReader.COLLECTION_PROP, collection,
        ZkStateReader.READ_ONLY, null);
    ocmh.overseer.offerStateUpdate(Utils.toJSON(props));
    removeReindexingState(collection);
  }
}
