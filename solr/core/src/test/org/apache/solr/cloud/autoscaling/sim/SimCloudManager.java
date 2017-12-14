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

package org.apache.solr.cloud.autoscaling.sim;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.cloud.DistributedQueueFactory;
import org.apache.solr.client.solrj.cloud.autoscaling.DistribStateManager;
import org.apache.solr.client.solrj.cloud.autoscaling.NodeStateProvider;
import org.apache.solr.client.solrj.cloud.autoscaling.SolrCloudManager;
import org.apache.solr.client.solrj.impl.ClusterStateProvider;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.client.solrj.response.SolrResponseBase;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.autoscaling.AutoScalingHandler;
import org.apache.solr.cloud.autoscaling.OverseerTriggerThread;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.rule.ImplicitSnitch;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CommonAdminParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.ObjectCache;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.CloudConfig;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.OverseerCollectionMessageHandler.REQUESTID;

/**
 * Simulated {@link SolrCloudManager}.
 */
public class SimCloudManager implements SolrCloudManager {
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final SimDistribStateManager stateManager;
  private final SimClusterStateProvider clusterStateProvider;
  private final SimNodeStateProvider nodeStateProvider;
  private final AutoScalingHandler autoScalingHandler;
  private final LiveNodesSet liveNodesSet = new LiveNodesSet();
  private final DistributedQueueFactory queueFactory;
  private final ObjectCache objectCache = new ObjectCache();
  private TimeSource timeSource;

  private final List<SolrInputDocument> systemColl = Collections.synchronizedList(new ArrayList<>());
  private final ExecutorService simCloudManagerPool;
  private final Map<String, AtomicLong> opCounts = new ConcurrentHashMap<>();


  private Overseer.OverseerThread triggerThread;
  private ThreadGroup triggerThreadGroup;
  private SolrResourceLoader loader;

  private static int nodeIdPort = 10000;

  /**
   * Create a simulated cluster. This cluster uses the following components:
   * <ul>
   *   <li>{@link SimDistribStateManager} with non-shared root node.</li>
   *   <li>{@link SimClusterStateProvider}</li>
   *   <li>{@link SimNodeStateProvider}, where node values are automatically initialized when using
   *   {@link #simAddNode()} method.</li>
   *   <li>{@link GenericDistributedQueueFactory} that uses {@link SimDistribStateManager} as its storage.</li>
   *   <li>an instance of {@link AutoScalingHandler} for managing AutoScalingConfig.</li>
   *   <li>an instance of {@link OverseerTriggerThread} for managing triggers and processing events.</li>
   * </ul>
   * @param timeSource time source to use.
   */
  public SimCloudManager(TimeSource timeSource) throws Exception {
    this.stateManager = new SimDistribStateManager(SimDistribStateManager.createNewRootNode());
    this.loader = new SolrResourceLoader();
    // init common paths
    stateManager.makePath(ZkStateReader.CLUSTER_STATE);
    stateManager.makePath(ZkStateReader.CLUSTER_PROPS);
    stateManager.makePath(ZkStateReader.SOLR_AUTOSCALING_CONF_PATH);
    stateManager.makePath(ZkStateReader.LIVE_NODES_ZKNODE);
    stateManager.makePath(ZkStateReader.ROLES);
    stateManager.makePath(ZkStateReader.SOLR_AUTOSCALING_EVENTS_PATH);
    stateManager.makePath(ZkStateReader.SOLR_AUTOSCALING_TRIGGER_STATE_PATH);
    stateManager.makePath(ZkStateReader.SOLR_AUTOSCALING_NODE_LOST_PATH);
    stateManager.makePath(ZkStateReader.SOLR_AUTOSCALING_NODE_ADDED_PATH);

    this.timeSource = timeSource != null ? timeSource : TimeSource.NANO_TIME;
    this.clusterStateProvider = new SimClusterStateProvider(liveNodesSet, this);
    this.nodeStateProvider = new SimNodeStateProvider(liveNodesSet, this.stateManager, this.clusterStateProvider, null);
    this.queueFactory = new GenericDistributedQueueFactory(stateManager);
    this.simCloudManagerPool = ExecutorUtil.newMDCAwareFixedThreadPool(200, new DefaultSolrThreadFactory("simCloudManagerPool"));
    this.autoScalingHandler = new AutoScalingHandler(this, loader);
    triggerThreadGroup = new ThreadGroup("Simulated Overseer autoscaling triggers");
    OverseerTriggerThread trigger = new OverseerTriggerThread(loader, this,
        new CloudConfig.CloudConfigBuilder("nonexistent", 0, "sim").build());
    triggerThread = new Overseer.OverseerThread(triggerThreadGroup, trigger, "Simulated OverseerAutoScalingTriggerThread");
    triggerThread.start();
  }

  // ---------- simulator setup methods -----------

  /**
   * Create a cluster with the specified number of nodes. Node metrics are pre-populated.
   * @param numNodes number of nodes to create
   * @param timeSource time source
   * @return instance of simulated cluster
   */
  public static SimCloudManager createCluster(int numNodes, TimeSource timeSource) throws Exception {
    SimCloudManager cloudManager = new SimCloudManager(timeSource);
    for (int i = 1; i <= numNodes; i++) {
      Map<String, Object> values = createNodeValues(null);
//      if (i == 1) { // designated Overseer ?
        //values.put(ImplicitSnitch.NODEROLE, "overseer");
//      }
      String nodeId = (String)values.get(ImplicitSnitch.NODE);
      cloudManager.getSimClusterStateProvider().simAddNode(nodeId);
      cloudManager.getSimNodeStateProvider().simSetNodeValues(nodeId, values);
    }
    return cloudManager;
  }

  /**
   * Create a cluster initialized from the provided cluster state.
   * @param initialState existing cluster state
   * @param timeSource time source
   * @return instance of simulated cluster with the same layout as the provided cluster state.
   */
  public static SimCloudManager createCluster(ClusterState initialState, TimeSource timeSource) throws Exception {
    SimCloudManager cloudManager = new SimCloudManager(timeSource);
    cloudManager.getSimClusterStateProvider().simSetClusterState(initialState);
    for (String node : cloudManager.getClusterStateProvider().getLiveNodes()) {
      cloudManager.getSimNodeStateProvider().simSetNodeValues(node, createNodeValues(node));
    }
    return cloudManager;
  }

  /**
   * Create simulated node values (metrics) for a node.
   * @param nodeName node name (eg. '127.0.0.1:10000_solr'). If null then a new node name will be
   *                 created using sequentially increasing port number.
   * @return node values
   */
  public static Map<String, Object> createNodeValues(String nodeName) {
    Map<String, Object> values = new HashMap<>();
    String host, nodeId;
    int port;
    if (nodeName == null) {
      host = "127.0.0.1";
      port = nodeIdPort++;
      nodeId = host + ":" + port + "_solr";
      values.put("ip_1", "127");
      values.put("ip_2", "0");
      values.put("ip_3", "0");
      values.put("ip_4", "1");
    } else {
      String[] hostPortCtx = nodeName.split(":");
      if (hostPortCtx.length != 2) {
        throw new RuntimeException("Invalid nodeName " + nodeName);
      }
      host = hostPortCtx[0];
      String[] portCtx = hostPortCtx[1].split("_");
      if (portCtx.length != 2) {
        throw new RuntimeException("Invalid port_context in nodeName " + nodeName);
      }
      port = Integer.parseInt(portCtx[0]);
      nodeId = host + ":" + port + "_" + portCtx[1];
      String[] ip = host.split("\\.");
      if (ip.length == 4) {
        values.put("ip_1", ip[0]);
        values.put("ip_2", ip[1]);
        values.put("ip_3", ip[2]);
        values.put("ip_4", ip[3]);
      }
    }
    values.put(ImplicitSnitch.HOST, host);
    values.put(ImplicitSnitch.PORT, port);
    values.put(ImplicitSnitch.NODE, nodeId);
    values.put(ImplicitSnitch.CORES, 0);
    values.put(ImplicitSnitch.DISK, 1000);
    values.put(ImplicitSnitch.SYSLOADAVG, 1.0);
    values.put(ImplicitSnitch.HEAPUSAGE, 123450000);
    values.put("sysprop.java.version", System.getProperty("java.version"));
    values.put("sysprop.java.vendor", System.getProperty("java.vendor"));
    // fake some metrics expected in tests
    values.put("metrics:solr.node:ADMIN./admin/authorization.clientErrors:count", 0);
    values.put("metrics:solr.jvm:buffers.direct.Count", 0);
    return values;
  }

  /**
   * Get the instance of {@link SolrResourceLoader} that is used by the cluster components.
   */
  public SolrResourceLoader getLoader() {
    return loader;
  }

  /**
   * Add a new node and initialize its node values (metrics). The
   * /live_nodes list is updated with the new node id.
   * @return new node id
   */
  public String simAddNode() throws Exception {
    Map<String, Object> values = createNodeValues(null);
    String nodeId = (String)values.get(ImplicitSnitch.NODE);
    clusterStateProvider.simAddNode(nodeId);
    nodeStateProvider.simSetNodeValues(nodeId, values);
    LOG.trace("-- added node " + nodeId);
    return nodeId;
  }

  /**
   * Remove a node from the cluster. This simulates a node lost scenario.
   * Node id is removed from the /live_nodes list.
   * @param nodeId node id
   * @param withValues when true, remove also simulated node values. If false
   *                   then node values are retained to later simulate
   *                   a node that comes back up
   */
  public void simRemoveNode(String nodeId, boolean withValues) throws Exception {
    clusterStateProvider.simRemoveNode(nodeId);
    if (withValues) {
      nodeStateProvider.simRemoveNodeValues(nodeId);
    }
    LOG.trace("-- removed node " + nodeId);
  }

  /**
   * Remove a number of randomly selected nodes
   * @param number number of nodes to remove
   * @param withValues when true, remove also simulated node values. If false
   *                   then node values are retained to later simulate
   *                   a node that comes back up
   * @param random random
   */
  public void simRemoveRandomNodes(int number, boolean withValues, Random random) throws Exception {
    List<String> nodes = new ArrayList<>(liveNodesSet.get());
    Collections.shuffle(nodes, random);
    int count = Math.min(number, nodes.size());
    for (int i = 0; i < count; i++) {
      simRemoveNode(nodes.get(i), withValues);
    }
  }

  /**
   * Clear the (simulated) .system collection.
   */
  public void simClearSystemCollection() {
    systemColl.clear();
  }

  /**
   * Get the content of (simulated) .system collection.
   * @return documents in the collection, in chronological order starting from the oldest.
   */
  public List<SolrInputDocument> simGetSystemCollection() {
    return systemColl;
  }

  /**
   * Get a {@link SolrClient} implementation where calls are forwarded to this
   * instance of the cluster.
   * @return simulated SolrClient.
   */
  public SolrClient simGetSolrClient() {
    return new SolrClient() {
      @Override
      public NamedList<Object> request(SolrRequest request, String collection) throws SolrServerException, IOException {
        SolrResponse rsp = SimCloudManager.this.request(request);
        return rsp.getResponse();
      }

      @Override
      public void close() throws IOException {

      }
    };
  }

  /**
   * Simulate the effect of restarting Overseer leader - in this case this means restarting the
   * OverseerTriggerThread and optionally killing a node.
   * @param killNodeId optional nodeId to kill. If null then don't kill any node, just restart the thread
   */
  public void simRestartOverseer(String killNodeId) throws Exception {
    LOG.info("=== Restarting OverseerTriggerThread and clearing object cache...");
    triggerThread.interrupt();
    IOUtils.closeQuietly(triggerThread);
    if (killNodeId != null) {
      simRemoveNode(killNodeId, true);
    }
    objectCache.clear();
    OverseerTriggerThread trigger = new OverseerTriggerThread(loader, this,
        new CloudConfig.CloudConfigBuilder("nonexistent", 0, "sim").build());
    triggerThread = new Overseer.OverseerThread(triggerThreadGroup, trigger, "Simulated OverseerAutoScalingTriggerThread");
    triggerThread.start();

  }

  /**
   * Submit a task to execute in a thread pool.
   * @param callable task to execute
   * @return future to obtain results
   */
  public <T> Future<T> submit(Callable<T> callable) {
    return simCloudManagerPool.submit(callable);
  }

  // ---------- type-safe methods to obtain simulator components ----------
  public SimClusterStateProvider getSimClusterStateProvider() {
    return clusterStateProvider;
  }

  public SimNodeStateProvider getSimNodeStateProvider() {
    return nodeStateProvider;
  }

  public SimDistribStateManager getSimDistribStateManager() {
    return stateManager;
  }

  public LiveNodesSet getLiveNodesSet() {
    return liveNodesSet;
  }

  /**
   * Get the number and type of operations processed by this cluster.
   */
  public Map<String, AtomicLong> simGetOpCounts() {
    return opCounts;
  }

  /**
   * Get the number of processed operations of a specified type.
   * @param op operation name, eg. MOVEREPLICA
   * @return number of operations
   */
  public long simGetOpCount(String op) {
    AtomicLong count = opCounts.get(op);
    return count != null ? count.get() : 0L;
  }

  // --------- interface methods -----------


  @Override
  public ObjectCache getObjectCache() {
    return objectCache;
  }

  @Override
  public TimeSource getTimeSource() {
    return timeSource;
  }

  @Override
  public ClusterStateProvider getClusterStateProvider() {
    return clusterStateProvider;
  }

  @Override
  public NodeStateProvider getNodeStateProvider() {
    return nodeStateProvider;
  }

  @Override
  public DistribStateManager getDistribStateManager() {
    return stateManager;
  }

  @Override
  public DistributedQueueFactory getDistributedQueueFactory() {
    return queueFactory;
  }

  @Override
  public SolrResponse request(SolrRequest req) throws IOException {
    try {
      Future<SolrResponse> rsp = submit(() -> simHandleSolrRequest(req));
      return rsp.get();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  private void incrementCount(String op) {
    AtomicLong count = opCounts.computeIfAbsent(op, o -> new AtomicLong());
    count.incrementAndGet();
  }

  /**
   * Handler method for autoscaling requests. NOTE: only a specific subset of autoscaling requests is
   * supported!
   * @param req autoscaling request
   * @return results
   */
  public SolrResponse simHandleSolrRequest(SolrRequest req) throws IOException, InterruptedException {
    // pay the penalty for remote request, at least 5 ms
    timeSource.sleep(5);

    LOG.trace("--- got SolrRequest: " + req.getMethod() + " " + req.getPath() +
        (req.getParams() != null ? " " + req.getParams().toQueryString() : ""));
    if (req.getPath() != null && req.getPath().startsWith("/admin/autoscaling") ||
        req.getPath().startsWith("/cluster/autoscaling")) {
      incrementCount("autoscaling");
      ModifiableSolrParams params = new ModifiableSolrParams(req.getParams());
      params.set(CommonParams.PATH, req.getPath());
      LocalSolrQueryRequest queryRequest = new LocalSolrQueryRequest(null, params);
      RequestWriter.ContentWriter cw = req.getContentWriter("application/json");
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      cw.write(baos);
      String payload = baos.toString("UTF-8");
      LOG.trace("-- payload: {}", payload);
      queryRequest.setContentStreams(Collections.singletonList(new ContentStreamBase.StringStream(payload)));
      queryRequest.getContext().put("httpMethod", req.getMethod().toString());
      SolrQueryResponse queryResponse = new SolrQueryResponse();
      autoScalingHandler.handleRequest(queryRequest, queryResponse);
      if (queryResponse.getException() != null) {
        throw new IOException(queryResponse.getException());
      }
      SolrResponse rsp = new SolrResponseBase();
      rsp.setResponse(queryResponse.getValues());
      LOG.trace("-- response: {}", rsp);
      return rsp;
    }
    if (req instanceof UpdateRequest) {
      incrementCount("update");
      // support only updates to the system collection
      UpdateRequest ureq = (UpdateRequest)req;
      if (ureq.getCollection() == null || !ureq.getCollection().equals(CollectionAdminParams.SYSTEM_COLL)) {
        throw new UnsupportedOperationException("Only .system updates are supported but got: " + req);
      }
      List<SolrInputDocument> docs = ureq.getDocuments();
      if (docs != null) {
        systemColl.addAll(docs);
      }
      return new UpdateResponse();
    }
    // support only a specific subset of collection admin ops
    if (!(req instanceof CollectionAdminRequest)) {
      throw new UnsupportedOperationException("Only some CollectionAdminRequest-s are supported: " + req.getClass().getName());
    }
    SolrParams params = req.getParams();
    String a = params.get(CoreAdminParams.ACTION);
    SolrResponse rsp = new SolrResponseBase();
    rsp.setResponse(new NamedList<>());
    if (a != null) {
      CollectionParams.CollectionAction action = CollectionParams.CollectionAction.get(a);
      if (action == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unknown action: " + a);
      }
      LOG.debug("Invoking Collection Action :{} with params {}", action.toLower(), req.getParams().toQueryString());
      NamedList results = new NamedList();
      rsp.setResponse(results);
      incrementCount(action.name());
      switch (action) {
        case REQUESTSTATUS:
          // we complete all async ops immediately
          String requestId = req.getParams().get(REQUESTID);
          SimpleOrderedMap<String> status = new SimpleOrderedMap<>();
          status.add("state", RequestStatusState.COMPLETED.getKey());
          status.add("msg", "found [" + requestId + "] in completed tasks");
          results.add("status", status);
          results.add("success", "");
          // ExecutePlanAction expects a specific response class
          rsp = new CollectionAdminRequest.RequestStatusResponse();
          rsp.setResponse(results);
          break;
        case DELETESTATUS:
          requestId = req.getParams().get(REQUESTID);
          results.add("status", "successfully removed stored response for [" + requestId + "]");
          results.add("success", "");
          break;
        case CREATE:
          try {
            clusterStateProvider.simCreateCollection(new ZkNodeProps(req.getParams().toNamedList().asMap(10)), results);
          } catch (Exception e) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
          }
          break;
        case DELETE:
          clusterStateProvider.simDeleteCollection(req.getParams().get(CommonParams.NAME),
              req.getParams().get(CommonAdminParams.ASYNC), results);
          break;
        case LIST:
          results.add("collections", clusterStateProvider.simListCollections());
          break;
        case ADDREPLICA:
          try {
            clusterStateProvider.simAddReplica(new ZkNodeProps(req.getParams().toNamedList().asMap(10)), results);
          } catch (Exception e) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
          }
          break;
        case MOVEREPLICA:
          try {
            clusterStateProvider.simMoveReplica(new ZkNodeProps(req.getParams().toNamedList().asMap(10)), results);
          } catch (Exception e) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
          }
          break;
        case OVERSEERSTATUS:
          if (req.getParams().get(CommonAdminParams.ASYNC) != null) {
            results.add(REQUESTID, req.getParams().get(CommonAdminParams.ASYNC));
          }
          if (!liveNodesSet.get().isEmpty()) {
            results.add("leader", liveNodesSet.get().iterator().next());
          }
          results.add("overseer_queue_size", 0);
          results.add("overseer_work_queue_size", 0);
          results.add("overseer_collection_queue_size", 0);
          results.add("success", "");
          break;
        case ADDROLE:
          nodeStateProvider.simAddNodeValue(req.getParams().get("node"), "nodeRole", req.getParams().get("role"));
          break;
        case CREATESHARD:
          try {
            clusterStateProvider.simCreateShard(new ZkNodeProps(req.getParams().toNamedList().asMap(10)), results);
          } catch (Exception e) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
          }
          break;
        case SPLITSHARD:
          try {
            clusterStateProvider.simSplitShard(new ZkNodeProps(req.getParams().toNamedList().asMap(10)), results);
          } catch (Exception e) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
          }
          break;
        default:
          throw new UnsupportedOperationException("Unsupported collection admin action=" + action + " in request: " + req.getParams());
      }
    } else {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "action is a required param in request: " + req.getParams());
    }
    return rsp;

  }

  /**
   * HTTP requests are not supported by this implementation.
   */
  @Override
  public byte[] httpRequest(String url, SolrRequest.METHOD method, Map<String, String> headers, String payload, int timeout, boolean followRedirects) throws IOException {
    throw new UnsupportedOperationException("general HTTP requests are not supported yet");
  }

  @Override
  public void close() throws IOException {
    IOUtils.closeQuietly(clusterStateProvider);
    IOUtils.closeQuietly(nodeStateProvider);
    IOUtils.closeQuietly(stateManager);
    triggerThread.interrupt();
    IOUtils.closeQuietly(triggerThread);
    IOUtils.closeQuietly(objectCache);
    simCloudManagerPool.shutdownNow();
  }
}
