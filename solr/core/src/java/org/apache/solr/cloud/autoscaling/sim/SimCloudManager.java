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
import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.codahale.metrics.jvm.ClassLoadingGaugeSet;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.cloud.DistribStateManager;
import org.apache.solr.client.solrj.cloud.DistributedQueueFactory;
import org.apache.solr.client.solrj.cloud.NodeStateProvider;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.client.solrj.cloud.autoscaling.ReplicaInfo;
import org.apache.solr.client.solrj.cloud.autoscaling.Variable;
import org.apache.solr.client.solrj.impl.ClusterStateProvider;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.client.solrj.response.SolrResponseBase;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.autoscaling.AutoScalingHandler;
import org.apache.solr.cloud.autoscaling.OverseerTriggerThread;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
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
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.handler.admin.MetricsHandler;
import org.apache.solr.handler.admin.MetricsHistoryHandler;
import org.apache.solr.metrics.AltBufferPoolMetricSet;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.OperatingSystemMetricSet;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.apache.solr.util.MockSearchableSolrClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.api.collections.OverseerCollectionMessageHandler.REQUESTID;

/**
 * Simulated {@link SolrCloudManager}.
 */
public class SimCloudManager implements SolrCloudManager {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final Random random;

  static {
    String seed = System.getProperty("tests.seed");
    if (seed == null) {
      random = new Random();
    } else {
      random = new Random(seed.hashCode());
    }
  }

  private final SimDistribStateManager stateManager;
  private final SimClusterStateProvider clusterStateProvider;
  private final SimNodeStateProvider nodeStateProvider;
  private final AutoScalingHandler autoScalingHandler;
  private final LiveNodesSet liveNodesSet = new LiveNodesSet();
  private final DistributedQueueFactory queueFactory;
  private final ObjectCache objectCache = new ObjectCache();
  private final SolrMetricManager metricManager = new SolrMetricManager();
  private final String metricTag;

  private final List<SolrInputDocument> systemColl = Collections.synchronizedList(new ArrayList<>());
  private final Map<String, Map<String, AtomicInteger>> eventCounts = new ConcurrentHashMap<>();
  private final MockSearchableSolrClient solrClient;
  private final Map<String, AtomicLong> opCounts = new ConcurrentSkipListMap<>();
  /**
   * @see #submit
   * @see #getBackgroundTaskFailureCount
   * @see LoggingCallable
   */
  private final AtomicLong backgroundTaskFailureCounter = new AtomicLong(0);

  private ExecutorService simCloudManagerPool;
  private Overseer.OverseerThread triggerThread;
  private ThreadGroup triggerThreadGroup;
  private SolrResourceLoader loader;
  private MetricsHandler metricsHandler;
  private MetricsHistoryHandler metricsHistoryHandler;
  private TimeSource timeSource;
  private boolean useSystemCollection = true;

  private static int nodeIdPort = 10000;
  public static int DEFAULT_FREE_DISK = 10240; // 10 TiB
  public static int DEFAULT_TOTAL_DISK = 10240; // 10 TiB
  public static long DEFAULT_IDX_SIZE_BYTES = 10240; // 10 kiB

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
    this(timeSource, null);
  }

  SimCloudManager(TimeSource timeSource, SimDistribStateManager distribStateManager) throws Exception {
    this.loader = new SolrResourceLoader();
    if (distribStateManager == null) {
      this.stateManager =  new SimDistribStateManager(SimDistribStateManager.createNewRootNode());
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
      stateManager.makePath(Overseer.OVERSEER_ELECT);
    } else {
      this.stateManager = distribStateManager;
    }

    // register common metrics
    metricTag = Integer.toHexString(hashCode());
    String registryName = SolrMetricManager.getRegistryName(SolrInfoBean.Group.jvm);
    metricManager.registerAll(registryName, new AltBufferPoolMetricSet(), true, "buffers");
    metricManager.registerAll(registryName, new ClassLoadingGaugeSet(), true, "classes");
    metricManager.registerAll(registryName, new OperatingSystemMetricSet(), true, "os");
    metricManager.registerAll(registryName, new GarbageCollectorMetricSet(), true, "gc");
    metricManager.registerAll(registryName, new MemoryUsageGaugeSet(), true, "memory");
    metricManager.registerAll(registryName, new ThreadStatesGaugeSet(), true, "threads"); // todo should we use CachedThreadStatesGaugeSet instead?
    MetricsMap sysprops = new MetricsMap((detailed, map) -> {
      System.getProperties().forEach((k, v) -> {
        map.put(String.valueOf(k), v);
      });
    });
    metricManager.registerGauge(null, registryName, sysprops, metricTag, true, "properties", "system");

    registryName = SolrMetricManager.getRegistryName(SolrInfoBean.Group.node);
    metricManager.registerGauge(null, registryName, () -> new File("/").getUsableSpace(),
        metricTag, true, "usableSpace", SolrInfoBean.Category.CONTAINER.toString(), "fs", "coreRoot");

    solrClient = new MockSearchableSolrClient() {
      @Override
      public NamedList<Object> request(SolrRequest request, String collection) throws SolrServerException, IOException {
        if (collection != null) {
          if (request instanceof AbstractUpdateRequest) {
            ((AbstractUpdateRequest)request).setParam("collection", collection);
          } else if (request instanceof QueryRequest) {
            if (request.getPath() != null && (
                request.getPath().startsWith("/admin/autoscaling") ||
                request.getPath().startsWith("/cluster/autoscaling") ||
            request.getPath().startsWith("/admin/metrics/history") ||
                request.getPath().startsWith("/cluster/metrics/history")
            )) {
              // forward it
              ModifiableSolrParams params = new ModifiableSolrParams(request.getParams());
              params.set("collection", collection);
              request = new QueryRequest(params);
            } else {
              // search request
              if (collection.equals(CollectionAdminParams.SYSTEM_COLL)) {
                return super.request(request, collection);
              } else {
                // forward it
                ModifiableSolrParams params = new ModifiableSolrParams(request.getParams());
                params.set("collection", collection);
                request = new QueryRequest(params);
              }
            }
          } else {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "when collection != null only UpdateRequest and QueryRequest are supported: request=" + request + ", collection=" + collection);
          }
        }
        try {
          SolrResponse rsp = SimCloudManager.this.request(request);
          return rsp.getResponse();
        } catch (UnsupportedOperationException e) {
          throw new SolrServerException(e);
        }
      }
    };


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
      cloudManager.simAddNode();
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

  public static SimCloudManager createCluster(SolrCloudManager other, AutoScalingConfig config, TimeSource timeSource) throws Exception {
    SimDistribStateManager distribStateManager = new SimDistribStateManager(SimDistribStateManager.createNewRootNode());
    distribStateManager.copyFrom(other.getDistribStateManager(), false);
    SimCloudManager cloudManager = new SimCloudManager(timeSource, distribStateManager);
    if (config != null) {
      cloudManager.getSimDistribStateManager().simSetAutoScalingConfig(config);
    } else {
      config = cloudManager.getDistribStateManager().getAutoScalingConfig();
    }
    Set<String> nodeTags = new HashSet<>(SimUtils.COMMON_NODE_TAGS);
    nodeTags.addAll(config.getPolicy().getParams());
    Set<String> replicaTags = new HashSet<>(SimUtils.COMMON_REPLICA_TAGS);
    replicaTags.addAll(config.getPolicy().getPerReplicaAttributes());
    cloudManager.getSimClusterStateProvider().copyFrom(other.getClusterStateProvider());
    for (String node : other.getClusterStateProvider().getLiveNodes()) {
      SimClusterStateProvider simClusterStateProvider = cloudManager.getSimClusterStateProvider();
      cloudManager.getSimNodeStateProvider().simSetNodeValues(node, other.getNodeStateProvider().getNodeValues(node, nodeTags));
      Map<String, Map<String, List<ReplicaInfo>>> infos = other.getNodeStateProvider().getReplicaInfo(node, replicaTags);
      simClusterStateProvider.simSetReplicaValues(node, infos, true);
    }
    SimUtils.checkConsistency(cloudManager, config);
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
    values.put(ImplicitSnitch.DISK, DEFAULT_FREE_DISK);
    values.put(Variable.Type.TOTALDISK.tagName, DEFAULT_TOTAL_DISK);
    values.put(ImplicitSnitch.SYSLOADAVG, 1.0);
    values.put(ImplicitSnitch.HEAPUSAGE, 123450000);
    values.put("sysprop.java.version", System.getProperty("java.version"));
    values.put("sysprop.java.vendor", System.getProperty("java.vendor"));
    // fake some metrics expected in tests
    values.put("metrics:solr.node:ADMIN./admin/authorization.clientErrors:count", 0);
    values.put("metrics:solr.jvm:buffers.direct.Count", 0);
    return values;
  }

  public void disableMetricsHistory() {
    metricsHistoryHandler.close();
  }

  public String dumpClusterState(boolean withCollections) throws Exception {
    StringBuilder sb = new StringBuilder();
    sb.append("#######################################\n");
    sb.append("############ CLUSTER STATE ############\n");
    sb.append("#######################################\n");
    sb.append("## Live nodes:\t\t").append(getLiveNodesSet().size()).append("\n");
    int emptyNodes = 0;
    int maxReplicas = 0;
    int minReplicas = Integer.MAX_VALUE;
    Map<String, Map<Replica.State, AtomicInteger>> replicaStates = new TreeMap<>();
    int numReplicas = 0;
    for (String node : getLiveNodesSet().get()) {
      List<ReplicaInfo> replicas = getSimClusterStateProvider().simGetReplicaInfos(node);
      numReplicas += replicas.size();
      if (replicas.size() > maxReplicas) {
        maxReplicas = replicas.size();
      }
      if (minReplicas > replicas.size()) {
        minReplicas = replicas.size();
      }
      for (ReplicaInfo ri : replicas) {
        replicaStates.computeIfAbsent(ri.getCollection(), c -> new TreeMap<>())
            .computeIfAbsent(ri.getState(), s -> new AtomicInteger())
            .incrementAndGet();
      }
      if (replicas.isEmpty()) {
        emptyNodes++;
      }
    }
    if (minReplicas == Integer.MAX_VALUE) {
      minReplicas = 0;
    }
    sb.append("## Empty nodes:\t").append(emptyNodes).append("\n");
    Set<String> deadNodes = getSimNodeStateProvider().simGetDeadNodes();
    sb.append("## Dead nodes:\t\t").append(deadNodes.size()).append("\n");
    deadNodes.forEach(n -> sb.append("##\t\t").append(n).append("\n"));
    sb.append("## Collections:\n");
      clusterStateProvider.simGetCollectionStats().forEach((coll, stats) -> {
        sb.append("##  * ").append(coll).append('\n');
        stats.forEach((k, v) -> {
          sb.append("##    ").append(k).append("\t").append(v).append("\n");
        });
      });
    if (withCollections) {
      ClusterState state = clusterStateProvider.getClusterState();
      state.forEachCollection(coll -> sb.append(coll.toString()).append("\n"));
    }
    sb.append("## Max replicas per node:\t").append(maxReplicas).append("\n");
    sb.append("## Min replicas per node:\t").append(minReplicas).append("\n");
    sb.append("## Total replicas:\t\t").append(numReplicas).append("\n");
    replicaStates.forEach((c, map) -> {
      AtomicInteger repCnt = new AtomicInteger();
      map.forEach((s, cnt) -> repCnt.addAndGet(cnt.get()));
      sb.append("## * ").append(c).append("\t\t").append(repCnt.get()).append("\n");
      map.forEach((s, cnt) -> sb.append("##\t\t- ").append(String.format(Locale.ROOT, "%-12s  %4d", s, cnt.get())).append("\n"));
    });
    sb.append("######### Solr op counts ##########\n");
    simGetOpCounts().forEach((k, cnt) -> sb.append("##\t\t- ").append(String.format(Locale.ROOT, "%-14s  %4d", k, cnt.get())).append("\n"));
    sb.append("######### Autoscaling event counts ###########\n");
    Map<String, Map<String, AtomicInteger>> counts = simGetEventCounts();
    counts.forEach((trigger, map) -> {
      sb.append("## * Trigger: ").append(trigger).append("\n");
      map.forEach((s, cnt) -> sb.append("##\t\t- ").append(String.format(Locale.ROOT, "%-11s  %4d", s, cnt.get())).append("\n"));
    });
    return sb.toString();
  }

  /**
   * Get the instance of {@link SolrResourceLoader} that is used by the cluster components.
   */
  public SolrResourceLoader getLoader() {
    return loader;
  }

  /**
   * Get the source of randomness (usually initialized by the test suite).
   */
  public Random getRandom() {
    return random;
  }

  /**
   * Add a new node and initialize its node values (metrics). The
   * /live_nodes list is updated with the new node id.
   * @return new node id
   */
  public String simAddNode() throws Exception {
    Map<String, Object> values = createNodeValues(null);
    String nodeId = (String)values.get(ImplicitSnitch.NODE);
    nodeStateProvider.simSetNodeValues(nodeId, values);
    clusterStateProvider.simAddNode(nodeId);
    log.trace("-- added node " + nodeId);
    // initialize history handler if this is the first node
    if (metricsHistoryHandler == null && liveNodesSet.size() == 1) {
      metricsHandler = new MetricsHandler(metricManager);
      metricsHistoryHandler = new MetricsHistoryHandler(nodeId, metricsHandler, solrClient, this, new HashMap<>());
      SolrMetricsContext solrMetricsContext = new SolrMetricsContext(metricManager, SolrMetricManager.getRegistryName(SolrInfoBean.Group.node), metricTag);
      metricsHistoryHandler.initializeMetrics(solrMetricsContext, CommonParams.METRICS_HISTORY_PATH);
    }
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
    if (liveNodesSet.isEmpty()) {
      // remove handlers
      if (metricsHistoryHandler != null) {
        IOUtils.closeQuietly(metricsHistoryHandler);
        metricsHistoryHandler = null;
      }
      if (metricsHandler != null) {
        metricsHandler = null;
      }
    }
    log.trace("-- removed node " + nodeId);
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

  public void simSetUseSystemCollection(boolean useSystemCollection) {
    this.useSystemCollection = useSystemCollection;
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

  public Map<String, Map<String, AtomicInteger>> simGetEventCounts() {
    TreeMap<String, Map<String, AtomicInteger>> counts = new TreeMap<>(eventCounts);
    return counts;
  }

  /**
   * Get a {@link SolrClient} implementation where calls are forwarded to this
   * instance of the cluster.
   * @return simulated SolrClient.
   */
  public SolrClient simGetSolrClient() {
    return solrClient;
//    return new SolrClient() {
//      @Override
//      public NamedList<Object> request(SolrRequest request, String collection) throws SolrServerException, IOException {
//        if (collection != null) {
//          if (request instanceof AbstractUpdateRequest) {
//            ((AbstractUpdateRequest)request).setParam("collection", collection);
//          } else if (request instanceof QueryRequest) {
//            ModifiableSolrParams params = new ModifiableSolrParams(request.getParams());
//            params.set("collection", collection);
//            request = new QueryRequest(params);
//          } else {
//            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "when collection != null only UpdateRequest and QueryRequest are supported: request=" + request + ", collection=" + collection);
//          }
//        }
//        SolrResponse rsp = SimCloudManager.this.request(request);
//        return rsp.getResponse();
//      }
//
//      @Override
//      public void close() throws IOException {
//
//      }
//    };
  }

  /**
   * Simulate the effect of restarting Overseer leader - in this case this means closing the current
   * {@link OverseerTriggerThread} (and optionally killing a node) then starting a new 
   * {@link OverseerTriggerThread}.
   * All background tasks currently in progress will be interrupted.
   * @param killNodeId optional nodeId to kill. If null then don't kill any node, just restart the thread
   * @see #getOverseerTriggerThread
   */
  public void simRestartOverseer(String killNodeId) throws Exception {
    log.info("=== Restarting OverseerTriggerThread and clearing object cache...");
    triggerThread.interrupt();
    IOUtils.closeQuietly(triggerThread);
    if (killNodeId != null) {
      log.info("  = killing node " + killNodeId);
      simRemoveNode(killNodeId, false);
    }
    objectCache.clear();

    try {
      simCloudManagerPool.shutdownNow();
    } catch (Exception e) {
      // ignore
    }
    simCloudManagerPool = ExecutorUtil.newMDCAwareFixedThreadPool(200, new DefaultSolrThreadFactory("simCloudManagerPool"));

    OverseerTriggerThread trigger = new OverseerTriggerThread(loader, this,
        new CloudConfig.CloudConfigBuilder("nonexistent", 0, "sim").build());
    triggerThread = new Overseer.OverseerThread(triggerThreadGroup, trigger, "Simulated OverseerAutoScalingTriggerThread");
    triggerThread.start();

  }

  /**
   * Submit a task to execute in a thread pool.
   * Every callable submitted will be wrapped such that errors not handled w/in the callable 
   * will be logged and counted for later assertions.
   *
   * @param callable task to execute
   * @return future to obtain results
   * @see #getBackgroundTaskFailureCount
   */
  public <T> Future<T> submit(Callable<T> callable) {
    return simCloudManagerPool.submit(new LoggingCallable(backgroundTaskFailureCounter, callable));
  }
  /** 
   * Returns a total count of the number of tasks submitted to {@link #submit} that have failed 
   * with any throwable other then <code>InteruptedException</code>
   *
   * @see #submit
   */
  public long getBackgroundTaskFailureCount() {
    return backgroundTaskFailureCounter.get();
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

  public void simResetOpCounts() {
    opCounts.clear();
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

  public SolrMetricManager getMetricManager() {
    return metricManager;
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
      // NOTE: we're doing 2 odd things here:
      // 1) rather then calling simHandleSolrRequest directly, we're submitting it to the
      //    executor service and immediately waiting on the Future.
      //    - This can introduce a delays if there are a lot of existing background tasks submitted
      // 2) we use simCloudManagerPool directly, instead of using the public submit() method
      //    - this is because there may be "user level" errors (ie: bad input) deliberately generated
      //      by the testcase.  we're going to immediately catch & re-throw any exceptions, so we don't
      //      need/want to be wrapped in a LoggingCallable w/getBackgroundTaskFailureCount() tracking
      Future<SolrResponse> rsp = simCloudManagerPool.submit(() -> simHandleSolrRequest(req));
      return rsp.get(120, TimeUnit.SECONDS); // longer then this and something is seriously wrong
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

    log.trace("--- got SolrRequest: " + req.getMethod() + " " + req.getPath() +
        (req.getParams() != null ? " " + req.getParams() : ""));
    if (req.getPath() != null) {
      if (req.getPath().startsWith("/admin/autoscaling") ||
          req.getPath().startsWith("/cluster/autoscaling") ||
          req.getPath().startsWith("/admin/metrics") ||
          req.getPath().startsWith("/cluster/metrics")
          ) {
        metricManager.registry("solr.node").counter("ADMIN." + req.getPath() + ".requests").inc();
        boolean autoscaling = req.getPath().contains("autoscaling");
        boolean history = req.getPath().contains("history");
        if (autoscaling) {
          incrementCount("autoscaling");
        } else if (history) {
          incrementCount("metricsHistory");
        } else {
          incrementCount("metrics");
        }
        ModifiableSolrParams params = new ModifiableSolrParams(req.getParams());
        params.set(CommonParams.PATH, req.getPath());
        LocalSolrQueryRequest queryRequest = new LocalSolrQueryRequest(null, params);
        if (autoscaling) {
          RequestWriter.ContentWriter cw = req.getContentWriter("application/json");
          if (null != cw) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            cw.write(baos);
            String payload = baos.toString("UTF-8");
            log.trace("-- payload: {}", payload);
            queryRequest.setContentStreams(Collections.singletonList(new ContentStreamBase.StringStream(payload)));
          }
        }
        queryRequest.getContext().put("httpMethod", req.getMethod().toString());
        SolrQueryResponse queryResponse = new SolrQueryResponse();
        queryResponse.addResponseHeader(new SimpleOrderedMap<>());
        if (autoscaling) {
          autoScalingHandler.handleRequest(queryRequest, queryResponse);
        } else {
          if (history) {
            if (metricsHistoryHandler != null) {
              metricsHistoryHandler.handleRequest(queryRequest, queryResponse);
            } else {
              throw new UnsupportedOperationException("must add at least 1 node first");
            }
          } else {
            if (metricsHandler != null) {
              metricsHandler.handleRequest(queryRequest, queryResponse);
            } else {
              throw new UnsupportedOperationException("must add at least 1 node first");
            }
          }
        }
        if (queryResponse.getException() != null) {
          log.debug("-- exception handling request", queryResponse.getException());
          throw new IOException(queryResponse.getException());
        }
        SolrResponse rsp = new SolrResponseBase();
        rsp.setResponse(queryResponse.getValues());
        log.trace("-- response: {}", rsp);
        return rsp;
      } else if (req instanceof QueryRequest) {
        incrementCount("query");
        return clusterStateProvider.simQuery((QueryRequest)req);
      }
    }
    if (req instanceof UpdateRequest) {
      incrementCount("update");
      UpdateRequest ureq = (UpdateRequest)req;
      String collection = ureq.getCollection();
      UpdateResponse rsp = clusterStateProvider.simUpdate(ureq);
      if (collection == null || collection.equals(CollectionAdminParams.SYSTEM_COLL)) {
        List<SolrInputDocument> docs = ureq.getDocuments();
        if (docs != null) {
          if (useSystemCollection) {
            systemColl.addAll(docs);
          }
          for (SolrInputDocument d : docs) {
            if (!"autoscaling_event".equals(d.getFieldValue("type"))) {
              continue;
            }
            eventCounts.computeIfAbsent((String)d.getFieldValue("event.source_s"), s -> new ConcurrentHashMap<>())
                .computeIfAbsent((String)d.getFieldValue("stage_s"), s -> new AtomicInteger())
                .incrementAndGet();
          }
        }
        return new UpdateResponse();
      } else {
        return rsp;
      }
    }
    // support only a specific subset of collection admin ops
    SolrParams params = req.getParams();
    String a = params != null ? params.get(CoreAdminParams.ACTION) : null;
    SolrResponse rsp = new SolrResponseBase();
    rsp.setResponse(new NamedList<>());
    String path = params != null ? params.get("path") : null;
    if (!(req instanceof CollectionAdminRequest)) {
      // maybe a V2Request?
      if (req instanceof V2Request) {
        params = SimUtils.v2AdminRequestToV1Params((V2Request)req);
        a = params.get(CoreAdminParams.ACTION);
      } else if (path != null && (path.startsWith("/admin/") || path.startsWith("/cluster/"))) {
        // pass it through, it's likely a generic request containing admin params
      } else {
        throw new UnsupportedOperationException("Only some CollectionAdminRequest-s are supported: " + req.getClass().getName() + ": " + req.getPath() + " " + req.getParams());
      }
    }
    metricManager.registry("solr.node").counter("ADMIN." + req.getPath() + ".requests").inc();
    if (a != null) {
      CollectionParams.CollectionAction action = CollectionParams.CollectionAction.get(a);
      if (action == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unknown action: " + a);
      }
      log.trace("Invoking Collection Action :{} with params {}", action.toLower(), params.toQueryString());
      NamedList results = new NamedList();
      rsp.setResponse(results);
      incrementCount(action.name());
      switch (action) {
        case REQUESTSTATUS:
          // we complete all async ops immediately
          String requestId = params.get(REQUESTID);
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
          requestId = params.get(REQUESTID);
          results.add("status", "successfully removed stored response for [" + requestId + "]");
          results.add("success", "");
          break;
        case CREATE:
          try {
            clusterStateProvider.simCreateCollection(new ZkNodeProps(params.toNamedList().asMap(10)), results);
          } catch (Exception e) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
          }
          break;
        case DELETE:
          try {
            clusterStateProvider.simDeleteCollection(params.get(CommonParams.NAME),
                params.get(CommonAdminParams.ASYNC), results);
          } catch (Exception e) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
          }
          break;
        case LIST:
          results.add("collections", clusterStateProvider.simListCollections());
          break;
        case ADDREPLICA:
          try {
            clusterStateProvider.simAddReplica(new ZkNodeProps(params.toNamedList().asMap(10)), results);
          } catch (Exception e) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
          }
          break;
        case MOVEREPLICA:
          try {
            clusterStateProvider.simMoveReplica(new ZkNodeProps(params.toNamedList().asMap(10)), results);
          } catch (Exception e) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
          }
          break;
        case OVERSEERSTATUS:
          if (params.get(CommonAdminParams.ASYNC) != null) {
            results.add(REQUESTID, params.get(CommonAdminParams.ASYNC));
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
          nodeStateProvider.simSetNodeValue(params.get("node"), "nodeRole", params.get("role"));
          break;
        case CREATESHARD:
          try {
            clusterStateProvider.simCreateShard(new ZkNodeProps(params.toNamedList().asMap(10)), results);
          } catch (Exception e) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
          }
          break;
        case SPLITSHARD:
          try {
            clusterStateProvider.simSplitShard(new ZkNodeProps(params.toNamedList().asMap(10)), results);
          } catch (Exception e) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
          }
          break;
        case DELETESHARD:
          try {
            clusterStateProvider.simDeleteShard(new ZkNodeProps(params.toNamedList().asMap(10)), results);
          } catch (Exception e) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
          }
          break;
        default:
          throw new UnsupportedOperationException("Unsupported collection admin action=" + action + " in request: " + params);
      }
    } else {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "action is a required param in request: " + params);
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
    // make sure we shutdown the pool first, so any in active background tasks get interupted
    // before we start closing resources they may be using.
    simCloudManagerPool.shutdownNow();
    
    if (metricsHistoryHandler != null) {
      IOUtils.closeQuietly(metricsHistoryHandler);
    }
    IOUtils.closeQuietly(clusterStateProvider);
    IOUtils.closeQuietly(nodeStateProvider);
    IOUtils.closeQuietly(stateManager);
    triggerThread.interrupt();
    IOUtils.closeQuietly(triggerThread);
    triggerThread.interrupt();
    try {
      triggerThread.join();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    IOUtils.closeQuietly(objectCache);
  }

  /**
   * Direct access to the current {@link OverseerTriggerThread}
   * @see #simRestartOverseer
   */
  public OverseerTriggerThread getOverseerTriggerThread() {
    return ((OverseerTriggerThread) triggerThread.getThread());
  }

  /**
   * Wrapper for any Callable that will log a warn/error in the event of InterruptException/Throwable.  
   * Also increments the passed in counter so the CloudManger can later report total errors programatically.
   *
   * @see #submit
   * @see #getBackgroundTaskFailureCount
   */
  private static final class LoggingCallable<T> implements Callable<T> {
    
    final AtomicLong failCounter;
    final Callable<T> inner;
    
    public LoggingCallable(final AtomicLong failCounter, final Callable<T> inner) {
      assert null != failCounter;
      assert null != inner;
      this.failCounter = failCounter;
      this.inner = inner;
    }
    
    public T call() throws Exception {
      try {
        return inner.call();
      } catch (InterruptedException ignored) {
        log.warn("Callable interupted", ignored);
        throw ignored;
      } catch (Throwable t) {
        // be forgiving of errors that occured as a result of interuption, even if
        // the inner Callable didn't realize it...
        if (Thread.currentThread().isInterrupted()) {
          log.warn("Callable interrupted w/o noticing", t);
          throw t;
        }
        Throwable cause = t;
        while ((cause = cause.getCause()) != null) {
          if (cause instanceof InterruptedException) {
            log.warn("Callable threw wrapped InterruptedException", t);
            throw t;
          }
        }

        // in all other situations, this is a problem that should be tracked in the failCounter
        failCounter.incrementAndGet();
        log.error("Callable failed", t);
        throw t;
      }
    }
  }
}
