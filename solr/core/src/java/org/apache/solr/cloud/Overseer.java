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

import org.apache.lucene.util.Version;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.LBHttp2SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.cloud.api.collections.CreateCollectionCmd;
import org.apache.solr.cloud.api.collections.OverseerCollectionMessageHandler;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.cloud.overseer.ZkStateWriter;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.ParWorkExecutor;
import org.apache.solr.common.SolrCloseable;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrThread;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.ConnectionManager;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.SysStats;
import org.apache.solr.core.CloudConfig;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.update.UpdateShardHandler;
import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.OverseerConfigSetMessageHandler.CONFIGSETS_ACTION_PREFIX;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;

/**
 * <p>Cluster leader. Responsible for processing state updates, node assignments, creating/deleting
 * collections, shards, replicas and setting various properties.</p>
 *
 * <p>The <b>Overseer</b> is a single elected node in the SolrCloud cluster that is in charge of interactions with
 * ZooKeeper that require global synchronization. </p>
 *
 * <p>The Overseer deals with:</p>
 * <ul>
 *   <li>Cluster State updates, i.e. updating Collections' <code>state.json</code> files in ZooKeeper,</li>
 *   <li>Collection API implementation, see
 *   {@link OverseerCollectionConfigSetProcessor} and {@link OverseerCollectionMessageHandler} (and the example below),</li>
 *   <li>Updating Config Sets, see {@link OverseerCollectionConfigSetProcessor} and {@link OverseerConfigSetMessageHandler},</li>
 * </ul>
 *
 * <p>The nodes in the cluster communicate with the Overseer over queues implemented in ZooKeeper. There are essentially
 * two queues:</p>
 * <ol>
 *   <li>The <b>state update queue</b>, through which nodes request the Overseer to update the <code>state.json</code> file of a
 *   Collection in ZooKeeper. This queue is in Zookeeper at <code>/overseer/queue</code>,</li>
 *   <li>A queue shared between <b>Collection API and Config Set API</b> requests. This queue is in Zookeeper at
 *   <code>/overseer/collection-queue-work</code>.</li>
 * </ol>
 *
 * <p>An example of the steps involved in the Overseer processing a Collection creation API call:</p>
 * <ol>
 *   <li>Client uses the Collection API with <code>CREATE</code> action and reaches a node of the cluster,</li>
 *   <li>The node (via {@link CollectionsHandler}) enqueues the request into the <code>/overseer/collection-queue-work</code>
 *   queue in ZooKeepeer,</li>
 *   <li>The {@link OverseerCollectionConfigSetProcessor} running on the Overseer node dequeues the message and using an
 *   executor service with a maximum pool size of {@link OverseerTaskProcessor#MAX_PARALLEL_TASKS} hands it for processing
 *   to {@link OverseerCollectionMessageHandler},</li>
 *   <li>Command {@link CreateCollectionCmd} then executes and does:
 *   <ol>
 *     <li>Update some state directly in ZooKeeper (creating collection znode),</li>
 *     <li>Compute replica placement on available nodes in the cluster,</li>
 *     <li>Enqueue a state change request for creating the <code>state.json</code> file for the collection in ZooKeeper.
 *     This is done by enqueuing a message in <code>/overseer/queue</code>,</li>
 *     <li>The command then waits for the update to be seen in ZooKeeper...</li>
 *   </ol></li>
 *   <li>The ClusterState Updater (also running on the Overseer node) dequeues the state change message and creates the
 *   <code>state.json</code> file in ZooKeeper for the Collection. All the work of the cluster state updater
 *   (creations, updates, deletes) is done sequentially for the whole cluster by a single thread.</li>
 *   <li>The {@link CreateCollectionCmd} sees the state change in
 *   ZooKeeper and:
 *   <ol start="5">
 *     <li>Builds and sends requests to each node to create the appropriate cores for all the replicas of all shards
 *     of the collection. Nodes create the replicas and set them to {@link org.apache.solr.common.cloud.Replica.State#ACTIVE}.</li>
 *   </ol></li>
 *   <li>The collection creation command has succeeded from the Overseer perspective,</li>
 *   <li>{@link CollectionsHandler} checks the replicas in Zookeeper and verifies they are all
 *   {@link org.apache.solr.common.cloud.Replica.State#ACTIVE},</li>
 *   <li>The client receives a success return.</li>
 * </ol>
 */
public class Overseer implements SolrCloseable {
  public static final String QUEUE_OPERATION = "op";

  public static final String OVERSEER_COLLECTION_QUEUE_WORK = "/overseer/collection-queue-work";

  public static final String OVERSEER_QUEUE = "/overseer/queue";

  public static final String OVERSEER_ASYNC_IDS = "/overseer/async_ids";

  public static final String OVERSEER_COLLECTION_MAP_FAILURE = "/overseer/collection-map-failure";

  public static final String OVERSEER_COLLECTION_MAP_COMPLETED = "/overseer/collection-map-completed";

  public static final String OVERSEER_COLLECTION_MAP_RUNNING = "/overseer/collection-map-running";


  public static final int STATE_UPDATE_MAX_QUEUE = 20000;

  public static final int NUM_RESPONSES_TO_STORE = 10000;
  public static final String OVERSEER_ELECT = "/overseer/overseer_elect";

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static volatile Overseer OUR_JVM_OVERSEER = null;

  private volatile boolean closeAndDone;
  private volatile boolean initedHttpClient = false;
  private volatile QueueWatcher queueWatcher;
  private volatile WorkQueueWatcher.CollectionWorkQueueWatcher collectionQueueWatcher;
  private volatile ParWorkExecutor taskExecutor;

  private volatile ParWorkExecutor zkWriterExecutor;

  public boolean isDone() {
    return closeAndDone;
  }

  public ExecutorService getTaskExecutor() {
    return taskExecutor;
  }

  public ExecutorService getTaskZkWriterExecutor() {
    return zkWriterExecutor;
  }

  private static class StringBiConsumer implements BiConsumer<String, Object> {
    boolean firstPair = true;

    @Override
    public void accept(String s, Object o) {
      if (firstPair) {
        log.warn("WARNING: Collection '.system' may need re-indexing due to compatibility issues listed below. See REINDEXCOLLECTION documentation for more details.");
        firstPair = false;
      }
      log.warn("WARNING: *\t{}:\t{}", s, o);
    }
  }

  private String printQueue(LinkedList<Pair<String,byte[]>> queue) {

    StringBuilder sb = new StringBuilder("Queue[");
    for (Pair<String,byte[]> item : queue) {
      sb.append(item.first()).append(":").append(ZkNodeProps.load(item.second())).append(", ");
    }
    sb.append("]");
    return sb.toString();
  }

  public static class OverseerThread extends SolrThread implements Closeable {

    protected volatile boolean isClosed;
    private final Closeable thread;

    public OverseerThread(ThreadGroup ccTg, Closeable thread, String name) {
      super(ccTg, (Runnable) thread, name);
      this.thread = thread;
    }

    @Override
    public void run() {
      super.run();
    }

    @Override
    public void close() throws IOException {
      this.isClosed = true;
      thread.close();
    }

    public Closeable getThread() {
      return thread;
    }

    public boolean isClosed() {
      return this.isClosed;
    }

  }


 // private volatile OverseerThread updaterThread;

//  private volatile ExecutorService stateManagmentExecutor;
//
//  private volatile ExecutorService taskExecutor;

  private final ZkStateWriter zkStateWriter;

  private final UpdateShardHandler updateShardHandler;

  private final String adminPath;

  private volatile OverseerCollectionConfigSetProcessor overseerCollectionConfigSetProcessor;

  private final ZkController zkController;

  private volatile Stats stats;
  private volatile String id;
  private volatile boolean closed = true;
  private volatile boolean systemCollCompatCheck = true;

  private final CloudConfig config;

  public volatile Http2SolrClient overseerOnlyClient;
  public volatile LBHttp2SolrClient overseerLbClient;

  // overseer not responsible for closing reader
  public Overseer(UpdateShardHandler updateShardHandler, String adminPath, ZkController zkController, CloudConfig config) {
    this.updateShardHandler = updateShardHandler;
    this.adminPath = adminPath;
    this.zkController = zkController;
    this.stats = new Stats();
    this.config = config;

    Stats stats = new Stats();
    this.zkStateWriter = new ZkStateWriter(zkController.getZkStateReader(), stats, this);
  }

  public synchronized void start(String id, ElectionContext context) throws KeeperException {
    log.info("Starting Overseer");
    if (getCoreContainer().isShutDown() || closeAndDone) {
      if (log.isDebugEnabled()) log.debug("Already closed, exiting");
      return;
    }

    if (!closed) {
      log.warn("Startomg an Overseer that was not closed");
      IOUtils.closeQuietly(zkController.overseerElector);
      IOUtils.closeQuietly(this);
    }

//    if (OUR_JVM_OVERSEER != null) {
//      throw new IllegalStateException("Cannot start an Overseer if another is running");
//    }

    OUR_JVM_OVERSEER = this;

  //  doClose();

    this.id = id;
//
//     stateManagmentExecutor = ParWork.getParExecutorService("stateManagmentExecutor",
//        1, 1, 3000, new SynchronousQueue());
     taskExecutor = (ParWorkExecutor) ParWork.getParExecutorService("overseerTaskExecutor",
         4, SysStats.PROC_COUNT * 2, 1000, new BlockingArrayQueue<>(32, 64));
    for (int i = 0; i < 4; i++) {
      taskExecutor.prestartCoreThread();
    }

    zkWriterExecutor = (ParWorkExecutor) ParWork.getParExecutorService("overseerZkWriterExecutor",
        4, SysStats.PROC_COUNT * 2, 1000, new BlockingArrayQueue<>(64, 128));
    for (int i = 0; i < 4; i++) {
      zkWriterExecutor.prestartCoreThread();
    }

    if (overseerOnlyClient == null && !closeAndDone && !initedHttpClient) {
      overseerOnlyClient = new Http2SolrClient.Builder().idleTimeout(60000).connectionTimeout(5000).markInternalRequest().build();
      overseerOnlyClient.enableCloseLock();
      this.overseerLbClient = new LBHttp2SolrClient(overseerOnlyClient);
      initedHttpClient = true;
    }


//    try {
//      if (log.isDebugEnabled()) {
//        log.debug("set watch on leader znode");
//      }
//      zkController.getZkClient().exists(Overseer.OVERSEER_ELECT + "/leader", new Watcher() {
//
//        @Override
//        public void process(WatchedEvent event) {
//          if (Event.EventType.None.equals(event.getType())) {
//            return;
//          }
//          if (!isClosed()) {
//            log.info("Overseer leader has changed, closing ...");
//            Overseer.this.close();
//          }
//        }}, true);
//    } catch (KeeperException.SessionExpiredException e) {
//      log.warn("ZooKeeper session expired");
//      return;
//    } catch (InterruptedException | AlreadyClosedException e) {
//      log.info("Already closed");
//      return;
//    } catch (Exception e) {
//      log.error("Unexpected error in Overseer state update loop", e);
//      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
//    }



    log.info("Overseer (id={}) starting", id);
    //launch cluster state updater thread

    ThreadGroup ccTg = new ThreadGroup("Overseer collection creation process.");


    //systemCollectionCompatCheck(new StringBiConsumer());

    queueWatcher = new WorkQueueWatcher(getCoreContainer());
    collectionQueueWatcher = new WorkQueueWatcher.CollectionWorkQueueWatcher(getCoreContainer(), id, overseerLbClient, adminPath, stats, Overseer.this);
    try {
      queueWatcher.start();
      collectionQueueWatcher.start();
    } catch (InterruptedException e) {
      log.warn("interrupted", e);
    }


    closed = false;
    // TODO: don't track for a moment, can leak out of collection api tests
    // assert ObjectReleaseTracker.track(this);
  }

  public void systemCollectionCompatCheck(final BiConsumer<String, Object> consumer) {
    ClusterState clusterState = zkController.getClusterState();
    if (clusterState == null) {
      log.warn("Unable to check back-compat of .system collection - can't obtain ClusterState.");
      return;
    }
    DocCollection coll = clusterState.getCollectionOrNull(CollectionAdminParams.SYSTEM_COLL);
    if (coll == null) {
      return;
    }
    // check that all shard leaders are active
    boolean allActive = true;
    for (Slice s : coll.getActiveSlices()) {
      if (s.getLeader() == null || !s.getLeader().isActive(zkController.getZkStateReader().getLiveNodes())) {
        allActive = false;
        break;
      }
    }
    if (allActive) {
      doCompatCheck(consumer);
    } else {
      // wait for all leaders to become active and then check
      zkController.zkStateReader.registerCollectionStateWatcher(CollectionAdminParams.SYSTEM_COLL, (liveNodes, state) -> {
        boolean active = true;
        if (state == null || liveNodes.isEmpty()) {
          return true;
        }
        for (Slice s : state.getActiveSlices()) {
          if (s.getLeader() == null || !s.getLeader().isActive(liveNodes)) {
            active = false;
            break;
          }
        }
        if (active) {
          doCompatCheck(consumer);
        }
        return active;
      });
    }
  }

  private void doCompatCheck(BiConsumer<String, Object> consumer) {
    if (systemCollCompatCheck) {
      systemCollCompatCheck = false;
    } else {
      return;
    }
    try {
      CloudHttp2SolrClient client = getCoreContainer().getZkController().getCloudSolrClient();
      CollectionAdminRequest.ColStatus req = CollectionAdminRequest.collectionStatus(CollectionAdminParams.SYSTEM_COLL)
          .setWithSegments(true)
          .setWithFieldInfo(true);
      CollectionAdminResponse rsp = req.process(client);
      NamedList<Object> status = (NamedList<Object>)rsp.getResponse().get(CollectionAdminParams.SYSTEM_COLL);
      Collection<String> nonCompliant = (Collection<String>)status.get("schemaNonCompliant");
      if (!nonCompliant.contains("(NONE)")) {
        consumer.accept("indexFieldsNotMatchingSchema", nonCompliant);
      }
      Set<Integer> segmentCreatedMajorVersions = new HashSet<>();
      Set<String> segmentVersions = new HashSet<>();
      int currentMajorVersion = Version.LATEST.major;
      String currentVersion = Version.LATEST.toString();
      segmentVersions.add(currentVersion);
      segmentCreatedMajorVersions.add(currentMajorVersion);
      NamedList<Object> shards = (NamedList<Object>)status.get("shards");
      for (Map.Entry<String, Object> entry : shards) {
        NamedList<Object> leader = (NamedList<Object>)((NamedList<Object>)entry.getValue()).get("leader");
        if (leader == null) {
          continue;
        }
        NamedList<Object> segInfos = (NamedList<Object>)leader.get("segInfos");
        if (segInfos == null) {
          continue;
        }
        NamedList<Object> infos = (NamedList<Object>)segInfos.get("info");
        if (((Number)infos.get("numSegments")).intValue() > 0) {
          segmentVersions.add(infos.get("minSegmentLuceneVersion").toString());
        }
        if (infos.get("commitLuceneVersion") != null) {
          segmentVersions.add(infos.get("commitLuceneVersion").toString());
        }
        NamedList<Object> segmentInfos = (NamedList<Object>)segInfos.get("segments");
        segmentInfos.forEach((k, v) -> {
          NamedList<Object> segment = (NamedList<Object>)v;
          segmentVersions.add(segment.get("version").toString());
          if (segment.get("minVersion") != null) {
            segmentVersions.add(segment.get("version").toString());
          }
          if (segment.get("createdVersionMajor") != null) {
            segmentCreatedMajorVersions.add(((Number)segment.get("createdVersionMajor")).intValue());
          }
        });
      }
      if (segmentVersions.size() > 1) {
        consumer.accept("differentSegmentVersions", segmentVersions);
        consumer.accept("currentLuceneVersion", currentVersion);
      }
      if (segmentCreatedMajorVersions.size() > 1) {
        consumer.accept("differentMajorSegmentVersions", segmentCreatedMajorVersions);
        consumer.accept("currentLuceneMajorVersion", currentMajorVersion);
      }

    } catch (SolrServerException | IOException e) {
      log.warn("Unable to perform back-compat check of .system collection", e);
    }
  }

  public Stats getStats() {
    return stats;
  }

  ZkController getZkController(){
    return zkController;
  }

  public CoreContainer getCoreContainer() {
    return zkController.getCoreContainer();
  }

  public SolrCloudManager getSolrCloudManager() {
    return zkController.getSolrCloudManager();
  }


  public void closeAndDone() {
    synchronized (this) {
      this.closed = true;
      this.closeAndDone = true;
    }
    close();
  }

  public boolean isCloseAndDone() {
    return closeAndDone;
  }

  public void close() {
    log.info("Overseer (id={}) closing closeAndDone={}}", id, closeAndDone);

    boolean cd = closeAndDone;

    OUR_JVM_OVERSEER = null;
    closed = true;


    if (!cd) {
      boolean retry;
      synchronized (this) {
        retry = !zkController.getCoreContainer().isShutDown() && !zkController.isShutdownCalled() && !zkController.isClosed() && !closeAndDone;
      }
      if (retry && zkController.getZkClient().isAlive()) {
        log.info("rejoining the overseer election after closing");
        try {
          zkController.rejoinOverseerElection(false);
        } catch (AlreadyClosedException e) {
          return;
        }
      }

    }

    if (cd) {

      if (taskExecutor != null) {
        taskExecutor.shutdown();
      }

      if (zkWriterExecutor != null) {
        zkWriterExecutor.shutdown();
      }

      if (overseerOnlyClient != null) {
        overseerOnlyClient.disableCloseLock();
      }

      if (overseerLbClient != null) {
        overseerLbClient.close();
        overseerLbClient = null;
      }

      if (overseerOnlyClient != null) {
        overseerOnlyClient.close();
        overseerOnlyClient = null;
      }
    }

    if (queueWatcher != null) {
      queueWatcher.close();
    }

    if (collectionQueueWatcher != null) {
      collectionQueueWatcher.close();
    }

    if (taskExecutor != null) {
      try {
        taskExecutor.awaitTermination(15, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        ParWork.propagateInterrupt(e, true);
      }
    }

    if (log.isDebugEnabled()) {
      log.debug("doClose - end");
    }

    assert ObjectReleaseTracker.release(this);
  }

  @Override
  public boolean isClosed() {
    return closed;
  }


  /**
   * Get queue that can be used to send messages to Overseer.
   * <p>
   * Any and all modifications to the cluster state must be sent to
   * the overseer via this queue. The complete list of overseer actions
   * supported by this queue are documented inside the {@link OverseerAction} enum.
   * <p>
   * Performance statistics on the returned queue
   * are <em>not</em> tracked by the Overseer Stats API,
   * see {@link org.apache.solr.common.params.CollectionParams.CollectionAction#OVERSEERSTATUS}.
   * Therefore, this method should be used only by clients for writing to the overseer queue.
   * <p>
   *
   * @return a {@link ZkDistributedQueue} object
   */
  public ZkDistributedQueue getStateUpdateQueue() {
    return getStateUpdateQueue(new Stats());
  }

  /**
   * The overseer uses the returned queue to read any operations submitted by clients.
   * This method should not be used directly by anyone other than the Overseer itself.
   * This method will create the /overseer znode in ZooKeeper if it does not exist already.
   *
   * @param zkStats  a {@link Stats} object which tracks statistics for all zookeeper operations performed by this queue
   * @return a {@link ZkDistributedQueue} object
   */
  ZkDistributedQueue getStateUpdateQueue(Stats zkStats) {
    return new ZkDistributedQueue(zkController.getZkClient(), "/overseer/queue", zkStats, STATE_UPDATE_MAX_QUEUE, new ConnectionManager.IsClosed(){
      public boolean isClosed() {
        return Overseer.this.isClosed() || zkController.getCoreContainer().isShutDown();
      }
    });
  }

//  static ZkDistributedQueue getInternalWorkQueue(final SolrZkClient zkClient, Stats zkStats) {
//    return new ZkDistributedQueue(zkClient, "/overseer/queue-work", zkStats);
//  }

  /* Internal map for failed tasks, not to be used outside of the Overseer */
  static DistributedMap getRunningMap(final SolrZkClient zkClient) throws KeeperException {
    return new DistributedMap(zkClient, "/overseer/collection-map-running");
  }

  /* Size-limited map for successfully completed tasks*/
  static DistributedMap getCompletedMap(final SolrZkClient zkClient) throws KeeperException {
    return new SizeLimitedDistributedMap(zkClient, "/overseer/collection-map-completed", NUM_RESPONSES_TO_STORE, (child) -> getAsyncIdsMap(zkClient).remove(child));
  }

  /* Map for failed tasks, not to be used outside of the Overseer */
  static DistributedMap getFailureMap(final SolrZkClient zkClient) throws KeeperException {
    return new SizeLimitedDistributedMap(zkClient, "/overseer/collection-map-failure", NUM_RESPONSES_TO_STORE, (child) -> getAsyncIdsMap(zkClient).remove(child));
  }

  /* Map of async IDs currently in use*/
  static DistributedMap getAsyncIdsMap(final SolrZkClient zkClient) throws KeeperException {
    return new DistributedMap(zkClient, Overseer.OVERSEER_ASYNC_IDS);
  }

  /**
   * Get queue that can be used to submit collection API tasks to the Overseer.
   * <p>
   * This queue is used internally by the {@link CollectionsHandler} to submit collection API
   * tasks which are executed by the {@link OverseerCollectionMessageHandler}. The actions supported
   * by this queue are listed in the {@link org.apache.solr.common.params.CollectionParams.CollectionAction}
   * enum.
   * <p>
   * Performance statistics on the returned queue
   * are <em>not</em> tracked by the Overseer Stats API,
   * see {@link org.apache.solr.common.params.CollectionParams.CollectionAction#OVERSEERSTATUS}.
   *
   * @param zkClient the {@link SolrZkClient} to be used for reading/writing to the queue
   * @return a {@link ZkDistributedQueue} object
   */
  OverseerTaskQueue getCollectionQueue(final SolrZkClient zkClient) {
    return getCollectionQueue(zkClient, new Stats());
  }

  /**
   * Get queue that can be used to read collection API tasks to the Overseer.
   * <p>
   * This queue is used internally by the {@link OverseerCollectionMessageHandler} to read collection API
   * tasks submitted by the {@link CollectionsHandler}. The actions supported
   * by this queue are listed in the {@link org.apache.solr.common.params.CollectionParams.CollectionAction}
   * enum.
   * <p>
   * Performance statistics on the returned queue are tracked by the Overseer Stats API,
   * see {@link org.apache.solr.common.params.CollectionParams.CollectionAction#OVERSEERSTATUS}.
   *
   * @param zkClient the {@link SolrZkClient} to be used for reading/writing to the queue
   * @return a {@link ZkDistributedQueue} object
   */
  OverseerTaskQueue getCollectionQueue(final SolrZkClient zkClient, Stats zkStats) {
    return new OverseerTaskQueue(zkClient, "/overseer/collection-queue-work", zkStats);
  }

  /**
   * Get queue that can be used to submit configset API tasks to the Overseer.
   * <p>
   * This queue is used internally by the {@link org.apache.solr.handler.admin.ConfigSetsHandler} to submit
   * tasks which are executed by the {@link OverseerConfigSetMessageHandler}. The actions supported
   * by this queue are listed in the {@link org.apache.solr.common.params.ConfigSetParams.ConfigSetAction}
   * enum.
   * <p>
   * Performance statistics on the returned queue
   * are <em>not</em> tracked by the Overseer Stats API,
   * see {@link org.apache.solr.common.params.CollectionParams.CollectionAction#OVERSEERSTATUS}.
   *
   * @param zkClient the {@link SolrZkClient} to be used for reading/writing to the queue
   * @return a {@link ZkDistributedQueue} object
   */
  OverseerTaskQueue getConfigSetQueue(final SolrZkClient zkClient)  {
    return getConfigSetQueue(zkClient, new Stats());
  }

  /**
   * Get queue that can be used to read configset API tasks to the Overseer.
   * <p>
   * This queue is used internally by the {@link OverseerConfigSetMessageHandler} to read configset API
   * tasks submitted by the {@link org.apache.solr.handler.admin.ConfigSetsHandler}. The actions supported
   * by this queue are listed in the {@link org.apache.solr.common.params.ConfigSetParams.ConfigSetAction}
   * enum.
   * <p>
   * Performance statistics on the returned queue are tracked by the Overseer Stats API,
   * see {@link org.apache.solr.common.params.CollectionParams.CollectionAction#OVERSEERSTATUS}.
   * <p>
   * For now, this internally returns the same queue as {@link #getCollectionQueue(SolrZkClient, Stats)}.
   * It is the responsibility of the client to ensure that configset API actions are prefixed with
   * {@link OverseerConfigSetMessageHandler#CONFIGSETS_ACTION_PREFIX} so that it is processed by
   * {@link OverseerConfigSetMessageHandler}.
   *
   * @param zkClient the {@link SolrZkClient} to be used for reading/writing to the queue
   * @return a {@link ZkDistributedQueue} object
   */
  OverseerTaskQueue getConfigSetQueue(final SolrZkClient zkClient, Stats zkStats) {
    // For now, we use the same queue as the collection queue, but ensure
    // that the actions are prefixed with a unique string.
    return getCollectionQueue(zkClient, zkStats);
  }

  public ZkStateReader getZkStateReader() {
    return zkController.getZkStateReader();
  }

  public ZkStateWriter getZkStateWriter() {
    return zkStateWriter;
  }

  public void offerStateUpdate(byte[] data) throws KeeperException, InterruptedException {
    getStateUpdateQueue().offer(data, false);
  }

  public boolean processQueueItem(ZkNodeProps message) throws InterruptedException {
    if (log.isDebugEnabled()) log.debug("processQueueItem {}", message);
    // MRM TODO: - may not need this now
    new OverseerTaskExecutorTask(getCoreContainer(), message).run();
//    try {
//      future.get();
//    } catch (ExecutionException e) {
//      log.error("", e);
//      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
//    }
    return true;
  }

  public void writePendingUpdates() {
    new OverseerTaskExecutorTask.WriteTask(getCoreContainer(), zkStateWriter).run();
  }

  private static abstract class QueueWatcher implements Watcher, Closeable {

    protected final CoreContainer cc;
    protected final ZkController zkController;
    protected final String path;
    protected final Overseer overseer;
    protected volatile List<String> startItems;
    protected volatile boolean closed;
    protected final ReentrantLock ourLock = new ReentrantLock(true);

    public QueueWatcher(CoreContainer cc, String path) throws KeeperException {
      this.cc = cc;
      this.zkController = cc.getZkController();
      this.overseer = zkController.getOverseer();
      this.path = path;
    }

    public abstract void start() throws KeeperException, InterruptedException;

    private List<String> getItems() {
      try {

        if (log.isDebugEnabled()) log.debug("set watch on Overseer work queue {}", path);

        List<String> children = zkController.getZkClient().getChildren(path, null, null, true, true);

        List<String> items = new ArrayList<>(children);
        Collections.sort(items);
        return items;
      } catch (KeeperException.SessionExpiredException e) {
        log.warn("ZooKeeper session expired");
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      } catch (InterruptedException | AlreadyClosedException e) {
        log.info("Already closed");
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      } catch (Exception e) {
        log.error("Unexpected error in Overseer state update loop", e);
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }
    }

    @Override
    public void process(WatchedEvent event) {
      if (Event.EventType.None.equals(event.getType())) {
        return;
      }
      if (this.closed) {
        log.info("Overseer is closed, do not process watcher for queue");
        return;
      }

      ourLock.lock();
      try {
        try {
          List<String> items = getItems();
          if (items.size() > 0) {
            processQueueItems(items, false);
          }
        } catch (AlreadyClosedException e) {

        } catch (Exception e) {
          log.error("Exception during overseer queue queue processing", e);
        }
      } finally {
        ourLock.unlock();
      }

    }

    protected abstract void processQueueItems(List<String> items, boolean onStart);

    @Override
    public void close() {
      this.closed = true;
      closeWatcher();
    }

    private void closeWatcher() {
      try {
        zkController.getZkClient().removeWatches(path, this, WatcherType.Any, true);
      } catch (KeeperException.NoWatcherException e) {

      } catch (Exception e) {
        log.info("could not remove watch {} {}", e.getClass().getSimpleName(), e.getMessage());
      }
    }
  }

  private static class WorkQueueWatcher extends QueueWatcher {

    public WorkQueueWatcher(CoreContainer cc) throws KeeperException {
      super(cc, Overseer.OVERSEER_QUEUE);
    }


    public void start() throws KeeperException, InterruptedException {
      zkController.getZkClient().addWatch(path, this, AddWatchMode.PERSISTENT);
      startItems = super.getItems();
      log.info("Overseer found entries on start {}", startItems);
      processQueueItems(startItems, true);
    }

    @Override
    protected void processQueueItems(List<String> items, boolean onStart) {
      ourLock.lock();
      try {
        if (log.isDebugEnabled()) log.debug("Found state update queue items {}", items);
        List<String> fullPaths = new ArrayList<>(items.size());
        for (String item : items) {
          fullPaths.add(path + "/" + item);
        }

        Map<String,byte[]> data = zkController.getZkClient().getData(fullPaths);

        for (byte[] item : data.values()) {
          final ZkNodeProps message = ZkNodeProps.load(item);
          try {
            boolean success = overseer.processQueueItem(message);
          } catch (Exception e) {
            log.error("Overseer state update queue processing failed", e);
          }
        }

        overseer.writePendingUpdates();

        if (overseer.zkStateWriter != null) {
          if (zkController.getZkClient().isAlive()) {
            try {
              zkController.getZkClient().delete(fullPaths, true);
            } catch (Exception e) {
              log.warn("Failed deleting processed items", e);
            }
          }
        }

      } finally {
        ourLock.unlock();
      }
    }

    private static class CollectionWorkQueueWatcher extends QueueWatcher {

      private final OverseerCollectionMessageHandler collMessageHandler;
      private final OverseerConfigSetMessageHandler configMessageHandler;
      private final DistributedMap failureMap;
      private final DistributedMap runningMap;

      private final DistributedMap completedMap;

      public CollectionWorkQueueWatcher(CoreContainer cc, String myId, LBHttp2SolrClient overseerLbClient, String adminPath, Stats stats, Overseer overseer) throws KeeperException {
        super(cc, Overseer.OVERSEER_COLLECTION_QUEUE_WORK);
        collMessageHandler = new OverseerCollectionMessageHandler(cc, myId, overseerLbClient, adminPath, stats, overseer);
        configMessageHandler = new OverseerConfigSetMessageHandler(cc);
        failureMap = Overseer.getFailureMap(cc.getZkController().getZkClient());
        runningMap = Overseer.getRunningMap(cc.getZkController().getZkClient());
        completedMap = Overseer.getCompletedMap(cc.getZkController().getZkClient());
      }

      @Override
      public void close() {
        super.close();
        IOUtils.closeQuietly(collMessageHandler);
        IOUtils.closeQuietly(configMessageHandler);
      }

      @Override
      public void start() throws KeeperException, InterruptedException {
        zkController.getZkClient().addWatch(path, this, AddWatchMode.PERSISTENT);

        startItems = super.getItems();

        log.info("Overseer found entries on start {}", startItems);
        processQueueItems(startItems, true);
      }

      @Override
      protected void processQueueItems(List<String> items, boolean onStart) {

        ourLock.lock();
        try {
          log.info("Found collection queue items {} onStart={}", items, onStart);
          List<String> fullPaths = new ArrayList<>(items.size());
          for (String item : items) {
            fullPaths.add(path + "/" + item);
          }

          Map<String,byte[]> data = zkController.getZkClient().getData(fullPaths);

          if (fullPaths.size() > 0) {
            try {
              zkController.getZkClient().delete(fullPaths, true);
            } catch (Exception e) {
              log.warn("Delete items failed {}", e.getMessage());
            }

            try {
              log.info("items in queue {} after delete {} {}", path, zkController.getZkClient().listZnode(path, false));
            } catch (Exception e) {
              log.warn("Check items failed {}", e.getMessage());
            }
          }

          overseer.getTaskZkWriterExecutor().submit(() -> {
            MDCLoggingContext.setNode(zkController.getNodeName());
            try {
              runAsync(items, fullPaths, data, onStart);
            } catch (Exception e) {
              log.error("failed processing collection queue items " + items, e);
            }
          });
        } finally {
          ourLock.unlock();
        }

      }

      private void runAsync(List<String> items, List<String> fullPaths, Map<String,byte[]> data, boolean onStart) {
        ZkStateWriter zkWriter = overseer.getZkStateWriter();
        if (zkWriter == null) {
          log.warn("Overseer appears closed");
          throw new AlreadyClosedException();
        }

        try (ParWork work = new ParWork(this, false, false)) {
          for (Map.Entry<String,byte[]> entry : data.entrySet()) {
            work.collect("", ()->{
              try {
                byte[] item = entry.getValue();
                if (item == null) {
                  log.error("empty item {}", entry.getKey());
                  return;
                }

                String responsePath = Overseer.OVERSEER_COLLECTION_MAP_COMPLETED + "/" + OverseerTaskQueue.RESPONSE_PREFIX + entry.getKey().substring(entry.getKey().lastIndexOf("-") + 1);

                final ZkNodeProps message = ZkNodeProps.load(item);
                try {
                  String operation = message.getStr(Overseer.QUEUE_OPERATION);

//                  if (onStart) {
//                    log.info("Found operation on start {} {}", responsePath, message);
//
//                    Stat stat = zkController.getZkClient().exists(responsePath, null);
//                    if (stat != null && stat.getDataLength() == 0) {
//                      log.info("Found response and no data on start for {} {}", message, responsePath);
//
//                      OverseerSolrResponse rsp = collMessageHandler.processMessage(message, "cleanup", zkWriter);
//                      if (rsp == null) {
//                      //  zkController.getZkClient().delete(entry.getKey(), -1);
//                        log.info("Set response data since operation looked okay {} {}", message, responsePath);
//                        NamedList response = new NamedList();
//                        response.add("success", true);
//                        OverseerSolrResponse osr = new OverseerSolrResponse(response);
//                        byte[] sdata = OverseerSolrResponseSerializer.serialize(osr);
//                        zkController.getZkClient().setData(responsePath, sdata, true);
//                        return;
//                      } else {
//                        log.info("Tried to cleanup partially executed cmd {} {}", message, responsePath);
//                      }
//                    }
//                  }

                  if (operation == null) {
                    log.error("Msg does not have required " + Overseer.QUEUE_OPERATION + ": {}", message);
                    return;
                  }

                  final String asyncId = message.getStr(ASYNC);

                  OverseerSolrResponse response;
                  if (operation != null && operation.startsWith(CONFIGSETS_ACTION_PREFIX)) {
                    response = configMessageHandler.processMessage(message, operation, zkWriter);
                  } else {
                    response = collMessageHandler.processMessage(message, operation, zkWriter);
                  }

                  if (log.isDebugEnabled()) log.debug("response {}", response);

                  if (response == null) {
                    NamedList nl = new NamedList();
                    nl.add("success", "true");
                    response = new OverseerSolrResponse(nl);
                  } else if (response.getResponse().size() == 0) {
                    response.getResponse().add("success", "true");
                  }

                  if (asyncId != null) {

                    if (log.isDebugEnabled()) {
                      log.debug("Updated completed map for task with zkid:[{}]", asyncId);
                    }
                    completedMap.put(asyncId, OverseerSolrResponseSerializer.serialize(response), CreateMode.PERSISTENT);

                  } else {
                    byte[] sdata = OverseerSolrResponseSerializer.serialize(response);
                    completedMap.update(entry.getKey().substring(entry.getKey().lastIndexOf("-") + 1), sdata);
                    log.info("Completed task:[{}] {} {}", message, response.getResponse(), responsePath);
                  }

                } catch (Exception e) {
                  log.error("Exception processing entry");
                }

              } catch (Exception e) {
                log.error("Exception processing entry", e);
              }
            });

          }
        }
      }
    }
  }
}
