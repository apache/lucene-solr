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
import java.util.function.BiConsumer;

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
import org.apache.solr.core.CloudConfig;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.update.UpdateShardHandler;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.OverseerConfigSetMessageHandler.CONFIGSETS_ACTION_PREFIX;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;

/**
 * <p>Cluster leader. Responsible for processing state updates, node assignments, creating/deleting
 * collections, shards, replicas and setting various properties.</p>
 *
 * <p>The <b>Overseer</b> is a single elected node in the SolrCloud cluster that is in charge of interactions with
 * ZooKeeper that require global synchronization. </p>
 *
 * <p>The Overseer deals with:</p>
 * <ul>
 *   <li>Cluster State updates, i.e. updating Collections' <code>state.json</code> files in ZooKeeper, see {@link ClusterStateUpdater},</li>
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
 *   <li>The {@link ClusterStateUpdater} (also running on the Overseer node) dequeues the state change message and creates the
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
  public static final String QUEUE_OPERATION = "operation";

  public static final String OVERSEER_COLLECTION_QUEUE_WORK = "/overseer/collection-queue-work";

  public static final String OVERSEER_QUEUE = "/overseer/queue";

  public static final String OVERSEER_ASYNC_IDS = "/overseer/async_ids";

  public static final String OVERSEER_COLLECTION_MAP_FAILURE = "/overseer/collection-map-failure";

  public static final String OVERSEER_COLLECTION_MAP_COMPLETED = "/overseer/collection-map-completed";

  public static final String OVERSEER_COLLECTION_MAP_RUNNING = "/overseer/collection-map-running";


  // System properties are used in tests to make them run fast
  public static final int STATE_UPDATE_DELAY = ZkStateReader.STATE_UPDATE_DELAY;
  public static final int STATE_UPDATE_BATCH_SIZE = Integer.getInteger("solr.OverseerStateUpdateBatchSize", 10000);
  public static final int STATE_UPDATE_MAX_QUEUE = 20000;

  public static final int NUM_RESPONSES_TO_STORE = 10000;
  public static final String OVERSEER_ELECT = "/overseer/overseer_elect";

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private volatile boolean closeAndDone;
  private volatile boolean initedHttpClient = false;
  private volatile QueueWatcher queueWatcher;
  private volatile CollectionWorkQueueWatcher collectionQueueWatcher;

  public boolean isDone() {
    return closeAndDone;
  }


  //  public ExecutorService getTaskExecutor() {
//    return taskExecutor;
//  }

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

  /**
   * <p>This class is responsible for dequeueing state change requests from the ZooKeeper queue at <code>/overseer/queue</code>
   * and executing the requested cluster change (essentially writing or updating <code>state.json</code> for a collection).</p>
   *
   * <p>The cluster state updater is a single thread dequeueing and executing requests.</p>
   */
  private class ClusterStateUpdater implements Runnable, Closeable {

    private final ZkStateReader reader;
    private final SolrZkClient zkClient;
    private final String myId;
    //queue where everybody can throw tasks
    private final ZkDistributedQueue stateUpdateQueue;


    private volatile boolean isClosed = false;

    public ClusterStateUpdater(final ZkStateReader reader, final String myId, Stats zkStats) {
      this.zkClient = reader.getZkClient();
      this.stateUpdateQueue = getStateUpdateQueue(zkStats);
      this.myId = myId;
      this.reader = reader;
    }

    @Override
    public void run() {
      if (log.isDebugEnabled()) {
        log.debug("Overseer run() - start");
      }

      MDCLoggingContext.setNode(zkController.getNodeName() );
      try {


        log.info("Starting to work on the main queue : {}", LeaderElector.getNodeName(myId));


//        ClusterState clusterState = reader.getClusterState();
//        assert clusterState != null;

        // we write updates in batch, but if an exception is thrown when writing new clusterstate,
        // we do not sure which message is bad message, therefore we will re-process node one by one

        while (!this.isClosed) {

            LinkedList<Pair<String,byte[]>> queue = null;
            try {
              // We do not need to filter any nodes here cause all processed nodes are removed once we flush clusterstate
              queue = new LinkedList<>(stateUpdateQueue.peekElements(1000, 3000L, (x) -> true));

              log.info("peeking at the status update queue {}", printQueue(queue));
            } catch (InterruptedException | AlreadyClosedException e) {
              ParWork.propagateInterrupt(e, true);
              log.error("Unexpected error in Overseer state update loop", e);
              return;
            } catch (KeeperException.SessionExpiredException e) {
              log.error("run()", e);

              log.warn("Solr cannot talk to ZK, exiting Overseer work queue loop", e);
              return;
            } catch (Exception e) {
              ParWork.propagateInterrupt(e, true);
              log.error("Unexpected error in Overseer state update loop", e);
              return;
            }
            try {
              Set<String> processedNodes = new HashSet<>();
              while (queue != null && !queue.isEmpty()) {
                for (Pair<String,byte[]> head : queue) {
                  byte[] data = head.second();
                  if (log.isDebugEnabled()) log.debug("look at node {} data={}", head.first(), head.second() == null ? null : head.second().length);

                  final ZkNodeProps message = ZkNodeProps.load(data);
                  if (log.isDebugEnabled()) log.debug("processMessage: queueSize: {}, message = {}", stateUpdateQueue.getZkStats().getQueueLength(), message);
                  if (log.isDebugEnabled()) log.debug("add processed node: {}, processedNodes = {}", head.first(), stateUpdateQueue.getZkStats().getQueueLength(), processedNodes);
                  processedNodes.add(head.first());

                  // The callback always be called on this thread
                  boolean success = processQueueItem(message);
                  if (success) {
                    // nocommit
                    stateUpdateQueue.remove(processedNodes);
                    processedNodes.clear();
                  }

                }
                //    if (isClosed) break;
                // if an event comes in the next 100ms batch it together
                log.info("peekElements");
                queue = new LinkedList<>(stateUpdateQueue.peekElements(1000, 100, node -> !processedNodes.contains(node)));
              }

              // we should force write all pending updates because the next iteration might sleep until there
              // are more items in the main queue
              log.info("writePending updates");
              writePendingUpdates();
              // clean work queue
              log.info("clean work queue");
              stateUpdateQueue.remove(processedNodes);
              processedNodes.clear();
            } catch (InterruptedException | AlreadyClosedException e) {
              Thread.currentThread().interrupt();
              return;
            } catch (KeeperException.SessionExpiredException e) {
              log.error("run()", e);

              log.warn("Solr cannot talk to ZK, exiting Overseer work queue loop", e);
              return;
            } catch (Exception e) {
              log.error("", e);
              throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
            }
          }
        } finally {
        log.info("Overseer Loop exiting : {}", LeaderElector.getNodeName(myId));

        if (!isClosed) {
          Overseer.this.close();
        }
      }

      if (log.isDebugEnabled()) {
        log.debug("run() - end");
      }
    }

    private boolean checkClosed() {
      boolean closed = isClosed();
      if (closed) {
        log.info("Overseer is closed, will not continue loop ...");
      }
      return closed;
    }

    // Return true whenever the exception thrown by ZkStateWriter is correspond
    // to a invalid state or 'bad' message (in this case, we should remove that message from queue)
    private boolean isBadMessage(Exception e) {
      if (log.isDebugEnabled()) {
        log.debug("isBadMessage(Exception e={}) - start", e);
      }

      if (e instanceof KeeperException) {
        KeeperException ke = (KeeperException) e;
        boolean isBadMessage = ke.code() == KeeperException.Code.NONODE || ke.code() == KeeperException.Code.NODEEXISTS;
        if (log.isDebugEnabled()) {
          log.debug("isBadMessage(Exception)={} - end", isBadMessage);
        }
        return isBadMessage;
      }
      if (log.isDebugEnabled()) {
        log.debug("isBadMessage(Exception)=false - end");
      }
      return false;
    }

    @Override
    public void close() {
      if (log.isDebugEnabled()) {
        log.debug("close() - start");
      }
      this.isClosed = true;
      if (log.isDebugEnabled()) {
        log.debug("close() - end");
      }
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

  private volatile ZkStateWriter zkStateWriter;

  private final ZkStateReader reader;

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
  public Overseer(UpdateShardHandler updateShardHandler, String adminPath,
      final ZkStateReader reader, ZkController zkController, CloudConfig config) {
    this.reader = reader;
    this.updateShardHandler = updateShardHandler;
    this.adminPath = adminPath;
    this.zkController = zkController;
    this.stats = new Stats();
    this.config = config;
  }

  public synchronized void start(String id, ElectionContext context) throws KeeperException {
    log.info("Staring Overseer");
    if (getCoreContainer().isShutDown() || closeAndDone) {
      if (log.isDebugEnabled()) log.debug("Already closed, exiting");
      return;
    }

    doClose();
    closed = false;

    MDCLoggingContext.setNode(zkController == null ?
        null :
        zkController.getNodeName());

    this.id = id;
//
//     stateManagmentExecutor = ParWork.getParExecutorService("stateManagmentExecutor",
//        1, 1, 3000, new SynchronousQueue());
//     taskExecutor = ParWork.getParExecutorService("overseerTaskExecutor",
//        4, 16, 3000, new SynchronousQueue());

//    try {
//      if (context != null) context.close();
//    } catch (Exception e) {
//      log.error("", e);
//    }
    if (overseerOnlyClient == null && !closeAndDone && !initedHttpClient) {
      overseerOnlyClient = new Http2SolrClient.Builder().idleTimeout(500000).markInternalRequest().build();
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


    stats = new Stats();
    log.info("Overseer (id={}) starting", id);
    //launch cluster state updater thread

    ThreadGroup ccTg = new ThreadGroup("Overseer collection creation process.");


    this.zkStateWriter = new ZkStateWriter(reader, stats);
    //systemCollectionCompatCheck(new StringBiConsumer());

    queueWatcher = new WorkQueueWatcher(getCoreContainer());
    collectionQueueWatcher = new CollectionWorkQueueWatcher(getCoreContainer(), id, overseerLbClient, adminPath, stats, Overseer.this);

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
      if (s.getLeader() == null || !s.getLeader().isActive(clusterState.getLiveNodes())) {
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
    this.closeAndDone = true;
    this.closed = true;
  }

  public boolean isCloseAndDone() {
    return closeAndDone;
  }

  public void close() {
    close(false);
  }

  public void close(boolean fromCSUpdateThread) {
    log.info("Overseer (id={}) closing closeAndDone={} frp,CSUpdateThread={}", id, closeAndDone, fromCSUpdateThread);

    if (closeAndDone) {
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
    } else {
      if (!zkController.getCoreContainer().isShutDown() && !zkController.isShudownCalled() && !zkController.isClosed()) {
        log.info("rejoining the overseer election after closing");
        zkController.rejoinOverseerElection( false);
      }
    }
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  void doClose() {
    doClose(false);
  }

  void doClose(boolean fromCSUpdateThread) {
    closed = true;

    log.info("doClose() - start fromCSUpdateThread={}  closeAndDone={}", fromCSUpdateThread, closeAndDone);
    this.zkStateWriter  = null;

    if (queueWatcher != null) {
      queueWatcher.close();
    }

    if (collectionQueueWatcher != null) {
      collectionQueueWatcher.close();
    }


//    if (stateManagmentExecutor != null) {
//      log.info("shutdown stateManagmentExecutor");
//      stateManagmentExecutor.shutdown();
//    }
//
//    if (taskExecutor != null) {
//      log.info("shutdown taskExecutor");
//      taskExecutor.shutdown();
//    }

//    if (stateManagmentExecutor != null) {
//      stateManagmentExecutor.shutdownNow();
//    }

 //   ExecutorUtil.shutdownAndAwaitTermination(stateManagmentExecutor );


//    if (taskExecutor != null) {
//      taskExecutor.shutdownNow();
//    }

 //   ExecutorUtil.shutdownAndAwaitTermination(taskExecutor );

    if (log.isDebugEnabled()) {
      log.debug("doClose() - end");
    }

    assert ObjectReleaseTracker.release(this);
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
    return new ZkDistributedQueue(reader.getZkClient(), "/overseer/queue", zkStats, STATE_UPDATE_MAX_QUEUE, new ConnectionManager.IsClosed(){
      public boolean isClosed() {
        return Overseer.this.isClosed() || zkController.getCoreContainer().isShutDown(); // nocommit use
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
    return reader;
  }

  public ZkStateWriter getZkStateWriter() {
    return zkStateWriter;
  }

  public void offerStateUpdate(byte[] data) throws KeeperException, InterruptedException {
    getStateUpdateQueue().offer(data);
  }

  public boolean processQueueItem(ZkNodeProps message) throws InterruptedException {
    log.info("processQueueItem {}", message);
    // nocommit - may not need this now
   new OverseerTaskExecutorTask(getCoreContainer(), zkStateWriter, message).run();
//    try {
//      future.get();
//    } catch (ExecutionException e) {
//      log.error("", e);
//      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
//    }
    return true;
  }

  public void writePendingUpdates() throws InterruptedException {
    log.info("writePendingUpdates ");

    new OverseerTaskExecutorTask.WriteTask(getCoreContainer(), zkStateWriter).run();
//    try {
//      future.get();
//    } catch (ExecutionException e) {
//      log.error("", e);
//      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
//    }

  }

  // nocommit use
  private static class OverseerWatcher implements Watcher, Closeable {

    private final CoreContainer cc;
    private final ZkController zkController;
    private final String path = Overseer.OVERSEER_ELECT + "/leader";
    private final Overseer overseer;

    public OverseerWatcher(CoreContainer cc) {
      this.cc = cc;
      this.zkController = cc.getZkController();
      this.overseer = zkController.getOverseer();
    }

    @Override
    public void process(WatchedEvent event) {
      try {
        if (log.isDebugEnabled()) {
          log.debug("set watch on leader znode");
        }
        zkController.getZkClient().exists(path, new Watcher() {

          @Override
          public void process(WatchedEvent event) {
            if (Event.EventType.None.equals(event.getType())) {
              return;
            }

            log.info("Overseer leader has changed, closing ...");
            overseer.close();

          }}, true);
      } catch (KeeperException.SessionExpiredException e) {
        log.warn("ZooKeeper session expired");
        overseer.doClose(false);
        return;
      } catch (InterruptedException | AlreadyClosedException e) {
        log.info("Already closed");
        overseer.doClose(false);
        return;
      } catch (Exception e) {
        log.error("Unexpected error in Overseer state update loop", e);
        overseer.doClose(false);
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }
    }

    @Override
    public void close() throws IOException {
      try {
        zkController.getZkClient().getSolrZooKeeper().removeWatches(path, this, WatcherType.Data, true);
      } catch (Exception e) {
        log.info("", e.getMessage());
      }
    }
  }

  private static abstract class QueueWatcher implements Watcher, Closeable {

    protected final CoreContainer cc;
    protected final ZkController zkController;
    protected final String path;
    protected final Overseer overseer;
    protected volatile boolean closed;

    public QueueWatcher(CoreContainer cc, String path) throws KeeperException {
      this.cc = cc;
      this.zkController = cc.getZkController();
      this.overseer = zkController.getOverseer();
      this.path = path;
      List<String> items = setWatch();
      log.info("Overseer found entries on start {}", items);
      processQueueItems(items);
    }

    private List<String> setWatch() {
      try {

        log.info("set watch on Overseer work queue {}", path);

        List<String> children = zkController.getZkClient().getChildren(path, this, true);
        Collections.sort(children);
        return children;
      } catch (KeeperException.SessionExpiredException e) {
        log.warn("ZooKeeper session expired");
        overseer.doClose(false);
        return null;
      } catch (InterruptedException | AlreadyClosedException e) {
        log.info("Already closed");
        overseer.doClose(false);
        return null;
      } catch (Exception e) {
        log.error("Unexpected error in Overseer state update loop", e);
        overseer.doClose(false);
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }
    }

    @Override
    public synchronized void process(WatchedEvent event) {
      if (Event.EventType.None.equals(event.getType())) {
        return;
      }
      if (this.closed) {
        return;
      }

      log.info("Overseer work queue has changed, processing...");

      try {
        List<String> items = setWatch();

        processQueueItems(items);
      } catch (Exception e) {
        log.error("Exception during overseer queue queue processing", e);
      }

    }

    protected abstract void processQueueItems(List<String> items) throws KeeperException;

    @Override
    public void close() {
      this.closed = true;
      try {
        zkController.getZkClient().getSolrZooKeeper().removeWatches(path, this, WatcherType.Data, true);
      } catch (Exception e) {
        log.info("", e.getMessage());
      }
    }
  }

  private static class WorkQueueWatcher extends QueueWatcher {

    public WorkQueueWatcher(CoreContainer cc) throws KeeperException {
      super(cc, Overseer.OVERSEER_QUEUE);
    }

    @Override
    protected void processQueueItems(List<String> items) {
      log.info("Found state update queue items {}", items);
      List<String> fullPaths = new ArrayList<>(items.size());
      for (String item : items) {
        fullPaths.add(path + "/" + item);
      }

      Map<String,byte[]> data = zkController.getZkClient().getData(fullPaths);

      for (byte[] item : data.values()) {
        final ZkNodeProps message = ZkNodeProps.load(item);
        try {
          boolean success = overseer.processQueueItem(message);
        } catch (InterruptedException e) {
          log.error("Overseer state update queue processing interrupted");
          return;
        }
      }

      try {
        overseer.writePendingUpdates();
      } catch (InterruptedException e) {
        log.error("Overseer state update queue processing interrupted");
        return;
      }

      zkController.getZkClient().delete(fullPaths, true);
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
      failureMap = Overseer.getFailureMap(cc.getZkController().getZkStateReader().getZkClient());
      runningMap = Overseer.getRunningMap(cc.getZkController().getZkStateReader().getZkClient());
      completedMap = Overseer.getCompletedMap(cc.getZkController().getZkStateReader().getZkClient());
    }

    @Override
    public void close() {
      super.close();
      IOUtils.closeQuietly(collMessageHandler);
      IOUtils.closeQuietly(configMessageHandler);
    }

    @Override
    protected synchronized void processQueueItems(List<String> items) throws KeeperException {
      log.info("Found collection queue items {}", items);
      List<String> fullPaths = new ArrayList<>(items.size());
      for (String item : items) {
        fullPaths.add(path + "/" + item);
      }

      Map<String,byte[]> data = zkController.getZkClient().getData(fullPaths);

      ParWork.getRootSharedExecutor().submit(()->{
        try {
          runAsync(items, fullPaths, data);
        } catch (Exception e) {
          log.error("failed processing collection queue items " + items);
        }
      });

    }

    private void runAsync(List<String> items, List<String> fullPaths, Map<String,byte[]> data) throws KeeperException {
      for (Map.Entry<String,byte[]> entry : data.entrySet()) {
        byte[] item = entry.getValue();
        if (item == null) {
          log.error("empty item {}", entry.getKey());
          continue;
        }

        final ZkNodeProps message = ZkNodeProps.load(item);
        try {
          String operation = message.getStr(Overseer.QUEUE_OPERATION);
          if (operation == null) {
            log.error("Msg does not have required " + Overseer.QUEUE_OPERATION + ": {}", message);
            continue;
          }

          final String asyncId = message.getStr(ASYNC);

          OverseerSolrResponse response;
          if (operation != null && operation.startsWith(CONFIGSETS_ACTION_PREFIX)) {
            response = configMessageHandler.processMessage(message, operation);
          } else {
            response = collMessageHandler.processMessage(message, operation);
          }


//          try {
//            overseer.writePendingUpdates();
//          } catch (InterruptedException e) {
//            log.error("Overseer state update queue processing interrupted");
//            return;
//          }

          log.info("response {}", response);


          if (asyncId != null) {
            if (response != null && (response.getResponse().get("failure") != null || response.getResponse().get("exception") != null)) {
              if (log.isDebugEnabled()) {
                log.debug("Updated failed map for task with id:[{}]", asyncId);
              }
              failureMap.put(asyncId, OverseerSolrResponseSerializer.serialize(response));
            } else {
              if (log.isDebugEnabled()) {
                log.debug("Updated completed map for task with zkid:[{}]", asyncId);
              }
              completedMap.put(asyncId, OverseerSolrResponseSerializer.serialize(response));

            }
          } else {
            byte[] sdata = OverseerSolrResponseSerializer.serialize(response);
            String responsePath = Overseer.OVERSEER_COLLECTION_MAP_COMPLETED + "/" + OverseerTaskQueue.RESPONSE_PREFIX
                + entry.getKey().substring(entry.getKey().lastIndexOf("-") + 1);
            zkController.getZkClient().setData( responsePath, sdata, true);
            log.debug("Completed task:[{}] {}", message, response.getResponse());
          }


        } catch (InterruptedException e) {
          log.error("Overseer state update queue processing interrupted");
          return;
        }
      }

      for (String item : items) {
        if (item.startsWith("qnr-")) {
          fullPaths.remove(path + "/" + item);
        }
      }

      zkController.getZkClient().delete(fullPaths, true);
    }
  }

}
