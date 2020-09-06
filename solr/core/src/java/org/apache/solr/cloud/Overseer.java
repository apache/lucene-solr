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
import org.apache.solr.client.solrj.impl.ClusterStateProvider;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.cloud.api.collections.CreateCollectionCmd;
import org.apache.solr.cloud.api.collections.OverseerCollectionMessageHandler;
import org.apache.solr.cloud.overseer.ClusterStateMutator;
import org.apache.solr.cloud.overseer.CollectionMutator;
import org.apache.solr.cloud.overseer.NodeMutator;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.cloud.overseer.ReplicaMutator;
import org.apache.solr.cloud.overseer.SliceMutator;
import org.apache.solr.cloud.overseer.ZkStateWriter;
import org.apache.solr.cloud.overseer.ZkWriteCommand;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrCloseable;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.ConnectionManager;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.Pair;
import org.apache.solr.core.CloudConfig;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.handler.component.HttpShardHandler;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.update.UpdateShardHandler;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CommonParams.ID;
import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * <p>Cluster leader. Responsible for processing state updates, node assignments, creating/deleting
 * collections, shards, replicas and setting various properties.</p>
 *
 * <p>The <b>Overseer</b> is a single elected node in the SolrCloud cluster that is in charge of interactions with
 * ZooKeeper that require global synchronization. It also hosts the Collection API implementation and the
 * Autoscaling framework.</p>
 *
 * <p>The Overseer deals with:</p>
 * <ul>
 *   <li>Cluster State updates, i.e. updating Collections' <code>state.json</code> files in ZooKeeper, see {@link ClusterStateUpdater},</li>
 *   <li>Collection API implementation, including Autoscaling replica placement computation, see
 *   {@link OverseerCollectionConfigSetProcessor} and {@link OverseerCollectionMessageHandler} (and the example below),</li>
 *   <li>Updating Config Sets, see {@link OverseerCollectionConfigSetProcessor} and {@link OverseerConfigSetMessageHandler},</li>
 *   <li>Autoscaling triggers, see {@link org.apache.solr.cloud.autoscaling.OverseerTriggerThread}.</li>
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

  public static final String OVERSEER_QUEUE_WORK = "/overseer/queue-work";

  // System properties are used in tests to make them run fast
  public static final int STATE_UPDATE_DELAY = ZkStateReader.STATE_UPDATE_DELAY;
  public static final int STATE_UPDATE_BATCH_SIZE = Integer.getInteger("solr.OverseerStateUpdateBatchSize", 10000);
  public static final int STATE_UPDATE_MAX_QUEUE = 20000;

  public static final int NUM_RESPONSES_TO_STORE = 10000;
  public static final String OVERSEER_ELECT = "/overseer/overseer_elect";

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private volatile OverseerElectionContext context;
  private volatile boolean closeAndDone;

  public boolean isDone() {
    return closeAndDone;
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
    //TODO remove in 9.0, we do not push message into this queue anymore
    //Internal queue where overseer stores events that have not yet been published into cloudstate
    //If Overseer dies while extracting the main queue a new overseer will start from this queue
    private final ZkDistributedQueue workQueue;

    private volatile boolean isClosed = false;

    public ClusterStateUpdater(final ZkStateReader reader, final String myId, Stats zkStats) {
      this.zkClient = reader.getZkClient();
      this.stateUpdateQueue = getStateUpdateQueue(zkStats);
      this.workQueue = getInternalWorkQueue(zkClient, zkStats);
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

        ZkStateWriter zkStateWriter = null;
        ClusterState clusterState = reader.getClusterState();
        assert clusterState != null;

        // we write updates in batch, but if an exception is thrown when writing new clusterstate,
        // we do not sure which message is bad message, therefore we will re-process node one by one
        int fallbackQueueSize = Integer.MAX_VALUE;
        ZkDistributedQueue fallbackQueue = workQueue;
        while (!isClosed() && !Thread.currentThread().isInterrupted()) {
          if (zkStateWriter == null) {
            try {
              zkStateWriter = new ZkStateWriter(reader, stats);
            //  clusterState = reader.getClusterState();
              // if there were any errors while processing
              // the state queue, items would have been left in the
              // work queue so let's process those first
              byte[] data = fallbackQueue.peek();
              clusterState = getZkStateReader().getClusterState();
              while (fallbackQueueSize > 0 && data != null) {
                final ZkNodeProps message = ZkNodeProps.load(data);
                if (log.isDebugEnabled()) log.debug("processMessage: fallbackQueueSize: {}, message = {}", fallbackQueue.getZkStats().getQueueLength(), message);
                // force flush to ZK after each message because there is no fallback if workQueue items
                // are removed from workQueue but fail to be written to ZK
                try {
                  processQueueItem(message, getZkStateReader().getClusterState(), zkStateWriter, false, null);
                } catch (InterruptedException | AlreadyClosedException e) {
                  ParWork.propegateInterrupt(e);
                  return;
                } catch (KeeperException.SessionExpiredException e) {
                  log.warn("Solr cannot talk to ZK, exiting Overseer work queue loop", e);
                  return;
                } catch (Exception e) {
                  SolrException exp = new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
                  try {
                    if (isBadMessage(e)) {
                      log.warn(
                              "Exception when process message = {}, consider as bad message and poll out from the queue",
                              message);
                      fallbackQueue.poll();
                    }
                  } catch (InterruptedException e1) {
                    ParWork.propegateInterrupt(e);
                    return;
                  } catch (Exception e1) {
                    exp.addSuppressed(e1);
                  }
                  throw exp;
                }
                fallbackQueue.poll(); // poll-ing removes the element we got by peek-ing
                data = fallbackQueue.peek();
                fallbackQueueSize--;
              }
              // force flush at the end of the loop, if there are no pending updates, this is a no op call
              clusterState = zkStateWriter.writePendingUpdates(clusterState);
              assert clusterState != null;
              // the workQueue is empty now, use stateUpdateQueue as fallback queue
              fallbackQueue = stateUpdateQueue;
              fallbackQueueSize = 0;
            } catch (KeeperException.SessionExpiredException e) {
              log.error("run()", e);

              log.warn("Solr cannot talk to ZK, exiting Overseer work queue loop", e);
              return;
            } catch (InterruptedException | AlreadyClosedException e) {
              ParWork.propegateInterrupt(e, true);
              return;
            } catch (Exception e) {
              log.error("Unexpected error in Overseer state update loop", e);
              return;
//              if (!isClosed()) {
//                continue;
//              }
            }
          }

          LinkedList<Pair<String, byte[]>> queue = null;
          try {
            // We do not need to filter any nodes here cause all processed nodes are removed once we flush clusterstate

            long wait = 10000;
//            if (zkStateWriter.getUpdatesToWrite().isEmpty()) {
//              wait = 100;
//            } else {
//              wait = 0;
//            }
            queue = new LinkedList<>(stateUpdateQueue.peekElements(1000, wait, (x) -> true));
          } catch (InterruptedException | AlreadyClosedException e) {
            ParWork.propegateInterrupt(e, true);
            return;
          } catch (KeeperException.SessionExpiredException e) {
            log.error("run()", e);

            log.warn("Solr cannot talk to ZK, exiting Overseer work queue loop", e);
            return;
          } catch (Exception e) {
            log.error("Unexpected error in Overseer state update loop", e);
            if (!isClosed()) {
              continue;
            } else {
              return;
            }
          }
          try {
            Set<String> processedNodes = new HashSet<>();
            while (queue != null && !queue.isEmpty()) {
              if (Thread.currentThread().isInterrupted() || isClosed) {
                log.info("Closing");
                return;
              }
              for (Pair<String, byte[]> head : queue) {
                byte[] data = head.second();
                final ZkNodeProps message = ZkNodeProps.load(data);
                // log.debug("processMessage: queueSize: {}, message = {} current state version: {}", stateUpdateQueue.getZkStats().getQueueLength(), message, clusterState.getZkClusterStateVersion());

                processedNodes.add(head.first());
                fallbackQueueSize = processedNodes.size();
                // The callback always be called on this thread
                  processQueueItem(message, getZkStateReader().getClusterState(), zkStateWriter, true, () -> {
                  stateUpdateQueue.remove(processedNodes);
                  processedNodes.clear();
                });
              }
              if (isClosed()) return;
              // if an event comes in the next *ms batch it together
              int wait = 0;
//              if (zkStateWriter.getUpdatesToWrite().isEmpty()) {
//                wait = 10000;
//              } else {
//                wait = 0;
//              }
              queue = new LinkedList<>(stateUpdateQueue.peekElements(1000, wait, node -> !processedNodes.contains(node)));
            }
            fallbackQueueSize = processedNodes.size();
            // we should force write all pending updates because the next iteration might sleep until there
            // are more items in the main queue
            clusterState = zkStateWriter.writePendingUpdates(clusterState);

            // clean work queue
            stateUpdateQueue.remove(processedNodes);
            processedNodes.clear();
          } catch (InterruptedException | AlreadyClosedException e) {
            ParWork.propegateInterrupt(e, true);
            return;
          } catch (KeeperException.SessionExpiredException e) {
            log.error("run()", e);

            log.warn("Solr cannot talk to ZK, exiting Overseer work queue loop", e);
            return;
          } catch (Exception e) {
            log.error("Unexpected error in Overseer state update loop, exiting ...", e);
            return;
          }
        }
      } finally {
        log.info("Overseer Loop exiting : {}", LeaderElector.getNodeName(myId));

        if (!isClosed && !closeAndDone) { // if we have not been closed, close so that we stop the other threads
          Overseer.this.close(true);
        }
      }

      if (log.isDebugEnabled()) {
        log.debug("run() - end");
      }
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

    private ClusterState processQueueItem(ZkNodeProps message, ClusterState clusterState, ZkStateWriter zkStateWriter, boolean enableBatching, ZkStateWriter.ZkWriteCallback callback) throws Exception {
      if (log.isDebugEnabled()) log.debug("Consume state update from queue {}", message);
     // assert clusterState != null;

      ClusterState cs = null;
    //  if (clusterState.getZNodeVersion() == 0 || clusterState.getZNodeVersion() > lastVersion) {


        final String operation = message.getStr(QUEUE_OPERATION);
        if (operation == null) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Message missing " + QUEUE_OPERATION + ":" + message);
        }

      ClusterState state = reader.getClusterState();
      LinkedHashMap collStates = new LinkedHashMap<>();

      Map<String,DocCollection> updatesToWrite = zkStateWriter
          .getUpdatesToWrite();
      for (DocCollection docCollection : updatesToWrite.values()) {
        Map<String,Slice> slicesMap = docCollection.getSlicesMap();
        for (Slice slice : slicesMap.values()) {
          Collection<Replica> existingReplicas = slice.getReplicas();
          for (Replica ereplica : existingReplicas) {
            if (!docCollection.getReplicas().contains(ereplica)) {
              Map<String,Replica> replicas = new HashMap<>(slice.getReplicasMap());
              replicas.put(ereplica.getName(), ereplica);
              slicesMap.put(slice.getName(), new Slice(slice.getName(), replicas, slice.getProperties(), docCollection.getName()));
            }
          }

          collStates.put(docCollection.getName(), new ClusterState.CollectionRef(new DocCollection(docCollection.getName(),
              slicesMap, docCollection.getProperties(), docCollection.getRouter(), docCollection.getZNodeVersion(), docCollection.getZNode())));
        }
      }

      ClusterState prevState = new ClusterState(state.getLiveNodes(),
          collStates, state.getZNodeVersion());

        List<ZkWriteCommand> zkWriteOps = processMessage(updatesToWrite.isEmpty() ? state : prevState, message, operation);

        cs = zkStateWriter.enqueueUpdate(clusterState, zkWriteOps,
                () -> {
                  // log.info("on write callback");
                });
    //  }

      return cs;
    }

    private List<ZkWriteCommand> processMessage(ClusterState clusterState,
                                                final ZkNodeProps message, final String operation) {
      if (log.isDebugEnabled()) {
        log.debug("processMessage(ClusterState clusterState={}, ZkNodeProps message={}, String operation={}) - start", clusterState, message, operation);
      }

      CollectionParams.CollectionAction collectionAction = CollectionParams.CollectionAction.get(operation);
      if (collectionAction != null) {
        switch (collectionAction) {
          case CREATE:
            return Collections.singletonList(new ClusterStateMutator(getSolrCloudManager()).createCollection(clusterState, message));
          case DELETE:
            return Collections.singletonList(new ClusterStateMutator(getSolrCloudManager()).deleteCollection(clusterState, message));
          case CREATESHARD:
            return Collections.singletonList(new CollectionMutator(getSolrCloudManager()).createShard(clusterState, message));
          case DELETESHARD:
            return Collections.singletonList(new CollectionMutator(getSolrCloudManager()).deleteShard(clusterState, message));
          case ADDREPLICA:
            return Collections.singletonList(new SliceMutator(getSolrCloudManager()).addReplica(clusterState, message));
          case ADDREPLICAPROP:
            return Collections.singletonList(new ReplicaMutator(getSolrCloudManager()).addReplicaProperty(clusterState, message));
          case DELETEREPLICAPROP:
            return Collections.singletonList(new ReplicaMutator(getSolrCloudManager()).deleteReplicaProperty(clusterState, message));
          case BALANCESHARDUNIQUE:
            ExclusiveSliceProperty dProp = new ExclusiveSliceProperty(clusterState, message);
            if (dProp.balanceProperty()) {
              String collName = message.getStr(ZkStateReader.COLLECTION_PROP);
              List<ZkWriteCommand> returnList = Collections.singletonList(new ZkWriteCommand(collName, dProp.getDocCollection()));
              if (log.isDebugEnabled()) {
                log.debug("processMessage(ClusterState, ZkNodeProps, String) - end");
              }
              return returnList;
            }
            break;
          case MODIFYCOLLECTION:
            CollectionsHandler.verifyRuleParams(zkController.getCoreContainer(), message.getProperties());
            ZkWriteCommand zkwrite = new CollectionMutator(getSolrCloudManager()).modifyCollection(clusterState, message);
            return Collections.singletonList(zkwrite);
          case MIGRATESTATEFORMAT:
            return Collections.singletonList(new ClusterStateMutator(getSolrCloudManager()).migrateStateFormat(clusterState, message));
          default:
            throw new RuntimeException("unknown operation:" + operation
                    + " contents:" + message.getProperties());
        }
      } else {
        OverseerAction overseerAction = OverseerAction.get(operation);
        if (overseerAction == null) {
          throw new RuntimeException("unknown operation:" + operation + " contents:" + message.getProperties());
        }
        switch (overseerAction) {
          case STATE:
            return Collections.singletonList(new ReplicaMutator(getSolrCloudManager()).setState(clusterState, message));
          case LEADER:
            return Collections.singletonList(new SliceMutator(getSolrCloudManager()).setShardLeader(clusterState, message));
          case DELETECORE:
            return Collections.singletonList(new SliceMutator(getSolrCloudManager()).removeReplica(clusterState, message));
          case ADDROUTINGRULE:
            return Collections.singletonList(new SliceMutator(getSolrCloudManager()).addRoutingRule(clusterState, message));
          case REMOVEROUTINGRULE:
            return Collections.singletonList(new SliceMutator(getSolrCloudManager()).removeRoutingRule(clusterState, message));
          case UPDATESHARDSTATE:
            return Collections.singletonList(new SliceMutator(getSolrCloudManager()).updateShardState(clusterState, message));
          case QUIT:
            if (myId.equals(message.get(ID))) {
              log.info("Quit command received {} {}", message, LeaderElector.getNodeName(myId));
              overseerCollectionConfigSetProcessor.close();
              close();
            } else {
              log.warn("Overseer received wrong QUIT message {}", message);
            }
            break;
          case DOWNNODE:
            return new NodeMutator().downNode(clusterState, message);
          default:
            throw new RuntimeException("unknown operation:" + operation + " contents:" + message.getProperties());
        }
      }

      List<ZkWriteCommand> returnList = Collections.singletonList(ZkStateWriter.NO_OP);
      if (log.isDebugEnabled()) {
        log.debug("processMessage(ClusterState, ZkNodeProps, String) - end");
      }
      return returnList;
    }

    @Override
    public void close() {
      if (log.isDebugEnabled()) {
        log.debug("close() - start");
      }
      //ExecutorUtil.shutdownAndAwaitTermination(executor);
      this.isClosed = true;

      if (log.isDebugEnabled()) {
        log.debug("close() - end");
      }
    }

  }

  public static class OverseerThread extends Thread implements Closeable {

    protected volatile boolean isClosed;
    private final Closeable thread;

    public OverseerThread(ThreadGroup ccTg, Closeable thread, String name) {
      super(ccTg, (Runnable) thread, name);
      this.thread = thread;
    }

    @Override
    public void run() {
      try {
        super.run();
      } finally {
        ParWork.closeMyPerThreadExecutor();
      }
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

  private volatile OverseerThread ccThread;

  private volatile OverseerThread updaterThread;

  private volatile OverseerThread triggerThread;

  private final ZkStateReader reader;

  private final HttpShardHandler shardHandler;

  private final UpdateShardHandler updateShardHandler;

  private final String adminPath;

  private volatile OverseerCollectionConfigSetProcessor overseerCollectionConfigSetProcessor;

  private final ZkController zkController;

  private volatile Stats stats;
  private volatile String id;
  private volatile boolean closed;
  private volatile boolean systemCollCompatCheck = true;

  private final CloudConfig config;

  // overseer not responsible for closing reader
  public Overseer(HttpShardHandler shardHandler,
      UpdateShardHandler updateShardHandler, String adminPath,
      final ZkStateReader reader, ZkController zkController, CloudConfig config) {
    this.reader = reader;
    this.shardHandler = shardHandler;
    this.updateShardHandler = updateShardHandler;
    this.adminPath = adminPath;
    this.zkController = zkController;
    this.stats = new Stats();
    this.config = config;

  }

  public synchronized void start(String id, ElectionContext context) throws KeeperException {
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
    this.context = (OverseerElectionContext) context;

//    try {
//      if (context != null) context.close();
//    } catch (Exception e) {
//      log.error("", e);
//    }


    try {
      if (log.isDebugEnabled()) {
        log.debug("set watch on leader znode");
      }
      zkController.getZkClient().exists(Overseer.OVERSEER_ELECT + "/leader", new Watcher() {

        @Override
        public void process(WatchedEvent event) {
          if (Event.EventType.None.equals(event.getType())) {
            return;
          }
          if (!isClosed()) {
            log.info("Overseer leader has changed, closing ...");
            Overseer.this.close();
          }
        }});
    } catch (KeeperException.SessionExpiredException e) {
      log.warn("ZooKeeper session expired");
      return;
    } catch (InterruptedException | AlreadyClosedException e) {
      ParWork.propegateInterrupt(e);
      return;
    } catch (Exception e) {
      ParWork.propegateInterrupt(e);
      log.error("Unexpected error in Overseer state update loop", e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }

    stats = new Stats();
    log.info("Overseer (id={}) starting", id);
    //launch cluster state updater thread
    ThreadGroup tg = new ThreadGroup("Overseer state updater.");
    updaterThread = new OverseerThread(tg, new ClusterStateUpdater(reader, id, stats), "OverseerStateUpdate-" + id);
    updaterThread.setDaemon(true);

    ThreadGroup ccTg = new ThreadGroup("Overseer collection creation process.");

    // nocommit - I don't know about this guy..
    OverseerNodePrioritizer overseerPrioritizer = null; // new OverseerNodePrioritizer(reader, getStateUpdateQueue(), adminPath, shardHandler.getShardHandlerFactory(), updateShardHandler.getUpdateOnlyHttpClient());
    overseerCollectionConfigSetProcessor = new OverseerCollectionConfigSetProcessor(zkController.getCoreContainer(), reader, id, shardHandler, adminPath, stats, Overseer.this, overseerPrioritizer);
    ccThread = new OverseerThread(ccTg, overseerCollectionConfigSetProcessor, "OverseerCollectionConfigSetProcessor-" + id);
    ccThread.setDaemon(true);

    ThreadGroup triggerThreadGroup = new ThreadGroup("Overseer autoscaling triggers");
    // nocommit - this guy is an enemy of the state
//    OverseerTriggerThread trigger = new OverseerTriggerThread(zkController.getCoreContainer().getResourceLoader(),
//        zkController.getSolrCloudManager());
//    triggerThread = new OverseerThread(triggerThreadGroup, trigger, "OverseerAutoScalingTriggerThread-" + id);

    updaterThread.start();
    ccThread.start();
    if (triggerThread != null) {
      triggerThread.start();
    }

    systemCollectionCompatCheck(new StringBiConsumer());

    assert ObjectReleaseTracker.track(this);
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

  /**
   * For tests.
   * 
   * @lucene.internal
   * @return state updater thread
   */
  public synchronized OverseerThread getUpdaterThread() {
    return updaterThread;
  }

  /**
   * For tests.
   * @lucene.internal
   * @return trigger thread
   */
  public synchronized OverseerThread getTriggerThread() {
    return triggerThread;
  }


  public void closeAndDone() {
    this.closeAndDone = true;
    this.closed = true;
    close();
  }

  public void close() {
    close(false);
  }

  public void close(boolean fromCSUpdateThread) {
    if (this.id != null) {
      log.info("Overseer (id={}) closing", id);
    }
    if (context != null) {
      context.close(fromCSUpdateThread);
    }
    //doClose(fromCSUpdateThread);
  }

  @Override
  public boolean isClosed() {
    return closed || zkController.getCoreContainer().isShutDown();
  }

  void doClose() {
    doClose(false);
  }

  void doClose(boolean fromCSUpdateThread) {
    closed = true;
    if (log.isDebugEnabled()) {
      log.debug("doClose() - start");
    }

    if (ccThread != null) {
      ccThread.interrupt();
    }
    if (updaterThread != null) {
      updaterThread.interrupt();
    }
//    if (overseerCollectionConfigSetProcessor != null) {
//      overseerCollectionConfigSetProcessor.interrupt();
//    }



    IOUtils.closeQuietly(ccThread);

    IOUtils.closeQuietly(updaterThread);

    if (ccThread != null) {
      while (true) {
        try {
          ccThread.join();
          break;
        } catch (InterruptedException e) {
          // okay
        }
      }
    }
    if (updaterThread != null && !fromCSUpdateThread) {
      while (true) {
        try {
          updaterThread.join();
          break;
        } catch (InterruptedException e) {
          // okay
        }
      }
    }
    //      closer.collect(() -> {
    //
    //        IOUtils.closeQuietly(triggerThread);
    //        triggerThread.interrupt();
    //      });

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
        return Overseer.this.isClosed() || zkController.getCoreContainer().isShutDown();
      }
    });
  }

  /**
   * Internal overseer work queue. This should not be used outside of Overseer.
   * <p>
   * This queue is used to store overseer operations that have been removed from the
   * state update queue but are being executed as part of a batch. Once
   * the result of the batch is persisted to zookeeper, these items are removed from the
   * work queue. If the overseer dies while processing a batch then a new overseer always
   * operates from the work queue first and only then starts processing operations from the
   * state update queue.
   * This method will create the /overseer znode in ZooKeeper if it does not exist already.
   *
   * @param zkClient the {@link SolrZkClient} to be used for reading/writing to the queue
   * @param zkStats  a {@link Stats} object which tracks statistics for all zookeeper operations performed by this queue
   * @return a {@link ZkDistributedQueue} object
   */
  static ZkDistributedQueue getInternalWorkQueue(final SolrZkClient zkClient, Stats zkStats) {
    return new ZkDistributedQueue(zkClient, "/overseer/queue-work", zkStats);
  }

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
  
  public static boolean isLegacy(ZkStateReader stateReader) {
    String legacyProperty = stateReader.getClusterProperty(ZkStateReader.LEGACY_CLOUD, "false");
    return "true".equals(legacyProperty);
  }

  public static boolean isLegacy(ClusterStateProvider clusterStateProvider) {
    String legacyProperty = clusterStateProvider.getClusterProperty(ZkStateReader.LEGACY_CLOUD, "false");
    return "true".equals(legacyProperty);
  }

  public ZkStateReader getZkStateReader() {
    return reader;
  }

  public void offerStateUpdate(byte[] data) throws KeeperException, InterruptedException {
    getStateUpdateQueue().offer(data);
  }

}
