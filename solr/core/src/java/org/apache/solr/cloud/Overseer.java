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
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.codahale.metrics.Timer;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.impl.ClusterStateProvider;
import org.apache.solr.cloud.api.collections.OverseerCollectionMessageHandler;
import org.apache.solr.cloud.autoscaling.OverseerTriggerThread;
import org.apache.solr.cloud.overseer.ClusterStateMutator;
import org.apache.solr.cloud.overseer.CollectionMutator;
import org.apache.solr.cloud.overseer.NodeMutator;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.cloud.overseer.ReplicaMutator;
import org.apache.solr.cloud.overseer.SliceMutator;
import org.apache.solr.cloud.overseer.ZkStateWriter;
import org.apache.solr.cloud.overseer.ZkWriteCommand;
import org.apache.solr.common.SolrCloseable;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CloudConfig;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.update.UpdateShardHandler;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CommonParams.ID;

/**
 * Cluster leader. Responsible for processing state updates, node assignments, creating/deleting
 * collections, shards, replicas and setting various properties.
 */
public class Overseer implements SolrCloseable {
  public static final String QUEUE_OPERATION = "operation";

  // System properties are used in tests to make them run fast
  public static final int STATE_UPDATE_DELAY = ZkStateReader.STATE_UPDATE_DELAY;
  public static final int STATE_UPDATE_BATCH_SIZE = Integer.getInteger("solr.OverseerStateUpdateBatchSize", 10000);
  public static final int STATE_UPDATE_MAX_QUEUE = 20000;

  public static final int NUM_RESPONSES_TO_STORE = 10000;
  public static final String OVERSEER_ELECT = "/overseer_elect";

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  enum LeaderStatus {DONT_KNOW, NO, YES}

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
    // Internal map which holds the information about running tasks.
    private final DistributedMap runningMap;
    // Internal map which holds the information about successfully completed tasks.
    private final DistributedMap completedMap;
    // Internal map which holds the information about failed tasks.
    private final DistributedMap failureMap;

    private final Stats zkStats;

    private boolean isClosed = false;

    public ClusterStateUpdater(final ZkStateReader reader, final String myId, Stats zkStats) {
      this.zkClient = reader.getZkClient();
      this.zkStats = zkStats;
      this.stateUpdateQueue = getStateUpdateQueue(zkClient, zkStats);
      this.workQueue = getInternalWorkQueue(zkClient, zkStats);
      this.failureMap = getFailureMap(zkClient);
      this.runningMap = getRunningMap(zkClient);
      this.completedMap = getCompletedMap(zkClient);
      this.myId = myId;
      this.reader = reader;
    }

    public Stats getStateUpdateQueueStats() {
      return stateUpdateQueue.getZkStats();
    }

    public Stats getWorkQueueStats()  {
      return workQueue.getZkStats();
    }

    @Override
    public void run() {
      MDCLoggingContext.setNode(zkController.getNodeName() );

      LeaderStatus isLeader = amILeader();
      while (isLeader == LeaderStatus.DONT_KNOW) {
        log.debug("am_i_leader unclear {}", isLeader);
        isLeader = amILeader();  // not a no, not a yes, try ask again
      }

      log.info("Starting to work on the main queue : {}", LeaderElector.getNodeName(myId));
      try {
        ZkStateWriter zkStateWriter = null;
        ClusterState clusterState = null;
        boolean refreshClusterState = true; // let's refresh in the first iteration
        // we write updates in batch, but if an exception is thrown when writing new clusterstate,
        // we do not sure which message is bad message, therefore we will re-process node one by one
        int fallbackQueueSize = Integer.MAX_VALUE;
        ZkDistributedQueue fallbackQueue = workQueue;
        while (!this.isClosed) {
          isLeader = amILeader();
          if (LeaderStatus.NO == isLeader) {
            break;
          }
          else if (LeaderStatus.YES != isLeader) {
            log.debug("am_i_leader unclear {}", isLeader);
            continue; // not a no, not a yes, try ask again
          }

          //TODO consider removing 'refreshClusterState' and simply check if clusterState is null
          if (refreshClusterState) {
            try {
              reader.forciblyRefreshAllClusterStateSlow();
              clusterState = reader.getClusterState();
              zkStateWriter = new ZkStateWriter(reader, stats);
              refreshClusterState = false;

              // if there were any errors while processing
              // the state queue, items would have been left in the
              // work queue so let's process those first
              byte[] data = fallbackQueue.peek();
              while (fallbackQueueSize > 0 && data != null)  {
                final ZkNodeProps message = ZkNodeProps.load(data);
                log.debug("processMessage: fallbackQueueSize: {}, message = {}", fallbackQueue.getZkStats().getQueueLength(), message);
                // force flush to ZK after each message because there is no fallback if workQueue items
                // are removed from workQueue but fail to be written to ZK
                try {
                  clusterState = processQueueItem(message, clusterState, zkStateWriter, false, null);
                } catch (Exception e) {
                  if (isBadMessage(e)) {
                    log.warn("Exception when process message = {}, consider as bad message and poll out from the queue", message);
                    fallbackQueue.poll();
                  }
                  throw e;
                }
                fallbackQueue.poll(); // poll-ing removes the element we got by peek-ing
                data = fallbackQueue.peek();
                fallbackQueueSize--;
              }
              // force flush at the end of the loop, if there are no pending updates, this is a no op call
              clusterState = zkStateWriter.writePendingUpdates();
              // the workQueue is empty now, use stateUpdateQueue as fallback queue
              fallbackQueue = stateUpdateQueue;
              fallbackQueueSize = 0;
            } catch (KeeperException.SessionExpiredException e) {
              log.warn("Solr cannot talk to ZK, exiting Overseer work queue loop", e);
              return;
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              return;
            } catch (Exception e) {
              log.error("Exception in Overseer when process message from work queue, retrying", e);
              refreshClusterState = true;
              continue;
            }
          }

          LinkedList<Pair<String, byte[]>> queue = null;
          try {
            // We do not need to filter any nodes here cause all processed nodes are removed once we flush clusterstate
            queue = new LinkedList<>(stateUpdateQueue.peekElements(1000, 3000L, (x) -> true));
          } catch (KeeperException.SessionExpiredException e) {
            log.warn("Solr cannot talk to ZK, exiting Overseer main queue loop", e);
            return;
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
          } catch (Exception e) {
            log.error("Exception in Overseer main queue loop", e);
          }
          try {
            Set<String> processedNodes = new HashSet<>();
            while (queue != null && !queue.isEmpty()) {
              for (Pair<String, byte[]> head : queue) {
                byte[] data = head.second();
                final ZkNodeProps message = ZkNodeProps.load(data);
                log.debug("processMessage: queueSize: {}, message = {} current state version: {}", stateUpdateQueue.getZkStats().getQueueLength(), message, clusterState.getZkClusterStateVersion());

                processedNodes.add(head.first());
                fallbackQueueSize = processedNodes.size();
                // The callback always be called on this thread
                clusterState = processQueueItem(message, clusterState, zkStateWriter, true, () -> {
                  stateUpdateQueue.remove(processedNodes);
                  processedNodes.clear();
                });
              }
              if (isClosed) break;
              // if an event comes in the next 100ms batch it together
              queue = new LinkedList<>(stateUpdateQueue.peekElements(1000, 100, node -> !processedNodes.contains(node)));
            }
            fallbackQueueSize = processedNodes.size();
            // we should force write all pending updates because the next iteration might sleep until there
            // are more items in the main queue
            clusterState = zkStateWriter.writePendingUpdates();
            // clean work queue
            stateUpdateQueue.remove(processedNodes);
            processedNodes.clear();
          } catch (KeeperException.SessionExpiredException e) {
            log.warn("Solr cannot talk to ZK, exiting Overseer main queue loop", e);
            return;
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
          } catch (Exception e) {
            log.error("Exception in Overseer main queue loop", e);
            refreshClusterState = true; // it might have been a bad version error
          }
        }
      } finally {
        log.info("Overseer Loop exiting : {}", LeaderElector.getNodeName(myId));
        //do this in a separate thread because any wait is interrupted in this main thread
        new Thread(this::checkIfIamStillLeader, "OverseerExitThread").start();
      }
    }

    // Return true whenever the exception thrown by ZkStateWriter is correspond
    // to a invalid state or 'bad' message (in this case, we should remove that message from queue)
    private boolean isBadMessage(Exception e) {
      if (e instanceof KeeperException) {
        KeeperException ke = (KeeperException) e;
        return ke.code() == KeeperException.Code.NONODE || ke.code() == KeeperException.Code.NODEEXISTS;
      }
      return !(e instanceof InterruptedException);
    }

    private ClusterState processQueueItem(ZkNodeProps message, ClusterState clusterState, ZkStateWriter zkStateWriter, boolean enableBatching, ZkStateWriter.ZkWriteCallback callback) throws Exception {
      final String operation = message.getStr(QUEUE_OPERATION);
      if (operation == null) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Message missing " + QUEUE_OPERATION + ":" + message);
      }
      List<ZkWriteCommand> zkWriteCommands = null;
      final Timer.Context timerContext = stats.time(operation);
      try {
        zkWriteCommands = processMessage(clusterState, message, operation);
        stats.success(operation);
      } catch (Exception e) {
        // generally there is nothing we can do - in most cases, we have
        // an issue that will fail again on retry or we cannot communicate with     a
        // ZooKeeper in which case another Overseer should take over
        // TODO: if ordering for the message is not important, we could
        // track retries and put it back on the end of the queue
        log.error("Overseer could not process the current clusterstate state update message, skipping the message: " + message, e);
        stats.error(operation);
      } finally {
        timerContext.stop();
      }
      if (zkWriteCommands != null) {
        clusterState = zkStateWriter.enqueueUpdate(clusterState, zkWriteCommands, callback);
        if (!enableBatching)  {
          clusterState = zkStateWriter.writePendingUpdates();
        }
      }
      return clusterState;
    }

    private void checkIfIamStillLeader() {
      if (zkController != null && (zkController.getCoreContainer().isShutDown() || zkController.isClosed())) {
        return;//shutting down no need to go further
      }
      org.apache.zookeeper.data.Stat stat = new org.apache.zookeeper.data.Stat();
      final String path = OVERSEER_ELECT + "/leader";
      byte[] data;
      try {
        data = zkClient.getData(path, null, stat, true);
      } catch (Exception e) {
        log.error("could not read the "+path+" data" ,e);
        return;
      }
      try {
        Map m = (Map) Utils.fromJSON(data);
        String id = (String) m.get(ID);
        if(overseerCollectionConfigSetProcessor.getId().equals(id)){
          try {
            log.warn("I (id={}) am exiting, but I'm still the leader",
                overseerCollectionConfigSetProcessor.getId());
            zkClient.delete(path,stat.getVersion(),true);
          } catch (KeeperException.BadVersionException e) {
            //no problem ignore it some other Overseer has already taken over
          } catch (Exception e) {
            log.error("Could not delete my leader node "+path, e);
          }

        } else{
          log.info("somebody else (id={}) has already taken up the overseer position", id);
        }
      } finally {
        //if I am not shutting down, Then I need to rejoin election
        try {
          if (zkController != null && !zkController.getCoreContainer().isShutDown()) {
            zkController.rejoinOverseerElection(null, false);
          }
        } catch (Exception e) {
          log.warn("Unable to rejoinElection ",e);
        }
      }
    }

    private List<ZkWriteCommand> processMessage(ClusterState clusterState,
        final ZkNodeProps message, final String operation) {
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
              return Collections.singletonList(new ZkWriteCommand(collName, dProp.getDocCollection()));
            }
            break;
          case MODIFYCOLLECTION:
            CollectionsHandler.verifyRuleParams(zkController.getCoreContainer() ,message.getProperties());
            return Collections.singletonList(new CollectionMutator(getSolrCloudManager()).modifyCollection(clusterState,message));
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

      return Collections.singletonList(ZkStateWriter.NO_OP);
    }

    private LeaderStatus amILeader() {
      Timer.Context timerContext = stats.time("am_i_leader");
      boolean success = true;
      String propsId = null;
      try {
        ZkNodeProps props = ZkNodeProps.load(zkClient.getData(
            OVERSEER_ELECT + "/leader", null, null, true));
        propsId = props.getStr(ID);
        if (myId.equals(propsId)) {
          return LeaderStatus.YES;
        }
      } catch (KeeperException e) {
        success = false;
        if (e.code() == KeeperException.Code.CONNECTIONLOSS) {
          log.error("", e);
          return LeaderStatus.DONT_KNOW;
        } else if (e.code() != KeeperException.Code.SESSIONEXPIRED) {
          log.warn("", e);
        } else {
          log.debug("", e);
        }
      } catch (InterruptedException e) {
        success = false;
        Thread.currentThread().interrupt();
      } finally {
        timerContext.stop();
        if (success)  {
          stats.success("am_i_leader");
        } else  {
          stats.error("am_i_leader");
        }
      }
      log.info("According to ZK I (id={}) am no longer a leader. propsId={}", myId, propsId);
      return LeaderStatus.NO;
    }

    @Override
      public void close() {
        this.isClosed = true;
      }

  }

  public static class OverseerThread extends Thread implements Closeable {

    protected volatile boolean isClosed;
    private Closeable thread;

    public OverseerThread(ThreadGroup tg, Closeable thread) {
      super(tg, (Runnable) thread);
      this.thread = thread;
    }

    public OverseerThread(ThreadGroup ccTg, Closeable thread, String name) {
      super(ccTg, (Runnable) thread, name);
      this.thread = thread;
    }

    @Override
    public void close() throws IOException {
      thread.close();
      this.isClosed = true;
    }

    public Closeable getThread() {
      return thread;
    }

    public boolean isClosed() {
      return this.isClosed;
    }

  }

  private OverseerThread ccThread;

  private OverseerThread updaterThread;

  private OverseerThread triggerThread;

  private final ZkStateReader reader;

  private final ShardHandler shardHandler;

  private final UpdateShardHandler updateShardHandler;

  private final String adminPath;

  private OverseerCollectionConfigSetProcessor overseerCollectionConfigSetProcessor;

  private ZkController zkController;

  private Stats stats;
  private String id;
  private boolean closed;
  private CloudConfig config;

  // overseer not responsible for closing reader
  public Overseer(ShardHandler shardHandler,
      UpdateShardHandler updateShardHandler, String adminPath,
      final ZkStateReader reader, ZkController zkController, CloudConfig config)
      throws KeeperException, InterruptedException {
    this.reader = reader;
    this.shardHandler = shardHandler;
    this.updateShardHandler = updateShardHandler;
    this.adminPath = adminPath;
    this.zkController = zkController;
    this.stats = new Stats();
    this.config = config;
  }

  public synchronized void start(String id) {
    MDCLoggingContext.setNode(zkController == null ?
        null :
        zkController.getNodeName());
    this.id = id;
    closed = false;
    doClose();
    stats = new Stats();
    log.info("Overseer (id=" + id + ") starting");
    createOverseerNode(reader.getZkClient());
    //launch cluster state updater thread
    ThreadGroup tg = new ThreadGroup("Overseer state updater.");
    updaterThread = new OverseerThread(tg, new ClusterStateUpdater(reader, id, stats), "OverseerStateUpdate-" + id);
    updaterThread.setDaemon(true);

    ThreadGroup ccTg = new ThreadGroup("Overseer collection creation process.");

    OverseerNodePrioritizer overseerPrioritizer = new OverseerNodePrioritizer(reader, adminPath, shardHandler.getShardHandlerFactory());
    overseerCollectionConfigSetProcessor = new OverseerCollectionConfigSetProcessor(reader, id, shardHandler, adminPath, stats, Overseer.this, overseerPrioritizer);
    ccThread = new OverseerThread(ccTg, overseerCollectionConfigSetProcessor, "OverseerCollectionConfigSetProcessor-" + id);
    ccThread.setDaemon(true);

    ThreadGroup triggerThreadGroup = new ThreadGroup("Overseer autoscaling triggers");
    OverseerTriggerThread trigger = new OverseerTriggerThread(zkController.getCoreContainer().getResourceLoader(),
        zkController.getSolrCloudManager(), config);
    triggerThread = new OverseerThread(triggerThreadGroup, trigger, "OverseerAutoScalingTriggerThread-" + id);

    updaterThread.start();
    ccThread.start();
    triggerThread.start();
    if (this.id != null) {
      assert ObjectReleaseTracker.track(this);
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
  
  public synchronized void close() {
    if (closed) return;
    if (this.id != null) {
      log.info("Overseer (id=" + id + ") closing");
    }
    
    doClose();
    this.closed = true;
    if (this.id != null) {
      assert ObjectReleaseTracker.release(this);
    }
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  private void doClose() {
    
    if (updaterThread != null) {
      IOUtils.closeQuietly(updaterThread);
      updaterThread.interrupt();
    }
    if (ccThread != null) {
      IOUtils.closeQuietly(ccThread);
      ccThread.interrupt();
    }
    if (triggerThread != null)  {
      IOUtils.closeQuietly(triggerThread);
      triggerThread.interrupt();
    }
    if (updaterThread != null) {
      try {
        updaterThread.join();
      } catch (InterruptedException e) {}
    }
    if (ccThread != null) {
      try {
        ccThread.join();
      } catch (InterruptedException e) {}
    }
    if (triggerThread != null)  {
      try {
        triggerThread.join();
      } catch (InterruptedException e)  {}
    }
    updaterThread = null;
    ccThread = null;
    triggerThread = null;
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
   * This method will create the /overseer znode in ZooKeeper if it does not exist already.
   *
   * @param zkClient the {@link SolrZkClient} to be used for reading/writing to the queue
   * @return a {@link ZkDistributedQueue} object
   */
  public static ZkDistributedQueue getStateUpdateQueue(final SolrZkClient zkClient) {
    return getStateUpdateQueue(zkClient, new Stats());
  }

  /**
   * The overseer uses the returned queue to read any operations submitted by clients.
   * This method should not be used directly by anyone other than the Overseer itself.
   * This method will create the /overseer znode in ZooKeeper if it does not exist already.
   *
   * @param zkClient the {@link SolrZkClient} to be used for reading/writing to the queue
   * @param zkStats  a {@link Stats} object which tracks statistics for all zookeeper operations performed by this queue
   * @return a {@link ZkDistributedQueue} object
   */
  static ZkDistributedQueue getStateUpdateQueue(final SolrZkClient zkClient, Stats zkStats) {
    createOverseerNode(zkClient);
    return new ZkDistributedQueue(zkClient, "/overseer/queue", zkStats, STATE_UPDATE_MAX_QUEUE);
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
    createOverseerNode(zkClient);
    return new ZkDistributedQueue(zkClient, "/overseer/queue-work", zkStats);
  }

  /* Internal map for failed tasks, not to be used outside of the Overseer */
  static DistributedMap getRunningMap(final SolrZkClient zkClient) {
    createOverseerNode(zkClient);
    return new DistributedMap(zkClient, "/overseer/collection-map-running");
  }

  /* Size-limited map for successfully completed tasks*/
  static DistributedMap getCompletedMap(final SolrZkClient zkClient) {
    createOverseerNode(zkClient);
    return new SizeLimitedDistributedMap(zkClient, "/overseer/collection-map-completed", NUM_RESPONSES_TO_STORE, (child) -> getAsyncIdsMap(zkClient).remove(child));
  }

  /* Map for failed tasks, not to be used outside of the Overseer */
  static DistributedMap getFailureMap(final SolrZkClient zkClient) {
    createOverseerNode(zkClient);
    return new SizeLimitedDistributedMap(zkClient, "/overseer/collection-map-failure", NUM_RESPONSES_TO_STORE, (child) -> getAsyncIdsMap(zkClient).remove(child));
  }
  
  /* Map of async IDs currently in use*/
  static DistributedMap getAsyncIdsMap(final SolrZkClient zkClient) {
    createOverseerNode(zkClient);
    return new DistributedMap(zkClient, "/overseer/async_ids");
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
  static OverseerTaskQueue getCollectionQueue(final SolrZkClient zkClient) {
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
  static OverseerTaskQueue getCollectionQueue(final SolrZkClient zkClient, Stats zkStats) {
    createOverseerNode(zkClient);
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
  static OverseerTaskQueue getConfigSetQueue(final SolrZkClient zkClient)  {
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
  static OverseerTaskQueue getConfigSetQueue(final SolrZkClient zkClient, Stats zkStats) {
    // For now, we use the same queue as the collection queue, but ensure
    // that the actions are prefixed with a unique string.
    createOverseerNode(zkClient);
    return getCollectionQueue(zkClient, zkStats);
  }
  

  private static void createOverseerNode(final SolrZkClient zkClient) {
    try {
      zkClient.create("/overseer", new byte[0], CreateMode.PERSISTENT, true);
    } catch (KeeperException.NodeExistsException e) {
      //ok
    } catch (InterruptedException e) {
      log.error("Could not create Overseer node", e);
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (KeeperException e) {
      log.error("Could not create Overseer node", e);
      throw new RuntimeException(e);
    }
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

}
