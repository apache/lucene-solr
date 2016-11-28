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
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.codahale.metrics.Timer;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.cloud.overseer.ClusterStateMutator;
import org.apache.solr.cloud.overseer.CollectionMutator;
import org.apache.solr.cloud.overseer.NodeMutator;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.cloud.overseer.ReplicaMutator;
import org.apache.solr.cloud.overseer.SliceMutator;
import org.apache.solr.cloud.overseer.ZkStateWriter;
import org.apache.solr.cloud.overseer.ZkWriteCommand;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CloudConfig;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.solr.update.UpdateShardHandler;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cluster leader. Responsible for processing state updates, node assignments, creating/deleting
 * collections, shards, replicas and setting various properties.
 */
public class Overseer implements Closeable {
  public static final String QUEUE_OPERATION = "operation";

  public static final int STATE_UPDATE_DELAY = 1500;  // delay between cloud state updates

  public static final int NUM_RESPONSES_TO_STORE = 10000;
  
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  enum LeaderStatus {DONT_KNOW, NO, YES}

  private class ClusterStateUpdater implements Runnable, Closeable {
    
    private final ZkStateReader reader;
    private final SolrZkClient zkClient;
    private final String myId;
    //queue where everybody can throw tasks
    private final DistributedQueue stateUpdateQueue; 
    //Internal queue where overseer stores events that have not yet been published into cloudstate
    //If Overseer dies while extracting the main queue a new overseer will start from this queue 
    private final DistributedQueue workQueue;
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
      return stateUpdateQueue.getStats();
    }

    public Stats getWorkQueueStats()  {
      return workQueue.getStats();
    }
    
    @Override
    public void run() {

      LeaderStatus isLeader = amILeader();
      while (isLeader == LeaderStatus.DONT_KNOW) {
        log.debug("am_i_leader unclear {}", isLeader);
        isLeader = amILeader();  // not a no, not a yes, try ask again
      }

      log.debug("Starting to work on the main queue");
      try {
        ZkStateWriter zkStateWriter = null;
        ClusterState clusterState = null;
        boolean refreshClusterState = true; // let's refresh in the first iteration
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
              reader.updateClusterState();
              clusterState = reader.getClusterState();
              zkStateWriter = new ZkStateWriter(reader, stats);
              refreshClusterState = false;

              // if there were any errors while processing
              // the state queue, items would have been left in the
              // work queue so let's process those first
              byte[] data = workQueue.peek();
              boolean hadWorkItems = data != null;
              while (data != null)  {
                final ZkNodeProps message = ZkNodeProps.load(data);
                log.debug("processMessage: workQueueSize: {}, message = {}", workQueue.getStats().getQueueLength(), message);
                // force flush to ZK after each message because there is no fallback if workQueue items
                // are removed from workQueue but fail to be written to ZK
                clusterState = processQueueItem(message, clusterState, zkStateWriter, false, null);
                workQueue.poll(); // poll-ing removes the element we got by peek-ing
                data = workQueue.peek();
              }
              // force flush at the end of the loop
              if (hadWorkItems) {
                clusterState = zkStateWriter.writePendingUpdates();
              }
            } catch (KeeperException e) {
              if (e.code() == KeeperException.Code.SESSIONEXPIRED) {
                log.warn("Solr cannot talk to ZK, exiting Overseer work queue loop", e);
                return;
              }
              log.error("Exception in Overseer work queue loop", e);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              return;
            } catch (Exception e) {
              log.error("Exception in Overseer work queue loop", e);
            }
          }

          byte[] head = null;
          try {
            head = stateUpdateQueue.peek(true);
          } catch (KeeperException e) {
            if (e.code() == KeeperException.Code.SESSIONEXPIRED) {
              log.warn("Solr cannot talk to ZK, exiting Overseer main queue loop", e);
              return;
            }
            log.error("Exception in Overseer main queue loop", e);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;

          } catch (Exception e) {
            log.error("Exception in Overseer main queue loop", e);
          }
          try {
            while (head != null) {
              byte[] data = head;
              final ZkNodeProps message = ZkNodeProps.load(data);
              log.debug("processMessage: queueSize: {}, message = {} current state version: {}", stateUpdateQueue.getStats().getQueueLength(), message, clusterState.getZkClusterStateVersion());
              // we can batch here because workQueue is our fallback in case a ZK write failed
              clusterState = processQueueItem(message, clusterState, zkStateWriter, true, new ZkStateWriter.ZkWriteCallback() {
                @Override
                public void onEnqueue() throws Exception {
                  workQueue.offer(data);
                }

                @Override
                public void onWrite() throws Exception {
                  // remove everything from workQueue
                  while (workQueue.poll() != null);
                }
              });

              // it is safer to keep this poll here because an invalid message might never be queued
              // and therefore we can't rely on the ZkWriteCallback to remove the item
              stateUpdateQueue.poll();

              if (isClosed) break;
              // if an event comes in the next 100ms batch it together
              head = stateUpdateQueue.peek(100);
            }
            // we should force write all pending updates because the next iteration might sleep until there
            // are more items in the main queue
            clusterState = zkStateWriter.writePendingUpdates();
            // clean work queue
            while (workQueue.poll() != null);

          } catch (KeeperException.BadVersionException bve) {
            log.warn("Bad version writing to ZK using compare-and-set, will force refresh cluster state: {}", bve.getMessage());
            refreshClusterState = true;
          } catch (KeeperException e) {
            if (e.code() == KeeperException.Code.SESSIONEXPIRED) {
              log.warn("Solr cannot talk to ZK, exiting Overseer main queue loop", e);
              return;
            }
            log.error("Exception in Overseer main queue loop", e);
            refreshClusterState = true; // force refresh state in case of all errors
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

    private ClusterState processQueueItem(ZkNodeProps message, ClusterState clusterState, ZkStateWriter zkStateWriter, boolean enableBatching, ZkStateWriter.ZkWriteCallback callback) throws Exception {
      final String operation = message.getStr(QUEUE_OPERATION);
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
        log.error("Overseer could not process the current clusterstate state update message, skipping the message.", e);
        stats.error(operation);
      } finally {
        timerContext.stop();
      }
      if (zkWriteCommands != null) {
        for (ZkWriteCommand zkWriteCommand : zkWriteCommands) {
          clusterState = zkStateWriter.enqueueUpdate(clusterState, zkWriteCommand, callback);
        }
        if (!enableBatching)  {
          clusterState = zkStateWriter.writePendingUpdates();
        }
      }
      return clusterState;
    }

    private void checkIfIamStillLeader() {
      if (zkController != null && zkController.getCoreContainer().isShutDown()) return;//shutting down no need to go further
      org.apache.zookeeper.data.Stat stat = new org.apache.zookeeper.data.Stat();
      String path = OverseerElectionContext.OVERSEER_ELECT + "/leader";
      byte[] data;
      try {
        data = zkClient.getData(path, null, stat, true);
      } catch (Exception e) {
        log.error("could not read the data" ,e);
        return;
      }
      try {
        Map m = (Map) Utils.fromJSON(data);
        String id = (String) m.get("id");
        if(overseerCollectionConfigSetProcessor.getId().equals(id)){
          try {
            log.warn("I'm exiting, but I'm still the leader");
            zkClient.delete(path,stat.getVersion(),true);
          } catch (KeeperException.BadVersionException e) {
            //no problem ignore it some other Overseer has already taken over
          } catch (Exception e) {
            log.error("Could not delete my leader node ", e);
          }

        } else{
          log.debug("somebody else has already taken up the overseer position");
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
            return Collections.singletonList(new ClusterStateMutator(getZkStateReader()).createCollection(clusterState, message));
          case DELETE:
            return Collections.singletonList(new ClusterStateMutator(getZkStateReader()).deleteCollection(clusterState, message));
          case CREATESHARD:
            return Collections.singletonList(new CollectionMutator(getZkStateReader()).createShard(clusterState, message));
          case DELETESHARD:
            return Collections.singletonList(new CollectionMutator(getZkStateReader()).deleteShard(clusterState, message));
          case ADDREPLICA:
            return Collections.singletonList(new SliceMutator(getZkStateReader()).addReplica(clusterState, message));
          case ADDREPLICAPROP:
            return Collections.singletonList(new ReplicaMutator(getZkStateReader()).addReplicaProperty(clusterState, message));
          case DELETEREPLICAPROP:
            return Collections.singletonList(new ReplicaMutator(getZkStateReader()).deleteReplicaProperty(clusterState, message));
          case BALANCESHARDUNIQUE:
            ExclusiveSliceProperty dProp = new ExclusiveSliceProperty(clusterState, message);
            if (dProp.balanceProperty()) {
              String collName = message.getStr(ZkStateReader.COLLECTION_PROP);
              return Collections.singletonList(new ZkWriteCommand(collName, dProp.getDocCollection()));
            }
            break;
          case MODIFYCOLLECTION:
            CollectionsHandler.verifyRuleParams(zkController.getCoreContainer() ,message.getProperties());
            return Collections.singletonList(new CollectionMutator(reader).modifyCollection(clusterState,message));
          case MIGRATESTATEFORMAT:
            return Collections.singletonList(new ClusterStateMutator(reader).migrateStateFormat(clusterState, message));
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
            return Collections.singletonList(new ReplicaMutator(getZkStateReader()).setState(clusterState, message));
          case LEADER:
            return Collections.singletonList(new SliceMutator(getZkStateReader()).setShardLeader(clusterState, message));
          case DELETECORE:
            return Collections.singletonList(new SliceMutator(getZkStateReader()).removeReplica(clusterState, message));
          case ADDROUTINGRULE:
            return Collections.singletonList(new SliceMutator(getZkStateReader()).addRoutingRule(clusterState, message));
          case REMOVEROUTINGRULE:
            return Collections.singletonList(new SliceMutator(getZkStateReader()).removeRoutingRule(clusterState, message));
          case UPDATESHARDSTATE:
            return Collections.singletonList(new SliceMutator(getZkStateReader()).updateShardState(clusterState, message));
          case QUIT:
            if (myId.equals(message.get("id"))) {
              log.info("Quit command received {}", LeaderElector.getNodeName(myId));
              overseerCollectionConfigSetProcessor.close();
              close();
            } else {
              log.warn("Overseer received wrong QUIT message {}", message);
            }
            break;
          case DOWNNODE:
            return new NodeMutator(getZkStateReader()).downNode(clusterState, message);
          default:
            throw new RuntimeException("unknown operation:" + operation + " contents:" + message.getProperties());
        }
      }

      return Collections.singletonList(ZkStateWriter.NO_OP);
    }

    private LeaderStatus amILeader() {
      Timer.Context timerContext = stats.time("am_i_leader");
      boolean success = true;
      try {
        ZkNodeProps props = ZkNodeProps.load(zkClient.getData(
            OverseerElectionContext.OVERSEER_ELECT + "/leader", null, null, true));
        if (myId.equals(props.getStr("id"))) {
          return LeaderStatus.YES;
        }
      } catch (KeeperException e) {
        success = false;
        if (e.code() == KeeperException.Code.CONNECTIONLOSS) {
          log.error("", e);
          return LeaderStatus.DONT_KNOW;
        } else if (e.code() != KeeperException.Code.SESSIONEXPIRED) {
          log.warn("", e);
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
      log.info("According to ZK I (id=" + myId + ") am no longer a leader.");
      return LeaderStatus.NO;
    }

    @Override
      public void close() {
        this.isClosed = true;
      }

  }

  class OverseerThread extends Thread implements Closeable {

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

    public boolean isClosed() {
      return this.isClosed;
    }
    
  }
  
  private OverseerThread ccThread;

  private OverseerThread updaterThread;
  
  private OverseerThread arfoThread;

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
    
    ThreadGroup ohcfTg = new ThreadGroup("Overseer Hdfs SolrCore Failover Thread.");

    OverseerAutoReplicaFailoverThread autoReplicaFailoverThread = new OverseerAutoReplicaFailoverThread(config, reader, updateShardHandler);
    arfoThread = new OverseerThread(ohcfTg, autoReplicaFailoverThread, "OverseerHdfsCoreFailoverThread-" + id);
    arfoThread.setDaemon(true);
    
    updaterThread.start();
    ccThread.start();
    arfoThread.start();
  }

  public Stats getStats() {
    return stats;
  }

  ZkController getZkController(){
    return zkController;
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
  
  public synchronized void close() {
    if (closed || id == null) return;
    log.info("Overseer (id=" + id + ") closing");
    
    doClose();
    this.closed = true;
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
    if (arfoThread != null) {
      IOUtils.closeQuietly(arfoThread);
      arfoThread.interrupt();
    }
    
    updaterThread = null;
    ccThread = null;
    arfoThread = null;
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
   * @return a {@link DistributedQueue} object
   */
  public static DistributedQueue getStateUpdateQueue(final SolrZkClient zkClient) {
    return getStateUpdateQueue(zkClient, new Stats());
  }

  /**
   * The overseer uses the returned queue to read any operations submitted by clients.
   * This method should not be used directly by anyone other than the Overseer itself.
   * This method will create the /overseer znode in ZooKeeper if it does not exist already.
   *
   * @param zkClient the {@link SolrZkClient} to be used for reading/writing to the queue
   * @param zkStats  a {@link Overseer.Stats} object which tracks statistics for all zookeeper operations performed by this queue
   * @return a {@link DistributedQueue} object
   */
  static DistributedQueue getStateUpdateQueue(final SolrZkClient zkClient, Stats zkStats) {
    createOverseerNode(zkClient);
    return new DistributedQueue(zkClient, "/overseer/queue", zkStats);
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
   * @param zkStats  a {@link Overseer.Stats} object which tracks statistics for all zookeeper operations performed by this queue
   * @return a {@link DistributedQueue} object
   */
  static DistributedQueue getInternalWorkQueue(final SolrZkClient zkClient, Stats zkStats) {
    createOverseerNode(zkClient);
    return new DistributedQueue(zkClient, "/overseer/queue-work", zkStats);
  }

  /* Internal map for failed tasks, not to be used outside of the Overseer */
  static DistributedMap getRunningMap(final SolrZkClient zkClient) {
    createOverseerNode(zkClient);
    return new DistributedMap(zkClient, "/overseer/collection-map-running");
  }

  /* Size-limited map for successfully completed tasks*/
  static DistributedMap getCompletedMap(final SolrZkClient zkClient) {
    createOverseerNode(zkClient);
    return new SizeLimitedDistributedMap(zkClient, "/overseer/collection-map-completed", NUM_RESPONSES_TO_STORE);
  }

  /* Map for failed tasks, not to be used outside of the Overseer */
  static DistributedMap getFailureMap(final SolrZkClient zkClient) {
    createOverseerNode(zkClient);
    return new SizeLimitedDistributedMap(zkClient, "/overseer/collection-map-failure", NUM_RESPONSES_TO_STORE);
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
   * @return a {@link DistributedQueue} object
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
   * @return a {@link DistributedQueue} object
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
   * @return a {@link DistributedQueue} object
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
   * @return a {@link DistributedQueue} object
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
    String legacyProperty = stateReader.getClusterProperty(ZkStateReader.LEGACY_CLOUD, "true");
    return !"false".equals(legacyProperty);
  }

  public ZkStateReader getZkStateReader() {
    return reader;
  }

  /**
   * Used to hold statistics about overseer operations. It will be exposed
   * to the OverseerCollectionProcessor to return statistics.
   *
   * This is experimental API and subject to change.
   */
  public static class Stats {
    static final int MAX_STORED_FAILURES = 10;

    final Map<String, Stat> stats = new ConcurrentHashMap<>();
    private volatile int queueLength;

    public Map<String, Stat> getStats() {
      return stats;
    }

    public int getSuccessCount(String operation) {
      Stat stat = stats.get(operation.toLowerCase(Locale.ROOT));
      return stat == null ? 0 : stat.success.get();
    }

    public int getErrorCount(String operation)  {
      Stat stat = stats.get(operation.toLowerCase(Locale.ROOT));
      return stat == null ? 0 : stat.errors.get();
    }

    public void success(String operation) {
      String op = operation.toLowerCase(Locale.ROOT);
      Stat stat = stats.get(op);
      if (stat == null) {
        stat = new Stat();
        stats.put(op, stat);
      }
      stat.success.incrementAndGet();
    }

    public void error(String operation) {
      String op = operation.toLowerCase(Locale.ROOT);
      Stat stat = stats.get(op);
      if (stat == null) {
        stat = new Stat();
        stats.put(op, stat);
      }
      stat.errors.incrementAndGet();
    }

    public Timer.Context time(String operation) {
      String op = operation.toLowerCase(Locale.ROOT);
      Stat stat = stats.get(op);
      if (stat == null) {
        stat = new Stat();
        stats.put(op, stat);
      }
      return stat.requestTime.time();
    }

    public void storeFailureDetails(String operation, ZkNodeProps request, SolrResponse resp) {
      String op = operation.toLowerCase(Locale.ROOT);
      Stat stat = stats.get(op);
      if (stat == null) {
        stat = new Stat();
        stats.put(op, stat);
      }
      LinkedList<FailedOp> failedOps = stat.failureDetails;
      synchronized (failedOps)  {
        if (failedOps.size() >= MAX_STORED_FAILURES)  {
          failedOps.removeFirst();
        }
        failedOps.addLast(new FailedOp(request, resp));
      }
    }

    public List<FailedOp> getFailureDetails(String operation) {
      Stat stat = stats.get(operation.toLowerCase(Locale.ROOT));
      if (stat == null || stat.failureDetails.isEmpty()) return null;
      LinkedList<FailedOp> failedOps = stat.failureDetails;
      synchronized (failedOps)  {
        ArrayList<FailedOp> ret = new ArrayList<>(failedOps);
        return ret;
      }
    }

    public int getQueueLength() {
      return queueLength;
    }

    public void setQueueLength(int queueLength) {
      this.queueLength = queueLength;
    }

    public void clear() {
      stats.clear();
    }
  }

  public static class Stat  {
    public final AtomicInteger success;
    public final AtomicInteger errors;
    public final Timer requestTime;
    public final LinkedList<FailedOp> failureDetails;

    public Stat() {
      this.success = new AtomicInteger();
      this.errors = new AtomicInteger();
      this.requestTime = new Timer();
      this.failureDetails = new LinkedList<>();
    }
  }

  public static class FailedOp  {
    public final ZkNodeProps req;
    public final SolrResponse resp;

    public FailedOp(ZkNodeProps req, SolrResponse resp) {
      this.req = req;
      this.resp = resp;
    }
  }
}
