package org.apache.solr.cloud;

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

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.cloud.DistributedQueue.QueueEvent;
import org.apache.solr.cloud.Overseer.LeaderStatus;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.Utils;
import org.apache.solr.handler.component.ShardHandlerFactory;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.apache.solr.util.stats.TimerContext;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CommonAdminParams.ASYNC;

/**
 * A generic processor run in the Overseer, used for handling items added
 * to a distributed work queue.  Has support for handling exclusive tasks
 * (i.e. tasks that should not run in parallel with each other).
 *
 * An {@link OverseerMessageHandlerSelector} determines which
 * {@link OverseerMessageHandler} handles specific messages in the
 * queue.
 */
public class OverseerProcessor implements Runnable, Closeable {

  public int maxParallelThreads = 10;

  public ExecutorService tpe ;

  private static Logger log = LoggerFactory
      .getLogger(OverseerProcessor.class);

  private DistributedQueue workQueue;
  private DistributedMap runningMap;
  private DistributedMap completedMap;
  private DistributedMap failureMap;

  // Set that maintains a list of all the tasks that are running. This is keyed on zk id of the task.
  final private Set runningTasks;

  // List of completed tasks. This is used to clean up workQueue in zk.
  final private HashMap<String, QueueEvent> completedTasks;

  private String myId;

  private final ShardHandlerFactory shardHandlerFactory;

  private String adminPath;

  private ZkStateReader zkStateReader;

  private boolean isClosed;

  private Overseer.Stats stats;

  // Set of tasks that have been picked up for processing but not cleaned up from zk work-queue.
  // It may contain tasks that have completed execution, have been entered into the completed/failed map in zk but not
  // deleted from the work-queue as that is a batched operation.
  final private Set<String> runningZKTasks;
  private final Object waitLock = new Object();

  private OverseerMessageHandlerSelector selector;

  private OverseerNodePrioritizer prioritizer;

  public OverseerProcessor(ZkStateReader zkStateReader, String myId,
                                        final ShardHandlerFactory shardHandlerFactory,
                                        String adminPath,
                                        Overseer.Stats stats,
                                        OverseerMessageHandlerSelector selector,
                                        OverseerNodePrioritizer prioritizer,
                                        DistributedQueue workQueue,
                                        DistributedMap runningMap,
                                        DistributedMap completedMap,
                                        DistributedMap failureMap) {
    this.zkStateReader = zkStateReader;
    this.myId = myId;
    this.shardHandlerFactory = shardHandlerFactory;
    this.adminPath = adminPath;
    this.stats = stats;
    this.selector = selector;
    this.prioritizer = prioritizer;
    this.workQueue = workQueue;
    this.runningMap = runningMap;
    this.completedMap = completedMap;
    this.failureMap = failureMap;
    this.runningZKTasks = new HashSet<>();
    this.runningTasks = new HashSet();
    this.completedTasks = new HashMap<>();
  }

  @Override
  public void run() {
    log.info("Process current queue of overseer operations");
    LeaderStatus isLeader = amILeader();
    while (isLeader == LeaderStatus.DONT_KNOW) {
      log.debug("am_i_leader unclear {}", isLeader);
      isLeader = amILeader();  // not a no, not a yes, try ask again
    }

    String oldestItemInWorkQueue = null;
    // hasLeftOverItems - used for avoiding re-execution of async tasks that were processed by a previous Overseer.
    // This variable is set in case there's any task found on the workQueue when the OCP starts up and
    // the id for the queue tail is used as a marker to check for the task in completed/failed map in zk.
    // Beyond the marker, all tasks can safely be assumed to have never been executed.
    boolean hasLeftOverItems = true;

    try {
      oldestItemInWorkQueue = workQueue.getTailId();
    } catch (KeeperException e) {
      // We don't need to handle this. This is just a fail-safe which comes in handy in skipping already processed
      // async calls.
      SolrException.log(log, "", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    if (oldestItemInWorkQueue == null)
      hasLeftOverItems = false;
    else
      log.debug("Found already existing elements in the work-queue. Last element: {}", oldestItemInWorkQueue);

    try {
      prioritizer.prioritizeOverseerNodes(myId);
    } catch (Exception e) {
      log.error("Unable to prioritize overseer ", e);
    }

    // TODO: Make maxThreads configurable.

    this.tpe = new ExecutorUtil.MDCAwareThreadPoolExecutor(5, 100, 0L, TimeUnit.MILLISECONDS,
        new SynchronousQueue<Runnable>(),
        new DefaultSolrThreadFactory("OverseerThreadFactory"));
    try {
      while (!this.isClosed) {
        try {
          isLeader = amILeader();
          if (LeaderStatus.NO == isLeader) {
            break;
          } else if (LeaderStatus.YES != isLeader) {
            log.debug("am_i_leader unclear {}", isLeader);
            continue; // not a no, not a yes, try asking again
          }

          log.debug("Cleaning up work-queue. #Running tasks: {}", runningTasks.size());
          cleanUpWorkQueue();

          printTrackingMaps();

          boolean waited = false;

          while (runningTasks.size() > maxParallelThreads) {
            synchronized (waitLock) {
              waitLock.wait(100);//wait for 100 ms or till a task is complete
            }
            waited = true;
          }

          if (waited)
            cleanUpWorkQueue();

          List<QueueEvent> heads = workQueue.peekTopN(maxParallelThreads, runningZKTasks, 2000L);

          if (heads == null)
            continue;

          log.debug("Got {} tasks from work-queue : [{}]", heads.size(), heads.toString());

          if (isClosed) break;

          for (QueueEvent head : heads) {
            final ZkNodeProps message = ZkNodeProps.load(head.getBytes());
            OverseerMessageHandler messageHandler = selector.selectOverseerMessageHandler(message);
            String taskKey = messageHandler.getTaskKey(message);
            final String asyncId = message.getStr(ASYNC);
            if (hasLeftOverItems) {
              if (head.getId().equals(oldestItemInWorkQueue))
                hasLeftOverItems = false;
              if (asyncId != null && (completedMap.contains(asyncId) || failureMap.contains(asyncId))) {
                log.debug("Found already processed task in workQueue, cleaning up. AsyncId [{}]",asyncId );
                workQueue.remove(head);
                continue;
              }
            }

            if (!checkExclusivity(messageHandler, message, head.getId())) {
              log.debug("Exclusivity check failed for [{}]", message.toString());
              continue;
            }

            try {
              markTaskAsRunning(messageHandler, head, taskKey, asyncId, message);
              log.debug("Marked task [{}] as running", head.getId());
            } catch (KeeperException.NodeExistsException e) {
              // This should never happen
              log.error("Tried to pick up task [{}] when it was already running!", head.getId());
            } catch (InterruptedException e) {
              log.error("Thread interrupted while trying to pick task for execution.", head.getId());
              Thread.currentThread().interrupt();
            }

            log.info(messageHandler.getName() + ": Get the message id:" + head.getId() + " message:" + message.toString());
            String operation = message.getStr(Overseer.QUEUE_OPERATION);
            Runner runner = new Runner(messageHandler, message,
                operation, head);
            tpe.execute(runner);
          }

        } catch (KeeperException e) {
          if (e.code() == KeeperException.Code.SESSIONEXPIRED) {
            log.warn("Overseer cannot talk to ZK");
            return;
          }
          SolrException.log(log, "", e);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        } catch (Exception e) {
          SolrException.log(log, "", e);
        }
      }
    } finally {
      this.close();
    }
  }

  protected boolean checkExclusivity(OverseerMessageHandler messageHandler, ZkNodeProps message, String id)
      throws KeeperException, InterruptedException {
    String taskKey = messageHandler.getTaskKey(message);

    if(taskKey == null)
      return true;

    OverseerMessageHandler.ExclusiveMarking marking = messageHandler.checkExclusiveMarking(taskKey, message);
    switch (marking) {
      case NOTDETERMINED:
        break;
      case EXCLUSIVE:
        return true;
      case NONEXCLUSIVE:
        return false;
      default:
        throw new IllegalArgumentException("Undefined marking: " + marking);
    }

    if(runningZKTasks.contains(id))
      return false;

    return true;
  }

  private void cleanUpWorkQueue() throws KeeperException, InterruptedException {
    synchronized (completedTasks) {
      for (String id : completedTasks.keySet()) {
        workQueue.remove(completedTasks.get(id));
        runningZKTasks.remove(id);
      }
      completedTasks.clear();
    }
  }

  public void close() {
    isClosed = true;
    if(tpe != null) {
      if (!tpe.isShutdown()) {
        tpe.shutdown();
        try {
          tpe.awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          log.warn("Thread interrupted while waiting for OCP threadpool close.");
          Thread.currentThread().interrupt();
        } finally {
          if (!tpe.isShutdown())
            tpe.shutdownNow();
        }
      }
    }
  }

  public static List<String> getSortedOverseerNodeNames(SolrZkClient zk) throws KeeperException, InterruptedException {
    List<String> children = null;
    try {
      children = zk.getChildren(OverseerElectionContext.PATH + LeaderElector.ELECTION_NODE, null, true);
    } catch (Exception e) {
      log.warn("error ", e);
      return new ArrayList<>();
    }
    LeaderElector.sortSeqs(children);
    ArrayList<String> nodeNames = new ArrayList<>(children.size());
    for (String c : children) nodeNames.add(LeaderElector.getNodeName(c));
    return nodeNames;
  }

  public static List<String> getSortedElectionNodes(SolrZkClient zk, String path) throws KeeperException, InterruptedException {
    List<String> children = null;
    try {
      children = zk.getChildren(path, null, true);
      LeaderElector.sortSeqs(children);
      return children;
    } catch (Exception e) {
      throw e;
    }

  }

  public static String getLeaderNode(SolrZkClient zkClient) throws KeeperException, InterruptedException {
    String id = getLeaderId(zkClient);
    return id==null ?
        null:
        LeaderElector.getNodeName( id);
  }

  public static String getLeaderId(SolrZkClient zkClient) throws KeeperException,InterruptedException{
    byte[] data = null;
    try {
      data = zkClient.getData("/overseer_elect/leader", null, new Stat(), true);
    } catch (KeeperException.NoNodeException e) {
      return null;
    }
    Map m = (Map) Utils.fromJSON(data);
    return  (String) m.get("id");
  }

  protected LeaderStatus amILeader() {
    String statsName = "collection_am_i_leader";
    TimerContext timerContext = stats.time(statsName);
    boolean success = true;
    try {
      ZkNodeProps props = ZkNodeProps.load(zkStateReader.getZkClient().getData(
          "/overseer_elect/leader", null, null, true));
      if (myId.equals(props.getStr("id"))) {
        return LeaderStatus.YES;
      }
    } catch (KeeperException e) {
      success = false;
      if (e.code() == KeeperException.Code.CONNECTIONLOSS) {
        log.error("", e);
        return LeaderStatus.DONT_KNOW;
      } else if (e.code() == KeeperException.Code.SESSIONEXPIRED) {
        log.info("", e);
      } else {
        log.warn("", e);
      }
    } catch (InterruptedException e) {
      success = false;
      Thread.currentThread().interrupt();
    } finally {
      timerContext.stop();
      if (success)  {
        stats.success(statsName);
      } else  {
        stats.error(statsName);
      }
    }
    log.info("According to ZK I (id=" + myId + ") am no longer a leader.");
    return LeaderStatus.NO;
  }

  public boolean isClosed() {
    return isClosed;
  }

  @SuppressWarnings("unchecked")
  private void markTaskAsRunning(OverseerMessageHandler messageHandler, QueueEvent head, String taskKey,
                                 String asyncId, ZkNodeProps message)
      throws KeeperException, InterruptedException {
    synchronized (runningZKTasks) {
      runningZKTasks.add(head.getId());
    }

    synchronized (runningTasks) {
      runningTasks.add(head.getId());
    }

    messageHandler.markExclusiveTask(taskKey, message);

    if(asyncId != null)
      runningMap.put(asyncId, null);
  }
  
  protected class Runner implements Runnable {
    ZkNodeProps message;
    String operation;
    SolrResponse response;
    QueueEvent head;
    OverseerMessageHandler messageHandler;
  
    public Runner(OverseerMessageHandler messageHandler, ZkNodeProps message, String operation, QueueEvent head) {
      this.message = message;
      this.operation = operation;
      this.head = head;
      this.messageHandler = messageHandler;
      response = null;
    }


    public void run() {
      String statsName = messageHandler.getTimerName(operation);
      final TimerContext timerContext = stats.time(statsName);

      boolean success = false;
      final String asyncId = message.getStr(ASYNC);
      String taskKey = messageHandler.getTaskKey(message);

      try {
        try {
          log.debug("Runner processing {}", head.getId());
          response = messageHandler.processMessage(message, operation);
        } finally {
          timerContext.stop();
          updateStats(statsName);
        }

        if(asyncId != null) {
          if (response != null && (response.getResponse().get("failure") != null 
              || response.getResponse().get("exception") != null)) {
            failureMap.put(asyncId, SolrResponse.serializable(response));
            log.debug("Updated failed map for task with zkid:[{}]", head.getId());
          } else {
            completedMap.put(asyncId, SolrResponse.serializable(response));
            log.debug("Updated completed map for task with zkid:[{}]", head.getId());
          }
        } else {
          head.setBytes(SolrResponse.serializable(response));
          log.debug("Completed task:[{}]", head.getId());
        }

        markTaskComplete(messageHandler, head.getId(), asyncId, taskKey);
        log.debug("Marked task [{}] as completed.", head.getId());
        printTrackingMaps();

        log.info(messageHandler.getName() + ": Message id:" + head.getId() +
            " complete, response:" + response.getResponse().toString());
        success = true;
      } catch (KeeperException e) {
        SolrException.log(log, "", e);
      } catch (InterruptedException e) {
        // Reset task from tracking data structures so that it can be retried.
        resetTaskWithException(messageHandler, head.getId(), asyncId, taskKey);
        log.warn("Resetting task {} as the thread was interrupted.", head.getId());
        Thread.currentThread().interrupt();
      } finally {
        if(!success) {
          // Reset task from tracking data structures so that it can be retried.
          resetTaskWithException(messageHandler, head.getId(), asyncId, taskKey);
        }
        synchronized (waitLock){
          waitLock.notifyAll();
        }
      }
    }

    private void markTaskComplete(OverseerMessageHandler messageHandler, String id, String asyncId, String taskKey)
        throws KeeperException, InterruptedException {
      synchronized (completedTasks) {
        completedTasks.put(id, head);
      }

      synchronized (runningTasks) {
        runningTasks.remove(id);
      }

      if(asyncId != null)
        runningMap.remove(asyncId);

      messageHandler.unmarkExclusiveTask(taskKey, operation);
    }

    private void resetTaskWithException(OverseerMessageHandler messageHandler, String id, String asyncId, String taskKey) {
      log.warn("Resetting task: {}, requestid: {}, taskKey: {}", id, asyncId, taskKey);
      try {
        if (asyncId != null)
          runningMap.remove(asyncId);

        synchronized (runningTasks) {
          runningTasks.remove(id);
        }

        messageHandler.unmarkExclusiveTask(taskKey, operation);
      } catch (KeeperException e) {
        SolrException.log(log, "", e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }

    }

    private void updateStats(String statsName) {
      if (isSuccessful()) {
        stats.success(statsName);
      } else {
        stats.error(statsName);
        stats.storeFailureDetails(statsName, message, response);
      }
    }

    private boolean isSuccessful() {
      if(response == null)
        return false;
      return !(response.getResponse().get("failure") != null || response.getResponse().get("exception") != null);
    }
  }

  private void printTrackingMaps() {
    if(log.isDebugEnabled()) {
      synchronized (runningTasks) {
        log.debug("RunningTasks: {}", runningTasks.toString());
      }
      synchronized (completedTasks) {
        log.debug("CompletedTasks: {}", completedTasks.keySet().toString());
      }
      synchronized (runningZKTasks) {
        log.debug("RunningZKTasks: {}", runningZKTasks.toString());
      }
    }
  }



  String getId(){
    return myId;
  }

  /**
   * An interface to determine which {@link OverseerMessageHandler}
   * handles a given message.  This could be a single OverseerMessageHandler
   * for the case where a single type of message is handled (e.g. collection
   * messages only) , or a different handler could be selected based on the
   * contents of the message.
   */
  public interface OverseerMessageHandlerSelector {
    OverseerMessageHandler selectOverseerMessageHandler(ZkNodeProps message);
  }

}
