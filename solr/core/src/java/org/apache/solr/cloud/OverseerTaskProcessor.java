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
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.io.IOUtils;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.cloud.OverseerTaskQueue.QueueEvent;
import org.apache.solr.cloud.Overseer.LeaderStatus;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.DefaultSolrThreadFactory;
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
public class OverseerTaskProcessor implements Runnable, Closeable {

  /**
   * Maximum number of overseer collection operations which can be
   * executed concurrently
   */
  public static final int MAX_PARALLEL_TASKS = 100;
  public static final int MAX_BLOCKED_TASKS = 1000;

  public ExecutorService tpe;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private OverseerTaskQueue workQueue;
  private DistributedMap runningMap;
  private DistributedMap completedMap;
  private DistributedMap failureMap;

  // Set that maintains a list of all the tasks that are running. This is keyed on zk id of the task.
  final private Set<String> runningTasks;

  // List of completed tasks. This is used to clean up workQueue in zk.
  final private HashMap<String, QueueEvent> completedTasks;

  private String myId;

  private ZkStateReader zkStateReader;

  private boolean isClosed;

  private Overseer.Stats stats;

  // Set of tasks that have been picked up for processing but not cleaned up from zk work-queue.
  // It may contain tasks that have completed execution, have been entered into the completed/failed map in zk but not
  // deleted from the work-queue as that is a batched operation.
  final private Set<String> runningZKTasks;
  // This map may contain tasks which are read from work queue but could not
  // be executed because they are blocked or the execution queue is full
  // This is an optimization to ensure that we do not read the same tasks
  // again and again from ZK.
  final private Map<String, QueueEvent> blockedTasks = new LinkedHashMap<>();
  final private Predicate<String> excludedTasks = new Predicate<String>() {
    @Override
    public boolean test(String s) {
      return runningTasks.contains(s) || blockedTasks.containsKey(s);
    }

    @Override
    public String toString() {
      return StrUtils.join(ImmutableSet.of(runningTasks, blockedTasks.keySet()), ',');
    }

  };

  private final Object waitLock = new Object();

  protected OverseerMessageHandlerSelector selector;

  private OverseerNodePrioritizer prioritizer;

  public OverseerTaskProcessor(ZkStateReader zkStateReader, String myId,
                                        Overseer.Stats stats,
                                        OverseerMessageHandlerSelector selector,
                                        OverseerNodePrioritizer prioritizer,
                                        OverseerTaskQueue workQueue,
                                        DistributedMap runningMap,
                                        DistributedMap completedMap,
                                        DistributedMap failureMap) {
    this.zkStateReader = zkStateReader;
    this.myId = myId;
    this.stats = stats;
    this.selector = selector;
    this.prioritizer = prioritizer;
    this.workQueue = workQueue;
    this.runningMap = runningMap;
    this.completedMap = completedMap;
    this.failureMap = failureMap;
    this.runningZKTasks = new HashSet<>();
    this.runningTasks = new HashSet<>();
    this.completedTasks = new HashMap<>();
  }

  @Override
  public void run() {
    log.debug("Process current queue of overseer operations");
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
      if (!zkStateReader.getZkClient().isClosed()) {
        log.error("Unable to prioritize overseer ", e);
      }
    }

    // TODO: Make maxThreads configurable.

    this.tpe = new ExecutorUtil.MDCAwareThreadPoolExecutor(5, MAX_PARALLEL_TASKS, 0L, TimeUnit.MILLISECONDS,
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

          while (runningTasks.size() > MAX_PARALLEL_TASKS) {
            synchronized (waitLock) {
              waitLock.wait(100);//wait for 100 ms or till a task is complete
            }
            waited = true;
          }

          if (waited)
            cleanUpWorkQueue();


          ArrayList<QueueEvent> heads = new ArrayList<>(blockedTasks.size() + MAX_PARALLEL_TASKS);
          heads.addAll(blockedTasks.values());

          //If we have enough items in the blocked tasks already, it makes
          // no sense to read more items from the work queue. it makes sense
          // to clear out at least a few items in the queue before we read more items
          if (heads.size() < MAX_BLOCKED_TASKS) {
            //instead of reading MAX_PARALLEL_TASKS items always, we should only fetch as much as we can execute
            int toFetch = Math.min(MAX_BLOCKED_TASKS - heads.size(), MAX_PARALLEL_TASKS - runningTasks.size());
            List<QueueEvent> newTasks = workQueue.peekTopN(toFetch, excludedTasks, 2000L);
            log.debug("Got {} tasks from work-queue : [{}]", newTasks.size(), newTasks);
            heads.addAll(newTasks);
          } else {
            // Prevent free-spinning this loop.
            Thread.sleep(1000);
          }

          if (isClosed) break;

          if (heads.isEmpty()) {
            continue;
          }

          blockedTasks.clear(); // clear it now; may get refilled below.

          taskBatch.batchId++;
          boolean tooManyTasks = false;
          for (QueueEvent head : heads) {
            if (!tooManyTasks) {
              synchronized (runningTasks) {
                tooManyTasks = runningTasks.size() >= MAX_PARALLEL_TASKS;
              }
            }
            if (tooManyTasks) {
              // Too many tasks are running, just shove the rest into the "blocked" queue.
              if(blockedTasks.size() < MAX_BLOCKED_TASKS)
                blockedTasks.put(head.getId(), head);
              continue;
            }
            if (runningZKTasks.contains(head.getId())) continue;
            final ZkNodeProps message = ZkNodeProps.load(head.getBytes());
            OverseerMessageHandler messageHandler = selector.selectOverseerMessageHandler(message);
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
            String operation = message.getStr(Overseer.QUEUE_OPERATION);
            OverseerMessageHandler.Lock lock = messageHandler.lockTask(message, taskBatch);
            if (lock == null) {
              log.debug("Exclusivity check failed for [{}]", message.toString());
              //we may end crossing the size of the MAX_BLOCKED_TASKS. They are fine
              if (blockedTasks.size() < MAX_BLOCKED_TASKS)
                blockedTasks.put(head.getId(), head);
              continue;
            }
            try {
              markTaskAsRunning(head, asyncId);
              log.debug("Marked task [{}] as running", head.getId());
            } catch (KeeperException.NodeExistsException e) {
              lock.unlock();
              // This should never happen
              log.error("Tried to pick up task [{}] when it was already running!", head.getId());
              continue;
            } catch (InterruptedException e) {
              lock.unlock();
              log.error("Thread interrupted while trying to pick task for execution.", head.getId());
              Thread.currentThread().interrupt();
              continue;
            }
            log.debug(messageHandler.getName() + ": Get the message id:" + head.getId() + " message:" + message.toString());
            Runner runner = new Runner(messageHandler, message,
                operation, head, lock);
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
    if (tpe != null) {
      if (!tpe.isShutdown()) {
        ExecutorUtil.shutdownAndAwaitTermination(tpe);
      }
    }
    IOUtils.closeQuietly(selector);
  }

  public static List<String> getSortedOverseerNodeNames(SolrZkClient zk) throws KeeperException, InterruptedException {
    List<String> children = null;
    try {
      children = zk.getChildren(OverseerElectionContext.OVERSEER_ELECT + LeaderElector.ELECTION_NODE, null, true);
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
      data = zkClient.getData(OverseerElectionContext.OVERSEER_ELECT + "/leader", null, new Stat(), true);
    } catch (KeeperException.NoNodeException e) {
      return null;
    }
    Map m = (Map) Utils.fromJSON(data);
    return  (String) m.get("id");
  }

  protected LeaderStatus amILeader() {
    String statsName = "collection_am_i_leader";
    Timer.Context timerContext = stats.time(statsName);
    boolean success = true;
    try {
      ZkNodeProps props = ZkNodeProps.load(zkStateReader.getZkClient().getData(
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
  private void markTaskAsRunning(QueueEvent head, String asyncId)
      throws KeeperException, InterruptedException {
    synchronized (runningZKTasks) {
      runningZKTasks.add(head.getId());
    }

    synchronized (runningTasks) {
      runningTasks.add(head.getId());
    }


    if (asyncId != null)
      runningMap.put(asyncId, null);
  }
  
  protected class Runner implements Runnable {
    ZkNodeProps message;
    String operation;
    SolrResponse response;
    QueueEvent head;
    OverseerMessageHandler messageHandler;
    private final OverseerMessageHandler.Lock lock;

    public Runner(OverseerMessageHandler messageHandler, ZkNodeProps message, String operation, QueueEvent head, OverseerMessageHandler.Lock lock) {
      this.message = message;
      this.operation = operation;
      this.head = head;
      this.messageHandler = messageHandler;
      this.lock = lock;
      response = null;
    }


    public void run() {
      String statsName = messageHandler.getTimerName(operation);
      final Timer.Context timerContext = stats.time(statsName);

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

        if (asyncId != null) {
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

        markTaskComplete(head.getId(), asyncId);
        log.debug("Marked task [{}] as completed.", head.getId());
        printTrackingMaps();

        log.debug(messageHandler.getName() + ": Message id:" + head.getId() +
            " complete, response:" + response.getResponse().toString());
        success = true;
      } catch (KeeperException e) {
        SolrException.log(log, "", e);
      } catch (InterruptedException e) {
        // Reset task from tracking data structures so that it can be retried.
        resetTaskWithException(messageHandler, head.getId(), asyncId, taskKey, message);
        log.warn("Resetting task {} as the thread was interrupted.", head.getId());
        Thread.currentThread().interrupt();
      } finally {
        lock.unlock();
        if (!success) {
          // Reset task from tracking data structures so that it can be retried.
          resetTaskWithException(messageHandler, head.getId(), asyncId, taskKey, message);
        }
        synchronized (waitLock){
          waitLock.notifyAll();
        }
      }
    }

    private void markTaskComplete(String id, String asyncId)
        throws KeeperException, InterruptedException {
      synchronized (completedTasks) {
        completedTasks.put(id, head);
      }

      synchronized (runningTasks) {
        runningTasks.remove(id);
      }

      if (asyncId != null) {
        if (!runningMap.remove(asyncId)) {
          log.warn("Could not find and remove async call [" + asyncId + "] from the running map.");
        }
      }

      workQueue.remove(head);
    }

    private void resetTaskWithException(OverseerMessageHandler messageHandler, String id, String asyncId, String taskKey, ZkNodeProps message) {
      log.warn("Resetting task: {}, requestid: {}, taskKey: {}", id, asyncId, taskKey);
      try {
        if (asyncId != null) {
          if (!runningMap.remove(asyncId)) {
            log.warn("Could not find and remove async call [" + asyncId + "] from the running map.");
          }
        }

        synchronized (runningTasks) {
          runningTasks.remove(id);
        }

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
      if (response == null)
        return false;
      return !(response.getResponse().get("failure") != null || response.getResponse().get("exception") != null);
    }
  }

  private void printTrackingMaps() {
    if (log.isDebugEnabled()) {
      synchronized (runningTasks) {
        log.debug("RunningTasks: {}", runningTasks.toString());
      }
      log.debug("BlockedTasks: {}", blockedTasks.keySet().toString());
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
  public interface OverseerMessageHandlerSelector extends Closeable {
    OverseerMessageHandler selectOverseerMessageHandler(ZkNodeProps message);
  }

  final private TaskBatch taskBatch = new TaskBatch();

  public class TaskBatch {
    private long batchId = 0;

    public long getId() {
      return batchId;
    }

    public int getRunningTasks() {
      synchronized (runningTasks) {
        return runningTasks.size();
      }
    }
  }

}
