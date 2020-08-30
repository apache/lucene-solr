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

import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableSet;
import org.apache.solr.cloud.OverseerTaskQueue.QueueEvent;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonParams.ID;
import java.io.Closeable;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

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
  public static final int MAX_PARALLEL_TASKS = 10;
  public static final int MAX_BLOCKED_TASKS = 1000;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final CoreContainer cc;

  private OverseerTaskQueue workQueue;
  private DistributedMap runningMap;
  private DistributedMap completedMap;
  private DistributedMap failureMap;

  // Set that maintains a list of all the tasks that are running. This is keyed on zk id of the task.
  private final Set<String> runningTasks = ConcurrentHashMap.newKeySet(500);

  // List of completed tasks. This is used to clean up workQueue in zk.
  private final Map<String, QueueEvent> completedTasks = new ConcurrentHashMap<>(132, 0.75f, 50);

  private final String myId;

  private volatile boolean isClosed;

  private final Stats stats;

  // Set of tasks that have been picked up for processing but not cleaned up from zk work-queue.
  // It may contain tasks that have completed execution, have been entered into the completed/failed map in zk but not
  // deleted from the work-queue as that is a batched operation.
  final private Set<String> runningZKTasks = ConcurrentHashMap.newKeySet(500);
  // This map may contain tasks which are read from work queue but could not
  // be executed because they are blocked or the execution queue is full
  // This is an optimization to ensure that we do not read the same tasks
  // again and again from ZK.
  final private Map<String, QueueEvent> blockedTasks = new ConcurrentSkipListMap<>();
  final private Predicate<String> excludedTasks = new Predicate<String>() {
    @Override
    public boolean test(String s) {
      // nocommit
      return runningTasks.contains(s) || blockedTasks.containsKey(s) || runningZKTasks.contains(s);
    }

    @Override
    public String toString() {
      return StrUtils.join(ImmutableSet.of(runningTasks, blockedTasks.keySet()), ',');
    }

  };

  private final Object waitLock = new Object();

  protected final OverseerMessageHandlerSelector selector;

  private final OverseerNodePrioritizer prioritizer;

  private final String thisNode;

  public OverseerTaskProcessor(CoreContainer cc, String myId,
                                        Stats stats,
                                        OverseerMessageHandlerSelector selector,
                                        OverseerNodePrioritizer prioritizer,
                                        OverseerTaskQueue workQueue,
                                        DistributedMap runningMap,
                                        DistributedMap completedMap,
                                        DistributedMap failureMap) {
    this.myId = myId;
    this.stats = stats;
    this.selector = selector;
    this.prioritizer = prioritizer;
    this.workQueue = workQueue;
    this.runningMap = runningMap;
    this.completedMap = completedMap;
    this.failureMap = failureMap;
    thisNode = Utils.getMDCNode();
    this.cc = cc;
  }

  @Override
  public void run() {
    MDCLoggingContext.setNode(thisNode);
    log.debug("Process current queue of overseer operations");

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
    } catch (Exception e) {
      ParWork.propegateInterrupt(e);
      if (e instanceof KeeperException.SessionExpiredException) {
        return;
      }
      if (e instanceof InterruptedException
          || e instanceof AlreadyClosedException) {
        return;
      }
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }

    if (oldestItemInWorkQueue == null) hasLeftOverItems = false;
    else log.debug(
        "Found already existing elements in the work-queue. Last element: {}",
        oldestItemInWorkQueue);

    if (prioritizer != null) {
      try {
        prioritizer.prioritizeOverseerNodes(myId);
      } catch (Exception e) {
        ParWork.propegateInterrupt(e);
        if (e instanceof KeeperException.SessionExpiredException) {
          return;
        }
        if (e instanceof InterruptedException
            || e instanceof AlreadyClosedException) {
          return;
        }
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }
    }

    try {
      while (!this.isClosed()) {
        try {

          if (log.isDebugEnabled()) log.debug(
              "Cleaning up work-queue. #Running tasks: {} #Completed tasks: {}",
              runningTasksSize(), completedTasks.size());
          cleanUpWorkQueue();

          printTrackingMaps();

          boolean waited = false;

          while (runningTasksSize() > MAX_PARALLEL_TASKS) {
            synchronized (waitLock) {
              waitLock.wait(1000);//wait for 1000 ms or till a task is complete
            }
            waited = true;
          }

          if (waited) cleanUpWorkQueue();

          ArrayList<QueueEvent> heads = new ArrayList<>(
              blockedTasks.size() + MAX_PARALLEL_TASKS);
          heads.addAll(blockedTasks.values());
          blockedTasks.clear(); // clear it now; may get refilled below.
          //If we have enough items in the blocked tasks already, it makes
          // no sense to read more items from the work queue. it makes sense
          // to clear out at least a few items in the queue before we read more items
          if (heads.size() < MAX_BLOCKED_TASKS) {
            //instead of reading MAX_PARALLEL_TASKS items always, we should only fetch as much as we can execute
            int toFetch = Math.min(MAX_BLOCKED_TASKS - heads.size(),
                MAX_PARALLEL_TASKS - runningTasksSize());
            List<QueueEvent> newTasks = workQueue
                .peekTopN(toFetch, excludedTasks, 10000);
            if (log.isDebugEnabled()) log.debug("Got {} tasks from work-queue : [{}]", newTasks.size(),
                newTasks);
            heads.addAll(newTasks);
          }

          if (isClosed) return;

          taskBatch.batchId++;

          for (QueueEvent head : heads) {

            if (runningZKTasks.contains(head.getId())) {
              log.warn("Task found in running ZKTasks already, continuing");
              continue;
            }

            final ZkNodeProps message = ZkNodeProps.load(head.getBytes());
            final String asyncId = message.getStr(ASYNC);
            if (hasLeftOverItems) {
              if (head.getId().equals(oldestItemInWorkQueue))
                hasLeftOverItems = false;
              if (asyncId != null && (completedMap.contains(asyncId)
                  || failureMap.contains(asyncId))) {
                log.debug(
                    "Found already processed task in workQueue, cleaning up. AsyncId [{}]",
                    asyncId);
                workQueue.remove(head);
                continue;
              }
            }
            String operation = message.getStr(Overseer.QUEUE_OPERATION);
            if (operation == null) {
              log.error("Msg does not have required " + Overseer.QUEUE_OPERATION
                  + ": {}", message);
              workQueue.remove(head);
              continue;
            }
            OverseerMessageHandler messageHandler = selector
                .selectOverseerMessageHandler(message);
            OverseerMessageHandler.Lock lock = messageHandler
                .lockTask(message, taskBatch);
            if (lock == null) {
              log.debug("Exclusivity check failed for [{}]",
                  message.toString());
              // we may end crossing the size of the MAX_BLOCKED_TASKS. They are fine
              if (blockedTasks.size() < MAX_BLOCKED_TASKS)
                blockedTasks.put(head.getId(), head);
              continue;
            }
            try {
              markTaskAsRunning(head, asyncId);
              if (log.isDebugEnabled()) {
                log.debug("Marked task [{}] as running", head.getId());
              }
            } catch (Exception e) {
              if (e instanceof KeeperException.SessionExpiredException
                  || e instanceof InterruptedException) {
                ParWork.propegateInterrupt(e);
                log.error("ZooKeeper session has expired");
                return;
              }

              throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
            }
            if (log.isDebugEnabled()) log.debug(
                messageHandler.getName() + ": Get the message id:" + head
                    .getId() + " message:" + message.toString());
            Runner runner = new Runner(messageHandler, message, operation, head,
                lock);
            ParWork.getRootSharedExecutor().execute(runner);
          }

        } catch (InterruptedException | AlreadyClosedException e) {
          ParWork.propegateInterrupt(e, true);
          return;
        } catch (KeeperException.SessionExpiredException e) {
          log.warn("Zookeeper expiration");
          return;
        } catch (Exception e) {
          log.error("Unexpected exception", e);
        }
      }
    } finally {
      this.close();
    }

    if (log.isDebugEnabled()) {
      log.debug("run() - end");
    }
  }

  private int runningTasksSize() {
    if (log.isDebugEnabled()) {
      log.debug("runningTasksSize() - start");
    }

    int returnint = runningTasks.size();
    if (log.isDebugEnabled()) {
      log.debug("runningTasksSize() - end");
    }
    return returnint;

  }

  private void cleanUpWorkQueue() throws KeeperException, InterruptedException {
    if (log.isDebugEnabled()) {
      log.debug("cleanUpWorkQueue() - start");
    }

    Set<Map.Entry<String, QueueEvent>> entrySet = completedTasks.entrySet();
    AtomicBoolean sessionExpired = new AtomicBoolean();
    AtomicBoolean interrupted = new AtomicBoolean();
    try (ParWork work = new ParWork(this)) {
      for (Map.Entry<String, QueueEvent> entry : entrySet) {
        work.collect("cleanWorkQueue", ()->{
          if (interrupted.get() || sessionExpired.get()) {
            return;
          }
          try {
            workQueue.remove(entry.getValue());
          } catch (KeeperException.SessionExpiredException e) {
            sessionExpired.set(true);
          } catch (InterruptedException e) {
            interrupted.set(true);
          } catch (KeeperException e) {
           log.error("Exception removing item from workQueue", e);
          }
          runningTasks.remove(entry.getKey());});
          completedTasks.remove(entry.getKey());
      }
    }


    if (interrupted.get()) {
      Thread.currentThread().interrupt();
      throw new InterruptedException();
    }

    if (sessionExpired.get()) {
      throw new KeeperException.SessionExpiredException();
    }

    if (log.isDebugEnabled()) {
      log.debug("cleanUpWorkQueue() - end");
    }
  }

  public void close() {
    if (log.isDebugEnabled()) {
      log.debug("close() - start");
    }
    ParWork.close(selector);
    isClosed = true;
  }

  public static List<String> getSortedOverseerNodeNames(SolrZkClient zk) throws KeeperException, InterruptedException {
    List<String> children = zk.getChildren(Overseer.OVERSEER_ELECT + LeaderElector.ELECTION_NODE, null, true);

    LeaderElector.sortSeqs(children);
    ArrayList<String> nodeNames = new ArrayList<>(children.size());
    for (String c : children) nodeNames.add(LeaderElector.getNodeName(c));
    return nodeNames;
  }

  public static List<String> getSortedElectionNodes(SolrZkClient zk, String path) throws KeeperException, InterruptedException {
    List<String> children = zk.getChildren(path, null, true);
      LeaderElector.sortSeqs(children);
      return children;
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
      data = zkClient.getData(Overseer.OVERSEER_ELECT + "/leader", null, new Stat());
    } catch (KeeperException.NoNodeException e) {
      return null;
    }
    Map m = (Map) Utils.fromJSON(data);
    return  (String) m.get(ID);
  }

  public boolean isClosed() {
    return isClosed || cc.isShutDown();
  }

  @SuppressWarnings("unchecked")
  private void markTaskAsRunning(QueueEvent head, String asyncId)
      throws KeeperException, InterruptedException {
    runningZKTasks.add(head.getId());

    runningTasks.add(head.getId());

    if (asyncId != null) {
      log.info("Add async task {} to running map", asyncId);
      runningMap.put(asyncId, null);
    }
  }

  protected class Runner implements Runnable {
    final ZkNodeProps message;
    final String operation;
    volatile OverseerSolrResponse response;
    final QueueEvent head;
    final OverseerMessageHandler messageHandler;

    public Runner(OverseerMessageHandler messageHandler, ZkNodeProps message, String operation, QueueEvent head, OverseerMessageHandler.Lock lock) {
      this.message = message;
      this.operation = operation;
      this.head = head;
      this.messageHandler = messageHandler;
    }


    public void run() {
      String statsName = messageHandler.getTimerName(operation);
      final Timer.Context timerContext = stats.time(statsName);

      boolean success = false;
      final String asyncId = message.getStr(ASYNC);
      String taskKey = messageHandler.getTaskKey(message);

      try {
        try {
          if (log.isDebugEnabled()) {
            log.debug("Runner processing {}", head.getId());
          }
          response = messageHandler.processMessage(message, operation);
        } finally {
          timerContext.stop();
          updateStats(statsName);
        }

        if (asyncId != null) {
          if (response != null && (response.getResponse().get("failure") != null
              || response.getResponse().get("exception") != null)) {
            failureMap.put(asyncId, OverseerSolrResponseSerializer.serialize(response));
            if (log.isDebugEnabled()) {
              log.debug("Updated failed map for task with zkid:[{}]", head.getId());
            }
          } else {
            completedMap.put(asyncId, OverseerSolrResponseSerializer.serialize(response));
            log.debug("Updated completed map for task with zkid:[{}]", head.getId());
          }
        } else {
          byte[] sdata = OverseerSolrResponseSerializer.serialize(response);
         // cc.getZkController().zkStateReader.getZkClient().setData(head.getId(), sdata, false);
          head.setBytes(sdata);
          log.debug("Completed task:[{}] {}", head.getId(), response.getResponse());
        }

        markTaskComplete(head.getId(), asyncId);
        log.debug("Marked task [{}] as completed.", head.getId());
        printTrackingMaps();

        log.debug(messageHandler.getName() + ": Message id:" + head.getId() +
            " complete, response:" + response.getResponse().toString());
        success = true;
      } catch (InterruptedException | AlreadyClosedException e) {
        ParWork.propegateInterrupt(e);
        return;
      } catch (Exception e) {
        if (e instanceof KeeperException.SessionExpiredException) {
          log.warn("Session expired, exiting...", e);
          return;
        }
        log.error("Exception running task", e);
      }

      if (log.isDebugEnabled()) {
        log.debug("run() - end");
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
          log.warn("Could not find and remove async call [{}] from the running map.", asyncId );
        }
      }

      workQueue.remove(head);
    }

    private void resetTaskWithException(OverseerMessageHandler messageHandler, String id, String asyncId, String taskKey, ZkNodeProps message) throws KeeperException, InterruptedException {
      log.warn("Resetting task: {}, requestid: {}, taskKey: {}", id, asyncId, taskKey);
        if (asyncId != null) {
          if (!runningMap.remove(asyncId)) {
            log.warn("Could not find and remove async call [{}] from the running map.", asyncId);
          }
        }

        synchronized (runningTasks) {
          runningTasks.remove(id);
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
      log.debug("RunningTasks: {}", runningTasks);

      if (log.isDebugEnabled()) {
        log.debug("BlockedTasks: {}", blockedTasks.keySet());
      }
      if (log.isDebugEnabled()) {
        log.debug("CompletedTasks: {}", completedTasks.keySet());
      }

      log.info("RunningZKTasks: {}", runningZKTasks);

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
      return runningTasks.size();
    }
  }

}
