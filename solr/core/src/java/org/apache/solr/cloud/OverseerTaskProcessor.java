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
import org.apache.solr.cloud.overseer.ZkStateWriter;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.zookeeper.CreateMode;
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
import java.util.concurrent.Future;
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
  public static final int MAX_PARALLEL_TASKS = 100;
  public static final int MAX_BLOCKED_TASKS = 1000;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final CoreContainer cc;

  private OverseerTaskQueue workQueue;
  private DistributedMap runningMap;
  private DistributedMap completedMap;
  private DistributedMap failureMap;

  // Set that maintains a list of all the tasks that are running. This is keyed on zk id of the task.
  private final Set<String> runningTasks = ConcurrentHashMap.newKeySet(32);

  // List of completed tasks. This is used to clean up workQueue in zk.
  private final Map<String, QueueEvent> completedTasks = new ConcurrentHashMap<>(32, 0.75f);

  private final String myId;

  private volatile boolean isClosed;

  private final Stats stats;

  // Set of tasks that have been picked up for processing but not cleaned up from zk work-queue.
  // It may contain tasks that have completed execution, have been entered into the completed/failed map in zk but not
  // deleted from the work-queue as that is a batched operation.
  final private Set<String> runningZKTasks = ConcurrentHashMap.newKeySet(32);
  // This map may contain tasks which are read from work queue but could not
  // be executed because they are blocked or the execution queue is full
  // This is an optimization to ensure that we do not read the same tasks
  // again and again from ZK.
  final private Map<String, QueueEvent> blockedTasks = new ConcurrentSkipListMap<>();
  final private Predicate<String> excludedTasks = new Predicate<>() {
    @Override
    public boolean test(String s) {
      // nocommit
      if (s.startsWith(OverseerTaskQueue.RESPONSE_PREFIX)) {
        if (log.isDebugEnabled()) log.debug("exclude {} due to prefix {}", s, OverseerTaskQueue.RESPONSE_PREFIX);
        return true;
      }

      boolean contains = runningTasks.contains(s) || blockedTasks.containsKey(s) || runningZKTasks.contains(s);
      if (log.isDebugEnabled()) log.debug("test {} against {}, {}, {}  : {}", s, runningTasks, blockedTasks, runningZKTasks, contains);
      return contains;
    }

    @Override
    public String toString() {
      return StrUtils.join(ImmutableSet.of(runningTasks, blockedTasks.keySet()), ',');
    }

  };

  private final Object waitLock = new Object();

  protected final OverseerMessageHandlerSelector selector;

  private final String thisNode;
  private Map<Runner,Future> taskFutures = new ConcurrentHashMap<>();

  public OverseerTaskProcessor(CoreContainer cc, String myId,
                                        Stats stats,
                                        OverseerMessageHandlerSelector selector,
                                        OverseerTaskQueue workQueue,
                                        DistributedMap runningMap,
                                        DistributedMap completedMap,
                                        DistributedMap failureMap) {
    this.myId = myId;
    this.stats = stats;
    this.selector = selector;
    this.workQueue = workQueue;
    this.runningMap = runningMap;
    this.completedMap = completedMap;
    this.failureMap = failureMap;
    thisNode = Utils.getMDCNode();
    this.cc = cc;
  }

  @Override
  public void run() {

  }

  private int runningTasksSize() {
    return runningTasks.size();
  }

  private void cleanUpWorkQueue() throws KeeperException, InterruptedException {
    Set<Map.Entry<String, QueueEvent>> entrySet = completedTasks.entrySet();
    AtomicBoolean sessionExpired = new AtomicBoolean();
    AtomicBoolean interrupted = new AtomicBoolean();
    // TODO: async
    try (ParWork work = new ParWork(this, true, false)) {
      for (Map.Entry<String, QueueEvent> entry : entrySet) {
        work.collect("cleanWorkQueue", () -> {
          try {
            workQueue.remove(entry.getValue());
          } catch (KeeperException.SessionExpiredException e) {
            sessionExpired.set(true);
          } catch (InterruptedException e) {
            interrupted.set(true);
          } catch (Exception e) {
            log.error("Exception removing item from workQueue", e);
          }
          runningTasks.remove(entry.getKey());
        });
      }

    }

  }

  public void closing() {
    isClosed = true;
  }

  public void close() {
    close(false);
  }

  public void close(boolean closeAndDone) {
    if (log.isDebugEnabled()) {
      log.debug("close() - start");
    }
    isClosed = true;


    IOUtils.closeQuietly(selector);


//    if (closeAndDone) {
//      // nocommit
//      //      for (Future future : taskFutures.values()) {
//      //        future.cancel(false);
//      //      }
//      for (Future future : taskFutures.values()) {
//        try {
//          future.get(1, TimeUnit.SECONDS);
//        } catch (InterruptedException e) {
//          ParWork.propagateInterrupt(e);
//          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
//        } catch (Exception e) {
//          log.info("Exception closing Overseer {} {}", e.getClass().getName(), e.getMessage());
//        }
//      }
//    }

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
      runningMap.put(asyncId, null, CreateMode.PERSISTENT);
    }
  }

  protected class Runner implements Runnable {
    final ZkNodeProps message;
    final String operation;
    private final OverseerMessageHandler.Lock lock;
    private final ZkStateWriter zkStateWriter;
    volatile OverseerSolrResponse response;
    final QueueEvent head;
    final OverseerMessageHandler messageHandler;

    public Runner(OverseerMessageHandler messageHandler, ZkNodeProps message, String operation, QueueEvent head, OverseerMessageHandler.Lock lock, ZkStateWriter zkStateWriter) {
      this.message = message;
      this.operation = operation;
      this.head = head;
      this.messageHandler = messageHandler;
      this.lock = lock;
      this.zkStateWriter = zkStateWriter;
    }


    public void run() {
      boolean success = false;
      final String asyncId = message.getStr(ASYNC);
      String taskKey = messageHandler.getTaskKey(message);
      try {
        String statsName = messageHandler.getTimerName(operation);
        final Timer.Context timerContext = stats.time(statsName);

        try {
          if (log.isDebugEnabled()) {
            log.debug("Runner processing {}", head.getId());
          }

          response = messageHandler.processMessage(message, operation, zkStateWriter);
        } finally {
          timerContext.stop();
          updateStats(statsName);
        }
        if (log.isDebugEnabled()) {
          log.debug("finished processing message asyncId={}", asyncId);
        }
        if (asyncId != null) {
          if (response != null && (response.getResponse().get("failure") != null || response.getResponse().get("exception") != null)) {
            if (log.isDebugEnabled()) {
              log.debug("Updated failed map for task with id:[{}]", asyncId);
            }
            failureMap.put(asyncId, OverseerSolrResponseSerializer.serialize(response), CreateMode.PERSISTENT);
          } else {
            if (log.isDebugEnabled()) {
              log.debug("Updated completed map for task with zkid:[{}]", asyncId);
            }
            completedMap.put(asyncId, OverseerSolrResponseSerializer.serialize(response), CreateMode.PERSISTENT);

          }
        } else {
          byte[] sdata = OverseerSolrResponseSerializer.serialize(response);
          head.setBytes(sdata);
          log.debug("Completed task:[{}] {}", head.getId(), response.getResponse());
        }

        if (log.isDebugEnabled()) log.debug("Marked task [{}] as completed. asyncId={}", head.getId(), asyncId);
        markTaskComplete(head.getId(), asyncId);

        printTrackingMaps();

        log.debug(messageHandler.getName() + ": Message id:" + head.getId() + " complete, response:" + response.getResponse().toString());

        taskFutures.remove(this);
        success = true;
      } catch (InterruptedException | AlreadyClosedException e) {
        ParWork.propagateInterrupt(e);
        return;
      } catch (Exception e) {
        if (e instanceof KeeperException.SessionExpiredException) {
          log.warn("Session expired, exiting...", e);
          return;
        }
        log.error("Exception running task", e);
      } finally {
        if (lock != null) lock.unlock();
        if (!success) {
          // Reset task from tracking data structures so that it can be retried.
          try {
            resetTaskWithException(head.getId(), asyncId, taskKey);
          } catch(AlreadyClosedException e) {

          } catch (Exception e) {
            log.error("", e);
          }
        }
      }

      if (log.isDebugEnabled()) {
        log.debug("run() - end");
      }
    }

    private void markTaskComplete(String id, String asyncId)
        throws KeeperException, InterruptedException {
      try {
        synchronized (completedTasks) {
          completedTasks.put(id, head);
        }

        synchronized (runningTasks) {
          runningTasks.remove(id);
        }

        if (asyncId != null) {
          if (!runningMap.remove(asyncId)) {
            log.warn("Could not find and remove async call [{}] from the running map.", asyncId);
          }
        }
      } finally {
        workQueue.remove(head);
      }
    }

    private void resetTaskWithException(String id, String asyncId, String taskKey) throws KeeperException, InterruptedException {
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

      log.debug("BlockedTasks: {}", blockedTasks.keySet());

      log.debug("CompletedTasks: {}", completedTasks.keySet());
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
