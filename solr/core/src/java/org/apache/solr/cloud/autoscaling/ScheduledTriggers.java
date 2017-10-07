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

package org.apache.solr.cloud.autoscaling;

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventProcessorStage;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest.RequestStatusResponse;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.cloud.ActionThrottle;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.autoscaling.ExecutePlanAction.waitForTaskToFinish;

/**
 * Responsible for scheduling active triggers, starting and stopping them and
 * performing actions when they fire
 */
public class ScheduledTriggers implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  static final int DEFAULT_SCHEDULED_TRIGGER_DELAY_SECONDS = 1;
  static final int DEFAULT_MIN_MS_BETWEEN_ACTIONS = 5000;

  private final Map<String, ScheduledTrigger> scheduledTriggers = new ConcurrentHashMap<>();

  /**
   * Thread pool for scheduling the triggers
   */
  private final ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;

  /**
   * Single threaded executor to run the actions upon a trigger event. We rely on this being a single
   * threaded executor to ensure that trigger fires do not step on each other as well as to ensure
   * that we do not run scheduled trigger threads while an action has been submitted to this executor
   */
  private final ExecutorService actionExecutor;

  private boolean isClosed = false;

  private final AtomicBoolean hasPendingActions = new AtomicBoolean(false);

  private final ActionThrottle actionThrottle;

  private final SolrZkClient zkClient;

  private final Overseer.Stats queueStats;

  private final CoreContainer coreContainer;

  private final TriggerListeners listeners;

  private AutoScalingConfig autoScalingConfig;

  public ScheduledTriggers(ZkController zkController) {
    // todo make the core pool size configurable
    // it is important to use more than one because a time taking trigger can starve other scheduled triggers
    // ideally we should have as many core threads as the number of triggers but firstly, we don't know beforehand
    // how many triggers we have and secondly, that many threads will always be instantiated and kept around idle
    // so it is wasteful as well. Hopefully 4 is a good compromise.
    scheduledThreadPoolExecutor = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(4,
        new DefaultSolrThreadFactory("ScheduledTrigger"));
    scheduledThreadPoolExecutor.setRemoveOnCancelPolicy(true);
    scheduledThreadPoolExecutor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    actionExecutor = ExecutorUtil.newMDCAwareSingleThreadExecutor(new DefaultSolrThreadFactory("AutoscalingActionExecutor"));
    // todo make the wait time configurable
    actionThrottle = new ActionThrottle("action", DEFAULT_MIN_MS_BETWEEN_ACTIONS);
    coreContainer = zkController.getCoreContainer();
    zkClient = zkController.getZkClient();
    queueStats = new Overseer.Stats();
    listeners = new TriggerListeners();
  }

  /**
   * Set the current autoscaling config. This is invoked by {@link OverseerTriggerThread} when autoscaling.json is updated,
   * and it re-initializes trigger listeners.
   * @param autoScalingConfig current autoscaling.json
   */
  public void setAutoScalingConfig(AutoScalingConfig autoScalingConfig) {
    this.autoScalingConfig = autoScalingConfig;
    listeners.setAutoScalingConfig(autoScalingConfig);
  }

  /**
   * Adds a new trigger or replaces an existing one. The replaced trigger, if any, is closed
   * <b>before</b> the new trigger is run. If a trigger is replaced with itself then this
   * operation becomes a no-op.
   *
   * @param newTrigger the trigger to be managed
   * @throws AlreadyClosedException if this class has already been closed
   */
  public synchronized void add(AutoScaling.Trigger newTrigger) {
    if (isClosed) {
      throw new AlreadyClosedException("ScheduledTriggers has been closed and cannot be used anymore");
    }
    ScheduledTrigger st;
    try {
      st = new ScheduledTrigger(newTrigger, zkClient, queueStats);
    } catch (Exception e) {
      if (isClosed) {
        throw new AlreadyClosedException("ScheduledTriggers has been closed and cannot be used anymore");
      }
      if (!zkClient.isConnected() || zkClient.isClosed()) {
        log.error("Failed to add trigger " + newTrigger.getName() + " - closing or disconnected from ZK", e);
      } else {
        log.error("Failed to add trigger " + newTrigger.getName(), e);
      }
      return;
    }
    ScheduledTrigger scheduledTrigger = st;

    ScheduledTrigger old = scheduledTriggers.putIfAbsent(newTrigger.getName(), scheduledTrigger);
    if (old != null) {
      if (old.trigger.equals(newTrigger)) {
        // the trigger wasn't actually modified so we do nothing
        return;
      }
      IOUtils.closeQuietly(old);
      newTrigger.restoreState(old.trigger);
      scheduledTrigger.setReplay(false);
      scheduledTriggers.replace(newTrigger.getName(), scheduledTrigger);
    }
    newTrigger.setProcessor(event -> {
      if (coreContainer.isShutDown()) {
        String msg = String.format(Locale.ROOT, "Ignoring autoscaling event %s because Solr has been shutdown.", event.toString());
        log.warn(msg);
        listeners.fireListeners(event.getSource(), event, TriggerEventProcessorStage.ABORTED, msg);
        return false;
      }
      ScheduledTrigger scheduledSource = scheduledTriggers.get(event.getSource());
      if (scheduledSource == null) {
        String msg = String.format(Locale.ROOT, "Ignoring autoscaling event %s because the source trigger: %s doesn't exist.", event.toString(), event.getSource());
        listeners.fireListeners(event.getSource(), event, TriggerEventProcessorStage.FAILED, msg);
        log.warn(msg);
        return false;
      }
      boolean replaying = event.getProperty(TriggerEvent.REPLAYING) != null ? (Boolean)event.getProperty(TriggerEvent.REPLAYING) : false;
      AutoScaling.Trigger source = scheduledSource.trigger;
      if (source.isClosed()) {
        String msg = String.format(Locale.ROOT, "Ignoring autoscaling event %s because the source trigger: %s has already been closed", event.toString(), source);
        listeners.fireListeners(event.getSource(), event, TriggerEventProcessorStage.ABORTED, msg);
        log.warn(msg);
        // we do not want to lose this event just because the trigger was closed, perhaps a replacement will need it
        return false;
      }
      if (hasPendingActions.compareAndSet(false, true)) {
        final boolean enqueued;
        if (replaying) {
          enqueued = false;
        } else {
          enqueued = scheduledTrigger.enqueue(event);
        }
        // fire STARTED event listeners after enqueuing the event is successful
        listeners.fireListeners(event.getSource(), event, TriggerEventProcessorStage.STARTED);
        List<TriggerAction> actions = source.getActions();
        if (actions != null) {
          actionExecutor.submit(() -> {
            assert hasPendingActions.get();
            log.debug("-- processing actions for " + event);
            try {
              // let the action executor thread wait instead of the trigger thread so we use the throttle here
              actionThrottle.minimumWaitBetweenActions();
              actionThrottle.markAttemptingAction();

              // in future, we could wait for pending tasks in a different thread and re-enqueue
              // this event so that we continue processing other events and not block this action executor
              waitForPendingTasks(newTrigger, actions);

              ActionContext actionContext = new ActionContext(coreContainer, newTrigger, new HashMap<>());
              for (TriggerAction action : actions) {
                List<String> beforeActions = (List<String>)actionContext.getProperties().computeIfAbsent(TriggerEventProcessorStage.BEFORE_ACTION.toString(), k -> new ArrayList<String>());
                beforeActions.add(action.getName());
                listeners.fireListeners(event.getSource(), event, TriggerEventProcessorStage.BEFORE_ACTION, action.getName(), actionContext);
                try {
                  action.process(event, actionContext);
                } catch (Exception e) {
                  listeners.fireListeners(event.getSource(), event, TriggerEventProcessorStage.FAILED, action.getName(), actionContext, e, null);
                  log.error("Error executing action: " + action.getName() + " for trigger event: " + event, e);
                  throw e;
                }
                List<String> afterActions = (List<String>)actionContext.getProperties().computeIfAbsent(TriggerEventProcessorStage.AFTER_ACTION.toString(), k -> new ArrayList<String>());
                afterActions.add(action.getName());
                listeners.fireListeners(event.getSource(), event, TriggerEventProcessorStage.AFTER_ACTION, action.getName(), actionContext);
              }
              if (enqueued) {
                TriggerEvent ev = scheduledTrigger.dequeue();
                assert ev.getId().equals(event.getId());
              }
              listeners.fireListeners(event.getSource(), event, TriggerEventProcessorStage.SUCCEEDED);
            } finally {
              hasPendingActions.set(false);
            }
          });
        } else {
          if (enqueued) {
            TriggerEvent ev = scheduledTrigger.dequeue();
            if (!ev.getId().equals(event.getId())) {
              throw new RuntimeException("Wrong event dequeued, queue of " + scheduledTrigger.trigger.getName()
              + " is broken! Expected event=" + event + " but got " + ev);
            }
          }
          listeners.fireListeners(event.getSource(), event, TriggerEventProcessorStage.SUCCEEDED);
          hasPendingActions.set(false);
        }
        return true;
      } else {
        // there is an action in the queue and we don't want to enqueue another until it is complete
        return false;
      }
    });
    newTrigger.init(); // mark as ready for scheduling
    scheduledTrigger.scheduledFuture = scheduledThreadPoolExecutor.scheduleWithFixedDelay(scheduledTrigger, 0, DEFAULT_SCHEDULED_TRIGGER_DELAY_SECONDS, TimeUnit.SECONDS);
  }

  private void waitForPendingTasks(AutoScaling.Trigger newTrigger, List<TriggerAction> actions) throws AlreadyClosedException {
    try (CloudSolrClient cloudSolrClient = new CloudSolrClient.Builder()
        .withZkHost(coreContainer.getZkController().getZkServerAddress())
        .withHttpClient(coreContainer.getUpdateShardHandler().getHttpClient())
        .build()) {

      SolrZkClient zkClient = coreContainer.getZkController().getZkClient();

      for (TriggerAction action : actions) {
        if (action instanceof ExecutePlanAction) {
          String parentPath = ZkStateReader.SOLR_AUTOSCALING_TRIGGER_STATE_PATH + "/" + newTrigger.getName() + "/" + action.getName();
          if (!zkClient.exists(parentPath, true))  {
            break;
          }
          List<String> children = zkClient.getChildren(parentPath, null, true);
          if (children != null) {
            for (String child : children) {
              String path = parentPath + '/' + child;
              byte[] data = zkClient.getData(path, null, null, true);
              if (data != null) {
                Map map = (Map) Utils.fromJSON(data);
                String requestid = (String) map.get("requestid");
                try {
                  log.debug("Found pending task with requestid={}", requestid);
                  RequestStatusResponse statusResponse = waitForTaskToFinish(cloudSolrClient, requestid,
                      ExecutePlanAction.DEFAULT_TASK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                  if (statusResponse != null) {
                    RequestStatusState state = statusResponse.getRequestStatus();
                    if (state == RequestStatusState.COMPLETED || state == RequestStatusState.FAILED || state == RequestStatusState.NOT_FOUND) {
                      zkClient.delete(path, -1, true);
                    }
                  }
                } catch (Exception e) {
                  if (coreContainer.isShutDown())  {
                    throw e; // propagate the abort to the caller
                  }
                  Throwable rootCause = ExceptionUtils.getRootCause(e);
                  if (rootCause instanceof IllegalStateException && rootCause.getMessage().contains("Connection pool shut down")) {
                    throw e;
                  }
                  if (rootCause instanceof TimeoutException && rootCause.getMessage().contains("Could not connect to ZooKeeper")) {
                    throw e;
                  }
                  log.error("Unexpected exception while waiting for pending task with requestid: " + requestid + " to finish", e);
                }
              }
            }
          }
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Thread interrupted", e);
    } catch (Exception e) {
      if (coreContainer.isShutDown())  {
        throw new AlreadyClosedException("The Solr instance has been shutdown");
      }
      // we catch but don't rethrow because a failure to wait for pending tasks
      // should not keep the actions from executing
      log.error("Unexpected exception while waiting for pending tasks to finish", e);
    }
  }

  /**
   * Removes and stops the trigger with the given name. Also cleans up any leftover
   * state / events in ZK.
   *
   * @param triggerName the name of the trigger to be removed
   */
  public synchronized void remove(String triggerName) {
    ScheduledTrigger removed = scheduledTriggers.remove(triggerName);
    IOUtils.closeQuietly(removed);
    removeTriggerZKData(triggerName);
  }

  private void removeTriggerZKData(String triggerName) {
    String statePath = ZkStateReader.SOLR_AUTOSCALING_TRIGGER_STATE_PATH + "/" + triggerName;
    String eventsPath = ZkStateReader.SOLR_AUTOSCALING_EVENTS_PATH + "/" + triggerName;
    try {
      zkDelTree(zkClient, statePath);
    } catch (KeeperException | InterruptedException e) {
      log.warn("Failed to remove state for removed trigger " + statePath, e);
    }
    try {
      zkDelTree(zkClient, eventsPath);
    } catch (KeeperException | InterruptedException e) {
      log.warn("Failed to remove events for removed trigger " + eventsPath, e);
    }
  }

  static List<OpResult> zkDelTree(SolrZkClient zkClient, String znode) throws KeeperException, InterruptedException {
    if (zkClient.exists(znode, true)) {
      ArrayList<Op> ops = new ArrayList<>();
      zkDelTree(zkClient, znode, ops);
      return zkClient.multi(ops, true);
    }
    return Collections.emptyList();
  }

  private static void zkDelTree(SolrZkClient zkClient, String znode, ArrayList<Op> ops) throws KeeperException, InterruptedException {
    if (zkClient.exists(znode, true)) {
      List<String> children = zkClient.getChildren(znode, null, true);
      if (children != null) {
        for (String child : children) {
          String path = znode + "/" + child;
          zkDelTree(zkClient, path, ops);
        }
      }
      ops.add(Op.delete(znode, -1));
    }
  }

  /**
   * @return an unmodifiable set of names of all triggers being managed by this class
   */
  public synchronized Set<String> getScheduledTriggerNames() {
    return Collections.unmodifiableSet(new HashSet<>(scheduledTriggers.keySet())); // shallow copy
  }

  @Override
  public void close() throws IOException {
    synchronized (this) {
      // mark that we are closed
      isClosed = true;
      for (ScheduledTrigger scheduledTrigger : scheduledTriggers.values()) {
        IOUtils.closeQuietly(scheduledTrigger);
      }
      scheduledTriggers.clear();
    }
    // shutdown and interrupt all running tasks because there's no longer any
    // guarantee about cluster state
    scheduledThreadPoolExecutor.shutdownNow();
    actionExecutor.shutdownNow();
    listeners.close();
  }

  private class ScheduledTrigger implements Runnable, Closeable {
    AutoScaling.Trigger trigger;
    ScheduledFuture<?> scheduledFuture;
    TriggerEventQueue queue;
    boolean replay;
    volatile boolean isClosed;

    ScheduledTrigger(AutoScaling.Trigger trigger, SolrZkClient zkClient, Overseer.Stats stats) {
      this.trigger = trigger;
      this.queue = new TriggerEventQueue(zkClient, trigger.getName(), stats);
      this.replay = true;
      this.isClosed = false;
    }

    public void setReplay(boolean replay) {
      this.replay = replay;
    }

    public boolean enqueue(TriggerEvent event) {
      if (isClosed) {
        throw new AlreadyClosedException("ScheduledTrigger " + trigger.getName() + " has been closed.");
      }
      return queue.offerEvent(event);
    }

    public TriggerEvent dequeue() {
      if (isClosed) {
        throw new AlreadyClosedException("ScheduledTrigger " + trigger.getName() + " has been closed.");
      }
      TriggerEvent event = queue.pollEvent();
      return event;
    }

    @Override
    public void run() {
      if (isClosed) {
        throw new AlreadyClosedException("ScheduledTrigger " + trigger.getName() + " has been closed.");
      }
      // fire a trigger only if an action is not pending
      // note this is not fool proof e.g. it does not prevent an action being executed while a trigger
      // is still executing. There is additional protection against that scenario in the event listener.
      if (!hasPendingActions.get())  {
        // replay accumulated events on first run, if any
        if (replay) {
          TriggerEvent event;
          // peek first without removing - we may crash before calling the listener
          while ((event = queue.peekEvent()) != null) {
            // override REPLAYING=true
            event.getProperties().put(TriggerEvent.REPLAYING, true);
            if (! trigger.getProcessor().process(event)) {
              log.error("Failed to re-play event, discarding: " + event);
            }
            queue.pollEvent(); // always remove it from queue
          }
          // now restore saved state to possibly generate new events from old state on the first run
          try {
            trigger.restoreState();
          } catch (Exception e) {
            // log but don't throw - see below
            log.error("Error restoring trigger state " + trigger.getName(), e);
          }
          replay = false;
        }
        try {
          trigger.run();
        } catch (Exception e) {
          // log but do not propagate exception because an exception thrown from a scheduled operation
          // will suppress future executions
          log.error("Unexpected exception from trigger: " + trigger.getName(), e);
        } finally {
          // checkpoint after each run
          trigger.saveState();
        }
      }
    }

    @Override
    public void close() throws IOException {
      isClosed = true;
      if (scheduledFuture != null) {
        scheduledFuture.cancel(true);
      }
      IOUtils.closeQuietly(trigger);
    }
  }

  private class TriggerListeners {
    Map<String, Map<TriggerEventProcessorStage, List<TriggerListener>>> listenersPerStage = new HashMap<>();
    Map<String, TriggerListener> listenersPerName = new HashMap<>();
    ReentrantLock updateLock = new ReentrantLock();

    void setAutoScalingConfig(AutoScalingConfig autoScalingConfig) {
      updateLock.lock();
      // we will recreate this from scratch
      listenersPerStage.clear();
      try {
        Set<String> triggerNames = autoScalingConfig.getTriggerConfigs().keySet();
        Map<String, AutoScalingConfig.TriggerListenerConfig> configs = autoScalingConfig.getTriggerListenerConfigs();
        Set<String> listenerNames = configs.entrySet().stream().map(entry -> entry.getValue().name).collect(Collectors.toSet());
        // close those for non-existent triggers and nonexistent listener configs
        for (Iterator<Map.Entry<String, TriggerListener>> it = listenersPerName.entrySet().iterator(); it.hasNext(); ) {
          Map.Entry<String, TriggerListener> entry = it.next();
          String name = entry.getKey();
          TriggerListener listener = entry.getValue();
          if (!triggerNames.contains(listener.getConfig().trigger) || !listenerNames.contains(name)) {
            try {
              listener.close();
            } catch (Exception e) {
              log.warn("Exception closing old listener " + listener.getConfig(), e);
            }
            it.remove();
          }
        }
        for (Map.Entry<String, AutoScalingConfig.TriggerListenerConfig> entry : configs.entrySet()) {
          AutoScalingConfig.TriggerListenerConfig config = entry.getValue();
          if (!triggerNames.contains(config.trigger)) {
            log.debug("-- skipping listener for non-existent trigger: {}", config);
            continue;
          }
          // find previous instance and reuse if possible
          TriggerListener oldListener = listenersPerName.get(config.name);
          TriggerListener listener = null;
          if (oldListener != null) {
            if (!oldListener.getConfig().equals(config)) { // changed config
              try {
                oldListener.close();
              } catch (Exception e) {
                log.warn("Exception closing old listener " + oldListener.getConfig(), e);
              }
            } else {
              listener = oldListener; // reuse
            }
          }
          if (listener == null) { // create new instance
            String clazz = config.listenerClass;
            try {
              listener = coreContainer.getResourceLoader().newInstance(clazz, TriggerListener.class);
            } catch (Exception e) {
              log.warn("Invalid TriggerListener class name '" + clazz + "', skipping...", e);
            }
            if (listener != null) {
              try {
                listener.init(coreContainer, config);
                listenersPerName.put(config.name, listener);
              } catch (Exception e) {
                log.warn("Error initializing TriggerListener " + config, e);
                IOUtils.closeQuietly(listener);
                listener = null;
              }
            }
          }
          if (listener == null) {
            continue;
          }
          // add per stage
          for (TriggerEventProcessorStage stage : config.stages) {
            addPerStage(config.trigger, stage, listener);
          }
          // add also for beforeAction / afterAction TriggerStage
          if (!config.beforeActions.isEmpty()) {
            addPerStage(config.trigger, TriggerEventProcessorStage.BEFORE_ACTION, listener);
          }
          if (!config.afterActions.isEmpty()) {
            addPerStage(config.trigger, TriggerEventProcessorStage.AFTER_ACTION, listener);
          }
        }
      } finally {
        updateLock.unlock();
      }
    }

    private void addPerStage(String triggerName, TriggerEventProcessorStage stage, TriggerListener listener) {
      Map<TriggerEventProcessorStage, List<TriggerListener>> perStage =
          listenersPerStage.computeIfAbsent(triggerName, k -> new HashMap<>());
      List<TriggerListener> lst = perStage.computeIfAbsent(stage, k -> new ArrayList<>(3));
      lst.add(listener);
    }

    void reset() {
      updateLock.lock();
      try {
        listenersPerStage.clear();
        for (TriggerListener listener : listenersPerName.values()) {
          IOUtils.closeQuietly(listener);
        }
        listenersPerName.clear();
      } finally {
        updateLock.unlock();
      }
    }

    void close() {
      reset();
    }

    List<TriggerListener> getTriggerListeners(String trigger, TriggerEventProcessorStage stage) {
      Map<TriggerEventProcessorStage, List<TriggerListener>> perStage = listenersPerStage.get(trigger);
      if (perStage == null) {
        return Collections.emptyList();
      }
      List<TriggerListener> lst = perStage.get(stage);
      if (lst == null) {
        return Collections.emptyList();
      } else {
        return Collections.unmodifiableList(lst);
      }
    }

    void fireListeners(String trigger, TriggerEvent event, TriggerEventProcessorStage stage) {
      fireListeners(trigger, event, stage, null, null, null, null);
    }

    void fireListeners(String trigger, TriggerEvent event, TriggerEventProcessorStage stage, String message) {
      fireListeners(trigger, event, stage, null, null, null, message);
    }

    void fireListeners(String trigger, TriggerEvent event, TriggerEventProcessorStage stage, String actionName,
                       ActionContext context) {
      fireListeners(trigger, event, stage, actionName, context, null, null);
    }

    void fireListeners(String trigger, TriggerEvent event, TriggerEventProcessorStage stage, String actionName,
                       ActionContext context, Throwable error, String message) {
      updateLock.lock();
      try {
        for (TriggerListener listener : getTriggerListeners(trigger, stage)) {
          if (actionName != null) {
            AutoScalingConfig.TriggerListenerConfig config = listener.getConfig();
            if (stage == TriggerEventProcessorStage.BEFORE_ACTION) {
              if (!config.beforeActions.contains(actionName)) {
                continue;
              }
            } else if (stage == TriggerEventProcessorStage.AFTER_ACTION) {
              if (!config.afterActions.contains(actionName)) {
                continue;
              }
            }
          }
          try {
            listener.onEvent(event, stage, actionName, context, error, message);
          } catch (Exception e) {
            log.warn("Exception running listener " + listener.getConfig(), e);
          }
        }
      } finally {
        updateLock.unlock();
      }
    }
  }
}
