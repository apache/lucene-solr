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
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.client.solrj.cloud.DistribStateManager;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventProcessorStage;
import org.apache.solr.client.solrj.cloud.autoscaling.VersionedData;
import org.apache.solr.client.solrj.request.CollectionAdminRequest.RequestStatusResponse;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.cloud.Stats;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.autoscaling.ExecutePlanAction.waitForTaskToFinish;
import static org.apache.solr.common.params.AutoScalingParams.ACTION_THROTTLE_PERIOD_SECONDS;
import static org.apache.solr.common.params.AutoScalingParams.TRIGGER_COOLDOWN_PERIOD_SECONDS;
import static org.apache.solr.common.params.AutoScalingParams.TRIGGER_CORE_POOL_SIZE;
import static org.apache.solr.common.params.AutoScalingParams.TRIGGER_SCHEDULE_DELAY_SECONDS;
import static org.apache.solr.common.util.ExecutorUtil.awaitTermination;

/**
 * Responsible for scheduling active triggers, starting and stopping them and
 * performing actions when they fire.
 *
 * @deprecated to be removed in Solr 9.0 (see SOLR-14656)
 */
public class ScheduledTriggers implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static final int DEFAULT_SCHEDULED_TRIGGER_DELAY_SECONDS = 1;
  public static final int DEFAULT_ACTION_THROTTLE_PERIOD_SECONDS = 5;
  public static final int DEFAULT_COOLDOWN_PERIOD_SECONDS = 5;
  public static final int DEFAULT_TRIGGER_CORE_POOL_SIZE = 4;

  static final Map<String, Object> DEFAULT_PROPERTIES = new HashMap<>();

  static {
    DEFAULT_PROPERTIES.put(TRIGGER_SCHEDULE_DELAY_SECONDS, DEFAULT_SCHEDULED_TRIGGER_DELAY_SECONDS);
    DEFAULT_PROPERTIES.put(TRIGGER_COOLDOWN_PERIOD_SECONDS, DEFAULT_COOLDOWN_PERIOD_SECONDS);
    DEFAULT_PROPERTIES.put(TRIGGER_CORE_POOL_SIZE, DEFAULT_TRIGGER_CORE_POOL_SIZE);
    DEFAULT_PROPERTIES.put(ACTION_THROTTLE_PERIOD_SECONDS, DEFAULT_ACTION_THROTTLE_PERIOD_SECONDS);
  }

  protected static final Random RANDOM;
  static {
    // We try to make things reproducible in the context of our tests by initializing the random instance
    // based on the current seed
    String seed = System.getProperty("tests.seed");
    if (seed == null) {
      RANDOM = new Random();
    } else {
      RANDOM = new Random(seed.hashCode());
    }
  }

  private final Map<String, TriggerWrapper> scheduledTriggerWrappers = new ConcurrentHashMap<>();

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

  private final AtomicLong cooldownStart = new AtomicLong();

  private final AtomicLong cooldownPeriod = new AtomicLong(TimeUnit.SECONDS.toNanos(DEFAULT_COOLDOWN_PERIOD_SECONDS));

  private final AtomicLong triggerDelay = new AtomicLong(DEFAULT_SCHEDULED_TRIGGER_DELAY_SECONDS);

  private final SolrCloudManager cloudManager;

  private final DistribStateManager stateManager;

  private final SolrResourceLoader loader;

  private final Stats queueStats;

  private final TriggerListeners listeners;

  private final List<TriggerListener> additionalListeners = new ArrayList<>();

  private AutoScalingConfig autoScalingConfig;

  public ScheduledTriggers(SolrResourceLoader loader, SolrCloudManager cloudManager) {
    scheduledThreadPoolExecutor = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(DEFAULT_TRIGGER_CORE_POOL_SIZE,
        new SolrNamedThreadFactory("ScheduledTrigger"));
    scheduledThreadPoolExecutor.setRemoveOnCancelPolicy(true);
    scheduledThreadPoolExecutor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    actionExecutor = ExecutorUtil.newMDCAwareSingleThreadExecutor(new SolrNamedThreadFactory("AutoscalingActionExecutor"));
    this.cloudManager = cloudManager;
    this.stateManager = cloudManager.getDistribStateManager();
    this.loader = loader;
    queueStats = new Stats();
    listeners = new TriggerListeners();
    // initialize cooldown timer
    cooldownStart.set(cloudManager.getTimeSource().getTimeNs() - cooldownPeriod.get());
  }

  /**
   * Set the current autoscaling config. This is invoked by {@link OverseerTriggerThread} when autoscaling.json is updated,
   * and it re-initializes trigger listeners and other properties used by the framework
   * @param autoScalingConfig current autoscaling.json
   */
  public void setAutoScalingConfig(AutoScalingConfig autoScalingConfig) {
    Map<String, Object> currentProps = new HashMap<>(DEFAULT_PROPERTIES);
    if (this.autoScalingConfig != null) {
      currentProps.putAll(this.autoScalingConfig.getProperties());
    }

    // reset listeners early in order to capture first execution of newly scheduled triggers
    listeners.setAutoScalingConfig(autoScalingConfig);

    for (Map.Entry<String, Object> entry : currentProps.entrySet()) {
      Map<String, Object> newProps = autoScalingConfig.getProperties();
      String key = entry.getKey();
      if (newProps.containsKey(key) && !entry.getValue().equals(newProps.get(key))) {
        if (log.isDebugEnabled()) {
          log.debug("Changing value of autoscaling property: {} from: {} to: {}", key, entry.getValue(), newProps.get(key));
        }
        switch (key) {
          case TRIGGER_SCHEDULE_DELAY_SECONDS:
            triggerDelay.set(((Number) newProps.get(key)).intValue());
            synchronized (this) {
              scheduledTriggerWrappers.forEach((s, triggerWrapper) -> {
                if (triggerWrapper.scheduledFuture.cancel(false)) {
                  triggerWrapper.scheduledFuture = scheduledThreadPoolExecutor.scheduleWithFixedDelay(
                      triggerWrapper, 0,
                      cloudManager.getTimeSource().convertDelay(TimeUnit.SECONDS, triggerDelay.get(), TimeUnit.MILLISECONDS),
                      TimeUnit.MILLISECONDS);
                } else  {
                  log.debug("Failed to cancel scheduled task: {}", s);
                }
              });
            }
            break;
          case TRIGGER_COOLDOWN_PERIOD_SECONDS:
            cooldownPeriod.set(TimeUnit.SECONDS.toNanos(((Number) newProps.get(key)).longValue()));
            break;
          case TRIGGER_CORE_POOL_SIZE:
            this.scheduledThreadPoolExecutor.setCorePoolSize(((Number) newProps.get(key)).intValue());
            break;
        }
      }
    }

    this.autoScalingConfig = autoScalingConfig;
    // reset cooldown
    cooldownStart.set(cloudManager.getTimeSource().getTimeNs() - cooldownPeriod.get());
  }

  /**
   * Adds a new trigger or replaces an existing one. The replaced trigger, if any, is closed
   * <b>before</b> the new trigger is run. If a trigger is replaced with itself then this
   * operation becomes a no-op.
   *
   * @param newTrigger the trigger to be managed
   * @throws AlreadyClosedException if this class has already been closed
   */
  public synchronized void add(AutoScaling.Trigger newTrigger) throws Exception {
    if (isClosed) {
      throw new AlreadyClosedException("ScheduledTriggers has been closed and cannot be used anymore");
    }
    TriggerWrapper st;
    try {
      st = new TriggerWrapper(newTrigger, cloudManager, queueStats);
    } catch (Exception e) {
      if (isClosed || e instanceof AlreadyClosedException) {
        throw new AlreadyClosedException("ScheduledTriggers has been closed and cannot be used anymore");
      }
      if (cloudManager.isClosed()) {
        log.error("Failed to add trigger {} - closing or disconnected from data provider", newTrigger.getName(), e);
      } else {
        log.error("Failed to add trigger {}", newTrigger.getName(), e);
      }
      return;
    }
    TriggerWrapper triggerWrapper = st;

    TriggerWrapper old = scheduledTriggerWrappers.putIfAbsent(newTrigger.getName(), triggerWrapper);
    if (old != null) {
      if (old.trigger.equals(newTrigger)) {
        // the trigger wasn't actually modified so we do nothing
        return;
      }
      IOUtils.closeQuietly(old);
      newTrigger.restoreState(old.trigger);
      triggerWrapper.setReplay(false);
      scheduledTriggerWrappers.replace(newTrigger.getName(), triggerWrapper);
    }
    newTrigger.setProcessor(event -> {
      TriggerListeners triggerListeners = listeners.copy();
      if (cloudManager.isClosed()) {
        String msg = String.format(Locale.ROOT, "Ignoring autoscaling event %s because Solr has been shutdown.", event.toString());
        log.warn(msg);
        triggerListeners.fireListeners(event.getSource(), event, TriggerEventProcessorStage.ABORTED, msg);
        return false;
      }
      TriggerWrapper scheduledSource = scheduledTriggerWrappers.get(event.getSource());
      if (scheduledSource == null) {
        String msg = String.format(Locale.ROOT, "Ignoring autoscaling event %s because the source trigger: %s doesn't exist.", event.toString(), event.getSource());
        triggerListeners.fireListeners(event.getSource(), event, TriggerEventProcessorStage.FAILED, msg);
        log.warn(msg);
        return false;
      }
      boolean replaying = event.getProperty(TriggerEvent.REPLAYING) != null ? (Boolean)event.getProperty(TriggerEvent.REPLAYING) : false;
      AutoScaling.Trigger source = scheduledSource.trigger;
      if (scheduledSource.isClosed || source.isClosed()) {
        String msg = String.format(Locale.ROOT, "Ignoring autoscaling event %s because the source trigger: %s has already been closed", event.toString(), source);
        triggerListeners.fireListeners(event.getSource(), event, TriggerEventProcessorStage.ABORTED, msg);
        log.warn(msg);
        // we do not want to lose this event just because the trigger was closed, perhaps a replacement will need it
        return false;
      }
      if (event.isIgnored())  {
        log.debug("-------- Ignoring event: {}", event);
        event.getProperties().put(TriggerEvent.IGNORED, true);
        triggerListeners.fireListeners(event.getSource(), event, TriggerEventProcessorStage.IGNORED, "Event was ignored.");
        return true; // always return true for ignored events
      }
      // even though we pause all triggers during action execution there is a possibility that a trigger was already
      // running at the time and would have already created an event so we reject such events during cooldown period
      if (cooldownStart.get() + cooldownPeriod.get() > cloudManager.getTimeSource().getTimeNs()) {
        log.debug("-------- Cooldown period - rejecting event: {}", event);
        event.getProperties().put(TriggerEvent.COOLDOWN, true);
        triggerListeners.fireListeners(event.getSource(), event, TriggerEventProcessorStage.IGNORED, "In cooldown period.");
        return false;
      } else {
        log.debug("++++++++ Cooldown inactive - processing event: {}", event);
        // start cooldown here to immediately reject other events
        cooldownStart.set(cloudManager.getTimeSource().getTimeNs());
      }
      if (hasPendingActions.compareAndSet(false, true)) {
        // pause all triggers while we execute actions so triggers do not operate on a cluster in transition
        pauseTriggers();

        final boolean enqueued;
        if (replaying) {
          enqueued = false;
        } else {
          enqueued = triggerWrapper.enqueue(event);
        }
        // fire STARTED event listeners after enqueuing the event is successful
        triggerListeners.fireListeners(event.getSource(), event, TriggerEventProcessorStage.STARTED);
        List<TriggerAction> actions = source.getActions();
        if (actions != null) {
          if (actionExecutor.isShutdown()) {
            String msg = String.format(Locale.ROOT, "Ignoring autoscaling event %s from trigger %s because the executor has already been closed", event.toString(), source);
            triggerListeners.fireListeners(event.getSource(), event, TriggerEventProcessorStage.ABORTED, msg);
            log.warn(msg);
            hasPendingActions.set(false);
            // we do not want to lose this event just because the trigger was closed, perhaps a replacement will need it
            return false;
          }
          actionExecutor.submit(() -> {
            assert hasPendingActions.get();
            long eventProcessingStart = cloudManager.getTimeSource().getTimeNs();
            TriggerListeners triggerListeners1 = triggerListeners.copy();
            log.debug("-- processing actions for {}", event);
            try {
              // in future, we could wait for pending tasks in a different thread and re-enqueue
              // this event so that we continue processing other events and not block this action executor
              waitForPendingTasks(newTrigger, actions);

              ActionContext actionContext = new ActionContext(cloudManager, newTrigger, new HashMap<>());
              for (TriggerAction action : actions) {
                @SuppressWarnings({"unchecked"})
                List<String> beforeActions = (List<String>) actionContext.getProperties().computeIfAbsent(TriggerEventProcessorStage.BEFORE_ACTION.toString(), k -> new ArrayList<String>());
                beforeActions.add(action.getName());
                triggerListeners1.fireListeners(event.getSource(), event, TriggerEventProcessorStage.BEFORE_ACTION, action.getName(), actionContext);
                try {
                  action.process(event, actionContext);
                } catch (Exception e) {
                  triggerListeners1.fireListeners(event.getSource(), event, TriggerEventProcessorStage.FAILED, action.getName(), actionContext, e, null);
                  throw new TriggerActionException(event.getSource(), action.getName(), "Error processing action for trigger event: " + event, e);
                }
                @SuppressWarnings({"unchecked"})
                List<String> afterActions = (List<String>) actionContext.getProperties().computeIfAbsent(TriggerEventProcessorStage.AFTER_ACTION.toString(), k -> new ArrayList<String>());
                afterActions.add(action.getName());
                triggerListeners1.fireListeners(event.getSource(), event, TriggerEventProcessorStage.AFTER_ACTION, action.getName(), actionContext);
              }
              if (enqueued) {
                TriggerEvent ev = triggerWrapper.dequeue();
                assert ev.getId().equals(event.getId());
              }
              triggerListeners1.fireListeners(event.getSource(), event, TriggerEventProcessorStage.SUCCEEDED);
            } catch (TriggerActionException e) {
              log.warn("Exception executing actions", e);
            } catch (Exception e) {
              triggerListeners1.fireListeners(event.getSource(), event, TriggerEventProcessorStage.FAILED);
              log.warn("Unhandled exception executing actions", e);
            } finally {
              // update cooldown to the time when we actually finished processing the actions
              cooldownStart.set(cloudManager.getTimeSource().getTimeNs());
              hasPendingActions.set(false);
              // resume triggers after cool down period
              resumeTriggers(cloudManager.getTimeSource().convertDelay(TimeUnit.NANOSECONDS, cooldownPeriod.get(), TimeUnit.MILLISECONDS));
            }
            if (log.isDebugEnabled()) {
              log.debug("-- processing took {} ms for event id={}",
                  TimeUnit.NANOSECONDS.toMillis(cloudManager.getTimeSource().getTimeNs() - eventProcessingStart), event.id);
            }
          });
        } else {
          if (enqueued) {
            TriggerEvent ev = triggerWrapper.dequeue();
            if (!ev.getId().equals(event.getId())) {
              throw new RuntimeException("Wrong event dequeued, queue of " + triggerWrapper.trigger.getName()
              + " is broken! Expected event=" + event + " but got " + ev);
            }
          }
          triggerListeners.fireListeners(event.getSource(), event, TriggerEventProcessorStage.SUCCEEDED);
          hasPendingActions.set(false);
          // resume triggers now
          resumeTriggers(0);
        }
        return true;
      } else {
        log.debug("Ignoring event {}, already processing other actions.", event.id);
        // there is an action in the queue and we don't want to enqueue another until it is complete
        triggerListeners.fireListeners(event.getSource(), event, TriggerEventProcessorStage.IGNORED, "Already processing another event.");
        return false;
      }
    });
    newTrigger.init(); // mark as ready for scheduling
    triggerWrapper.scheduledFuture = scheduledThreadPoolExecutor.scheduleWithFixedDelay(triggerWrapper, 0,
        cloudManager.getTimeSource().convertDelay(TimeUnit.SECONDS, triggerDelay.get(), TimeUnit.MILLISECONDS),
        TimeUnit.MILLISECONDS);
  }

  /**
   * Pauses all scheduled trigger invocations without interrupting any that are in progress
   * @lucene.internal
   */
  public synchronized void pauseTriggers()  {
    if (log.isDebugEnabled()) {
      log.debug("Pausing all triggers: {}", scheduledTriggerWrappers.keySet());
    }
    scheduledTriggerWrappers.forEach((s, triggerWrapper) -> triggerWrapper.scheduledFuture.cancel(false));
  }

  /**
   * Resumes all previously cancelled triggers to be scheduled after the given initial delay
   * @param afterDelayMillis the initial delay in milliseconds after which triggers should be resumed
   * @lucene.internal
   */
  public synchronized void resumeTriggers(long afterDelayMillis) {
    List<Map.Entry<String, TriggerWrapper>> entries = new ArrayList<>(scheduledTriggerWrappers.entrySet());
    Collections.shuffle(entries, RANDOM);
    entries.forEach(e ->  {
      String key = e.getKey();
      TriggerWrapper triggerWrapper = e.getValue();
      if (triggerWrapper.scheduledFuture.isCancelled()) {
        log.debug("Resuming trigger: {} after {}ms", key, afterDelayMillis);
        triggerWrapper.scheduledFuture = scheduledThreadPoolExecutor.scheduleWithFixedDelay(triggerWrapper, afterDelayMillis,
            cloudManager.getTimeSource().convertDelay(TimeUnit.SECONDS, triggerDelay.get(), TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
      }
    });
  }

  private void waitForPendingTasks(AutoScaling.Trigger newTrigger, List<TriggerAction> actions) throws AlreadyClosedException {
    DistribStateManager stateManager = cloudManager.getDistribStateManager();
    try {

      for (TriggerAction action : actions) {
        if (action instanceof ExecutePlanAction) {
          String parentPath = ZkStateReader.SOLR_AUTOSCALING_TRIGGER_STATE_PATH + "/" + newTrigger.getName() + "/" + action.getName();
          if (!stateManager.hasData(parentPath))  {
            break;
          }
          List<String> children = stateManager.listData(parentPath);
          if (children != null) {
            for (String child : children) {
              String path = parentPath + '/' + child;
              VersionedData data = stateManager.getData(path, null);
              if (data != null) {
                @SuppressWarnings({"rawtypes"})
                Map map = (Map) Utils.fromJSON(data.getData());
                String requestid = (String) map.get("requestid");
                try {
                  log.debug("Found pending task with requestid={}", requestid);
                  RequestStatusResponse statusResponse = waitForTaskToFinish(cloudManager, requestid,
                      ExecutePlanAction.DEFAULT_TASK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                  if (statusResponse != null) {
                    RequestStatusState state = statusResponse.getRequestStatus();
                    if (state == RequestStatusState.COMPLETED || state == RequestStatusState.FAILED || state == RequestStatusState.NOT_FOUND) {
                      stateManager.removeData(path, -1);
                    }
                  }
                } catch (Exception e) {
                  if (cloudManager.isClosed())  {
                    throw e; // propagate the abort to the caller
                  }
                  Throwable rootCause = ExceptionUtils.getRootCause(e);
                  if (rootCause instanceof IllegalStateException && rootCause.getMessage().contains("Connection pool shut down")) {
                    throw e;
                  }
                  if (rootCause instanceof TimeoutException && rootCause.getMessage().contains("Could not connect to ZooKeeper")) {
                    throw e;
                  }
                  log.error("Unexpected exception while waiting for pending task with requestid: {} to finish", requestid,  e);
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
      if (cloudManager.isClosed())  {
        throw new AlreadyClosedException("The Solr instance has been shutdown");
      }
      // we catch but don't rethrow because a failure to wait for pending tasks
      // should not keep the actions from executing
      log.error("Unexpected exception while waiting for pending tasks to finish", e);
    }
  }

  /**
   * Remove and stop all triggers. Also cleans up any leftover
   * state / events in ZK.
   */
  public synchronized void removeAll() {
    getScheduledTriggerNames().forEach(t -> {
      log.info("-- removing trigger: {}", t);
      remove(t);
    });
  }

  /**
   * Removes and stops the trigger with the given name. Also cleans up any leftover
   * state / events in ZK.
   *
   * @param triggerName the name of the trigger to be removed
   */
  public synchronized void remove(String triggerName) {
    TriggerWrapper removed = scheduledTriggerWrappers.remove(triggerName);
    IOUtils.closeQuietly(removed);
    removeTriggerZKData(triggerName);
  }

  private void removeTriggerZKData(String triggerName) {
    String statePath = ZkStateReader.SOLR_AUTOSCALING_TRIGGER_STATE_PATH + "/" + triggerName;
    String eventsPath = ZkStateReader.SOLR_AUTOSCALING_EVENTS_PATH + "/" + triggerName;
    try {
      stateManager.removeRecursively(statePath, true, true);
    } catch (Exception e) {
      log.warn("Failed to remove state for removed trigger {}", statePath, e);
    }
    try {
      stateManager.removeRecursively(eventsPath, true, true);
    } catch (Exception e) {
      log.warn("Failed to remove events for removed trigger {}", eventsPath, e);
    }
  }

  /**
   * @return an unmodifiable set of names of all triggers being managed by this class
   */
  public synchronized Set<String> getScheduledTriggerNames() {
    return Collections.unmodifiableSet(new HashSet<>(scheduledTriggerWrappers.keySet())); // shallow copy
  }

  /**
   * For use in white/grey box testing: The Trigger returned may be inspected, 
   * but should not be modified in any way.
   *
   * @param name the name of an existing trigger
   * @return the current scheduled trigger with that name, or null if none exists
   * @lucene.internal
   */
  public synchronized AutoScaling.Trigger getTrigger(String name) {
    TriggerWrapper w = scheduledTriggerWrappers.get(name);
    return (null == w) ? null : w.trigger;
  }
  
  @Override
  public void close() throws IOException {
    synchronized (this) {
      // mark that we are closed
      isClosed = true;
      for (TriggerWrapper triggerWrapper : scheduledTriggerWrappers.values()) {
        IOUtils.closeQuietly(triggerWrapper);
      }
      scheduledTriggerWrappers.clear();
    }
    // shutdown and interrupt all running tasks because there's no longer any
    // guarantee about cluster state
    log.debug("Shutting down scheduled thread pool executor now");
    scheduledThreadPoolExecutor.shutdownNow();

    log.debug("Shutting down action executor now");
    actionExecutor.shutdownNow();

    listeners.close();

    log.debug("Awaiting termination for action executor");
    awaitTermination(actionExecutor);

    log.debug("Awaiting termination for scheduled thread pool executor");
    awaitTermination(scheduledThreadPoolExecutor);

    log.debug("ScheduledTriggers closed completely");
  }

  /**
   * Add a temporary listener for internal use (tests, simulation).
   * @param listener listener instance
   */
  public void addAdditionalListener(TriggerListener listener) {
    listeners.addAdditionalListener(listener);
  }

  /**
   * Remove a temporary listener for internal use (tests, simulation).
   * @param listener listener instance
   */
  public void removeAdditionalListener(TriggerListener listener) {
    listeners.removeAdditionalListener(listener);
  }

  private class TriggerWrapper implements Runnable, Closeable {
    AutoScaling.Trigger trigger;
    ScheduledFuture<?> scheduledFuture;
    TriggerEventQueue queue;
    boolean replay;
    volatile boolean isClosed;

    TriggerWrapper(AutoScaling.Trigger trigger, SolrCloudManager cloudManager, Stats stats) throws IOException {
      this.trigger = trigger;
      this.queue = new TriggerEventQueue(cloudManager, trigger.getName(), stats);
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
      if (!hasPendingActions.get()) {
        // this synchronization is usually never under contention
        // but the only reason to have it here is to ensure that when the set-properties API is used
        // to change the schedule delay, we can safely cancel the old scheduled task
        // and create another one with the new delay without worrying about concurrent
        // execution of the same trigger instance
        synchronized (TriggerWrapper.this) {
          // replay accumulated events on first run, if any

          try {
            if (replay) {
              TriggerEvent event;
              // peek first without removing - we may crash before calling the listener
              while ((event = queue.peekEvent()) != null) {
                // override REPLAYING=true
                event.getProperties().put(TriggerEvent.REPLAYING, true);
                if (!trigger.getProcessor().process(event)) {
                  log.error("Failed to re-play event, discarding: {}", event);
                }
                queue.pollEvent(); // always remove it from queue
              }
              // now restore saved state to possibly generate new events from old state on the first run
              try {
                trigger.restoreState();
              } catch (Exception e) {
                // log but don't throw - see below
                log.error("Error restoring trigger state {}", trigger.getName(), e);
              }
              replay = false;
            }
          } catch (AlreadyClosedException e) {
            
          } catch (Exception e) {
            log.error("Unexpected exception from trigger: {}", trigger.getName(), e);
          }
          try {
            trigger.run();
          } catch (AlreadyClosedException e) {

          } catch (Exception e) {
            // log but do not propagate exception because an exception thrown from a scheduled operation
            // will suppress future executions
            log.error("Unexpected exception from trigger: {}", trigger.getName(), e);
          } finally {
            // checkpoint after each run
            trigger.saveState();
          }
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
    List<TriggerListener> additionalListeners = new ArrayList<>();
    ReentrantLock updateLock = new ReentrantLock();

    public TriggerListeners() {

    }

    private TriggerListeners(Map<String, Map<TriggerEventProcessorStage, List<TriggerListener>>> listenersPerStage,
                             Map<String, TriggerListener> listenersPerName) {
      this.listenersPerStage = new HashMap<>();
      listenersPerStage.forEach((n, listeners) -> {
        Map<TriggerEventProcessorStage, List<TriggerListener>> perStage = this.listenersPerStage.computeIfAbsent(n, name -> new HashMap<>());
        listeners.forEach((s, lst) -> {
          List<TriggerListener> newLst = perStage.computeIfAbsent(s, stage -> new ArrayList<>());
          newLst.addAll(lst);
        });
      });
      this.listenersPerName = new HashMap<>(listenersPerName);
    }

    public TriggerListeners copy() {
      return new TriggerListeners(listenersPerStage, listenersPerName);
    }

    public void addAdditionalListener(TriggerListener listener) {
      updateLock.lock();
      try {
        AutoScalingConfig.TriggerListenerConfig config = listener.getConfig();
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
        additionalListeners.add(listener);
      } finally {
        updateLock.unlock();
      }
    }

    public void removeAdditionalListener(TriggerListener listener) {
      updateLock.lock();
      try {
        listenersPerName.remove(listener.getConfig().name);
        listenersPerStage.forEach((trigger, perStage) -> {
          perStage.forEach((stage, listeners) -> {
            listeners.remove(listener);
          });
        });
        additionalListeners.remove(listener);
      } finally {
        updateLock.unlock();
      }
    }

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
              log.warn("Exception closing old listener {}", listener.getConfig(), e);
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
                log.warn("Exception closing old listener {}", oldListener.getConfig(), e);
              }
            } else {
              listener = oldListener; // reuse
            }
          }
          if (listener == null) { // create new instance
            String clazz = config.listenerClass;
            try {
              listener = loader.newInstance(clazz, TriggerListener.class);
            } catch (Exception e) {
              log.warn("Invalid TriggerListener class name '{}', skipping...", clazz, e);
            }
            if (listener != null) {
              try {
                listener.configure(loader, cloudManager, config);
                listener.init();
                listenersPerName.put(config.name, listener);
              } catch (Exception e) {
                log.warn("Error initializing TriggerListener {}", config, e);
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
        // re-add additional listeners
        List<TriggerListener> additional = new ArrayList<>(additionalListeners);
        additionalListeners.clear();
        for (TriggerListener listener : additional) {
          addAdditionalListener(listener);
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
          if (!listener.isEnabled()) {
            continue;
          }
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
            log.warn("Exception running listener {}", listener.getConfig(), e);
          }
        }
      } finally {
        updateLock.unlock();
      }
    }
  }
}
