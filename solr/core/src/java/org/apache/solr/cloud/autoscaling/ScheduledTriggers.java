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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.solr.cloud.ActionThrottle;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Responsible for scheduling active triggers, starting and stopping them and
 * performing actions when they fire
 */
public class ScheduledTriggers implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  static final int DEFAULT_SCHEDULED_TRIGGER_DELAY_SECONDS = 1;
  static final int DEFAULT_MIN_MS_BETWEEN_ACTIONS = 5000;

  private final Map<String, ScheduledTrigger> scheduledTriggers = new HashMap<>();

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

  public ScheduledTriggers() {
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
    ScheduledTrigger scheduledTrigger = new ScheduledTrigger(newTrigger);
    ScheduledTrigger old = scheduledTriggers.putIfAbsent(newTrigger.getName(), scheduledTrigger);
    if (old != null) {
      if (old.trigger.equals(newTrigger)) {
        // the trigger wasn't actually modified so we do nothing
        return;
      }
      IOUtils.closeQuietly(old);
      newTrigger.restoreState(old.trigger);
      scheduledTriggers.replace(newTrigger.getName(), scheduledTrigger);
    }
    newTrigger.setListener(event -> {
      AutoScaling.Trigger source = event.getSource();
      if (source.isClosed()) {
        log.warn("Ignoring autoscaling event because the source trigger: " + source + " has already been closed");
        // we do not want to lose this event just because the trigger were closed, perhaps a replacement will need it
        return false;
      }
      if (hasPendingActions.compareAndSet(false, true)) {
        List<TriggerAction> actions = source.getActions();
        if (actions != null) {
          actionExecutor.submit(() -> {
            assert hasPendingActions.get();
            // let the action executor thread wait instead of the trigger thread so we use the throttle here
            actionThrottle.minimumWaitBetweenActions();
            actionThrottle.markAttemptingAction();
            for (TriggerAction action : actions) {
              try {
                action.process(event);
              } catch (Exception e) {
                log.error("Error executing action: " + action.getName() + " for trigger event: " + event, e);
                throw e;
              } finally {
                hasPendingActions.set(false);
              }
            }
          });
        }
        return true;
      } else {
        // there is an action in the queue and we don't want to enqueue another until it is complete
        return false;
      }
    });
    scheduledTrigger.scheduledFuture = scheduledThreadPoolExecutor.scheduleWithFixedDelay(scheduledTrigger, 0, DEFAULT_SCHEDULED_TRIGGER_DELAY_SECONDS, TimeUnit.SECONDS);
  }

  /**
   * Removes and stops the trigger with the given name
   *
   * @param triggerName the name of the trigger to be removed
   * @throws AlreadyClosedException if this class has already been closed
   */
  public synchronized void remove(String triggerName) {
    if (isClosed) {
      throw new AlreadyClosedException("ScheduledTriggers has been closed and cannot be used any more");
    }
    ScheduledTrigger removed = scheduledTriggers.remove(triggerName);
    IOUtils.closeQuietly(removed);
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
    ExecutorUtil.shutdownAndAwaitTermination(scheduledThreadPoolExecutor);
    ExecutorUtil.shutdownAndAwaitTermination(actionExecutor);
  }

  private class ScheduledTrigger implements Runnable, Closeable {
    AutoScaling.Trigger trigger;
    ScheduledFuture<?> scheduledFuture;

    ScheduledTrigger(AutoScaling.Trigger trigger) {
      this.trigger = trigger;
    }

    @Override
    public void run() {
      // fire a trigger only if an action is not pending
      // note this is not fool proof e.g. it does not prevent an action being executed while a trigger
      // is still executing. There is additional protection against that scenario in the event listener.
      if (!hasPendingActions.get())  {
        try {
          trigger.run();
        } catch (Exception e) {
          // log but do not propagate exception because an exception thrown from a scheduled operation
          // will suppress future executions
          log.error("Unexpected execution from trigger: " + trigger.getName(), e);
        }
      }
    }

    @Override
    public void close() throws IOException {
      if (scheduledFuture != null) {
        scheduledFuture.cancel(true);
      }
      IOUtils.closeQuietly(trigger);
    }
  }
}
