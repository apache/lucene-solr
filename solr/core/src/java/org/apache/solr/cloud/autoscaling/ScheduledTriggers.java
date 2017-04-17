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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.store.AlreadyClosedException;
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

  private final Map<String, ScheduledTrigger> scheduledTriggers = new HashMap<>();

  /**
   * Thread pool for scheduling the triggers
   */
  private final ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;

  /**
   * Single threaded executor to run the actions upon a trigger event
   */
  private final ExecutorService actionExecutor;

  private boolean isClosed = false;

  public ScheduledTriggers() {
    // todo make the core pool size configurable
    // it is important to use more than one because a taking time trigger can starve other scheduled triggers
    // ideally we should have as many core threads as the number of triggers but firstly, we don't know beforehand
    // how many triggers we have and secondly, that many threads will always be instantiated and kept around idle
    // so it is wasteful as well. Hopefully 4 is a good compromise.
    scheduledThreadPoolExecutor = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(4,
        new DefaultSolrThreadFactory("ScheduledTrigger-"));
    scheduledThreadPoolExecutor.setRemoveOnCancelPolicy(true);
    scheduledThreadPoolExecutor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    actionExecutor = ExecutorUtil.newMDCAwareSingleThreadExecutor(new DefaultSolrThreadFactory("AutoscalingActionExecutor"));
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
      scheduledTriggers.replace(newTrigger.getName(), scheduledTrigger);
    }
    newTrigger.setListener(event -> {
      AutoScaling.Trigger source = event.getSource();
      if (source.isClosed()) {
        log.warn("Ignoring autoscaling event because the source trigger: " + source + " has already been closed");
        return;
      }
      List<TriggerAction> actions = source.getActions();
      if (actions != null) {
        actionExecutor.submit(() -> {
          for (TriggerAction action : actions) {
            try {
              action.process(event);
            } catch (Exception e) {
              log.error("Error executing action: " + action.getName() + " for trigger event: " + event, e);
              throw e;
            }
          }
        });
      }
    });
    scheduledTrigger.scheduledFuture = scheduledThreadPoolExecutor.scheduleWithFixedDelay(newTrigger, 0, 1, TimeUnit.SECONDS);
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
    return Collections.unmodifiableSet(scheduledTriggers.keySet());
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

  private static class ScheduledTrigger implements Closeable {
    AutoScaling.Trigger trigger;
    ScheduledFuture<?> scheduledFuture;

    ScheduledTrigger(AutoScaling.Trigger trigger) {
      this.trigger = trigger;
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
