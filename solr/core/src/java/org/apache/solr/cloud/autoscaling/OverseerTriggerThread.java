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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.ConnectException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.client.solrj.cloud.autoscaling.BadVersionException;
import org.apache.solr.client.solrj.cloud.DistribStateManager;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventType;
import org.apache.solr.common.SolrCloseable;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CloudConfig;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.ZkStateReader.SOLR_AUTOSCALING_CONF_PATH;

/**
 * Overseer thread responsible for reading triggers from zookeeper and
 * adding/removing them from {@link ScheduledTriggers}
 */
public class OverseerTriggerThread implements Runnable, SolrCloseable {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final SolrCloudManager cloudManager;

  private final CloudConfig cloudConfig;

  private final ScheduledTriggers scheduledTriggers;

  private final AutoScaling.TriggerFactory triggerFactory;

  private final ReentrantLock updateLock = new ReentrantLock();

  private final Condition updated = updateLock.newCondition();

  /*
  Following variables are only accessed or modified when updateLock is held
   */
  private int znodeVersion = -1;

  private Map<String, AutoScaling.Trigger> activeTriggers = new HashMap<>();

  private volatile boolean isClosed = false;

  private AutoScalingConfig autoScalingConfig;

  public OverseerTriggerThread(SolrResourceLoader loader, SolrCloudManager cloudManager, CloudConfig cloudConfig) {
    this.cloudManager = cloudManager;
    this.cloudConfig = cloudConfig;
    scheduledTriggers = new ScheduledTriggers(loader, cloudManager);
    triggerFactory = new AutoScaling.TriggerFactoryImpl(loader, cloudManager);
  }

  @Override
  public void close() throws IOException {
    updateLock.lock();
    try {
      isClosed = true;
      activeTriggers.clear();
      updated.signalAll();
    } finally {
      updateLock.unlock();
    }
    IOUtils.closeQuietly(triggerFactory);
    IOUtils.closeQuietly(scheduledTriggers);
    log.debug("OverseerTriggerThread has been closed explicitly");
  }

  /**
   * For tests.
   * @lucene.internal
   * @return current {@link ScheduledTriggers} instance
   */
  public ScheduledTriggers getScheduledTriggers() {
    return scheduledTriggers;
  }

  @Override
  public boolean isClosed() {
    return isClosed;
  }

  @Override
  public void run() {
    int lastZnodeVersion = znodeVersion;

    // we automatically add a trigger for auto add replicas if it does not exists already
    // we also automatically add a scheduled maintenance trigger
    while (!isClosed)  {
      try {
        if (Thread.currentThread().isInterrupted()) {
          log.warn("Interrupted");
          break;
        }
        AutoScalingConfig autoScalingConfig = cloudManager.getDistribStateManager().getAutoScalingConfig();
        AutoScalingConfig updatedConfig = withAutoAddReplicasTrigger(autoScalingConfig);
        updatedConfig = withScheduledMaintenanceTrigger(updatedConfig);
        if (updatedConfig.equals(autoScalingConfig)) break;
        log.debug("Adding .auto_add_replicas and .scheduled_maintenance triggers");
        cloudManager.getDistribStateManager().setData(SOLR_AUTOSCALING_CONF_PATH, Utils.toJSON(updatedConfig), updatedConfig.getZkVersion());
        break;
      } catch (BadVersionException bve) {
        // somebody else has changed the configuration so we must retry
      } catch (InterruptedException e) {
        // Restore the interrupted status
        Thread.currentThread().interrupt();
        log.warn("Interrupted", e);
        break;
      }
      catch (IOException | KeeperException e) {
        if (e instanceof KeeperException.SessionExpiredException ||
            (e.getCause()!=null && e.getCause() instanceof KeeperException.SessionExpiredException)) {
          log.warn("Solr cannot talk to ZK, exiting " + 
              getClass().getSimpleName() + " main queue loop", e);
          return;
        } else {
          log.error("A ZK error has occurred", e);
        }
      }
    }

    if (isClosed || Thread.currentThread().isInterrupted())  return;

    try {
      refreshAutoScalingConf(new AutoScalingWatcher());
    } catch (ConnectException e) {
      log.warn("ZooKeeper watch triggered for autoscaling conf, but Solr cannot talk to ZK: [{}]", e.getMessage());
    } catch (InterruptedException e) {
      // Restore the interrupted status
      Thread.currentThread().interrupt();
      log.warn("Interrupted", e);
    } catch (Exception e)  {
      log.error("Unexpected exception", e);
    }

    while (true) {
      Map<String, AutoScaling.Trigger> copy = null;
      try {
        // this can throw InterruptedException and we don't want to unlock if it did, so we keep this outside
        // of the try/finally block
        updateLock.lockInterruptibly();

        // must check for close here before we await on the condition otherwise we can only be woken up on interruption
        if (isClosed) {
          log.warn("OverseerTriggerThread has been closed, exiting.");
          break;
        }

        log.debug("Current znodeVersion {}, lastZnodeVersion {}", znodeVersion, lastZnodeVersion);

        try {
          if (znodeVersion == lastZnodeVersion) {
            updated.await();

            // are we closed?
            if (isClosed) {
              log.warn("OverseerTriggerThread woken up but we are closed, exiting.");
              break;
            }

            // spurious wakeup?
            if (znodeVersion == lastZnodeVersion) continue;
          }
          copy = new HashMap<>(activeTriggers);
          lastZnodeVersion = znodeVersion;
          log.debug("Processed trigger updates upto znodeVersion {}", znodeVersion);
        } catch (InterruptedException e) {
          // Restore the interrupted status
          Thread.currentThread().interrupt();
          log.warn("Interrupted", e);
          break;
        } finally {
          updateLock.unlock();
        }
      } catch (InterruptedException e) {
        // Restore the interrupted status
        Thread.currentThread().interrupt();
        log.warn("Interrupted", e);
        break;
      }

      // update the current config
      scheduledTriggers.setAutoScalingConfig(autoScalingConfig);

      Set<String> managedTriggerNames = scheduledTriggers.getScheduledTriggerNames();
      // remove the triggers which are no longer active
      for (String managedTriggerName : managedTriggerNames) {
        if (!copy.containsKey(managedTriggerName)) {
          scheduledTriggers.remove(managedTriggerName);
        }
      }
      // check for nodeLost triggers in the current config, and if
      // absent then clean up old nodeLost / nodeAdded markers
      boolean cleanOldNodeLostMarkers = true;
      boolean cleanOldNodeAddedMarkers = true;
      try {
        // add new triggers and/or replace and close the replaced triggers
        for (Map.Entry<String, AutoScaling.Trigger> entry : copy.entrySet()) {
          if (entry.getValue().getEventType().equals(TriggerEventType.NODELOST)) {
            cleanOldNodeLostMarkers = false;
          }
          if (entry.getValue().getEventType().equals(TriggerEventType.NODEADDED)) {
            cleanOldNodeAddedMarkers = false;
          }
          try {
            scheduledTriggers.add(entry.getValue());
          } catch (Exception e) {
            log.warn("Exception initializing trigger " + entry.getKey() + ", configuration ignored", e);
          }
        }
      } catch (AlreadyClosedException e) {
        // this _should_ mean that we're closing, complain loudly if that's not the case
        if (isClosed) {
          return;
        } else {
          throw new IllegalStateException("Caught AlreadyClosedException from ScheduledTriggers, but we're not closed yet!", e);
        }
      }
      DistribStateManager stateManager = cloudManager.getDistribStateManager();
      if (cleanOldNodeLostMarkers) {
        log.debug("-- clean old nodeLost markers");
        try {
          List<String> markers = stateManager.listData(ZkStateReader.SOLR_AUTOSCALING_NODE_LOST_PATH);
          markers.forEach(n -> {
            removeNodeMarker(ZkStateReader.SOLR_AUTOSCALING_NODE_LOST_PATH, n);
          });
        } catch (NoSuchElementException e) {
          // ignore
        } catch (Exception e) {
          log.warn("Error removing old nodeLost markers", e);
        }
      }
      if (cleanOldNodeAddedMarkers) {
        log.debug("-- clean old nodeAdded markers");
        try {
          List<String> markers = stateManager.listData(ZkStateReader.SOLR_AUTOSCALING_NODE_ADDED_PATH);
          markers.forEach(n -> {
            removeNodeMarker(ZkStateReader.SOLR_AUTOSCALING_NODE_ADDED_PATH, n);
          });
        } catch (NoSuchElementException e) {
          // ignore
        } catch (Exception e) {
          log.warn("Error removing old nodeAdded markers", e);
        }

      }
    }
  }

  private void removeNodeMarker(String path, String nodeName) {
    path = path + "/" + nodeName;
    try {
      cloudManager.getDistribStateManager().removeData(path, -1);
      log.debug("  -- deleted " + path);
    } catch (NoSuchElementException e) {
      // ignore
    } catch (Exception e) {
      log.warn("Error removing old marker " + path, e);
    }
  }

  class AutoScalingWatcher implements Watcher  {
    @Override
    public void process(WatchedEvent watchedEvent) {
      // session events are not change events, and do not remove the watcher
      if (Event.EventType.None.equals(watchedEvent.getType())) {
        return;
      }

      try {
        refreshAutoScalingConf(this);
      } catch (ConnectException e) {
        log.warn("ZooKeeper watch triggered for autoscaling conf, but we cannot talk to ZK: [{}]", e.getMessage());
      } catch (InterruptedException e) {
        // Restore the interrupted status
        Thread.currentThread().interrupt();
        log.warn("Interrupted", e);
      } catch (Exception e)  {
        log.error("Unexpected exception", e);
      }
    }

  }

  private void refreshAutoScalingConf(Watcher watcher) throws InterruptedException, IOException {
    updateLock.lock();
    try {
      if (isClosed) {
        return;
      }
      AutoScalingConfig currentConfig = cloudManager.getDistribStateManager().getAutoScalingConfig(watcher);
      log.debug("Refreshing {} with znode version {}", ZkStateReader.SOLR_AUTOSCALING_CONF_PATH, currentConfig.getZkVersion());
      if (znodeVersion >= currentConfig.getZkVersion()) {
        // protect against reordered watcher fires by ensuring that we only move forward
        return;
      }
      autoScalingConfig = currentConfig;
      znodeVersion = autoScalingConfig.getZkVersion();
      Map<String, AutoScaling.Trigger> triggerMap = loadTriggers(triggerFactory, autoScalingConfig);

      // remove all active triggers that have been removed from ZK
      Set<String> trackingKeySet = activeTriggers.keySet();
      trackingKeySet.retainAll(triggerMap.keySet());

      // now lets add or remove triggers which have been enabled or disabled respectively
      for (Map.Entry<String, AutoScaling.Trigger> entry : triggerMap.entrySet()) {
        String triggerName = entry.getKey();
        AutoScaling.Trigger trigger = entry.getValue();
        if (trigger.isEnabled()) {
          activeTriggers.put(triggerName, trigger);
        } else {
          activeTriggers.remove(triggerName);
        }
      }
      updated.signalAll();
    } finally {
      updateLock.unlock();
    }
  }

  private AutoScalingConfig withAutoAddReplicasTrigger(AutoScalingConfig autoScalingConfig) {
    Map<String, Object> triggerProps = AutoScaling.AUTO_ADD_REPLICAS_TRIGGER_PROPS;
    return withDefaultTrigger(triggerProps, autoScalingConfig);
  }

  private AutoScalingConfig withScheduledMaintenanceTrigger(AutoScalingConfig autoScalingConfig) {
    Map<String, Object> triggerProps = AutoScaling.SCHEDULED_MAINTENANCE_TRIGGER_PROPS;
    return withDefaultTrigger(triggerProps, autoScalingConfig);
  }

  private AutoScalingConfig withDefaultTrigger(Map<String, Object> triggerProps, AutoScalingConfig autoScalingConfig) {
    String triggerName = (String) triggerProps.get("name");
    Map<String, AutoScalingConfig.TriggerConfig> configs = autoScalingConfig.getTriggerConfigs();
    for (AutoScalingConfig.TriggerConfig cfg : configs.values()) {
      if (triggerName.equals(cfg.name)) {
        // already has this trigger
        return autoScalingConfig;
      }
    }
    // need to add
    triggerProps.computeIfPresent("waitFor", (k, v) -> (long) (cloudConfig.getAutoReplicaFailoverWaitAfterExpiration() / 1000));
    AutoScalingConfig.TriggerConfig config = new AutoScalingConfig.TriggerConfig(triggerName, triggerProps);
    autoScalingConfig = autoScalingConfig.withTriggerConfig(config);
    // need to add SystemLogListener explicitly here
    autoScalingConfig = AutoScalingHandler.withSystemLogListener(autoScalingConfig, triggerName);
    return autoScalingConfig;
  }

  private static Map<String, AutoScaling.Trigger> loadTriggers(AutoScaling.TriggerFactory triggerFactory, AutoScalingConfig autoScalingConfig) {
    Map<String, AutoScalingConfig.TriggerConfig> triggers = autoScalingConfig.getTriggerConfigs();
    if (triggers == null) {
      return Collections.emptyMap();
    }

    Map<String, AutoScaling.Trigger> triggerMap = new HashMap<>(triggers.size());

    for (Map.Entry<String, AutoScalingConfig.TriggerConfig> entry : triggers.entrySet()) {
      AutoScalingConfig.TriggerConfig cfg = entry.getValue();
      TriggerEventType eventType = cfg.event;
      String triggerName = entry.getKey();
      try {
        triggerMap.put(triggerName, triggerFactory.create(eventType, triggerName, cfg.properties));
      } catch (TriggerValidationException e) {
        log.warn("Error in trigger '" + triggerName + "' configuration, trigger config ignored: " + cfg, e);
      }
    }
    return triggerMap;
  }
}
