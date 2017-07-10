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
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.common.params.AutoScalingParams;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Overseer thread responsible for reading triggers from zookeeper and
 * adding/removing them from {@link ScheduledTriggers}
 */
public class OverseerTriggerThread implements Runnable, Closeable {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final ZkController zkController;

  private final ZkStateReader zkStateReader;

  private final SolrZkClient zkClient;

  private final ScheduledTriggers scheduledTriggers;

  private final AutoScaling.TriggerFactory triggerFactory;

  private final ReentrantLock updateLock = new ReentrantLock();

  private final Condition updated = updateLock.newCondition();

  /*
  Following variables are only accessed or modified when updateLock is held
   */
  private int znodeVersion = -1;

  private Map<String, AutoScaling.Trigger> activeTriggers = new HashMap<>();

  private boolean isClosed = false;

  private AutoScalingConfig autoScalingConfig;

  public OverseerTriggerThread(ZkController zkController) {
    this.zkController = zkController;
    zkStateReader = zkController.getZkStateReader();
    zkClient = zkController.getZkClient();
    scheduledTriggers = new ScheduledTriggers(zkController);
    triggerFactory = new AutoScaling.TriggerFactory(zkController.getCoreContainer());
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

  @Override
  public void run() {
    int lastZnodeVersion = znodeVersion;

    try {
      refreshAutoScalingConf(new AutoScalingWatcher());
    } catch (KeeperException.ConnectionLossException | KeeperException.SessionExpiredException e) {
      log.warn("ZooKeeper watch triggered for autoscaling conf, but Solr cannot talk to ZK: [{}]", e.getMessage());
    } catch (KeeperException e) {
      log.error("A ZK error has occurred", e);
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "A ZK error has occurred", e);
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
      // add new triggers and/or replace and close the replaced triggers
      for (Map.Entry<String, AutoScaling.Trigger> entry : copy.entrySet()) {
        if (entry.getValue().getEventType().equals(AutoScaling.EventType.NODELOST)) {
          cleanOldNodeLostMarkers = false;
        }
        if (entry.getValue().getEventType().equals(AutoScaling.EventType.NODEADDED)) {
          cleanOldNodeAddedMarkers = false;
        }
        scheduledTriggers.add(entry.getValue());
      }
      if (cleanOldNodeLostMarkers) {
        log.debug("-- clean old nodeLost markers");
        try {
          List<String> markers = zkClient.getChildren(ZkStateReader.SOLR_AUTOSCALING_NODE_LOST_PATH, null, true);
          markers.forEach(n -> {
            removeNodeMarker(ZkStateReader.SOLR_AUTOSCALING_NODE_LOST_PATH, n);
          });
        } catch (KeeperException.NoNodeException e) {
          // ignore
        } catch (KeeperException | InterruptedException e) {
          log.warn("Error removing old nodeLost markers", e);
        }
      }
      if (cleanOldNodeAddedMarkers) {
        log.debug("-- clean old nodeAdded markers");
        try {
          List<String> markers = zkClient.getChildren(ZkStateReader.SOLR_AUTOSCALING_NODE_ADDED_PATH, null, true);
          markers.forEach(n -> {
            removeNodeMarker(ZkStateReader.SOLR_AUTOSCALING_NODE_ADDED_PATH, n);
          });
        } catch (KeeperException.NoNodeException e) {
          // ignore
        } catch (KeeperException | InterruptedException e) {
          log.warn("Error removing old nodeAdded markers", e);
        }

      }
    }
  }

  private void removeNodeMarker(String path, String nodeName) {
    path = path + "/" + nodeName;
    try {
      zkClient.delete(path, -1, true);
      log.debug("  -- deleted " + path);
    } catch (KeeperException.NoNodeException e) {
      // ignore
    } catch (KeeperException | InterruptedException e) {
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
      } catch (KeeperException.ConnectionLossException | KeeperException.SessionExpiredException e) {
        log.warn("ZooKeeper watch triggered for autoscaling conf, but Solr cannot talk to ZK: [{}]", e.getMessage());
      } catch (KeeperException e) {
        log.error("A ZK error has occurred", e);
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "A ZK error has occurred", e);
      } catch (InterruptedException e) {
        // Restore the interrupted status
        Thread.currentThread().interrupt();
        log.warn("Interrupted", e);
      } catch (Exception e)  {
        log.error("Unexpected exception", e);
      }
    }

  }

  private void refreshAutoScalingConf(Watcher watcher) throws KeeperException, InterruptedException {
    updateLock.lock();
    try {
      if (isClosed) {
        return;
      }
      final Stat stat = new Stat();
      final byte[] data = zkClient.getData(ZkStateReader.SOLR_AUTOSCALING_CONF_PATH, watcher, stat, true);
      log.debug("Refreshing {} with znode version {}", ZkStateReader.SOLR_AUTOSCALING_CONF_PATH, stat.getVersion());
      if (znodeVersion >= stat.getVersion()) {
        // protect against reordered watcher fires by ensuring that we only move forward
        return;
      }
      autoScalingConfig = new AutoScalingConfig(data);
      znodeVersion = stat.getVersion();
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

  private static Map<String, AutoScaling.Trigger> loadTriggers(AutoScaling.TriggerFactory triggerFactory, AutoScalingConfig autoScalingConfig) {
    Map<String, AutoScalingConfig.TriggerConfig> triggers = autoScalingConfig.getTriggerConfigs();
    if (triggers == null) {
      return Collections.emptyMap();
    }

    Map<String, AutoScaling.Trigger> triggerMap = new HashMap<>(triggers.size());

    for (Map.Entry<String, AutoScalingConfig.TriggerConfig> entry : triggers.entrySet()) {
      AutoScalingConfig.TriggerConfig cfg = entry.getValue();
      AutoScaling.EventType eventType = cfg.eventType;
      String triggerName = entry.getKey();
      triggerMap.put(triggerName, triggerFactory.create(eventType, triggerName, cfg.properties));
    }
    return triggerMap;
  }
}
