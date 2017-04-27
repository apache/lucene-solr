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
import java.util.HashMap;
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
import org.apache.solr.common.util.IOUtils;
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

  public OverseerTriggerThread(ZkController zkController) {
    this.zkController = zkController;
    zkStateReader = zkController.getZkStateReader();
    scheduledTriggers = new ScheduledTriggers();
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
  }

  @Override
  public void run() {
    int lastZnodeVersion = znodeVersion;
    SolrZkClient zkClient = zkStateReader.getZkClient();
    createWatcher(zkClient);

    while (true) {
      Map<String, AutoScaling.Trigger> copy = null;
      try {
        updateLock.lockInterruptibly();
        if (znodeVersion == lastZnodeVersion) {
          updated.await();

          // are we closed?
          if (isClosed) break;

          // spurious wakeup?
          if (znodeVersion == lastZnodeVersion) continue;
          lastZnodeVersion = znodeVersion;
        }
        copy = new HashMap<>(activeTriggers);
      } catch (InterruptedException e) {
        // Restore the interrupted status
        Thread.currentThread().interrupt();
        log.warn("Interrupted", e);
        break;
      } finally {
        updateLock.unlock();
      }

      Set<String> managedTriggerNames = scheduledTriggers.getScheduledTriggerNames();
      // remove the triggers which are no longer active
      for (String managedTriggerName : managedTriggerNames) {
        if (!copy.containsKey(managedTriggerName)) {
          scheduledTriggers.remove(managedTriggerName);
        }
      }
      // add new triggers and/or replace and close the replaced triggers
      for (Map.Entry<String, AutoScaling.Trigger> entry : copy.entrySet()) {
        scheduledTriggers.add(entry.getValue());
      }
    }
  }

  private void createWatcher(SolrZkClient zkClient) {
    try {
      zkClient.exists(ZkStateReader.SOLR_AUTOSCALING_CONF_PATH, new Watcher() {
        @Override
        public void process(WatchedEvent watchedEvent) {
          // session events are not change events, and do not remove the watcher
          if (Event.EventType.None.equals(watchedEvent.getType())) {
            return;
          }

          try {
            refreshAndWatch();
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

        private void refreshAndWatch() throws KeeperException, InterruptedException {
          updateLock.lock();
          try {
            if (isClosed) {
              return;
            }
            final Stat stat = new Stat();
            final byte[] data = zkClient.getData(ZkStateReader.SOLR_AUTOSCALING_CONF_PATH, this, stat, true);
            if (znodeVersion >= stat.getVersion()) {
              // protect against reordered watcher fires by ensuring that we only move forward
              return;
            }
            znodeVersion = stat.getVersion();
            Map<String, AutoScaling.Trigger> triggerMap = loadTriggers(triggerFactory, data);
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
      }, true);
    } catch (KeeperException e) {
      log.error("Exception in OverseerTriggerThread", e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    } catch (InterruptedException e) {
      // Restore the interrupted status
      Thread.currentThread().interrupt();
      log.error("OverseerTriggerThread interrupted", e);
    }
  }

  private static Map<String, AutoScaling.Trigger> loadTriggers(AutoScaling.TriggerFactory triggerFactory, byte[] data) {
    ZkNodeProps loaded = ZkNodeProps.load(data);
    Map<String, Object> triggers = (Map<String, Object>) loaded.get("triggers");

    Map<String, AutoScaling.Trigger> triggerMap = new HashMap<>(triggers.size());

    for (Map.Entry<String, Object> entry : triggers.entrySet()) {
      Map<String, Object> props = (Map<String, Object>) entry.getValue();
      String event = (String) props.get("event");
      AutoScaling.EventType eventType = AutoScaling.EventType.valueOf(event.toUpperCase(Locale.ROOT));
      String triggerName = entry.getKey();
      triggerMap.put(triggerName, triggerFactory.create(eventType, triggerName, props));
    }
    return triggerMap;
  }
}
