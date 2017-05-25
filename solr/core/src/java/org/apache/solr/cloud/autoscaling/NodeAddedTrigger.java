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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.util.IOUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.CoreContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Trigger for the {@link org.apache.solr.cloud.autoscaling.AutoScaling.EventType#NODEADDED} event
 */
public class NodeAddedTrigger implements AutoScaling.Trigger<NodeAddedTrigger.NodeAddedEvent> {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final String name;
  private final Map<String, Object> properties;
  private final CoreContainer container;
  private final List<TriggerAction> actions;
  private final AtomicReference<AutoScaling.TriggerListener<NodeAddedEvent>> listenerRef;
  private final boolean enabled;
  private final int waitForSecond;
  private final AutoScaling.EventType eventType;

  private boolean isClosed = false;

  private Set<String> lastLiveNodes;

  private Map<String, Long> nodeNameVsTimeAdded = new HashMap<>();

  public NodeAddedTrigger(String name, Map<String, Object> properties,
                          CoreContainer container) {
    this.name = name;
    this.properties = properties;
    this.container = container;
    this.listenerRef = new AtomicReference<>();
    List<Map<String, String>> o = (List<Map<String, String>>) properties.get("actions");
    if (o != null && !o.isEmpty()) {
      actions = new ArrayList<>(3);
      for (Map<String, String> map : o) {
        TriggerAction action = container.getResourceLoader().newInstance(map.get("class"), TriggerAction.class);
        actions.add(action);
      }
    } else {
      actions = Collections.emptyList();
    }
    this.enabled = (boolean) properties.getOrDefault("enabled", true);
    this.waitForSecond = ((Long) properties.getOrDefault("waitFor", -1L)).intValue();
    this.eventType = AutoScaling.EventType.valueOf(properties.get("event").toString().toUpperCase(Locale.ROOT));
    log.debug("NodeAddedTrigger {} instantiated with properties: {}", name, properties);
  }

  @Override
  public void init() {
    List<Map<String, String>> o = (List<Map<String, String>>) properties.get("actions");
    if (o != null && !o.isEmpty()) {
      for (int i = 0; i < o.size(); i++) {
        Map<String, String> map = o.get(i);
        actions.get(i).init(map);
      }
    }
  }

  @Override
  public void setListener(AutoScaling.TriggerListener<NodeAddedEvent> listener) {
    listenerRef.set(listener);
  }

  @Override
  public AutoScaling.TriggerListener<NodeAddedEvent> getListener() {
    return listenerRef.get();
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public AutoScaling.EventType getEventType() {
    return eventType;
  }

  @Override
  public boolean isEnabled() {
    return enabled;
  }

  @Override
  public int getWaitForSecond() {
    return waitForSecond;
  }

  @Override
  public Map<String, Object> getProperties() {
    return properties;
  }

  @Override
  public List<TriggerAction> getActions() {
    return actions;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof NodeAddedTrigger) {
      NodeAddedTrigger that = (NodeAddedTrigger) obj;
      return this.name.equals(that.name)
          && this.properties.equals(that.properties);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, properties);
  }

  @Override
  public void close() throws IOException {
    synchronized (this) {
      isClosed = true;
      IOUtils.closeWhileHandlingException(actions);
    }
  }

  @Override
  public void restoreState(AutoScaling.Trigger<NodeAddedEvent> old) {
    assert old.isClosed();
    if (old instanceof NodeAddedTrigger) {
      NodeAddedTrigger that = (NodeAddedTrigger) old;
      assert this.name.equals(that.name);
      this.lastLiveNodes = new HashSet<>(that.lastLiveNodes);
      this.nodeNameVsTimeAdded = new HashMap<>(that.nodeNameVsTimeAdded);
    } else  {
      throw new SolrException(SolrException.ErrorCode.INVALID_STATE,
          "Unable to restore state from an unknown type of trigger");
    }
  }

  @Override
  public void run() {
    try {
      synchronized (this) {
        if (isClosed) {
          log.warn("NodeAddedTrigger ran but was already closed");
          throw new RuntimeException("Trigger has been closed");
        }
      }
      log.debug("Running NodeAddedTrigger {}", name);

      ZkStateReader reader = container.getZkController().getZkStateReader();
      Set<String> newLiveNodes = reader.getClusterState().getLiveNodes();
      log.debug("Found livenodes: {}", newLiveNodes);
      if (lastLiveNodes == null) {
        lastLiveNodes = newLiveNodes;
        return;
      }

      // have any nodes that we were tracking been removed from the cluster?
      // if so, remove them from the tracking map
      Set<String> trackingKeySet = nodeNameVsTimeAdded.keySet();
      trackingKeySet.retainAll(newLiveNodes);

      // have any new nodes been added?
      Set<String> copyOfNew = new HashSet<>(newLiveNodes);
      copyOfNew.removeAll(lastLiveNodes);
      copyOfNew.forEach(n -> {
        long nanoTime = System.nanoTime();
        nodeNameVsTimeAdded.put(n, nanoTime);
        log.info("Tracking new node: {} at {} nanotime", n, nanoTime);
      });

      // has enough time expired to trigger events for a node?
      for (Map.Entry<String, Long> entry : nodeNameVsTimeAdded.entrySet()) {
        String nodeName = entry.getKey();
        Long timeAdded = entry.getValue();
        long now = System.nanoTime();
        if (TimeUnit.SECONDS.convert(now - timeAdded, TimeUnit.NANOSECONDS) >= getWaitForSecond()) {
          // fire!
          AutoScaling.TriggerListener<NodeAddedEvent> listener = listenerRef.get();
          if (listener != null) {
            log.info("NodeAddedTrigger {} firing registered listener for node: {} added at {} nanotime, now: {} nanotime", name, nodeName, timeAdded, now);
            if (listener.triggerFired(new NodeAddedEvent(this, timeAdded, nodeName))) {
              // remove from tracking set only if the fire was accepted
              trackingKeySet.remove(nodeName);
            }
          } else  {
            trackingKeySet.remove(nodeName);
          }
        }
      }

      lastLiveNodes = newLiveNodes;
    } catch (RuntimeException e) {
      log.error("Unexpected exception in NodeAddedTrigger", e);
    }
  }

  @Override
  public boolean isClosed() {
    synchronized (this) {
      return isClosed;
    }
  }

  public static class NodeAddedEvent implements AutoScaling.TriggerEvent<NodeAddedTrigger> {
    private final NodeAddedTrigger source;
    private final long nodeAddedNanoTime;
    private final String nodeName;

    private Map<String, Object> context;

    public NodeAddedEvent(NodeAddedTrigger source, long nodeAddedNanoTime, String nodeAdded) {
      this.source = source;
      this.nodeAddedNanoTime = nodeAddedNanoTime;
      this.nodeName = nodeAdded;
    }

    @Override
    public NodeAddedTrigger getSource() {
      return source;
    }

    @Override
    public long getEventNanoTime() {
      return nodeAddedNanoTime;
    }

    public String getNodeName() {
      return nodeName;
    }

    public AutoScaling.EventType getType() {
      return source.getEventType();
    }

    @Override
    public void setContext(Map<String, Object> context) {
      this.context = context;
    }

    @Override
    public Map<String, Object> getContext() {
      return context;
    }
  }
}
