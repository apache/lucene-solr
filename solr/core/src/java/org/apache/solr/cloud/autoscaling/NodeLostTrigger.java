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
 * Trigger for the {@link AutoScaling.EventType#NODELOST} event
 */
public class NodeLostTrigger implements AutoScaling.Trigger<NodeLostTrigger.NodeLostEvent> {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final String name;
  private final Map<String, Object> properties;
  private final CoreContainer container;
  private final List<TriggerAction> actions;
  private final AtomicReference<AutoScaling.TriggerListener<NodeLostEvent>> listenerRef;
  private final boolean enabled;
  private final int waitForSecond;
  private final AutoScaling.EventType eventType;

  private boolean isClosed = false;

  private Set<String> lastLiveNodes;

  private Map<String, Long> nodeNameVsTimeRemoved = new HashMap<>();

  public NodeLostTrigger(String name, Map<String, Object> properties,
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
        action.init(map);
        actions.add(action);
      }
    } else {
      actions = Collections.emptyList();
    }
    lastLiveNodes = container.getZkController().getZkStateReader().getClusterState().getLiveNodes();
    log.debug("Initial livenodes: {}", lastLiveNodes);
    this.enabled = (boolean) properties.getOrDefault("enabled", true);
    this.waitForSecond = ((Long) properties.getOrDefault("waitFor", -1L)).intValue();
    this.eventType = AutoScaling.EventType.valueOf(properties.get("event").toString().toUpperCase(Locale.ROOT));
  }

  @Override
  public void setListener(AutoScaling.TriggerListener<NodeLostEvent> listener) {
    listenerRef.set(listener);
  }

  @Override
  public AutoScaling.TriggerListener<NodeLostEvent> getListener() {
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
    if (obj instanceof NodeLostTrigger) {
      NodeLostTrigger that = (NodeLostTrigger) obj;
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
  public void restoreState(AutoScaling.Trigger<NodeLostEvent> old) {
    assert old.isClosed();
    if (old instanceof NodeLostTrigger) {
      NodeLostTrigger that = (NodeLostTrigger) old;
      assert this.name.equals(that.name);
      this.lastLiveNodes = new HashSet<>(that.lastLiveNodes);
      this.nodeNameVsTimeRemoved = new HashMap<>(that.nodeNameVsTimeRemoved);
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
          log.warn("NodeLostTrigger ran but was already closed");
          throw new RuntimeException("Trigger has been closed");
        }
      }
      log.debug("Running NodeLostTrigger: {}", name);

      ZkStateReader reader = container.getZkController().getZkStateReader();
      Set<String> newLiveNodes = reader.getClusterState().getLiveNodes();
      log.debug("Found livenodes: {}", newLiveNodes);

      // have any nodes that we were tracking been added to the cluster?
      // if so, remove them from the tracking map
      Set<String> trackingKeySet = nodeNameVsTimeRemoved.keySet();
      trackingKeySet.removeAll(newLiveNodes);

      // have any nodes been removed?
      Set<String> copyOfLastLiveNodes = new HashSet<>(lastLiveNodes);
      copyOfLastLiveNodes.removeAll(newLiveNodes);
      copyOfLastLiveNodes.forEach(n -> {
        log.info("Tracking lost node: {}", n);
        nodeNameVsTimeRemoved.put(n, System.nanoTime());
      });

      // has enough time expired to trigger events for a node?
      for (Map.Entry<String, Long> entry : nodeNameVsTimeRemoved.entrySet()) {
        String nodeName = entry.getKey();
        Long timeRemoved = entry.getValue();
        if (TimeUnit.SECONDS.convert(System.nanoTime() - timeRemoved, TimeUnit.NANOSECONDS) >= getWaitForSecond()) {
          // fire!
          AutoScaling.TriggerListener<NodeLostEvent> listener = listenerRef.get();
          if (listener != null) {
            log.info("NodeLostTrigger firing registered listener");
            if (listener.triggerFired(new NodeLostEvent(this, timeRemoved, nodeName)))  {
              trackingKeySet.remove(nodeName);
            }
          } else  {
            trackingKeySet.remove(nodeName);
          }
        }
      }

      lastLiveNodes = newLiveNodes;
    } catch (RuntimeException e) {
      log.error("Unexpected exception in NodeLostTrigger", e);
    }
  }

  @Override
  public boolean isClosed() {
    synchronized (this) {
      return isClosed;
    }
  }

  public static class NodeLostEvent implements AutoScaling.TriggerEvent<NodeLostTrigger> {
    private final NodeLostTrigger source;
    private final long nodeLostNanoTime;
    private final String nodeName;

    private Map<String, Object> context;

    public NodeLostEvent(NodeLostTrigger source, long nodeLostNanoTime, String nodeRemoved) {
      this.source = source;
      this.nodeLostNanoTime = nodeLostNanoTime;
      this.nodeName = nodeRemoved;
    }

    @Override
    public NodeLostTrigger getSource() {
      return source;
    }

    @Override
    public long getEventNanoTime() {
      return nodeLostNanoTime;
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
