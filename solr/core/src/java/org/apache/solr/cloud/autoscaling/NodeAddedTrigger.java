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

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.cloud.SolrCloudManager;

import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventType;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.SolrResourceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Trigger for the {@link TriggerEventType#NODEADDED} event
 */
public class NodeAddedTrigger extends TriggerBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private Set<String> lastLiveNodes;

  private Map<String, Long> nodeNameVsTimeAdded = new HashMap<>();

  public NodeAddedTrigger(String name, Map<String, Object> properties,
                          SolrResourceLoader loader,
                          SolrCloudManager cloudManager) {
    super(TriggerEventType.NODEADDED, name, properties, loader, cloudManager);
    lastLiveNodes = new HashSet<>(cloudManager.getClusterStateProvider().getLiveNodes());
    log.debug("Initial livenodes: {}", lastLiveNodes);
    log.debug("NodeAddedTrigger {} instantiated with properties: {}", name, properties);
  }

  @Override
  public void init() {
    super.init();
    // pick up added nodes for which marker paths were created
    try {
      List<String> added = stateManager.listData(ZkStateReader.SOLR_AUTOSCALING_NODE_ADDED_PATH);
      added.forEach(n -> {
        // don't add nodes that have since gone away
        if (lastLiveNodes.contains(n)) {
          log.debug("Adding node from marker path: {}", n);
          nodeNameVsTimeAdded.put(n, cloudManager.getTimeSource().getTimeNs());
        }
        removeMarker(n);
      });
    } catch (NoSuchElementException e) {
      // ignore
    } catch (Exception e) {
      log.warn("Exception retrieving nodeLost markers", e);
    }

  }

  @Override
  public void restoreState(AutoScaling.Trigger old) {
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
  protected Map<String, Object> getState() {
    Map<String,Object> state = new HashMap<>();
    state.put("lastLiveNodes", lastLiveNodes);
    state.put("nodeNameVsTimeAdded", nodeNameVsTimeAdded);
    return state;
  }

  @Override
  protected void setState(Map<String, Object> state) {
    this.lastLiveNodes.clear();
    this.nodeNameVsTimeAdded.clear();
    Collection<String> lastLiveNodes = (Collection<String>)state.get("lastLiveNodes");
    if (lastLiveNodes != null) {
      this.lastLiveNodes.addAll(lastLiveNodes);
    }
    Map<String,Long> nodeNameVsTimeAdded = (Map<String,Long>)state.get("nodeNameVsTimeAdded");
    if (nodeNameVsTimeAdded != null) {
      this.nodeNameVsTimeAdded.putAll(nodeNameVsTimeAdded);
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

      Set<String> newLiveNodes = new HashSet<>(cloudManager.getClusterStateProvider().getLiveNodes());
      log.debug("Found livenodes: {}", newLiveNodes.size());

      // have any nodes that we were tracking been removed from the cluster?
      // if so, remove them from the tracking map
      Set<String> trackingKeySet = nodeNameVsTimeAdded.keySet();
      trackingKeySet.retainAll(newLiveNodes);

      // have any new nodes been added?
      Set<String> copyOfNew = new HashSet<>(newLiveNodes);
      copyOfNew.removeAll(lastLiveNodes);
      copyOfNew.forEach(n -> {
        long eventTime = cloudManager.getTimeSource().getTimeNs();
        log.debug("Tracking new node: {} at time {}", n, eventTime);
        nodeNameVsTimeAdded.put(n, eventTime);
      });

      // has enough time expired to trigger events for a node?
      List<String> nodeNames = new ArrayList<>();
      List<Long> times = new ArrayList<>();
      for (Iterator<Map.Entry<String, Long>> it = nodeNameVsTimeAdded.entrySet().iterator(); it.hasNext(); ) {
        Map.Entry<String, Long> entry = it.next();
        String nodeName = entry.getKey();
        Long timeAdded = entry.getValue();
        long now = cloudManager.getTimeSource().getTimeNs();
        if (TimeUnit.SECONDS.convert(now - timeAdded, TimeUnit.NANOSECONDS) >= getWaitForSecond()) {
          nodeNames.add(nodeName);
          times.add(timeAdded);
        }
      }
      AutoScaling.TriggerEventProcessor processor = processorRef.get();
      if (!nodeNames.isEmpty()) {
        if (processor != null) {
          log.debug("NodeAddedTrigger {} firing registered processor for nodes: {} added at times {}, now={}", name,
              nodeNames, times, cloudManager.getTimeSource().getTimeNs());
          if (processor.process(new NodeAddedEvent(getEventType(), getName(), times, nodeNames))) {
            // remove from tracking set only if the fire was accepted
            nodeNames.forEach(n -> {
              nodeNameVsTimeAdded.remove(n);
              removeMarker(n);
            });
          }
        } else  {
          nodeNames.forEach(n -> {
            nodeNameVsTimeAdded.remove(n);
            removeMarker(n);
          });
        }
      }
      lastLiveNodes = new HashSet<>(newLiveNodes);
    } catch (RuntimeException e) {
      log.error("Unexpected exception in NodeAddedTrigger", e);
    }
  }

  private void removeMarker(String nodeName) {
    String path = ZkStateReader.SOLR_AUTOSCALING_NODE_ADDED_PATH + "/" + nodeName;
    try {
      if (stateManager.hasData(path)) {
        stateManager.removeData(path, -1);
      }
    } catch (NoSuchElementException e) {
      // ignore
    } catch (Exception e) {
      log.debug("Exception removing nodeAdded marker " + nodeName, e);
    }

  }

  public static class NodeAddedEvent extends TriggerEvent {

    public NodeAddedEvent(TriggerEventType eventType, String source, List<Long> times, List<String> nodeNames) {
      // use the oldest time as the time of the event
      super(eventType, source, times.get(0), null);
      properties.put(NODE_NAMES, nodeNames);
      properties.put(EVENT_TIMES, times);
    }
  }
}
