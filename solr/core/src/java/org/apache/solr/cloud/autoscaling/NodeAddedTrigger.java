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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventType;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.autoscaling.OverseerTriggerThread.MARKER_ACTIVE;
import static org.apache.solr.cloud.autoscaling.OverseerTriggerThread.MARKER_INACTIVE;
import static org.apache.solr.cloud.autoscaling.OverseerTriggerThread.MARKER_STATE;
import static org.apache.solr.common.params.AutoScalingParams.PREFERRED_OP;
import static org.apache.solr.common.params.AutoScalingParams.REPLICA_TYPE;

/**
 * Trigger for the {@link TriggerEventType#NODEADDED} event
 */
public class NodeAddedTrigger extends TriggerBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private Set<String> lastLiveNodes = new HashSet<>();

  private Map<String, Long> nodeNameVsTimeAdded = new HashMap<>();

  private String preferredOp;
  private Replica.Type replicaType = Replica.Type.NRT;

  public NodeAddedTrigger(String name) {
    super(TriggerEventType.NODEADDED, name);
    TriggerUtils.validProperties(validProperties, PREFERRED_OP, REPLICA_TYPE);
  }

  @Override
  public void init() throws Exception {
    super.init();
    lastLiveNodes = new HashSet<>(cloudManager.getClusterStateProvider().getLiveNodes());
    log.debug("NodeAddedTrigger {} - Initial livenodes: {}", name, lastLiveNodes);
    log.debug("NodeAddedTrigger {} instantiated with properties: {}", name, properties);
    // pick up added nodes for which marker paths were created
    try {
      List<String> added = stateManager.listData(ZkStateReader.SOLR_AUTOSCALING_NODE_ADDED_PATH);
      added.forEach(n -> {
        String markerPath = ZkStateReader.SOLR_AUTOSCALING_NODE_ADDED_PATH + "/" + n;
        try {
          Map<String, Object> markerData = Utils.getJson(stateManager, markerPath);
          // skip inactive markers
          if (markerData.getOrDefault(MARKER_STATE, MARKER_ACTIVE).equals(MARKER_INACTIVE)) {
            return;
          }
        } catch (InterruptedException | IOException | KeeperException e) {
          log.debug("-- ignoring marker " + markerPath + " state due to error", e);
        }
        // don't add nodes that have since gone away
        if (lastLiveNodes.contains(n) && !nodeNameVsTimeAdded.containsKey(n)) {
          // since {@code #restoreState(AutoScaling.Trigger)} is called first, the timeAdded for a node may also be restored
          log.debug("Adding node from marker path: {}", n);
          nodeNameVsTimeAdded.put(n, cloudManager.getTimeSource().getTimeNs());
        }
      });
    } catch (NoSuchElementException e) {
      // ignore
    } catch (Exception e) {
      log.warn("Exception retrieving nodeLost markers", e);
    }
  }

  @Override
  public void configure(SolrResourceLoader loader, SolrCloudManager cloudManager, Map<String, Object> properties) throws TriggerValidationException {
    super.configure(loader, cloudManager, properties);
    preferredOp = (String) properties.getOrDefault(PREFERRED_OP, CollectionParams.CollectionAction.MOVEREPLICA.toLower());
    preferredOp = preferredOp.toLowerCase(Locale.ROOT);
    String replicaTypeStr = (String) properties.getOrDefault(REPLICA_TYPE, Replica.Type.NRT.name());
    // verify
    try {
      replicaType = Replica.Type.valueOf(replicaTypeStr);
    } catch (IllegalArgumentException | NullPointerException e) {
      throw new TriggerValidationException("Unsupported replicaType=" + replicaTypeStr + " specified for node added trigger");
    }

    CollectionParams.CollectionAction action = CollectionParams.CollectionAction.get(preferredOp);
    switch (action) {
      case ADDREPLICA:
      case MOVEREPLICA:
      case NONE:
        break;
      default:
        throw new TriggerValidationException("Unsupported preferredOperation=" + preferredOp + " specified for node added trigger");
    }
  }

  @Override
  public void restoreState(AutoScaling.Trigger old) {
    assert old.isClosed();
    if (old instanceof NodeAddedTrigger) {
      NodeAddedTrigger that = (NodeAddedTrigger) old;
      assert this.name.equals(that.name);
      this.lastLiveNodes.clear();
      this.lastLiveNodes.addAll(that.lastLiveNodes);
      this.nodeNameVsTimeAdded.clear();
      this.nodeNameVsTimeAdded.putAll(that.nodeNameVsTimeAdded);
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
          if (processor.process(new NodeAddedEvent(getEventType(), getName(), times, nodeNames, preferredOp, replicaType))) {
            // remove from tracking set only if the fire was accepted
            nodeNames.forEach(n -> {
              log.debug("Removing new node from tracking: {}", n);
              nodeNameVsTimeAdded.remove(n);
            });
          } else {
            log.debug("Processor returned false for {}!", nodeNames);
          }
        } else  {
          nodeNames.forEach(n -> {
            nodeNameVsTimeAdded.remove(n);
          });
        }
      }
      lastLiveNodes = new HashSet<>(newLiveNodes);
    } catch (RuntimeException e) {
      log.error("Unexpected exception in NodeAddedTrigger", e);
    }
  }

  public static class NodeAddedEvent extends TriggerEvent {

    public NodeAddedEvent(TriggerEventType eventType, String source, List<Long> times, List<String> nodeNames, String preferredOp, Replica.Type replicaType) {
      // use the oldest time as the time of the event
      super(eventType, source, times.get(0), null);
      properties.put(NODE_NAMES, nodeNames);
      properties.put(EVENT_TIMES, times);
      properties.put(PREFERRED_OP, preferredOp);
      properties.put(REPLICA_TYPE, replicaType);
    }
  }
}
