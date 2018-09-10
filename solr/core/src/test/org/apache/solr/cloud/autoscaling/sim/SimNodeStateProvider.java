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
package org.apache.solr.cloud.autoscaling.sim;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.cloud.NodeStateProvider;
import org.apache.solr.client.solrj.cloud.autoscaling.ReplicaInfo;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simulated {@link NodeStateProvider}.
 * Note: in order to setup node-level metrics use {@link #simSetNodeValues(String, Map)}. However, in order
 * to setup core-level metrics use {@link SimClusterStateProvider#simSetCollectionValue(String, String, Object, boolean, boolean)}.
 */
public class SimNodeStateProvider implements NodeStateProvider {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Map<String, Map<String, Object>> nodeValues = new ConcurrentHashMap<>();
  private final SimClusterStateProvider clusterStateProvider;
  private final SimDistribStateManager stateManager;
  private final LiveNodesSet liveNodesSet;
  private final ReentrantLock lock = new ReentrantLock();

  public SimNodeStateProvider(LiveNodesSet liveNodesSet, SimDistribStateManager stateManager,
                              SimClusterStateProvider clusterStateProvider,
                              Map<String, Map<String, Object>> nodeValues) {
    this.liveNodesSet = liveNodesSet;
    this.stateManager = stateManager;
    this.clusterStateProvider = clusterStateProvider;
    if (nodeValues != null) {
      this.nodeValues.putAll(nodeValues);
    }
  }

  // -------- simulator setup methods ------------

  /**
   * Get a node value
   * @param node node id
   * @param key property name
   * @return property value or null if property or node doesn't exist.
   */
  public Object simGetNodeValue(String node, String key) {
    Map<String, Object> values = nodeValues.get(node);
    if (values == null) {
      return null;
    }
    return values.get(key);
  }

  /**
   * Set node values.
   * NOTE: if values contain 'nodeRole' key then /roles.json is updated.
   * @param node node id
   * @param values values.
   */
  public void simSetNodeValues(String node, Map<String, Object> values) throws InterruptedException {
    lock.lockInterruptibly();
    try {
      Map<String, Object> existing = nodeValues.computeIfAbsent(node, n -> new ConcurrentHashMap<>());
      existing.clear();
      if (values != null) {
        existing.putAll(values);
      }
      if (values == null || values.isEmpty() || values.containsKey("nodeRole")) {
        saveRoles();
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Set a node value, replacing any previous value.
   * NOTE: if key is 'nodeRole' then /roles.json is updated.
   * @param node node id
   * @param key property name
   * @param value property value
   */
  public void simSetNodeValue(String node, String key, Object value) throws InterruptedException {
    lock.lockInterruptibly();
    try {
      Map<String, Object> existing = nodeValues.computeIfAbsent(node, n -> new ConcurrentHashMap<>());
      if (value == null) {
        existing.remove(key);
      } else {
        existing.put(key, value);
      }
      if (key.equals("nodeRole")) {
        saveRoles();
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Add a node value, creating a list of values if necessary.
   * NOTE: if key is 'nodeRole' then /roles.json is updated.
   * @param node node id
   * @param key property name
   * @param value property value.
   */
  public void simAddNodeValue(String node, String key, Object value) throws InterruptedException {
    lock.lockInterruptibly();
    try {
      Map<String, Object> values = nodeValues.computeIfAbsent(node, n -> new ConcurrentHashMap<>());
      Object existing = values.get(key);
      if (existing == null) {
        values.put(key, value);
      } else if (existing instanceof Set) {
        ((Set)existing).add(value);
      } else {
        Set<Object> vals = new HashSet<>();
        vals.add(existing);
        vals.add(value);
        values.put(key, vals);
      }
      if (key.equals("nodeRole")) {
        saveRoles();
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Remove node values. If values contained a 'nodeRole' key then
   * /roles.json is updated.
   * @param node node id
   */
  public void simRemoveNodeValues(String node) throws InterruptedException {
    log.debug("--removing value for " + node);
    lock.lockInterruptibly();
    try {
      Map<String, Object> values = nodeValues.remove(node);
      if (values != null && values.containsKey("nodeRole")) {
        saveRoles();
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Remove values that correspond to dead nodes. If values contained a 'nodeRole'
   * key then /roles.json is updated.
   */
  public void simRemoveDeadNodes() throws InterruptedException {
    Set<String> myNodes = new HashSet<>(nodeValues.keySet());
    myNodes.removeAll(liveNodesSet.get());
    lock.lockInterruptibly();
    try {
      AtomicBoolean updateRoles = new AtomicBoolean(false);
      myNodes.forEach(n -> {
        log.debug("- removing dead node values: " + n);
        Map<String, Object> vals = nodeValues.remove(n);
        if (vals.containsKey("nodeRole")) {
          updateRoles.set(true);
        }
      });
      if (updateRoles.get()) {
        saveRoles();
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Return a set of nodes that are not live but their values are still present.
   */
  public Set<String> simGetDeadNodes() {
    Set<String> myNodes = new TreeSet<>(nodeValues.keySet());
    myNodes.removeAll(liveNodesSet.get());
    return myNodes;
  }

  /**
   * Get all node values.
   */
  public Map<String, Map<String, Object>> simGetAllNodeValues() {
    return nodeValues;
  }

  private void saveRoles() {
    final Map<String, Set<String>> roles = new HashMap<>();
    nodeValues.forEach((n, values) -> {
      String nodeRole = (String)values.get("nodeRole");
      if (nodeRole != null) {
        roles.computeIfAbsent(nodeRole, role -> new HashSet<>()).add(n);
      }
    });
    try {
      stateManager.setData(ZkStateReader.ROLES, Utils.toJSON(roles), -1);
    } catch (Exception e) {
      throw new RuntimeException("Unexpected exception saving roles " + roles, e);
    }
  }

  private static final Pattern REGISTRY_PATTERN = Pattern.compile("^solr\\.core\\.([\\w.-_]+?)\\.(shard[\\d_]+?)\\.(replica.*)");
  /**
   * Simulate getting replica metrics values. This uses per-replica properties set in
   * {@link SimClusterStateProvider#simSetCollectionValue(String, String, Object, boolean, boolean)} and
   * similar methods.
   * @param node node id
   * @param tags metrics names
   * @return map of metrics names / values
   */
  public Map<String, Object> getReplicaMetricsValues(String node, Collection<String> tags) {
    if (!liveNodesSet.contains(node)) {
      throw new RuntimeException("non-live node " + node);
    }
    List<ReplicaInfo> replicas = clusterStateProvider.simGetReplicaInfos(node);
    if (replicas == null || replicas.isEmpty()) {
      return Collections.emptyMap();
    }
    Map<String, Object> values = new HashMap<>();
    for (String tag : tags) {
      String[] parts = tag.split(":");
      if (parts.length < 3 || !parts[0].equals("metrics")) {
        log.warn("Invalid metrics: tag: " + tag);
        continue;
      }
      if (!parts[1].startsWith("solr.core.")) {
        // skip - this is probably solr.node or solr.jvm metric
        continue;
      }
      Matcher m = REGISTRY_PATTERN.matcher(parts[1]);

      if (!m.matches()) {
        log.warn("Invalid registry name: " + parts[1]);
        continue;
      }
      String collection = m.group(1);
      String shard = m.group(2);
      String replica = m.group(3);
      String key = parts.length > 3 ? parts[2] + ":" + parts[3] : parts[2];
      replicas.forEach(r -> {
        if (r.getCollection().equals(collection) && r.getShard().equals(shard) && r.getCore().endsWith(replica)) {
          Object value = r.getVariables().get(key);
          if (value != null) {
            values.put(tag, value);
          } else {
            value = r.getVariables().get(tag);
            if (value != null) {
              values.put(tag, value);
            }
          }
        }
      });
    }
    return values;
  }

  // ---------- interface methods -------------

  @Override
  public Map<String, Object> getNodeValues(String node, Collection<String> tags) {
    log.trace("-- requested values for " + node + ": " + tags);
    if (!liveNodesSet.contains(node)) {
      throw new RuntimeException("non-live node " + node);
    }
    if (tags.isEmpty()) {
      return Collections.emptyMap();
    }
    Map<String, Object> result = new HashMap<>();
    Map<String, Object> metrics = getReplicaMetricsValues(node, tags.stream().filter(s -> s.startsWith("metrics:solr.core.")).collect(Collectors.toList()));
    result.putAll(metrics);
    Map<String, Object> values = nodeValues.get(node);
    if (values == null) {
      return result;
    }
    result.putAll(values.entrySet().stream().filter(e -> tags.contains(e.getKey())).collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue())));
    return result;
  }

  @Override
  public Map<String, Map<String, List<ReplicaInfo>>> getReplicaInfo(String node, Collection<String> keys) {
    List<ReplicaInfo> replicas = clusterStateProvider.simGetReplicaInfos(node);
    if (replicas == null || replicas.isEmpty()) {
      return Collections.emptyMap();
    }
    Map<String, Map<String, List<ReplicaInfo>>> res = new HashMap<>();
    // TODO: probably needs special treatment for "metrics:solr.core..." tags
    for (ReplicaInfo r : replicas) {
      Map<String, List<ReplicaInfo>> perCollection = res.computeIfAbsent(r.getCollection(), s -> new HashMap<>());
      List<ReplicaInfo> perShard = perCollection.computeIfAbsent(r.getShard(), s -> new ArrayList<>());
      perShard.add(r);
    }
    return res;
  }

  @Override
  public void close() throws IOException {

  }
}
