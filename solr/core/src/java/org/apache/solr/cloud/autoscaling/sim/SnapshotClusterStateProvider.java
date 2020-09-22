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
import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.solr.client.solrj.impl.ClusterStateProvider;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;
import org.noggit.CharArr;
import org.noggit.JSONWriter;

/**
 * Read-only snapshot of another {@link ClusterStateProvider}.
 *
 * @deprecated to be removed in Solr 9.0 (see SOLR-14656)
 */
public class SnapshotClusterStateProvider implements ClusterStateProvider {

  final Set<String> liveNodes;
  final ClusterState clusterState;
  final Map<String, Object> clusterProperties;

  public SnapshotClusterStateProvider(ClusterStateProvider other) throws Exception {
    liveNodes = Collections.unmodifiableSet(new HashSet<>(other.getLiveNodes()));
    ClusterState otherState = other.getClusterState();
    clusterState = new ClusterState(otherState.getZNodeVersion(), liveNodes, otherState.getCollectionsMap());
    clusterProperties = new HashMap<>(other.getClusterProperties());
  }

  public SnapshotClusterStateProvider(Map<String, Object> snapshot) {
    Objects.requireNonNull(snapshot);
    liveNodes = Collections.unmodifiableSet(new HashSet<>((Collection<String>)snapshot.getOrDefault("liveNodes", Collections.emptySet())));
    clusterProperties = (Map<String, Object>)snapshot.getOrDefault("clusterProperties", Collections.emptyMap());
    Map<String, Object> stateMap = new HashMap<>((Map<String, Object>)snapshot.getOrDefault("clusterState", Collections.emptyMap()));
    Number version = (Number)stateMap.remove("version");
    clusterState = ClusterState.load(version != null ? version.intValue() : null, stateMap, liveNodes, ZkStateReader.CLUSTER_STATE);
  }

  public Map<String, Object> getSnapshot() {
    Map<String, Object> snapshot = new HashMap<>();
    snapshot.put("liveNodes", liveNodes);
    if (clusterProperties != null) {
      snapshot.put("clusterProperties", clusterProperties);
    }
    Map<String, Object> stateMap = new HashMap<>();
    snapshot.put("clusterState", stateMap);
    stateMap.put("version", clusterState.getZNodeVersion());
    clusterState.forEachCollection(coll -> {
      CharArr out = new CharArr();
      JSONWriter writer = new JSONWriter(out, 2);
      coll.write(writer);
      String json = out.toString();
      try {
        stateMap.put(coll.getName(), Utils.fromJSON(json.getBytes("UTF-8")));
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException("should not happen!", e);
      }
    });
    return snapshot;
  }

  @Override
  public ClusterState.CollectionRef getState(String collection) {
    return clusterState.getCollectionRef(collection);
  }

  @Override
  public Set<String> getLiveNodes() {
    return liveNodes;
  }

  @Override
  public List<String> resolveAlias(String alias) {
    throw new UnsupportedOperationException("resolveAlias");
  }

  @Override
  public Map<String, String> getAliasProperties(String alias) {
    throw new UnsupportedOperationException("getAliasProperties");
  }

  @Override
  public ClusterState getClusterState() throws IOException {
    return clusterState;
  }

  @Override
  public Map<String, Object> getClusterProperties() {
    return clusterProperties;
  }

  @Override
  public String getPolicyNameByCollection(String coll) {
    DocCollection collection = clusterState.getCollectionOrNull(coll);
    return collection == null ? null : (String)collection.getProperties().get("policy");
  }

  @Override
  public void connect() {

  }

  @Override
  public void close() throws IOException {

  }
}
