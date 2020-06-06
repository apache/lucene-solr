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
import java.util.LinkedHashMap;
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
 */
public class SnapshotClusterStateProvider implements ClusterStateProvider {

  final Set<String> liveNodes;
  final ClusterState clusterState;
  final Map<String, Object> clusterProperties;

  public SnapshotClusterStateProvider(ClusterStateProvider other) throws Exception {
    liveNodes = Set.copyOf(other.getLiveNodes());
    ClusterState otherState = other.getClusterState();
    clusterState = new ClusterState(otherState.getZNodeVersion(), liveNodes, otherState.getCollectionsMap());
    clusterProperties = new HashMap<>(other.getClusterProperties());
  }

  @SuppressWarnings({"unchecked"})
  public SnapshotClusterStateProvider(Map<String, Object> snapshot) {
    Objects.requireNonNull(snapshot);
    liveNodes = Set.copyOf((Collection<String>)snapshot.getOrDefault("liveNodes", Collections.emptySet()));
    clusterProperties = (Map<String, Object>)snapshot.getOrDefault("clusterProperties", Collections.emptyMap());
    Map<String, Object> stateMap = new HashMap<>((Map<String, Object>)snapshot.getOrDefault("clusterState", Collections.emptyMap()));
    Map<String, DocCollection> collectionStates = new HashMap<>();
    // back-compat with format = 1
    Integer stateVersion = Integer.valueOf(String.valueOf(stateMap.getOrDefault("version", 0)));
    stateMap.remove("version");
    stateMap.forEach((name, state) -> {
      Map<String, Object> mutableState = (Map<String, Object>)state;
      Map<String, Object> collMap = (Map<String, Object>) mutableState.get(name);
      if (collMap == null) {
        // snapshot in format 1
        collMap = mutableState;
        mutableState = Collections.singletonMap(name, state);
      }
      Integer version = Integer.parseInt(String.valueOf(collMap.getOrDefault("zNodeVersion", stateVersion)));
      String path = String.valueOf(collMap.getOrDefault("zNode", ZkStateReader.getCollectionPath(name)));
      collMap.remove("zNodeVersion");
      collMap.remove("zNode");
      byte[] data = Utils.toJSON(mutableState);
      ClusterState collState = ClusterState.load(version, data, Collections.emptySet(), path);
      collectionStates.put(name, collState.getCollection(name));
    });
    clusterState = new ClusterState(stateVersion, liveNodes, collectionStates);
  }

  public Map<String, Object> getSnapshot() {
    Map<String, Object> snapshot = new HashMap<>();
    snapshot.put("liveNodes", liveNodes);
    if (clusterProperties != null) {
      snapshot.put("clusterProperties", clusterProperties);
    }
    Map<String, Object> stateMap = new HashMap<>();
    snapshot.put("clusterState", stateMap);
    clusterState.forEachCollection(coll -> {
      CharArr out = new CharArr();
      JSONWriter writer = new JSONWriter(out, 2);
      coll.write(writer);
      String json = out.toString();
      try {
        @SuppressWarnings({"unchecked"})
        Map<String, Object> collMap = new LinkedHashMap<>((Map<String, Object>)Utils.fromJSON(json.getBytes("UTF-8")));
        collMap.put("zNodeVersion", coll.getZNodeVersion());
        collMap.put("zNode", coll.getZNode());
        // format compatible with the real /state.json, which uses a mini-ClusterState
        // consisting of a single collection
        stateMap.put(coll.getName(), Collections.singletonMap(coll.getName(), collMap));
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
