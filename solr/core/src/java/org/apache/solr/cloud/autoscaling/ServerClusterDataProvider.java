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


import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.cloud.rule.ServerSnitchContext;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.recipe.Policy.ClusterDataProvider;
import org.apache.solr.recipe.Policy.ReplicaInfo;

public class ServerClusterDataProvider implements ClusterDataProvider {

  private final CoreContainer coreContainer;
  private Set<String> liveNodes;
  private Map<String,Object> snitchSession = new HashMap<>();
  private final Map<String, Map<String, Map<String, List<ReplicaInfo>>>> data = new HashMap<>();

  public ServerClusterDataProvider(CoreContainer coreContainer) {
    this.coreContainer = coreContainer;
    ClusterState clusterState = coreContainer.getZkController().getZkStateReader().getClusterState();
    this.liveNodes = clusterState.getLiveNodes();
    Map<String, ClusterState.CollectionRef> all = clusterState.getCollectionStates();
    all.forEach((collName, ref) -> {
      DocCollection coll = ref.get();
      if (coll == null) return;
      coll.forEachReplica((shard, replica) -> {
        Map<String, Map<String, List<ReplicaInfo>>> nodeData = data.get(replica.getNodeName());
        if (nodeData == null) data.put(replica.getNodeName(), nodeData = new HashMap<>());
        Map<String, List<ReplicaInfo>> collData = nodeData.get(collName);
        if (collData == null) nodeData.put(collName, collData = new HashMap<>());
        List<ReplicaInfo> replicas = collData.get(shard);
        if (replicas == null) collData.put(shard, replicas = new ArrayList<>());
        replicas.add(new ReplicaInfo(replica.getName(), new HashMap<>()));
      });
    });
  }

  @Override
  public Map<String, Object> getNodeValues(String node, Collection<String> keys) {
    AutoScalingSnitch  snitch = new AutoScalingSnitch();
    ServerSnitchContext ctx = new ServerSnitchContext(null, node, snitchSession, coreContainer);
    snitch.getRemoteInfo(node, new HashSet<>(keys), ctx);
    return ctx.getTags();
  }

  @Override
  public Map<String, Map<String, List<ReplicaInfo>>> getReplicaInfo(String node, Collection<String> keys) {
    return data.get(node);//todo fill other details
  }

  @Override
  public Collection<String> getNodes() {
    return liveNodes;
  }
}
