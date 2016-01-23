package org.apache.solr.cloud;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.solr.cloud.rule.ReplicaAssigner;
import org.apache.solr.cloud.rule.Rule;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.CoreContainer;

import static org.apache.solr.common.cloud.ZkStateReader.CORE_NAME_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.MAX_SHARDS_PER_NODE;


public class Assign {
  private static Pattern COUNT = Pattern.compile("core_node(\\d+)");

  public static String assignNode(String collection, ClusterState state) {
    Map<String, Slice> sliceMap = state.getSlicesMap(collection);
    if (sliceMap == null) {
      return "core_node1";
    }

    int max = 0;
    for (Slice slice : sliceMap.values()) {
      for (Replica replica : slice.getReplicas()) {
        Matcher m = COUNT.matcher(replica.getName());
        if (m.matches()) {
          max = Math.max(max, Integer.parseInt(m.group(1)));
        }
      }
    }

    return "core_node" + (max + 1);
  }

  /**
   * Assign a new unique id up to slices count - then add replicas evenly.
   *
   * @return the assigned shard id
   */
  public static String assignShard(String collection, ClusterState state, Integer numShards) {
    if (numShards == null) {
      numShards = 1;
    }
    String returnShardId = null;
    Map<String, Slice> sliceMap = state.getActiveSlicesMap(collection);


    // TODO: now that we create shards ahead of time, is this code needed?  Esp since hash ranges aren't assigned when creating via this method?

    if (sliceMap == null) {
      return "shard1";
    }

    List<String> shardIdNames = new ArrayList<>(sliceMap.keySet());

    if (shardIdNames.size() < numShards) {
      return "shard" + (shardIdNames.size() + 1);
    }

    // TODO: don't need to sort to find shard with fewest replicas!

    // else figure out which shard needs more replicas
    final Map<String, Integer> map = new HashMap<>();
    for (String shardId : shardIdNames) {
      int cnt = sliceMap.get(shardId).getReplicasMap().size();
      map.put(shardId, cnt);
    }

    Collections.sort(shardIdNames, new Comparator<String>() {

      @Override
      public int compare(String o1, String o2) {
        Integer one = map.get(o1);
        Integer two = map.get(o2);
        return one.compareTo(two);
      }
    });

    returnShardId = shardIdNames.get(0);
    return returnShardId;
  }

  static String buildCoreName(DocCollection collection, String shard) {
    Slice slice = collection.getSlice(shard);
    int replicaNum = slice.getReplicas().size();
    for (; ; ) {
      String replicaName = collection.getName() + "_" + shard + "_replica" + replicaNum;
      boolean exists = false;
      for (Replica replica : slice.getReplicas()) {
        if (replicaName.equals(replica.getStr(CORE_NAME_PROP))) {
          exists = true;
          break;
        }
      }
      if (exists) replicaNum++;
      else break;
    }
    return collection.getName() + "_" + shard + "_replica" + replicaNum;
  }

  static class ReplicaCount {
    public final String nodeName;
    public int thisCollectionNodes = 0;
    public int totalNodes = 0;

    ReplicaCount(String nodeName) {
      this.nodeName = nodeName;
    }

    public int weight() {
      return (thisCollectionNodes * 100) + totalNodes;
    }
  }

  // Only called from createShard and addReplica (so far).
  //
  // Gets a list of candidate nodes to put the required replica(s) on. Throws errors if not enough replicas
  // could be created on live nodes given maxShardsPerNode, Replication factor (if from createShard) etc.
  public static List<ReplicaCount> getNodesForNewReplicas(ClusterState clusterState, String collectionName,
                                                          String shard, int numberOfNodes,
                                                          String createNodeSetStr, CoreContainer cc) {
    DocCollection coll = clusterState.getCollection(collectionName);
    Integer maxShardsPerNode = coll.getInt(MAX_SHARDS_PER_NODE, 1);
    List<String> createNodeList = createNodeSetStr  == null ? null: StrUtils.splitSmart(createNodeSetStr, ",", true);

     HashMap<String, ReplicaCount> nodeNameVsShardCount = getNodeNameVsShardCount(collectionName, clusterState, createNodeList);

    if (createNodeList == null) { // We only care if we haven't been told to put new replicas on specific nodes.
      int availableSlots = 0;
      for (Map.Entry<String, ReplicaCount> ent : nodeNameVsShardCount.entrySet()) {
        //ADDREPLICA can put more than maxShardsPerNode on an instnace, so this test is necessary.
        if (maxShardsPerNode > ent.getValue().thisCollectionNodes) {
          availableSlots += (maxShardsPerNode - ent.getValue().thisCollectionNodes);
        }
      }
      if (availableSlots < numberOfNodes) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            String.format(Locale.ROOT, "Cannot create %d new replicas for collection %s given the current number of live nodes and a maxShardsPerNode of %d",
                numberOfNodes, collectionName, maxShardsPerNode));
      }
    }

    List l = (List) coll.get(DocCollection.RULE);
    if (l != null) {
      return getNodesViaRules(clusterState, shard, numberOfNodes, cc, coll, createNodeList, l);
    }

    ArrayList<ReplicaCount> sortedNodeList = new ArrayList<>(nodeNameVsShardCount.values());
    Collections.sort(sortedNodeList, new Comparator<ReplicaCount>() {
      @Override
      public int compare(ReplicaCount x, ReplicaCount y) {
        return (x.weight() < y.weight()) ? -1 : ((x.weight() == y.weight()) ? 0 : 1);
      }
    });
    return sortedNodeList;

  }

  private static List<ReplicaCount> getNodesViaRules(ClusterState clusterState, String shard, int numberOfNodes,
                                                     CoreContainer cc, DocCollection coll, List<String> createNodeList, List l) {
    ArrayList<Rule> rules = new ArrayList<>();
    for (Object o : l) rules.add(new Rule((Map) o));
    Map<String, Map<String, Integer>> shardVsNodes = new LinkedHashMap<>();
    for (Slice slice : coll.getSlices()) {
      LinkedHashMap<String, Integer> n = new LinkedHashMap<>();
      shardVsNodes.put(slice.getName(), n);
      for (Replica replica : slice.getReplicas()) {
        Integer count = n.get(replica.getNodeName());
        if (count == null) count = 0;
        n.put(replica.getNodeName(), ++count);
      }
    }
    List snitches = (List) coll.get(DocCollection.SNITCH);
    List<String> nodesList = createNodeList == null ?
        new ArrayList<>(clusterState.getLiveNodes()) :
        createNodeList;
    Map<ReplicaAssigner.Position, String> positions = new ReplicaAssigner(
        rules,
        Collections.singletonMap(shard, numberOfNodes),
        snitches,
        shardVsNodes,
        nodesList, cc, clusterState).getNodeMappings();

    List<ReplicaCount> repCounts = new ArrayList<>();
    for (String s : positions.values()) {
      repCounts.add(new ReplicaCount(s));
    }
    return repCounts;
  }

  private static HashMap<String, ReplicaCount> getNodeNameVsShardCount(String collectionName,
                                                                       ClusterState clusterState, List<String> createNodeList) {
    Set<String> nodes = clusterState.getLiveNodes();

    List<String> nodeList = new ArrayList<>(nodes.size());
    nodeList.addAll(nodes);
    if (createNodeList != null) nodeList.retainAll(createNodeList);

    HashMap<String, ReplicaCount> nodeNameVsShardCount = new HashMap<>();
    for (String s : nodeList) {
      nodeNameVsShardCount.put(s, new ReplicaCount(s));
    }
    if (createNodeList != null) { // Overrides petty considerations about maxShardsPerNode
      if (createNodeList.size() != nodeNameVsShardCount.size()) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "At least one of the node(s) specified are not currently active, no action taken.");
      }
      return nodeNameVsShardCount;
    }
    DocCollection coll = clusterState.getCollection(collectionName);
    Integer maxShardsPerNode = coll.getInt(MAX_SHARDS_PER_NODE, 1);
    for (String s : clusterState.getCollections()) {
      DocCollection c = clusterState.getCollection(s);
      //identify suitable nodes  by checking the no:of cores in each of them
      for (Slice slice : c.getSlices()) {
        Collection<Replica> replicas = slice.getReplicas();
        for (Replica replica : replicas) {
          ReplicaCount count = nodeNameVsShardCount.get(replica.getNodeName());
          if (count != null) {
            count.totalNodes++; // Used ot "weigh" whether this node should be used later.
            if (s.equals(collectionName)) {
              count.thisCollectionNodes++;
              if (count.thisCollectionNodes >= maxShardsPerNode) nodeNameVsShardCount.remove(replica.getNodeName());
            }
          }
        }
      }
    }

    return nodeNameVsShardCount;
  }


}
