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
package org.apache.solr.cloud.rule;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ReplicaPosition;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.rule.ImplicitSnitch;
import org.apache.solr.common.cloud.rule.Snitch;
import org.apache.solr.common.cloud.rule.SnitchContext;
import org.apache.solr.common.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.singletonList;
import static org.apache.solr.cloud.rule.Rule.MatchStatus.NODE_CAN_BE_ASSIGNED;
import static org.apache.solr.cloud.rule.Rule.MatchStatus.NOT_APPLICABLE;
import static org.apache.solr.cloud.rule.Rule.Phase.ASSIGN;
import static org.apache.solr.cloud.rule.Rule.Phase.FUZZY_ASSIGN;
import static org.apache.solr.cloud.rule.Rule.Phase.FUZZY_VERIFY;
import static org.apache.solr.cloud.rule.Rule.Phase.VERIFY;
import static org.apache.solr.common.util.Utils.getDeepCopy;

/** @deprecated to be removed in Solr 9.0 (see SOLR-14930)
 *
 */
public class ReplicaAssigner {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  List<Rule> rules;
  Map<String, Integer> shardVsReplicaCount;
  Map<String, Map<String, Object>> nodeVsTags;
  Map<String, HashMap<String, Integer>> shardVsNodes;
  List<String> participatingLiveNodes;
  Set<String> tagNames = new HashSet<>();
  private Map<String, AtomicInteger> nodeVsCores = new HashMap<>();


  /**
   * @param shardVsReplicaCount shard names vs no:of replicas required for each of those shards
   * @param snitches            snitches details
   * @param shardVsNodes        The current state of the system. can be an empty map if no nodes
   *                            are created in this collection till now
   */
  @SuppressWarnings({"unchecked"})
  public ReplicaAssigner(List<Rule> rules,
                         Map<String, Integer> shardVsReplicaCount,
                         @SuppressWarnings({"rawtypes"})List snitches,
                         Map<String, Map<String, Integer>> shardVsNodes,
                         List<String> participatingLiveNodes,
                         SolrCloudManager cloudManager, ClusterState clusterState) {
    this.rules = rules;
    for (Rule rule : rules) tagNames.add(rule.tag.name);
    this.shardVsReplicaCount = shardVsReplicaCount;
    this.participatingLiveNodes = new ArrayList<>(participatingLiveNodes);
    this.nodeVsTags = getTagsForNodes(cloudManager, snitches);
    this.shardVsNodes = getDeepCopy(shardVsNodes, 2);

    if (clusterState != null) {
      Map<String, DocCollection> collections = clusterState.getCollectionsMap();
      for (Map.Entry<String, DocCollection> entry : collections.entrySet()) {
        DocCollection coll = entry.getValue();
        for (Slice slice : coll.getSlices()) {
          for (Replica replica : slice.getReplicas()) {
            AtomicInteger count = nodeVsCores.get(replica.getNodeName());
            if (count == null) nodeVsCores.put(replica.getNodeName(), count = new AtomicInteger());
            count.incrementAndGet();
          }
        }
      }
    }
  }

  public Map<String, Map<String, Object>> getNodeVsTags() {
    return nodeVsTags;

  }


  /**
   * For each shard return a new set of nodes where the replicas need to be created satisfying
   * the specified rule
   */
  public Map<ReplicaPosition, String> getNodeMappings() {
    Map<ReplicaPosition, String> result = getNodeMappings0();
    if (result == null) {
      String msg = "Could not identify nodes matching the rules " + rules;
      if (!failedNodes.isEmpty()) {
        Map<String, String> failedNodes = new HashMap<>();
        for (Map.Entry<String, SnitchContext> e : this.failedNodes.entrySet()) {
          failedNodes.put(e.getKey(), e.getValue().getErrMsg());
        }
        msg += " Some nodes where excluded from assigning replicas because tags could not be obtained from them " + failedNodes;
      }
      msg += "\n tag values" + Utils.toJSONString(getNodeVsTags());
      if (!shardVsNodes.isEmpty()) {
        msg += "\nInitial state for the coll : " + Utils.toJSONString(shardVsNodes);
      }
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, msg);
    }
    return result;

  }

  Map<ReplicaPosition, String> getNodeMappings0() {
    List<String> shardNames = new ArrayList<>(shardVsReplicaCount.keySet());
    int[] shardOrder = new int[shardNames.size()];
    for (int i = 0; i < shardNames.size(); i++) shardOrder[i] = i;

    boolean hasFuzzyRules = false;
    int nonWildCardShardRules = 0;
    for (Rule r : rules) {
      if (r.isFuzzy()) hasFuzzyRules = true;
      if (!r.shard.isWildCard()) {
        nonWildCardShardRules++;
        //we will have to try all combinations
        if (shardNames.size() > 10) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
              "Max 10 shards allowed if there is a non wild card shard specified in rule");
        }
      }
    }

    Map<ReplicaPosition, String> result = tryAllPermutations(shardNames, shardOrder, nonWildCardShardRules, false);
    if (result == null && hasFuzzyRules) {
      result = tryAllPermutations(shardNames, shardOrder, nonWildCardShardRules, true);
    }
    return result;
  }

  private Map<ReplicaPosition, String> tryAllPermutations(List<String> shardNames,
                                                          int[] shardOrder,
                                                          int nonWildCardShardRules,
                                                          boolean fuzzyPhase) {


    Iterator<int[]> shardPermutations = nonWildCardShardRules > 0 ?
        permutations(shardNames.size()) :
        singletonList(shardOrder).iterator();

    for (; shardPermutations.hasNext(); ) {
      int[] p = shardPermutations.next();
      List<ReplicaPosition> replicaPositions = new ArrayList<>();
      for (int pos : p) {
        for (int j = 0; j < shardVsReplicaCount.get(shardNames.get(pos)); j++) {
          replicaPositions.add(new ReplicaPosition(shardNames.get(pos), j, Replica.Type.NRT));
        }
      }
      Collections.sort(replicaPositions);
      for (Iterator<int[]> it = permutations(rules.size()); it.hasNext(); ) {
        int[] permutation = it.next();
        Map<ReplicaPosition, String> result = tryAPermutationOfRules(permutation, replicaPositions, fuzzyPhase);
        if (result != null) return result;
      }
    }

    return null;
  }


  @SuppressWarnings({"unchecked"})
  private Map<ReplicaPosition, String> tryAPermutationOfRules(int[] rulePermutation, List<ReplicaPosition> replicaPositions, boolean fuzzyPhase) {
    Map<String, Map<String, Object>> nodeVsTagsCopy = getDeepCopy(nodeVsTags, 2);
    Map<ReplicaPosition, String> result = new LinkedHashMap<>();
    int startPosition = 0;
    Map<String, Map<String, Integer>> copyOfCurrentState = getDeepCopy(shardVsNodes, 2);
    List<String> sortedLiveNodes = new ArrayList<>(this.participatingLiveNodes);
    Collections.sort(sortedLiveNodes, (String n1, String n2) -> {
      int result1 = 0;
      for (int i = 0; i < rulePermutation.length; i++) {
        Rule rule = rules.get(rulePermutation[i]);
        int val = rule.compare(n1, n2, nodeVsTagsCopy, copyOfCurrentState);
        if (val != 0) {//atleast one non-zero compare break now
          result1 = val;
          break;
        }
        if (result1 == 0) {//if all else is equal, prefer nodes with fewer cores
          AtomicInteger n1Count = nodeVsCores.get(n1);
          AtomicInteger n2Count = nodeVsCores.get(n2);
          int a = n1Count == null ? 0 : n1Count.get();
          int b = n2Count == null ? 0 : n2Count.get();
          result1 = a > b ? 1 : a == b ? 0 : -1;
        }

      }
      return result1;
    });
    forEachPosition:
    for (ReplicaPosition replicaPosition : replicaPositions) {
      //trying to assign a node by verifying each rule in this rulePermutation
      forEachNode:
      for (int j = 0; j < sortedLiveNodes.size(); j++) {
        String liveNode = sortedLiveNodes.get(startPosition % sortedLiveNodes.size());
        startPosition++;
        for (int i = 0; i < rulePermutation.length; i++) {
          Rule rule = rules.get(rulePermutation[i]);
          //trying to assign a replica into this node in this shard
          Rule.MatchStatus status = rule.tryAssignNodeToShard(liveNode,
              copyOfCurrentState, nodeVsTagsCopy, replicaPosition.shard, fuzzyPhase ? FUZZY_ASSIGN : ASSIGN);
          if (status == Rule.MatchStatus.CANNOT_ASSIGN_FAIL) {
            continue forEachNode;//try another node for this position
          }
        }
        //We have reached this far means this node can be applied to this position
        //and all rules are fine. So let us change the currentState
        result.put(replicaPosition, liveNode);
        Map<String, Integer> nodeNames = copyOfCurrentState.get(replicaPosition.shard);
        if (nodeNames == null) copyOfCurrentState.put(replicaPosition.shard, nodeNames = new HashMap<>());
        Integer n = nodeNames.get(liveNode);
        n = n == null ? 1 : n + 1;
        nodeNames.put(liveNode, n);
        Map<String, Object> tagsMap = nodeVsTagsCopy.get(liveNode);
        Number coreCount = tagsMap == null ? null: (Number) tagsMap.get(ImplicitSnitch.CORES);
        if (coreCount != null) {
          nodeVsTagsCopy.get(liveNode).put(ImplicitSnitch.CORES, coreCount.intValue() + 1);
        }

        continue forEachPosition;
      }
      //if it reached here, we could not find a node for this position
      return null;
    }

    if (replicaPositions.size() > result.size()) {
      return null;
    }

    for (Map.Entry<ReplicaPosition, String> e : result.entrySet()) {
      for (int i = 0; i < rulePermutation.length; i++) {
        Rule rule = rules.get(rulePermutation[i]);
        Rule.MatchStatus matchStatus = rule.tryAssignNodeToShard(e.getValue(),
            copyOfCurrentState, nodeVsTagsCopy, e.getKey().shard, fuzzyPhase ? FUZZY_VERIFY : VERIFY);
        if (matchStatus != NODE_CAN_BE_ASSIGNED && matchStatus != NOT_APPLICABLE) return null;
      }
    }
    return result;
  }

  /**
   * get all permutations for the int[] whose items are 0..level
   */
  public static Iterator<int[]> permutations(final int level) {
    return new Iterator<int[]>() {
      int i = 0;
      int[] next;

      @Override
      public boolean hasNext() {
        AtomicReference<int[]> nthval = new AtomicReference<>();
        permute(0, new int[level], new BitSet(level), nthval, i, new AtomicInteger());
        i++;
        next = nthval.get();
        return next != null;
      }

      @Override
      public int[] next() {
        return next;
      }
    };

  }


  private static void permute(int level, int[] permuted, BitSet used, AtomicReference<int[]> nthval,
                              int requestedIdx, AtomicInteger seenSoFar) {
    if (level == permuted.length) {
      if (seenSoFar.get() == requestedIdx) nthval.set(permuted);
      else seenSoFar.incrementAndGet();
    } else {
      for (int i = 0; i < permuted.length; i++) {
        if (!used.get(i)) {
          used.set(i);
          permuted[level] = i;
          permute(level + 1, permuted, used, nthval, requestedIdx, seenSoFar);
          if (nthval.get() != null) break;
          used.set(i, false);
        }
      }
    }
  }


  public Map<String, SnitchContext> failedNodes = new HashMap<>();

  static class SnitchInfoImpl extends SnitchContext.SnitchInfo {
    final Snitch snitch;
    final Set<String> myTags = new HashSet<>();
    final Map<String, SnitchContext> nodeVsContext = new HashMap<>();
    private final SolrCloudManager cloudManager;

    SnitchInfoImpl(Map<String, Object> conf, Snitch snitch, SolrCloudManager cloudManager) {
      super(conf);
      this.snitch = snitch;
      this.cloudManager = cloudManager;
    }

    @Override
    public Set<String> getTagNames() {
      return myTags;
    }


  }

  /**
   * This method uses the snitches and get the tags for all the nodes
   */
  @SuppressWarnings({"unchecked"})
  private Map<String, Map<String, Object>> getTagsForNodes(final SolrCloudManager cloudManager, @SuppressWarnings({"rawtypes"})List snitchConf) {

    @SuppressWarnings({"rawtypes"})
    Map<Class, SnitchInfoImpl> snitches = getSnitchInfos(cloudManager, snitchConf);
    for (@SuppressWarnings({"rawtypes"})Class c : Snitch.WELL_KNOWN_SNITCHES) {
      if (snitches.containsKey(c)) continue;// it is already specified explicitly , ignore
      try {
        snitches.put(c, new SnitchInfoImpl(Collections.EMPTY_MAP, (Snitch) c.newInstance(), cloudManager));
      } catch (Exception e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Error instantiating Snitch " + c.getName());
      }
    }
    for (String tagName : tagNames) {
      //identify which snitch is going to provide values for a given tag
      boolean foundProvider = false;
      for (SnitchInfoImpl info : snitches.values()) {
        if (info.snitch.isKnownTag(tagName)) {
          foundProvider = true;
          info.myTags.add(tagName);
          break;
        }
      }
      if (!foundProvider)
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unknown tag in rules " + tagName);
    }


    for (String node : participatingLiveNodes) {
      //now use the Snitch to get the tags
      for (SnitchInfoImpl info : snitches.values()) {
        if (!info.myTags.isEmpty()) {
          SnitchContext context = getSnitchCtx(node, info, cloudManager);
          info.nodeVsContext.put(node, context);
          try {
            info.snitch.getTags(node, info.myTags, context);
          } catch (Exception e) {
            context.exception = e;
          }
        }
      }
    }

    Map<String, Map<String, Object>> result = new HashMap<>();
    for (SnitchInfoImpl info : snitches.values()) {
      for (Map.Entry<String, SnitchContext> e : info.nodeVsContext.entrySet()) {
        SnitchContext context = e.getValue();
        String node = e.getKey();
        if (context.exception != null) {
          failedNodes.put(node, context);
          participatingLiveNodes.remove(node);
          log.warn("Not all tags were obtained from node {}", node, context.exception);
          context.exception = new SolrException(SolrException.ErrorCode.SERVER_ERROR,
              "Not all tags were obtained from node " + node);
        } else {
          Map<String, Object> tags = result.get(node);
          if (tags == null) {
            tags = new HashMap<>();
            result.put(node, tags);
          }
          tags.putAll(context.getTags());
        }
      }
    }

    if (participatingLiveNodes.isEmpty()) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Could not get all tags for any nodes");

    }
    return result;

  }

  private Map<String, Object> snitchSession = new HashMap<>();

  protected SnitchContext getSnitchCtx(String node, SnitchInfoImpl info, SolrCloudManager cloudManager) {
    return new ServerSnitchContext(info, node, snitchSession, cloudManager);
  }

  public static void verifySnitchConf(SolrCloudManager cloudManager, @SuppressWarnings({"rawtypes"})List snitchConf) {
    getSnitchInfos(cloudManager, snitchConf);
  }


  @SuppressWarnings({"unchecked", "rawtypes"})
  static Map<Class, SnitchInfoImpl> getSnitchInfos(SolrCloudManager cloudManager, List snitchConf) {
    if (snitchConf == null) snitchConf = Collections.emptyList();
    Map<Class, SnitchInfoImpl> snitches = new LinkedHashMap<>();
    for (Object o : snitchConf) {
      //instantiating explicitly specified snitches
      String klas = null;
      Map map = Collections.emptyMap();
      if (o instanceof Map) {//it can be a Map
        map = (Map) o;
        klas = (String) map.get("class");
        if (klas == null) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "snitch must have  a class attribute");
        }
      } else { //or just the snitch name
        klas = o.toString();
      }
      try {
        if (klas.indexOf('.') == -1) klas = Snitch.class.getPackage().getName() + "." + klas;
        Snitch inst =
            (Snitch) Snitch.class.getClassLoader().loadClass(klas).newInstance() ;
        snitches.put(inst.getClass(), new SnitchInfoImpl(map, inst, cloudManager));
      } catch (Exception e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);

      }

    }
    return snitches;
  }

}
