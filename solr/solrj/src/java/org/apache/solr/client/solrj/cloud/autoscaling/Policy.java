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

package org.apache.solr.client.solrj.cloud.autoscaling;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.cloud.NodeStateProvider;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.impl.ClusterStateProvider;
import org.apache.solr.common.IteratorWriter;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.rule.ImplicitSnitch;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toList;

/*The class that reads, parses and applies policies specified in
 * autoscaling.json
 *
 * Create one instance of this class per unique autoscaling.json.
 * This is immutable and is thread-safe
 *
 * Create a fresh new session for each use
 *
 */
public class Policy implements MapWriter {
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String POLICY = "policy";
  public static final String EACH = "#EACH";
  public static final String ANY = "#ANY";
  public static final String POLICIES = "policies";
  public static final String CLUSTER_POLICY = "cluster-policy";
  public static final String CLUSTER_PREFERENCES = "cluster-preferences";
  public static final Set<String> GLOBAL_ONLY_TAGS = Collections.singleton("cores");
  public static final List<Preference> DEFAULT_PREFERENCES = Collections.unmodifiableList(
      Arrays.asList(
          new Preference((Map<String, Object>) Utils.fromJSONString("{minimize : cores, precision:1}")),
          new Preference((Map<String, Object>) Utils.fromJSONString("{maximize : freedisk}"))));

  /**
   * These parameters are always fetched for all nodes regardless of whether they are used in preferences or not
   */
  private static final List<String> DEFAULT_PARAMS_OF_INTEREST = Arrays.asList(ImplicitSnitch.DISK, ImplicitSnitch.CORES);

  final Map<String, List<Clause>> policies;
  final List<Clause> clusterPolicy;
  final List<Preference> clusterPreferences;
  final List<Pair<String, Suggestion.ConditionType>> params;
  final List<String> perReplicaAttributes;

  public Policy() {
    this(Collections.emptyMap());
  }

  @SuppressWarnings("unchecked")
  public Policy(Map<String, Object> jsonMap) {
    int[] idx = new int[1];
    List<Preference> initialClusterPreferences = ((List<Map<String, Object>>) jsonMap.getOrDefault(CLUSTER_PREFERENCES, emptyList())).stream()
        .map(m -> new Preference(m, idx[0]++))
        .collect(toList());
    for (int i = 0; i < initialClusterPreferences.size() - 1; i++) {
      Preference preference = initialClusterPreferences.get(i);
      preference.next = initialClusterPreferences.get(i + 1);
    }
    if (initialClusterPreferences.isEmpty()) {
      initialClusterPreferences.addAll(DEFAULT_PREFERENCES);
    }
    this.clusterPreferences = Collections.unmodifiableList(initialClusterPreferences);
    final SortedSet<String> paramsOfInterest = new TreeSet<>(DEFAULT_PARAMS_OF_INTEREST);
    clusterPreferences.forEach(preference -> paramsOfInterest.add(preference.name.toString()));
    List<String> newParams = new ArrayList<>(paramsOfInterest);
    clusterPolicy = ((List<Map<String, Object>>) jsonMap.getOrDefault(CLUSTER_POLICY, emptyList())).stream()
        .map(Clause::new)
        .filter(clause -> {
          clause.addTags(newParams);
          return true;
        })
        .collect(collectingAndThen(toList(), Collections::unmodifiableList));

    this.policies = Collections.unmodifiableMap(
        policiesFromMap((Map<String, List<Map<String, Object>>>) jsonMap.getOrDefault(POLICIES, emptyMap()), newParams));
    this.params = Collections.unmodifiableList(newParams.stream()
        .map(s -> new Pair<>(s, Suggestion.getTagType(s)))
        .collect(toList()));
    perReplicaAttributes = readPerReplicaAttrs();
  }

  private List<String> readPerReplicaAttrs() {
    return this.params.stream()
        .map(s -> Suggestion.tagVsPerReplicaVal.get(s.first()))
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  private Policy(Map<String, List<Clause>> policies, List<Clause> clusterPolicy, List<Preference> clusterPreferences) {
    this.policies = policies != null ? Collections.unmodifiableMap(policies) : Collections.emptyMap();
    this.clusterPolicy = clusterPolicy != null ? Collections.unmodifiableList(clusterPolicy) : Collections.emptyList();
    this.clusterPreferences = clusterPreferences != null ? Collections.unmodifiableList(clusterPreferences) : DEFAULT_PREFERENCES;
    this.params = Collections.unmodifiableList(
        buildParams(this.clusterPreferences, this.clusterPolicy, this.policies).stream()
            .map(s -> new Pair<>(s, Suggestion.getTagType(s)))
            .collect(toList())
    );
    perReplicaAttributes = readPerReplicaAttrs();
  }

  private List<String> buildParams(List<Preference> preferences, List<Clause> policy, Map<String, List<Clause>> policies) {
    final SortedSet<String> paramsOfInterest = new TreeSet<>();
    preferences.forEach(p -> {
      if (paramsOfInterest.contains(p.name.name())) {
        throw new RuntimeException(p.name + " is repeated");
      }
      paramsOfInterest.add(p.name.toString());
    });
    List<String> newParams = new ArrayList<>(paramsOfInterest);
    policy.forEach(c -> c.addTags(newParams));
    policies.values().forEach(clauses -> clauses.forEach(c -> c.addTags(newParams)));
    return newParams;
  }

  public Policy withPolicies(Map<String, List<Clause>> policies) {
    return new Policy(policies, clusterPolicy, clusterPreferences);
  }

  public Policy withClusterPreferences(List<Preference> clusterPreferences) {
    return new Policy(policies, clusterPolicy, clusterPreferences);
  }

  public Policy withClusterPolicy(List<Clause> clusterPolicy) {
    return new Policy(policies, clusterPolicy, clusterPreferences);
  }

  public Policy withParams(List<String> params) {
    return new Policy(policies, clusterPolicy, clusterPreferences);
  }

  public List<Clause> getClusterPolicy() {
    return clusterPolicy;
  }

  public List<Preference> getClusterPreferences() {
    return clusterPreferences;
  }

  @Override
  public void writeMap(EntryWriter ew) throws IOException {
    if (!policies.isEmpty()) {
      ew.put(POLICIES, (MapWriter) ew1 -> {
        for (Map.Entry<String, List<Clause>> e : policies.entrySet()) {
          ew1.put(e.getKey(), e.getValue());
        }
      });
    }
    if (!clusterPreferences.isEmpty()) {
      ew.put(CLUSTER_PREFERENCES, (IteratorWriter) iw -> {
        for (Preference p : clusterPreferences) iw.add(p);
      });
    }
    if (!clusterPolicy.isEmpty()) {
      ew.put(CLUSTER_POLICY, (IteratorWriter) iw -> {
        for (Clause c : clusterPolicy) {
          iw.add(c);
        }
      });
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Policy policy = (Policy) o;

    if (!getPolicies().equals(policy.getPolicies())) return false;
    if (!getClusterPolicy().equals(policy.getClusterPolicy())) return false;
    return getClusterPreferences().equals(policy.getClusterPreferences());
  }

  /*This stores the logical state of the system, given a policy and
   * a cluster state.
   *
   */
  public class Session implements MapWriter {
    final List<String> nodes;
    final SolrCloudManager cloudManager;
    final List<Row> matrix;
    Set<String> collections = new HashSet<>();
    List<Clause> expandedClauses;
    List<Violation> violations = new ArrayList<>();
    final NodeStateProvider nodeStateProvider;
    final int znodeVersion;

    private Session(List<String> nodes, SolrCloudManager cloudManager,
                    List<Row> matrix, List<Clause> expandedClauses, int znodeVersion, NodeStateProvider nodeStateProvider) {
      this.nodes = nodes;
      this.cloudManager = cloudManager;
      this.matrix = matrix;
      this.expandedClauses = expandedClauses;
      this.znodeVersion = znodeVersion;
      this.nodeStateProvider = nodeStateProvider;
      for (Row row : matrix) row.session = this;
    }


    Session(SolrCloudManager cloudManager) {
      ClusterState state = null;
      this.nodeStateProvider = cloudManager.getNodeStateProvider();
      try {
        state = cloudManager.getClusterStateProvider().getClusterState();
        LOG.trace("-- session created with cluster state: {}", state);
      } catch (Exception e) {
        LOG.trace("-- session created, can't obtain cluster state", e);
      }
      this.znodeVersion = state != null ? state.getZNodeVersion() : -1;
      this.nodes = new ArrayList<>(cloudManager.getClusterStateProvider().getLiveNodes());
      this.cloudManager = cloudManager;
      for (String node : nodes) {
        collections.addAll(nodeStateProvider.getReplicaInfo(node, Collections.emptyList()).keySet());
      }

      expandedClauses = clusterPolicy.stream()
          .filter(clause -> !clause.isPerCollectiontag())
          .collect(Collectors.toList());

      ClusterStateProvider stateProvider = cloudManager.getClusterStateProvider();
      for (String c : collections) {
        addClausesForCollection(stateProvider, c);
      }

      Collections.sort(expandedClauses);

      matrix = new ArrayList<>(nodes.size());
      for (String node : nodes) matrix.add(new Row(node, params, perReplicaAttributes, this));
      applyRules();
    }

    void addClausesForCollection(ClusterStateProvider stateProvider, String c) {
      String p = stateProvider.getPolicyNameByCollection(c);
      if (p != null) {
        List<Clause> perCollPolicy = policies.get(p);
        if (perCollPolicy == null) {
          return;
        }
      }
      expandedClauses.addAll(mergePolicies(c, policies.getOrDefault(p, emptyList()), clusterPolicy));
    }

    Session copy() {
      return new Session(nodes, cloudManager, getMatrixCopy(), expandedClauses, znodeVersion, nodeStateProvider);
    }

    public Row getNode(String node) {
      for (Row row : matrix) if (row.node.equals(node)) return row;
      return null;
    }

    List<Row> getMatrixCopy() {
      return matrix.stream()
          .map(row -> row.copy(this))
          .collect(Collectors.toList());
    }

    Policy getPolicy() {
      return Policy.this;

    }

    /**
     * Apply the preferences and conditions
     */
    void applyRules() {
      setApproxValuesAndSortNodes(clusterPreferences, matrix);

      for (Clause clause : expandedClauses) {
        List<Violation> errs = clause.test(matrix);
        violations.addAll(errs);
      }
    }


    public List<Violation> getViolations() {
      return violations;
    }

    public Suggester getSuggester(CollectionAction action) {
      Suggester op = ops.get(action).get();
      if (op == null) throw new UnsupportedOperationException(action.toString() + "is not supported");
      op._init(this);
      return op;
    }

    @Override
    public void writeMap(EntryWriter ew) throws IOException {
      ew.put("znodeVersion", znodeVersion);
      for (Row row : matrix) {
        ew.put(row.node, row);
      }
    }

    @Override
    public String toString() {
      return Utils.toJSONString(toMap(new LinkedHashMap<>()));
    }

    public List<Row> getSorted() {
      return Collections.unmodifiableList(matrix);
    }

    public NodeStateProvider getNodeStateProvider() {
      return nodeStateProvider;
    }

    public int indexOf(String node) {
      for (int i = 0; i < matrix.size(); i++) if (matrix.get(i).node.equals(node)) return i;
      throw new RuntimeException("NO such node found " + node);
    }
  }

  static void setApproxValuesAndSortNodes(List<Preference> clusterPreferences, List<Row> matrix) {
    List<Row> deadNodes = null;
    Iterator<Row> it =matrix.iterator();
    while (it.hasNext()){
      Row row = it.next();
      if(!row.isLive){
        if(deadNodes == null) deadNodes = new ArrayList<>();
        deadNodes.add(row);
        it.remove();
      }
    }

    if (!clusterPreferences.isEmpty()) {
      //this is to set the approximate value according to the precision
      ArrayList<Row> tmpMatrix = new ArrayList<>(matrix);
      Row[] lastComparison = new Row[2];
      for (Preference p : clusterPreferences) {
        try {
          tmpMatrix.sort((r1, r2) -> {
            lastComparison[0] = r1;
            lastComparison[1] = r2;
            return p.compare(r1, r2, false);
          });
        } catch (Exception e) {
          LOG.error("Exception! prefs = {}, recent r1 = {}, r2 = {}, compare : {} matrix = {}",
              clusterPreferences,
              lastComparison[0].node,
              lastComparison[1].node,
              p.compare(lastComparison[0],lastComparison[1], false ),
              Utils.toJSONString(Utils.getDeepCopy(tmpMatrix, 6, false)));
          throw e;
        }
        p.setApproxVal(tmpMatrix);
      }
      // the tmpMatrix was needed only to set the approximate values, now we sort the real matrix
      // recursing through each preference
      matrix.sort((Row r1, Row r2) -> {
        int result = clusterPreferences.get(0).compare(r1, r2, true);
        if (result == 0) result = clusterPreferences.get(0).compare(r1, r2, false);
        return result;
      });

      if(deadNodes != null){
        for (Row deadNode : deadNodes) {
          matrix.add(0, deadNode);
        }
      }
    }
  }

  public Session createSession(SolrCloudManager cloudManager) {
    return new Session(cloudManager);
  }

  public enum SortParam {
    freedisk(0, Integer.MAX_VALUE), cores(0, Integer.MAX_VALUE), heapUsage(0, Integer.MAX_VALUE), sysLoadAvg(0, 100);

    public final int min, max;

    SortParam(int min, int max) {
      this.min = min;
      this.max = max;
    }

    static SortParam get(String m) {
      for (SortParam p : values()) if (p.name().equals(m)) return p;
      throw new RuntimeException(StrUtils.formatString("Invalid sort {0} Sort must be on one of these {1}", m, Arrays.asList(values())));
    }
  }

  enum Sort {
    maximize(1), minimize(-1);
    final int sortval;

    Sort(int i) {
      sortval = i;
    }

    static Sort get(Map<String, Object> m) {
      if (m.containsKey(maximize.name()) && m.containsKey(minimize.name())) {
        throw new RuntimeException("Cannot have both 'maximize' and 'minimize'");
      }
      if (m.containsKey(maximize.name())) return maximize;
      if (m.containsKey(minimize.name())) return minimize;
      throw new RuntimeException("must have either 'maximize' or 'minimize'");
    }
  }


  public static Map<String, List<Clause>> policiesFromMap(Map<String, List<Map<String, Object>>> map, List<String> newParams) {
    Map<String, List<Clause>> newPolicies = new HashMap<>();
    map.forEach((s, l1) ->
        newPolicies.put(s, l1.stream()
            .map(Clause::new)
            .filter(clause -> {
              if (!clause.isPerCollectiontag())
                throw new RuntimeException(clause.globalTag.name + " is only allowed in 'cluster-policy'");
              clause.addTags(newParams);
              return true;
            })
            .sorted()
            .collect(collectingAndThen(toList(), Collections::unmodifiableList))));
    return newPolicies;
  }

  public static List<Clause> mergePolicies(String coll,
                                           List<Clause> collPolicy,
                                           List<Clause> globalPolicy) {

    List<Clause> merged = insertColl(coll, collPolicy);
    List<Clause> global = insertColl(coll, globalPolicy);
    merged.addAll(global.stream()
        .filter(clusterPolicyClause -> merged.stream().noneMatch(perCollPolicy -> perCollPolicy.doesOverride(clusterPolicyClause)))
        .collect(Collectors.toList()));
    return merged;
  }

  /**
   * Insert the collection name into the clauses where collection is not specified
   */
  static List<Clause> insertColl(String coll, Collection<Clause> conditions) {
    return conditions.stream()
        .filter(Clause::isPerCollectiontag)
        .map(clause -> {
          Map<String, Object> copy = new LinkedHashMap<>(clause.original);
          if (!copy.containsKey("collection")) copy.put("collection", coll);
          return new Clause(copy);
        })
        .filter(it -> (it.collection.isPass(coll)))
        .collect(Collectors.toList());

  }

  private static final Map<CollectionAction, Supplier<Suggester>> ops = new HashMap<>();

  static {
    ops.put(CollectionAction.ADDREPLICA, AddReplicaSuggester::new);
    ops.put(CollectionAction.DELETEREPLICA, DeleteReplicaSuggester::new);
    ops.put(CollectionAction.DELETENODE, DeleteNodeSuggester::new);
    ops.put(CollectionAction.MOVEREPLICA, MoveReplicaSuggester::new);
    ops.put(CollectionAction.SPLITSHARD, SplitShardSuggester::new);
    ops.put(CollectionAction.MERGESHARDS, () -> new UnsupportedSuggester(CollectionAction.MERGESHARDS));
    ops.put(CollectionAction.NONE, () -> new UnsupportedSuggester(CollectionAction.NONE));
  }

  public Map<String, List<Clause>> getPolicies() {
    return policies;
  }

  public List<String> getParams() {
    return params.stream().map(Pair::first).collect(toList());
  }

  /**
   * Compares two {@link Row} loads according to a policy.
   *
   * @param r1 the first {@link Row} to compare
   * @param r2 the second {@link Row} to compare
   * @return the value {@code 0} if r1 and r2 are equally loaded
   * a value {@code -1} if r1 is more loaded than r2
   * a value {@code 1} if  r1 is less loaded than r2
   */
  static int compareRows(Row r1, Row r2, Policy policy) {
    return policy.clusterPreferences.get(0).compare(r1, r2, true);
  }

  @Override
  public String toString() {
    return Utils.toJSONString(this);
  }
}
