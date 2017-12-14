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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.impl.ClusterStateProvider;
import org.apache.solr.common.IteratorWriter;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
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
  public static final Preference DEFAULT_PREFERENCE = new Preference((Map<String, Object>) Utils.fromJSONString("{minimize : cores, precision:1}"));
  final Map<String, List<Clause>> policies;
  final List<Clause> clusterPolicy;
  final List<Preference> clusterPreferences;
  final List<String> params;
  final List<String> perReplicaAttributes;

  public Policy() {
    this(Collections.emptyMap());
  }

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
      initialClusterPreferences.add(DEFAULT_PREFERENCE);
    }
    this.clusterPreferences = Collections.unmodifiableList(initialClusterPreferences);
    final SortedSet<String> paramsOfInterest = new TreeSet<>();
    for (Preference preference : clusterPreferences) {
      if (paramsOfInterest.contains(preference.name.name())) {
        throw new RuntimeException(preference.name + " is repeated");
      }
      paramsOfInterest.add(preference.name.toString());
    }
    List<String> newParams = new ArrayList<>(paramsOfInterest);
    clusterPolicy = ((List<Map<String, Object>>) jsonMap.getOrDefault(CLUSTER_POLICY, emptyList())).stream()
        .map(Clause::new)
        .filter(clause -> {
          clause.addTags(newParams);
          return true;
        })
        .collect(collectingAndThen(toList(), Collections::unmodifiableList));

    this.policies = Collections.unmodifiableMap(
        policiesFromMap((Map<String, List<Map<String, Object>>>)jsonMap.getOrDefault(POLICIES, emptyMap()), newParams));
    this.params = Collections.unmodifiableList(newParams);
    perReplicaAttributes = readPerReplicaAttrs();
  }
  private List<String> readPerReplicaAttrs() {
    return this.params.stream()
        .map(Suggestion.tagVsPerReplicaVal::get)
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  private Policy(Map<String, List<Clause>> policies, List<Clause> clusterPolicy, List<Preference> clusterPreferences) {
    this.policies = policies != null ? Collections.unmodifiableMap(policies) : Collections.emptyMap();
    this.clusterPolicy = clusterPolicy != null ? Collections.unmodifiableList(clusterPolicy) : Collections.emptyList();
    this.clusterPreferences = clusterPreferences != null ? Collections.unmodifiableList(clusterPreferences) :
        Collections.singletonList(DEFAULT_PREFERENCE);
    this.params = Collections.unmodifiableList(buildParams(this.clusterPreferences, this.clusterPolicy, this.policies));
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
    policy.forEach(c -> {
      c.addTags(newParams);
    });
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
    if (!getClusterPreferences().equals(policy.getClusterPreferences())) return false;
    return params.equals(policy.params);
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
    final int znodeVersion;

    private Session(List<String> nodes, SolrCloudManager cloudManager,
                    List<Row> matrix, List<Clause> expandedClauses, int znodeVersion) {
      this.nodes = nodes;
      this.cloudManager = cloudManager;
      this.matrix = matrix;
      this.expandedClauses = expandedClauses;
      this.znodeVersion = znodeVersion;
    }

    Session(SolrCloudManager cloudManager) {
      ClusterState state = null;
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
        collections.addAll(cloudManager.getNodeStateProvider().getReplicaInfo(node, Collections.emptyList()).keySet());
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
      for (String node : nodes) matrix.add(new Row(node, params, perReplicaAttributes,cloudManager));
      applyRules();
    }

    void addClausesForCollection(ClusterStateProvider stateProvider, String c) {
      String p = stateProvider.getPolicyNameByCollection(c);
      if (p != null) {
        List<Clause> perCollPolicy = policies.get(p);
        if (perCollPolicy == null) {
          return;
//          throw new RuntimeException(StrUtils.formatString("Policy for collection {0} is {1} . It does not exist", c, p));
        }
      }
      expandedClauses.addAll(mergePolicies(c, policies.getOrDefault(p, emptyList()), clusterPolicy));
    }

    Session copy() {
      return new Session(nodes, cloudManager, getMatrixCopy(), expandedClauses, znodeVersion);
    }

    List<Row> getMatrixCopy() {
      return matrix.stream()
          .map(Row::copy)
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
      for (int i = 0; i < matrix.size(); i++) {
        Row row = matrix.get(i);
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
  }

  static void setApproxValuesAndSortNodes(List<Preference> clusterPreferences, List<Row> matrix) {
    if (!clusterPreferences.isEmpty()) {
      //this is to set the approximate value according to the precision
      ArrayList<Row> tmpMatrix = new ArrayList<>(matrix);
      for (Preference p : clusterPreferences) {
        Collections.sort(tmpMatrix, (r1, r2) -> p.compare(r1, r2, false));
        p.setApproxVal(tmpMatrix);
      }
      //approximate values are set now. Let's do recursive sorting
      Collections.sort(matrix, (Row r1, Row r2) -> {
        int result = clusterPreferences.get(0).compare(r1, r2, true);
        if (result == 0) result = clusterPreferences.get(0).compare(r1, r2, false);
        return result;
      });
    }
  }

  public Session createSession(SolrCloudManager cloudManager) {
    return new Session(cloudManager);
  }

  public enum SortParam {
    freedisk(0, Integer.MAX_VALUE), cores(0, Integer.MAX_VALUE), heapUsage(0, Integer.MAX_VALUE), sysLoadAvg(0, 100);

    public final int min,max;

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
    ops.put(CollectionAction.ADDREPLICA, () -> new AddReplicaSuggester());
    ops.put(CollectionAction.MOVEREPLICA, () -> new MoveReplicaSuggester());
  }

  public Map<String, List<Clause>> getPolicies() {
    return policies;
  }

  public List<String> getParams() {
    return params;
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
