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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
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

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.cloud.autoscaling.Clause.Violation;
import org.apache.solr.common.IteratorWriter;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;

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

  }

  private Policy(Map<String, List<Clause>> policies, List<Clause> clusterPolicy, List<Preference> clusterPreferences,
                 List<String> params) {
    this.policies = policies != null ? Collections.unmodifiableMap(policies) : Collections.emptyMap();
    this.clusterPolicy = clusterPolicy != null ? Collections.unmodifiableList(clusterPolicy) : Collections.emptyList();
    this.clusterPreferences = clusterPreferences != null ? Collections.unmodifiableList(clusterPreferences) :
        Collections.singletonList(DEFAULT_PREFERENCE);
    this.params = params != null ? Collections.unmodifiableList(params) : Collections.emptyList();
  }

  public Policy withPolicies(Map<String, List<Clause>> policies) {
    return new Policy(policies, clusterPolicy, clusterPreferences, params);
  }

  public Policy withClusterPreferences(List<Preference> clusterPreferences) {
    return new Policy(policies, clusterPolicy, clusterPreferences, params);
  }

  public Policy withClusterPolicy(List<Clause> clusterPolicy) {
    return new Policy(policies, clusterPolicy, clusterPreferences, params);
  }

  public Policy withParams(List<String> params) {
    return new Policy(policies, clusterPolicy, clusterPreferences, params);
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
    final ClusterDataProvider dataProvider;
    final List<Row> matrix;
    Set<String> collections = new HashSet<>();
    List<Clause> expandedClauses;
    List<Violation> violations = new ArrayList<>();

    private Session(List<String> nodes, ClusterDataProvider dataProvider,
                    List<Row> matrix, List<Clause> expandedClauses) {
      this.nodes = nodes;
      this.dataProvider = dataProvider;
      this.matrix = matrix;
      this.expandedClauses = expandedClauses;
    }

    Session(ClusterDataProvider dataProvider) {
      this.nodes = new ArrayList<>(dataProvider.getNodes());
      this.dataProvider = dataProvider;
      for (String node : nodes) {
        collections.addAll(dataProvider.getReplicaInfo(node, Collections.emptyList()).keySet());
      }

      expandedClauses = clusterPolicy.stream()
          .filter(clause -> !clause.isPerCollectiontag())
          .collect(Collectors.toList());

      for (String c : collections) {
        addClausesForCollection(dataProvider, c);
      }

      Collections.sort(expandedClauses);

      matrix = new ArrayList<>(nodes.size());
      for (String node : nodes) matrix.add(new Row(node, params, dataProvider));
      applyRules();
    }

    private void addClausesForCollection(ClusterDataProvider dataProvider, String c) {
      String p = dataProvider.getPolicyNameByCollection(c);
      if (p != null) {
        List<Clause> perCollPolicy = policies.get(p);
        if (perCollPolicy == null)
          throw new RuntimeException(StrUtils.formatString("Policy for collection {0} is {1} . It does not exist", c, p));
      }
      expandedClauses.addAll(mergePolicies(c, policies.getOrDefault(p, emptyList()), clusterPolicy));
    }

    Session copy() {
      return new Session(nodes, dataProvider, getMatrixCopy(), expandedClauses);
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
    private void applyRules() {
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


  public Session createSession(ClusterDataProvider dataProvider) {
    return new Session(dataProvider);
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


  /* A suggester is capable of suggesting a collection operation
   * given a particular session. Before it suggests a new operation,
   * it ensures that ,
   *  a) load is reduced on the most loaded node
   *  b) it causes no new violations
   *
   */
  public static abstract class Suggester {
    protected final EnumMap<Hint, Object> hints = new EnumMap<>(Hint.class);
    Policy.Session session;
    SolrRequest operation;
    protected List<Violation> originalViolations = new ArrayList<>();
    private boolean isInitialized = false;

    private void _init(Session session) {
      this.session = session.copy();
    }

    public Suggester hint(Hint hint, Object value) {
      if (hint == Hint.TARGET_NODE || hint == Hint.SRC_NODE || hint == Hint.COLL) {
        Collection<?> values = value instanceof Collection ? (Collection)value : Collections.singletonList(value);
        ((Set) hints.computeIfAbsent(hint, h -> new HashSet<>())).addAll(values);
      } else {
        hints.put(hint, value == null ? null : String.valueOf(value));
      }
      return this;
    }

    abstract SolrRequest init();


    public SolrRequest getOperation() {
      if (!isInitialized) {
        Set<String> collections = (Set<String>) hints.get(Hint.COLL);
        String shard = (String) hints.get(Hint.SHARD);
        if (collections != null) {
          for (String coll : collections) {
            // if this is not a known collection from the existing clusterstate,
            // then add it
            if (session.matrix.stream().noneMatch(row -> row.collectionVsShardVsReplicas.containsKey(coll))) {
              session.addClausesForCollection(session.dataProvider, coll);
            }
            for (Row row : session.matrix) {
              if (!row.collectionVsShardVsReplicas.containsKey(coll))
                row.collectionVsShardVsReplicas.put(coll, new HashMap<>());
              if (shard != null) {
                Map<String, List<ReplicaInfo>> shardInfo = row.collectionVsShardVsReplicas.get(coll);
                if (!shardInfo.containsKey(shard)) shardInfo.put(shard, new ArrayList<>());
              }
            }
          }
          Collections.sort(session.expandedClauses);
        }
        Set<String> srcNodes = (Set<String>) hints.get(Hint.SRC_NODE);
        if (srcNodes != null && !srcNodes.isEmpty()) {
          // the source node is dead so live nodes may not have it
          for (String srcNode : srcNodes) {
            if(session.matrix.stream().noneMatch(row -> row.node.equals(srcNode)))
            session.matrix.add(new Row(srcNode, session.getPolicy().params, session.dataProvider));
          }
        }
        session.applyRules();
        originalViolations.addAll(session.getViolations());
        this.operation = init();
        isInitialized = true;
      }
      return operation;
    }

    public Session getSession() {
      return session;
    }

    List<Row> getMatrix() {
      return session.matrix;

    }

    //check if the fresh set of violations is less serious than the last set of violations
    boolean isLessSerious(List<Violation> fresh, List<Violation> old) {
      if (old == null || fresh.size() < old.size()) return true;
      if (fresh.size() == old.size()) {
        for (int i = 0; i < fresh.size(); i++) {
          Violation freshViolation = fresh.get(i);
          Violation oldViolation = null;
          for (Violation v : old) {
            if (v.equals(freshViolation)) oldViolation = v;
          }
          if (oldViolation != null && freshViolation.isLessSerious(oldViolation)) return true;
        }
      }
      return false;
    }

    boolean containsNewErrors(List<Violation> violations) {
      for (Violation v : violations) {
        int idx = originalViolations.indexOf(v);
        if (idx < 0 || originalViolations.get(idx).isLessSerious(v)) return true;
      }
      return false;
    }

    List<Pair<ReplicaInfo, Row>> getValidReplicas(boolean sortDesc, boolean isSource, int until) {
      List<Pair<ReplicaInfo, Row>> allPossibleReplicas = new ArrayList<>();

      if (sortDesc) {
        if (until == -1) until = getMatrix().size();
        for (int i = 0; i < until; i++) addReplicaToList(getMatrix().get(i), isSource, allPossibleReplicas);
      } else {
        if (until == -1) until = 0;
        for (int i = getMatrix().size() - 1; i >= until; i--)
          addReplicaToList(getMatrix().get(i), isSource, allPossibleReplicas);
      }
      return allPossibleReplicas;
    }

    void addReplicaToList(Row r, boolean isSource, List<Pair<ReplicaInfo, Row>> replicaList) {
      if (!isAllowed(r.node, isSource ? Hint.SRC_NODE : Hint.TARGET_NODE)) return;
      for (Map.Entry<String, Map<String, List<ReplicaInfo>>> e : r.collectionVsShardVsReplicas.entrySet()) {
        if (!isAllowed(e.getKey(), Hint.COLL)) continue;
        for (Map.Entry<String, List<ReplicaInfo>> shard : e.getValue().entrySet()) {
          if (!isAllowed(e.getKey(), Hint.SHARD)) continue;//todo fix
          if(shard.getValue() == null || shard.getValue().isEmpty()) continue;
          replicaList.add(new Pair<>(shard.getValue().get(0), r));
        }
      }
    }

    protected List<Violation> testChangedMatrix(boolean strict, List<Row> rows) {
      List<Violation> errors = new ArrayList<>();
      for (Clause clause : session.expandedClauses) {
        if (strict || clause.strict) {
          List<Violation> errs = clause.test(rows);
          if (!errs.isEmpty()) {
            errors.addAll(errs);
          }
        }
      }
      return errors;
    }

    ArrayList<Row> getModifiedMatrix(List<Row> matrix, Row tmpRow, int i) {
      ArrayList<Row> copy = new ArrayList<>(matrix);
      copy.set(i, tmpRow);
      return copy;
    }

    protected boolean isAllowed(Object v, Hint hint) {
      Object hintVal = hints.get(hint);
      if (hint == Hint.TARGET_NODE || hint == Hint.SRC_NODE || hint == Hint.COLL) {
        Set set = (Set) hintVal;
        return set == null || set.contains(v);
      } else {
        return hintVal == null || Objects.equals(v, hintVal);
      }
    }

    public enum Hint {
      COLL, SHARD, SRC_NODE, TARGET_NODE, REPLICATYPE
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
    return policy.clusterPreferences.get(0).compare(r1, r2, false);
  }
}
