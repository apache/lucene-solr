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
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.cloud.autoscaling.Clause.Violation;
import org.apache.solr.common.IteratorWriter;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toList;

public class Policy implements MapWriter {
  public static final String EACH = "#EACH";
  public static final String ANY = "#ANY";
  public static final String CLUSTER_POLICY = "cluster-policy";
  public static final String CLUSTER_PREFERENCE = "cluster-preferences";
  public static final Set<String> GLOBAL_ONLY_TAGS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList("cores")));
  Map<String, List<Clause>> policies = new HashMap<>();
  List<Clause> clusterPolicy;
  List<Preference> clusterPreferences;
  List<String> params = new ArrayList<>();


  public Policy(Map<String, Object> jsonMap) {

    clusterPreferences = ((List<Map<String, Object>>) jsonMap.getOrDefault(CLUSTER_PREFERENCE, emptyList())).stream()
        .map(Preference::new)
        .collect(toList());
    for (int i = 0; i < clusterPreferences.size() - 1; i++) {
      Preference preference = clusterPreferences.get(i);
      preference.next = clusterPreferences.get(i + 1);
    }
    if (clusterPreferences.isEmpty()) {
      clusterPreferences.add(new Preference((Map<String, Object>) Utils.fromJSONString("{minimize : cores, precision:1}")));
    }
    clusterPolicy = ((List<Map<String, Object>>) jsonMap.getOrDefault(CLUSTER_POLICY, emptyList())).stream()
        .map(Clause::new)
        .collect(Collectors.toList());

    ((Map<String, List<Map<String, Object>>>) jsonMap.getOrDefault("policies", emptyMap())).forEach((s, l1) ->
        this.policies.put(s, l1.stream()
            .map(Clause::new)
            .sorted()
            .collect(toList())));

    this.policies.forEach((s, c) -> {
      for (Clause clause : c) {
        if (!clause.isPerCollectiontag())
          throw new RuntimeException(clause.globalTag.name + " is only allowed in 'cluster-policy'");
      }
    });

    for (Preference preference : clusterPreferences) {
      if (params.contains(preference.name.name())) {
        throw new RuntimeException(preference.name + " is repeated");
      }
      params.add(preference.name.toString());
      preference.idx = params.size() - 1;
    }
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
      ew.put("policies", (MapWriter) ew1 -> {
        for (Map.Entry<String, List<Clause>> e : policies.entrySet()) {
          ew1.put(e.getKey(), e.getValue());
        }
      });
    }
    if (!clusterPreferences.isEmpty()) {
      ew.put("preferences", (IteratorWriter) iw -> {
        for (Preference p : clusterPreferences) iw.add(p);
      });
    }

  }

  public class Session implements MapWriter {
    final List<String> nodes;
    final ClusterDataProvider dataProvider;
    final List<Row> matrix;
    Set<String> collections = new HashSet<>();
    List<Clause> expandedClauses;
    List<Violation> violations = new ArrayList<>();
    private List<String> paramsOfInterest;

    private Session(List<String> nodes, ClusterDataProvider dataProvider,
                    List<Row> matrix, List<Clause> expandedClauses,
                    List<String> paramsOfInterest) {
      this.nodes = nodes;
      this.dataProvider = dataProvider;
      this.matrix = matrix;
      this.expandedClauses = expandedClauses;
      this.paramsOfInterest = paramsOfInterest;
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
        String p = dataProvider.getPolicy(c);
        if (p != null) {
          List<Clause> perCollPolicy = policies.get(p);
          if (perCollPolicy == null)
            throw new RuntimeException(StrUtils.formatString("Policy for collection {0} is {1} . It does not exist", c, p));
        }
        expandedClauses.addAll(mergePolicies(c, policies.getOrDefault(p, emptyList()), clusterPolicy));
      }

      Collections.sort(expandedClauses);
      List<String> p = new ArrayList<>(params);
      p.addAll(expandedClauses.stream().map(clause -> clause.tag.name).distinct().collect(Collectors.toList()));
      paramsOfInterest = new ArrayList<>(p);
      matrix = new ArrayList<>(nodes.size());
      for (String node : nodes) matrix.add(new Row(node, paramsOfInterest, dataProvider));
      applyRules();
    }

    Session copy() {
      return new Session(nodes, dataProvider, getMatrixCopy(), expandedClauses, paramsOfInterest);
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
        Collections.sort(matrix, (r1, r2) -> {
          int result = clusterPreferences.get(0).compare(r1, r2, true);
          if(result == 0) result = clusterPreferences.get(0).compare(r1, r2, false);
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

  enum SortParam {
    freedisk, cores, heapUsage, sysLoadAvg;

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


  public static class ReplicaInfo implements MapWriter {
    final String name;
    String core,collection,shard;
    Map<String, Object> variables;

    public ReplicaInfo(String name, String coll, String shard, Map<String, Object> vals) {
      this.name = name;
      this.variables = vals;
      this.collection = coll;
      this.shard = shard;
    }

    @Override
    public void writeMap(EntryWriter ew) throws IOException {
      ew.put(name, variables);
    }
    public String getCore(){
      return core;
    }
    public String getCollection(){
      return collection;
    }

    public String getShard(){
      return shard;
    }
  }


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
      hints.put(hint, value);
      return this;
    }

    abstract SolrRequest init();


    public SolrRequest getOperation() {
      if (!isInitialized) {
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
    boolean containsNewErrors(List<Clause.Violation> errs){
      for (Clause.Violation err : errs) {
        if(!originalViolations.contains(err)) return true;
      }
      return false;
    }


    List<Pair<ReplicaInfo, Row>> getValidReplicas(boolean sortDesc, boolean isSource, int until) {
      List<Pair<Policy.ReplicaInfo, Row>> allPossibleReplicas = new ArrayList<>();

      if (sortDesc) {
        if(until == -1) until = getMatrix().size();
        for (int i = 0; i < until; i++) addReplicaToList(getMatrix().get(i), isSource, allPossibleReplicas);
      } else {
        if(until == -1) until = 0;
        for (int i = getMatrix().size() - 1; i >= until; i--) addReplicaToList(getMatrix().get(i), isSource, allPossibleReplicas);
      }
      return allPossibleReplicas;
    }

    void addReplicaToList(Row r, boolean isSource, List<Pair<Policy.ReplicaInfo, Row>> replicaList) {
      if (!isAllowed(r.node, isSource ? Hint.SRC_NODE : Hint.TARGET_NODE)) return;
      for (Map.Entry<String, Map<String, List<Policy.ReplicaInfo>>> e : r.replicaInfo.entrySet()) {
        if (!isAllowed(e.getKey(), Hint.COLL)) continue;
        for (Map.Entry<String, List<Policy.ReplicaInfo>> shard : e.getValue().entrySet()) {
          if (!isAllowed(e.getKey(), Hint.SHARD)) continue;
          replicaList.add(new Pair<>(shard.getValue().get(0), r));
        }
      }
    }
    protected List<Violation> testChangedRow(boolean strict,List<Row> rows) {
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

    ArrayList<Row> getModifiedMatrix(List<Row> matrix ,Row tmpRow, int i) {
      ArrayList<Row> copy = new ArrayList<>(matrix);
      copy.set(i, tmpRow);
      return copy;
    }

    protected boolean isAllowed(Object v, Hint hint) {
      Object hintVal = hints.get(hint);
      return hintVal == null || Objects.equals(v, hintVal);
    }

    public enum Hint {
      COLL, SHARD, SRC_NODE, TARGET_NODE
    }


  }

  static List<Clause> mergePolicies(String coll,
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


}
