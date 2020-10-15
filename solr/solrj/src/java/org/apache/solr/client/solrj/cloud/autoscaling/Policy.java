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
import java.io.StringWriter;
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
import org.apache.solr.client.solrj.cloud.autoscaling.Variable.Type;
import org.apache.solr.client.solrj.impl.ClusterStateProvider;
import org.apache.solr.common.IteratorWriter;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.rule.ImplicitSnitch;
import org.apache.solr.common.params.CollectionAdminParams;
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
import static org.apache.solr.client.solrj.cloud.autoscaling.Variable.Type.NODE;
import static org.apache.solr.client.solrj.cloud.autoscaling.Variable.Type.WITH_COLLECTION;

/**
 * The class that reads, parses and applies policies specified in
 * autoscaling.json
 *
 * Create one instance of this class per unique autoscaling.json.
 * This is immutable and is thread-safe
 *
 * Create a fresh new session for each use.
 *
 * @deprecated to be removed in Solr 9.0 (see SOLR-14656)
 */
public class Policy implements MapWriter {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String POLICY = "policy";
  public static final String EACH = "#EACH";
  public static final String ANY = "#ANY";
  public static final String POLICIES = "policies";
  public static final String CLUSTER_POLICY = "cluster-policy";
  public static final String CLUSTER_PREFERENCES = "cluster-preferences";
  public static final Set<String> GLOBAL_ONLY_TAGS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList("cores", CollectionAdminParams.WITH_COLLECTION)));
  @SuppressWarnings({"unchecked"})
  public static final List<Preference> DEFAULT_PREFERENCES = Collections.unmodifiableList(
      Arrays.asList(
          // NOTE - if you change this, make sure to update the solrcloud-autoscaling-overview.adoc which
          // lists the default preferences
          new Preference((Map<String, Object>) Utils.fromJSONString("{minimize : cores, precision:1}")),
          new Preference((Map<String, Object>) Utils.fromJSONString("{maximize : freedisk}"))));

  /**
   * These parameters are always fetched for all nodes regardless of whether they are used in preferences or not
   */
  private static final List<String> DEFAULT_PARAMS_OF_INTEREST = Arrays.asList(ImplicitSnitch.DISK, ImplicitSnitch.CORES);

  private final Map<String, List<Clause>> policies;
  private final List<Clause> clusterPolicy;
  private final List<Preference> clusterPreferences;
  private final List<Pair<String, Type>> params;
  private final List<String> perReplicaAttributes;
  private final int zkVersion;
  /**
   * True if cluster policy, preferences and custom policies are all non-existent
   */
  private final boolean empty;
  /**
   * True if cluster preferences was originally empty, false otherwise. It is used to figure out if
   * the current preferences were implicitly added or not.
   */
  private final boolean emptyPreferences;

  public Policy() {
    this(Collections.emptyMap());
  }

  public Policy(Map<String, Object> jsonMap) {
    this(jsonMap, 0);
  }
  @SuppressWarnings("unchecked")
  public Policy(Map<String, Object> jsonMap, int version) {
    this.empty = jsonMap.get(CLUSTER_PREFERENCES) == null && jsonMap.get(CLUSTER_POLICY) == null && jsonMap.get(POLICIES) == null;
    this.zkVersion = version;
    int[] idx = new int[1];
    List<Preference> initialClusterPreferences = ((List<Map<String, Object>>) jsonMap.getOrDefault(CLUSTER_PREFERENCES, emptyList())).stream()
        .map(m -> new Preference(m, idx[0]++))
        .collect(toList());
    for (int i = 0; i < initialClusterPreferences.size() - 1; i++) {
      Preference preference = initialClusterPreferences.get(i);
      preference.next = initialClusterPreferences.get(i + 1);
    }
    emptyPreferences = initialClusterPreferences.isEmpty();
    if (emptyPreferences) {
      initialClusterPreferences.addAll(DEFAULT_PREFERENCES);
    }
    this.clusterPreferences = Collections.unmodifiableList(initialClusterPreferences);
    final SortedSet<String> paramsOfInterest = new TreeSet<>(DEFAULT_PARAMS_OF_INTEREST);
    clusterPreferences.forEach(preference -> paramsOfInterest.add(preference.name.toString()));
    List<String> newParams = new ArrayList<>(paramsOfInterest);
    clusterPolicy = ((List<Map<String, Object>>) jsonMap.getOrDefault(CLUSTER_POLICY, emptyList())).stream()
        .map(Clause::create)
        .filter(clause -> {
          clause.addTags(newParams);
          return true;
        })
        .collect(collectingAndThen(toList(), Collections::unmodifiableList));

    for (String newParam : new ArrayList<>(newParams)) {
      Type t = VariableBase.getTagType(newParam);
      if(t != null && !t.associatedPerNodeValues.isEmpty()){
        for (String s : t.associatedPerNodeValues) {
          if(!newParams.contains(s)) newParams.add(s);
        }
      }
    }

    this.policies = Collections.unmodifiableMap(
        clausesFromMap((Map<String, List<Map<String, Object>>>) jsonMap.getOrDefault(POLICIES, emptyMap()), newParams));
    List<Pair<String, Type>> params = newParams.stream()
        .map(s -> new Pair<>(s, VariableBase.getTagType(s)))
        .collect(toList());
    //let this be there always, there is no extra cost
    params.add(new Pair<>(WITH_COLLECTION.tagName, WITH_COLLECTION));
    this.params = Collections.unmodifiableList(params);
    perReplicaAttributes = readPerReplicaAttrs();
  }

  private List<String> readPerReplicaAttrs() {
    return this.params.stream()
        .map(s -> s.second().perReplicaValue)
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  private Policy(Map<String, List<Clause>> policies, List<Clause> clusterPolicy, List<Preference> clusterPreferences, int version) {
    this.empty = policies == null && clusterPolicy == null && clusterPreferences == null;
    this.zkVersion = version;
    this.policies = policies != null ? Collections.unmodifiableMap(policies) : Collections.emptyMap();
    this.clusterPolicy = clusterPolicy != null ? Collections.unmodifiableList(clusterPolicy) : Collections.emptyList();
    this.emptyPreferences = clusterPreferences == null;
    this.clusterPreferences = emptyPreferences ? DEFAULT_PREFERENCES : Collections.unmodifiableList(clusterPreferences);
    this.params = Collections.unmodifiableList(
        buildParams(this.clusterPreferences, this.clusterPolicy, this.policies).stream()
            .map(s -> new Pair<>(s, VariableBase.getTagType(s)))
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
    return new Policy(policies, clusterPolicy, clusterPreferences, 0);
  }

  public Policy withClusterPreferences(List<Preference> clusterPreferences) {
    return new Policy(policies, clusterPolicy, clusterPreferences, 0);
  }

  public Policy withClusterPolicy(List<Clause> clusterPolicy) {
    return new Policy(policies, clusterPolicy, clusterPreferences, 0);
  }

  public Policy withParams(List<String> params) {
    return new Policy(policies, clusterPolicy, clusterPreferences, 0);
  }

  public List<Clause> getClusterPolicy() {
    return Collections.unmodifiableList(clusterPolicy);
  }

  public List<Preference> getClusterPreferences() {
    return Collections.unmodifiableList(clusterPreferences);
  }

  @Override
  public void writeMap(EntryWriter ew) throws IOException {
    // if we were initially empty then we don't want to persist any implicitly added
    // policy or preferences
    if (empty)  return;

    if (!policies.isEmpty()) {
      ew.put(POLICIES, (MapWriter) ew1 -> {
        for (Map.Entry<String, List<Clause>> e : policies.entrySet()) {
          ew1.put(e.getKey(), e.getValue());
        }
      });
    }
    if (!emptyPreferences && !clusterPreferences.isEmpty()) {
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

  @Override
  public int hashCode() { return Objects.hash(getPolicies()); }

  public static Map<String, List<Clause>> clausesFromMap(Map<String, List<Map<String, Object>>> map, List<String> newParams) {
    Map<String, List<Clause>> newPolicies = new HashMap<>();
    map.forEach((s, l1) ->
        newPolicies.put(s, l1.stream()
            .map(Clause::create)
            .filter(clause -> {
              if (!clause.isPerCollectiontag())
                throw new RuntimeException(clause.getGlobalTag().name + " is only allowed in 'cluster-policy'");
              clause.addTags(newParams);
              return true;
            })
            .sorted()
            .collect(collectingAndThen(toList(), Collections::unmodifiableList))));
    return newPolicies;
  }

  static void setApproxValuesAndSortNodes(List<Preference> clusterPreferences, List<Row> matrix) {
    List<Row> matrixCopy = new ArrayList<>(matrix);
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
          try {
            @SuppressWarnings({"rawtypes"})
            Map m = Collections.singletonMap("diagnostics", (MapWriter) ew -> {
              PolicyHelper.writeNodes(ew, matrixCopy);
              ew.put("config", matrix.get(0).session.getPolicy());
            });
            log.error("Exception! prefs = {}, recent r1 = {}, r2 = {}, matrix = {}",
                clusterPreferences,
                lastComparison[0].node,
                lastComparison[1].node,
                Utils.writeJson(m, new StringWriter(), true).toString());
          } catch (IOException e1) {
            //
          }
          throw new RuntimeException(e.getMessage(), e);
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

  /**
   * Insert the collection name into the clauses where collection is not specified
   */
  static List<Clause> insertColl(String coll, Collection<Clause> conditions) {
    return conditions.stream()
        .filter(Clause::isPerCollectiontag)
        .map(clause -> {
          Map<String, Object> copy = new LinkedHashMap<>(clause.original);
          if (!copy.containsKey("collection")) {
            copy.put("collection", coll);
            copy.put(Clause.class.getName(), clause);
          }
          return Clause.create(copy);
        })
        .filter(it -> (it.getCollection().isPass(coll)))
        .collect(Collectors.toList());

  }

  public Session createSession(SolrCloudManager cloudManager) {
    return createSession(cloudManager, null);
  }

  public Session createSession(SolrCloudManager cloudManager, Transaction tx) {
    return new Session(cloudManager, this, tx);
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

  static class Transaction {
    private final Policy policy;
    private boolean open = false;
    private Session firstSession;
    private Session currentSession;


    public Transaction(Policy config) {
      this.policy = config;

    }

    public Session open(SolrCloudManager cloudManager) {
      firstSession = currentSession = policy.createSession(cloudManager, Transaction.this);
      open = true;
      return firstSession;
    }


    public boolean isOpen() {
      return open;
    }

    List<Violation> close() {
      if (!open) throw new RuntimeException("Already closed");
      open = false;
      return currentSession.getViolations();
    }

    public Session getCurrentSession() {
      return currentSession;
    }

    void updateSession(Session session) {
      currentSession = session;
    }
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

  public List<Pair<String, Type>> getParams() {
    return Collections.unmodifiableList(params);
  }

  public List<String> getParamNames() {
    return params.stream().map(Pair::first).collect(toList());
  }

  public List<String> getPerReplicaAttributes() {
    return Collections.unmodifiableList(perReplicaAttributes);
  }

  public int getZkVersion() {
    return zkVersion;
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

  public boolean isEmpty() {
    return empty;
  }

  /**
   * @return true if no preferences were specified by the user, false otherwise
   */
  public boolean isEmptyPreferences() {
    return emptyPreferences;
  }

  /*This stores the logical state of the system, given a policy and
   * a cluster state.
   *
   */
  public static class Session implements MapWriter {
    final List<String> nodes;
    final SolrCloudManager cloudManager;
    final List<Row> matrix;
    final NodeStateProvider nodeStateProvider;
    Set<String> collections = new HashSet<>();
    final Policy policy;
    List<Clause> expandedClauses;
    List<Violation> violations = new ArrayList<>();
    Transaction transaction;


    /**
     * This constructor creates a Session from the current Zookeeper collection, replica and node states.
     */
    Session(SolrCloudManager cloudManager, Policy policy, Transaction transaction) {
      collections = new HashSet<>();
      this.transaction = transaction;
      this.policy = policy;
      ClusterState state = null;
      this.nodeStateProvider = cloudManager.getNodeStateProvider();
      try {
        state = cloudManager.getClusterStateProvider().getClusterState();
        log.trace("-- session created with cluster state: {}", state);
      } catch (Exception e) {
        log.trace("-- session created, can't obtain cluster state", e);
      }
      this.nodes = new ArrayList<>(cloudManager.getClusterStateProvider().getLiveNodes());
      this.cloudManager = cloudManager;
      for (String node : nodes) {
        collections.addAll(nodeStateProvider.getReplicaInfo(node, Collections.emptyList()).keySet());
      }

      expandedClauses = policy.getClusterPolicy().stream()
          .filter(clause -> !clause.isPerCollectiontag())
          .collect(Collectors.toList());

      if (nodes.size() > 0) {
        //if any collection has 'withCollection' irrespective of the node, the NodeStateProvider returns a map value
        Map<String, Object> vals = nodeStateProvider.getNodeValues(nodes.get(0), Collections.singleton("withCollection"));
        if (!vals.isEmpty() && vals.get("withCollection") != null) {
          @SuppressWarnings({"unchecked"})
          Map<String, String> withCollMap = (Map<String, String>) vals.get("withCollection");
          if (!withCollMap.isEmpty()) {
            @SuppressWarnings({"unchecked"})
            Clause withCollClause = new Clause((Map<String,Object>)Utils.fromJSONString("{withCollection:'*' , node: '#ANY'}") ,
                new Condition(NODE.tagName, "#ANY", Operand.EQUAL, null, null),
                new Condition(WITH_COLLECTION.tagName,"*" , Operand.EQUAL, null, null), true, null, false
            );
            expandedClauses.add(withCollClause);
          }
        }
      }

      ClusterStateProvider stateProvider = cloudManager.getClusterStateProvider();
      for (String c : collections) {
        addClausesForCollection(policy, expandedClauses, stateProvider, c);
      }

      Collections.sort(expandedClauses);

      matrix = new ArrayList<>(nodes.size());
      for (String node : nodes) matrix.add(new Row(node, policy.getParams(), policy.getPerReplicaAttributes(), this));
      applyRules();
    }

    /**
     * Creates a new Session and updates the Rows in the internal matrix to reference this session.
     */
    private Session(List<String> nodes, SolrCloudManager cloudManager,
                   List<Row> matrix, Set<String> collections, List<Clause> expandedClauses,
                   NodeStateProvider nodeStateProvider, Policy policy, Transaction transaction) {
      this.transaction = transaction;
      this.policy = policy;
      this.nodes = nodes;
      this.cloudManager = cloudManager;
      this.collections = collections;
      this.matrix = matrix;
      this.expandedClauses = expandedClauses;
      this.nodeStateProvider = nodeStateProvider;
      for (Row row : matrix) row.session = this;
    }

    /**
     * Given a session (this one), creates a new one for placement simulations that retains all the relevant information,
     * whether or not that info already made it to Zookeeper.
     */
    public Session cloneToNewSession(SolrCloudManager cloudManager) {
      NodeStateProvider nodeStateProvider = cloudManager.getNodeStateProvider();
      ClusterStateProvider clusterStateProvider = cloudManager.getClusterStateProvider();

      List<String> nodes = new ArrayList<>(clusterStateProvider.getLiveNodes());

      // Copy all collections from old session, even those not yet in ZK state
      Set<String> collections = new HashSet<>(this.collections);

      // (shallow) copy the expanded clauses
      List<Clause> expandedClauses = new ArrayList<>(this.expandedClauses);

      List<Row> matrix = new ArrayList<>(nodes.size());
      Map<String, Row> copyNodes = new HashMap<>();
      for (Row oldRow: this.matrix) {
        copyNodes.put(oldRow.node, oldRow.copy());
      }
      for (String node : nodes) {
        // Do we have a row for that node in this session? If yes, reuse without trying to fetch from cluster state (latest changes might not be there)
        Row newRow = copyNodes.get(node);
        if (newRow == null) {
          // Dealing with a node that doesn't exist in this Session. Need to create related data from scratch.
          // We pass null for the Session in purpose. The current (this) session in not the correct one for this Row.
          // The correct session will be set when we build the new Session instance at the end of this method.
          newRow = new Row(node, this.policy.getParams(), this.policy.getPerReplicaAttributes(), null, nodeStateProvider, cloudManager);
          // Get info for collections on that node
          Set<String> collectionsOnNewNode = nodeStateProvider.getReplicaInfo(node, Collections.emptyList()).keySet();
          collections.addAll(collectionsOnNewNode);

          // Adjust policies to take into account new collections
          for (String collection : collectionsOnNewNode) {
            // We pass this.policy but it is not modified so will not impact this session being cloned
            addClausesForCollection(this.policy, expandedClauses, clusterStateProvider, collection);
          }
        }
        matrix.add(newRow);
      }

      if (nodes.size() > 0) {
        //if any collection has 'withCollection' irrespective of the node, the NodeStateProvider returns a map value
        Map<String, Object> vals = nodeStateProvider.getNodeValues(nodes.get(0), Collections.singleton("withCollection"));
        if (!vals.isEmpty() && vals.get("withCollection") != null) {
          @SuppressWarnings({"unchecked"})
          Map<String, String> withCollMap = (Map<String, String>) vals.get("withCollection");
          if (!withCollMap.isEmpty()) {
            @SuppressWarnings({"unchecked"})
            Clause withCollClause = new Clause((Map<String,Object>)Utils.fromJSONString("{withCollection:'*' , node: '#ANY'}") ,
                new Condition(NODE.tagName, "#ANY", Operand.EQUAL, null, null),
                new Condition(WITH_COLLECTION.tagName,"*" , Operand.EQUAL, null, null), true, null, false
            );
            expandedClauses.add(withCollClause);
          }
        }
      }

      Collections.sort(expandedClauses);

      Session newSession = new Session(nodes, cloudManager, matrix, collections, expandedClauses,
          nodeStateProvider, this.policy, this.transaction);
      newSession.applyRules();

      return newSession;
    }

    void addClausesForCollection(ClusterStateProvider stateProvider, String collection) {
      addClausesForCollection(policy, expandedClauses, stateProvider, collection);
    }

    public static void addClausesForCollection(Policy policy, List<Clause> clauses, ClusterStateProvider stateProvider, String collectionName) {
      String p = stateProvider.getPolicyNameByCollection(collectionName);
      if (p != null) {
        List<Clause> perCollPolicy = policy.getPolicies().get(p);
        if (perCollPolicy == null) {
          return;
        }
      }
      clauses.addAll(mergePolicies(collectionName, policy.getPolicies().getOrDefault(p, emptyList()), policy.getClusterPolicy()));
    }

    Session copy() {
      return new Session(nodes, cloudManager, getMatrixCopy(), new HashSet<>(), expandedClauses,  nodeStateProvider, policy, transaction);
    }

    public Row getNode(String node) {
      for (Row row : matrix) if (row.node.equals(node)) return row;
      return null;
    }

    List<Row> getMatrixCopy() {
      return matrix.stream()
          .map(row -> row.copy())
          .collect(Collectors.toList());
    }

    public Policy getPolicy() {
      return policy;

    }

    /**
     * Apply the preferences and conditions
     */
    void applyRules() {
      sortNodes();

      for (Clause clause : expandedClauses) {
        List<Violation> errs = clause.test(this, null);
        violations.addAll(errs);
      }
    }

    void sortNodes() {
      setApproxValuesAndSortNodes(policy.getClusterPreferences(), matrix);
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
      for (Row row : matrix) {
        ew.put(row.node, row);
      }
    }

    @Override
    public String toString() {
      return Utils.toJSONString(toMap(new LinkedHashMap<>()));
    }

    public List<Row> getSortedNodes() {
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
}
