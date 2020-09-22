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
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.ClusterStateProvider;
import org.apache.solr.common.ConditionalMapWriter;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.rule.ImplicitSnitch;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.client.solrj.cloud.autoscaling.Variable.Type.FREEDISK;
import static org.apache.solr.common.params.CollectionAdminParams.WITH_COLLECTION;

/**
 * A suggester is capable of suggesting a collection operation
 * given a particular session. Before it suggests a new operation,
 * it ensures that ,
 *  a) load is reduced on the most loaded node
 *  b) it causes no new violations
 *
 *
 * @deprecated to be removed in Solr 9.0 (see SOLR-14656)
 */
public abstract class Suggester implements MapWriter {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected final EnumMap<Hint, Object> hints = new EnumMap<>(Hint.class);
  Policy.Session session;
  @SuppressWarnings({"rawtypes"})
  SolrRequest operation;
  boolean force;
  protected List<Violation> originalViolations = new ArrayList<>();
  private boolean isInitialized = false;
  LinkedHashMap<Clause, double[]> deviations, lastBestDeviation;


  void _init(Policy.Session session) {
    this.session = session.copy();
  }

  boolean isLessDeviant() {
    if (lastBestDeviation == null && deviations == null) return false;
    if (deviations == null) return true;
    if (lastBestDeviation == null) return false;
    if (lastBestDeviation.size() < deviations.size()) return true;
    for (Map.Entry<Clause, double[]> currentDeviation : deviations.entrySet()) {
      double[] lastDeviation = lastBestDeviation.get(currentDeviation.getKey());
      if (lastDeviation == null) return false;
      int result = Preference.compareWithTolerance(currentDeviation.getValue()[0],
          lastDeviation[0], 1);
      if (result < 0) return true;
      if (result > 0) return false;
    }
    return false;
  }
  @SuppressWarnings({"unchecked", "rawtypes"})
  public Suggester hint(Hint hint, Object value) {
    hint.validator.accept(value);
    if (hint.multiValued) {
      Collection<?> values = value instanceof Collection ? (Collection) value : Collections.singletonList(value);
      ((Set) hints.computeIfAbsent(hint, h -> new HashSet<>())).addAll(values);
    } else {
      if (value == null) {
        hints.put(hint, null);
      } else {
        if ((value instanceof Map) || (value instanceof Number)) {
          hints.put(hint, value);
        } else {
          hints.put(hint, String.valueOf(value));
        }
      }
    }
    return this;
  }

  public CollectionParams.CollectionAction getAction() {
    return null;
  }

  /**
   * Normally, only less loaded nodes are used for moving replicas. If this is a violation and a MOVE must be performed,
   * set the flag to true.
   */
  public Suggester forceOperation(boolean force) {
    this.force = force;
    return this;
  }

  protected boolean isNodeSuitableForReplicaAddition(Row targetRow, Row srcRow) {
    if (!targetRow.isLive) return false;
    if (!isAllowed(targetRow.node, Hint.TARGET_NODE)) return false;
    if (!isAllowed(targetRow.getVal(ImplicitSnitch.DISK), Hint.MINFREEDISK)) return false;

    if (srcRow != null) {// if the src row has the same violation it's not
      for (Violation v1 : originalViolations) {
        if (!v1.getClause().getThirdTag().varType.meta.isNodeSpecificVal()) continue;
        if (v1.getClause().hasComputedValue) continue;
        if (targetRow.node.equals(v1.node)) {
          for (Violation v2 : originalViolations) {
            if (srcRow.node.equals(v2.node)) {
              if (v1.getClause().equals(v2.getClause()))
                return false;
            }
          }
        }
      }
    }

    return true;
  }

  @SuppressWarnings({"rawtypes"})
  abstract SolrRequest init();

  @SuppressWarnings({"unchecked", "rawtypes"})
  public SolrRequest getSuggestion() {
    if (!isInitialized) {
      Set<String> collections = (Set<String>) hints.getOrDefault(Hint.COLL, Collections.emptySet());
      Set<Pair<String, String>> s = (Set<Pair<String, String>>) hints.getOrDefault(Hint.COLL_SHARD, Collections.emptySet());
      if (!collections.isEmpty() || !s.isEmpty()) {
        HashSet<Pair<String, String>> collectionShardPairs = new HashSet<>(s);
        collections.forEach(c -> collectionShardPairs.add(new Pair<>(c, null)));
        collections.forEach(c -> {
          try {
            getWithCollection(c).ifPresent(withCollection -> collectionShardPairs.add(new Pair<>(withCollection, null)));
          } catch (IOException e) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                "Exception while fetching 'withCollection' attribute for collection: " + c, e);
          }
        });
        s.forEach(kv -> {
          try {
            getWithCollection(kv.first()).ifPresent(withCollection -> collectionShardPairs.add(new Pair<>(withCollection, null)));
          } catch (IOException e) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                "Exception while fetching 'withCollection' attribute for collection: " + kv.first(), e);
          }
        });
        setupCollection(collectionShardPairs);
        Collections.sort(session.expandedClauses);
      }
      Set<String> srcNodes = (Set<String>) hints.get(Hint.SRC_NODE);
      if (srcNodes != null && !srcNodes.isEmpty()) {
        // the source node is dead so live nodes may not have it
        for (String srcNode : srcNodes) {
          if (session.matrix.stream().noneMatch(row -> row.node.equals(srcNode))) {
            session.matrix.add(new Row(srcNode, session.getPolicy().getParams(), session.getPolicy().getPerReplicaAttributes(), session));
          }
        }
      }
      session.applyRules();
      originalViolations.addAll(session.getViolations());
      this.operation = init();
      isInitialized = true;
    }
    if (operation != null && session.transaction != null && session.transaction.isOpen()) {
      session.transaction.updateSession(session);
    }
    return operation;
  }

  protected Optional<String> getWithCollection(String collectionName) throws IOException {
    DocCollection collection = session.cloudManager.getClusterStateProvider().getCollection(collectionName);
    if (collection != null) {
      return Optional.ofNullable(collection.getStr(WITH_COLLECTION));
    } else {
      return Optional.empty();
    }
  }

  private void setupCollection(HashSet<Pair<String, String>> collectionShardPairs) {
    ClusterStateProvider stateProvider = session.cloudManager.getClusterStateProvider();
    for (Pair<String, String> shard : collectionShardPairs) {
      // if this is not a known collection from the existing clusterstate,
      // then add it
      if (session.matrix.stream().noneMatch(row -> row.hasColl(shard.first()))) {
        session.addClausesForCollection(stateProvider, shard.first());
      }
      for (Row row : session.matrix) row.createCollShard(shard);
    }
  }


  public Policy.Session getSession() {
    return session;
  }

  List<Row> getMatrix() {
    return session.matrix;

  }

  public static class SuggestionInfo implements MapWriter {
    Suggestion.Type type;
    Violation violation;
    @SuppressWarnings({"rawtypes"})
    SolrRequest operation;

    public SuggestionInfo(Violation violation, @SuppressWarnings({"rawtypes"})SolrRequest op, Suggestion.Type type) {
      this.violation = violation;
      this.operation = op;
      this.type = type;
    }

    @SuppressWarnings({"rawtypes"})
    public SolrRequest getOperation() {
      return operation;
    }

    public Violation getViolation() {
      return violation;
    }

    @Override
    public void writeMap(EntryWriter ew) throws IOException {
      ew.put("type", type.name());
      if(violation!= null) ew.put("violation",
          new ConditionalMapWriter(violation,
              (k, v) -> !"violatingReplicas".equals(k)));
      ew.put("operation", operation);
    }

    @Override
    public String toString() {
      return Utils.toJSONString(this);
    }
  }

  //check if the fresh set of violations is less serious than the last set of violations
  boolean isLessSerious(List<Violation> fresh, List<Violation> old) {
    if (old == null || fresh.size() < old.size()) return true;
    if (fresh.size() == old.size()) {
      for (int i = 0; i < fresh.size(); i++) {
        Violation freshViolation = fresh.get(i);
        Violation oldViolation = null;
        for (Violation v : old) {//look for exactly same clause being violated
          if (v.equals(freshViolation)) oldViolation = v;
        }
        if (oldViolation == null) {//if no match, look for similar violation
          for (Violation v : old) {
            if (v.isSimilarViolation(freshViolation)) oldViolation = v;
          }
        }

        if (oldViolation != null && freshViolation.isLessSerious(oldViolation)) return true;
      }
    }
    return false;
  }

  boolean containsNewErrors(List<Violation> violations) {
    boolean isTxOpen = session.transaction != null && session.transaction.isOpen();
    if (violations.size() > originalViolations.size()) return true;
    for (Violation v : violations) {
      //the computed value can change over time. So it's better to evaluate it in the end
      if (isTxOpen && v.getClause().hasComputedValue) continue;
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
        if (!isAllowed(new Pair<>(e.getKey(), shard.getKey()), Hint.COLL_SHARD)) continue;//todo fix
        if (shard.getValue() == null || shard.getValue().isEmpty()) continue;
        for (ReplicaInfo replicaInfo : shard.getValue()) {
          if (replicaInfo.getName().startsWith("SYNTHETIC.")) continue;
          replicaList.add(new Pair<>(shard.getValue().get(0), r));
          break;
        }
      }
    }
  }

  List<Violation> testChangedMatrix(boolean executeInStrictMode, Policy.Session session) {
    if (this.deviations != null) this.lastBestDeviation = this.deviations;
    this.deviations = null;
    Policy.setApproxValuesAndSortNodes(session.getPolicy().getClusterPreferences(), session.matrix);
    List<Violation> errors = new ArrayList<>();
    for (Clause clause : session.expandedClauses) {
      Clause originalClause = clause.derivedFrom == null ? clause : clause.derivedFrom;
      if (this.deviations == null) this.deviations = new LinkedHashMap<>();
      this.deviations.put(originalClause, new double[1]);
      List<Violation> errs = clause.test(session, this.deviations == null ? null : this.deviations.get(originalClause));
      if (!errs.isEmpty() &&
          (executeInStrictMode || clause.strict)) errors.addAll(errs);
    }
    session.violations = errors;
    if (!errors.isEmpty()) deviations = null;
    return errors;
  }


  protected boolean isAllowed(Object v, Hint hint) {
    Object hintVal = hints.get(hint);
    if (hintVal == null) return true;
    if (hint.multiValued) {
      @SuppressWarnings({"rawtypes"})
      Set set = (Set) hintVal;
      return set == null || set.contains(v);
    } else {
      return hintVal == null || hint.valueValidator.test(new Pair<>(hintVal, v));
    }
  }

  public enum Hint {
    COLL(true),
    // collection shard pair
    // this should be a Pair<String, String> , (collection,shard)
    COLL_SHARD(true, v -> {
      @SuppressWarnings({"rawtypes"})
      Collection c = v instanceof Collection ? (Collection) v : Collections.singleton(v);
      for (Object o : c) {
        if (!(o instanceof Pair)) {
          throw new RuntimeException("COLL_SHARD hint must use a Pair");
        }
        @SuppressWarnings({"rawtypes"})
        Pair p = (Pair) o;
        if (p.first() == null || p.second() == null) {
          throw new RuntimeException("Both collection and shard must not be null");
        }
      }

    }) {
      @Override
      public Object parse(Object v) {
        if (v instanceof Map) {
          @SuppressWarnings({"rawtypes"})
          Map map = (Map) v;
          return Pair.parse(map);
        }
        return super.parse(v);
      }
    },
    SRC_NODE(true),
    TARGET_NODE(true),
    REPLICATYPE(false, o -> {
      if (!(o instanceof Replica.Type)) {
        throw new RuntimeException("REPLICATYPE hint must use a ReplicaType");
      }
    }),

    MINFREEDISK(false, o -> {
      if (!(o instanceof Number)) throw new RuntimeException("MINFREEDISK hint must be a number");
    }, hintValVsActual -> {
      Double hintFreediskInGb = (Double) FREEDISK.validate(null, hintValVsActual.first(), false);
      Double actualFreediskInGb = (Double) FREEDISK.validate(null, hintValVsActual.second(), false);
      if (actualFreediskInGb == null) return false;
      return actualFreediskInGb > hintFreediskInGb;
    }),
    NUMBER(true, o -> {
      if (!(o instanceof Number)) throw new RuntimeException("NUMBER hint must be a number");
    }),
    PARAMS(false, o -> {
      if (!(o instanceof Map)) {
        throw new RuntimeException("PARAMS hint must be a Map<String, Object>");
      }
    }),
    REPLICA(true);

    public final boolean multiValued;
    public final Consumer<Object> validator;
    public final Predicate<Pair<Object, Object>> valueValidator;

    Hint(boolean multiValued) {
      this(multiValued, v -> {
        @SuppressWarnings({"rawtypes"})
        Collection c = v instanceof Collection ? (Collection) v : Collections.singleton(v);
        for (Object o : c) {
          if (!(o instanceof String)) throw new RuntimeException("hint must be of type String");
        }
      });
    }

    Hint(boolean multiValued, Consumer<Object> c) {
      this(multiValued, c, equalsPredicate);
    }

    Hint(boolean multiValued, Consumer<Object> c, Predicate<Pair<Object, Object>> testval) {
      this.multiValued = multiValued;
      this.validator = c;
      this.valueValidator = testval;
    }

    public static Hint get(String s) {
      for (Hint hint : values()) {
        if (hint.name().equals(s)) return hint;
      }
      return null;
    }

    public Object parse(Object v) {
      return v;
    }


  }

  static Predicate<Pair<Object, Object>> equalsPredicate = valPair -> Objects.equals(valPair.first(), valPair.second());

  @Override
  public String toString() {
    return jsonStr();
  }

  @Override
  public void writeMap(EntryWriter ew) throws IOException {
    ew.put("action", String.valueOf(getAction()));
    ew.put("hints", (MapWriter) ew1 -> hints.forEach((hint, o) -> ew1.putNoEx(hint.toString(), o)));
  }

  @SuppressWarnings({"rawtypes"})
  protected Collection setupWithCollectionTargetNodes(Set<String> collections, Set<Pair<String, String>> s, String withCollection) {
    Collection originalTargetNodesCopy = null;
    if (withCollection != null) {
      if (log.isDebugEnabled()) {
        HashSet<String> set = new HashSet<>(collections);
        s.forEach(kv -> set.add(kv.first()));
        log.debug("Identified withCollection = {} for collection: {}", withCollection, set);
      }

      originalTargetNodesCopy = Utils.getDeepCopy((Collection) hints.get(Hint.TARGET_NODE), 10, true);

      Set<String> withCollectionNodes = new HashSet<>();

      for (Row row : getMatrix()) {
        row.forEachReplica(r -> {
          if (withCollection.equals(r.getCollection()) &&
              "shard1".equals(r.getShard())) {
            withCollectionNodes.add(r.getNode());
          }
        });
      }

      if (originalTargetNodesCopy != null && !originalTargetNodesCopy.isEmpty()) {
        // find intersection of the set of target nodes with the set of 'withCollection' nodes
        @SuppressWarnings({"unchecked"})
        Set<String> set = (Set<String>) hints.computeIfAbsent(Hint.TARGET_NODE, h -> new HashSet<>());
        set.retainAll(withCollectionNodes);
        if (set.isEmpty()) {
          // no nodes common between the sets, we have no choice but to restore the original target node hint
          hints.put(Hint.TARGET_NODE, originalTargetNodesCopy);
        }
      } else if (originalTargetNodesCopy == null) {
        hints.put(Hint.TARGET_NODE, withCollectionNodes);
      }
    }
    return originalTargetNodesCopy;
  }

  protected String findWithCollection(Set<String> collections, Set<Pair<String, String>> s) {
    List<String> withCollections = new ArrayList<>(1);
    collections.forEach(c -> {
      try {
        getWithCollection(c).ifPresent(withCollections::add);
      } catch (IOException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "Exception while fetching 'withCollection' attribute for collection: " + c, e);
      }
    });
    s.forEach(kv -> {
      try {
        getWithCollection(kv.first()).ifPresent(withCollections::add);
      } catch (IOException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "Exception while fetching 'withCollection' attribute for collection: " + kv.first(), e);
      }
    });

    if (withCollections.size() > 1) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "The number of 'withCollection' attributes should be exactly 1 for any policy but found: " + withCollections);
    }
    return withCollections.isEmpty() ? null : withCollections.get(0);
  }
}
