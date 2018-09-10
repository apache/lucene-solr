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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.cloud.autoscaling.Variable.Type;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;

import static java.util.Collections.singletonMap;
import static org.apache.solr.client.solrj.cloud.autoscaling.Operand.EQUAL;
import static org.apache.solr.client.solrj.cloud.autoscaling.Operand.GREATER_THAN;
import static org.apache.solr.client.solrj.cloud.autoscaling.Operand.LESS_THAN;
import static org.apache.solr.client.solrj.cloud.autoscaling.Operand.NOT_EQUAL;
import static org.apache.solr.client.solrj.cloud.autoscaling.Operand.RANGE_EQUAL;
import static org.apache.solr.client.solrj.cloud.autoscaling.Operand.WILDCARD;
import static org.apache.solr.client.solrj.cloud.autoscaling.Policy.ANY;
import static org.apache.solr.common.params.CoreAdminParams.COLLECTION;
import static org.apache.solr.common.params.CoreAdminParams.NODE;
import static org.apache.solr.common.params.CoreAdminParams.REPLICA;
import static org.apache.solr.common.params.CoreAdminParams.SHARD;
import static org.apache.solr.common.util.StrUtils.formatString;
import static org.apache.solr.common.util.Utils.toJSONString;

/**
 * Represents a set of conditions in the policy
 */
public class Clause implements MapWriter, Comparable<Clause> {
  private static final Set<String> IGNORE_TAGS = new HashSet<>(Arrays.asList(REPLICA, COLLECTION, SHARD, "strict", "type"));

  final boolean hasComputedValue;
  final Map<String, Object> original;
  Condition collection, shard, replica, tag, globalTag;
  final Replica.Type type;
  boolean strict;

  protected Clause(Clause clause, Function<Condition, Object> computedValueEvaluator) {
    this.original = clause.original;
    this.type = clause.type;
    this.collection = clause.collection;
    this.shard = clause.shard;
    this.tag = evaluateValue(clause.tag, computedValueEvaluator);
    this.replica = evaluateValue(clause.replica, computedValueEvaluator);
    this.globalTag = evaluateValue(clause.globalTag, computedValueEvaluator);
    this.hasComputedValue = clause.hasComputedValue;
    this.strict = clause.strict;
  }

  // internal use only
  Clause(Map<String, Object> original, Condition tag, Condition globalTag, boolean isStrict)  {
    this.original = original;
    this.tag = tag;
    this.globalTag = globalTag;
    this.globalTag.clause = this;
    this.type = null;
    this.hasComputedValue = false;
    this.strict = isStrict;
  }

  private Clause(Map<String, Object> m) {
    this.original = Utils.getDeepCopy(m, 10);
    String type = (String) m.get("type");
    this.type = type == null || ANY.equals(type) ? null : Replica.Type.valueOf(type.toUpperCase(Locale.ROOT));
    strict = Boolean.parseBoolean(String.valueOf(m.getOrDefault("strict", "true")));
    Optional<String> globalTagName = m.keySet().stream().filter(Policy.GLOBAL_ONLY_TAGS::contains).findFirst();
    if (globalTagName.isPresent()) {
      globalTag = parse(globalTagName.get(), m);
      if (m.size() > 2) {
        throw new RuntimeException("Only one extra tag supported for the tag " + globalTagName.get() + " in " + toJSONString(m));
      }
      tag = parse(m.keySet().stream()
          .filter(s -> (!globalTagName.get().equals(s) && !IGNORE_TAGS.contains(s)))
          .findFirst().get(), m);
    } else {
      collection = parse(COLLECTION, m);
      shard = parse(SHARD, m);
      if (m.get(REPLICA) == null) {
        throw new IllegalArgumentException(formatString("'replica' is required in {0}", toJSONString(m)));
      }
      this.replica = parse(REPLICA, m);
      if (replica.op == WILDCARD) throw new IllegalArgumentException("replica val cannot be null" + toJSONString(m));
      m.forEach(this::parseCondition);
    }
    if (tag == null)
      throw new RuntimeException("Invalid op, must have one and only one tag other than collection, shard,replica " + toJSONString(m));
    if (tag.name.startsWith(Clause.METRICS_PREFIX)) {
      List<String> ss = StrUtils.splitSmart(tag.name, ':');
      if (ss.size() < 3 || ss.size() > 4) {
        throw new RuntimeException("Invalid metrics: param in " + toJSONString(m) + " must have at 2 or 3 segments after 'metrics:' separated by ':'");
      }
    }
    doPostValidate(collection, shard, replica, tag, globalTag);
    hasComputedValue = hasComputedValue();
  }

  private void doPostValidate(Condition... conditions) {
    for (Condition condition : conditions) {
      if (condition == null) continue;
      String err = condition.varType.postValidate(condition);
      if (err != null) {
        throw new IllegalArgumentException(formatString("Error in clause : {0}, caused by : {1}", toJSONString(original), err));
      }
    }
  }

  public static Clause create(String json) {
    return create((Map<String, Object>) Utils.fromJSONString(json));
  }

  public static Clause create(Map<String, Object> m) {
    Clause clause = new Clause(m);
    return clause.hasComputedValue() ?
        clause :
        clause.getSealedClause(null);
  }

  public static String parseString(Object val) {
    if (val instanceof Condition) val = ((Condition) val).val;
    return val == null ? null : String.valueOf(val);
  }

  public Condition getCollection() {
    return collection;
  }

  public Condition getShard() {
    return shard;
  }

  public Condition getReplica() {
    return replica;
  }

  public Condition getTag() {
    return tag;
  }

  public Condition getGlobalTag() {
    return globalTag;
  }

  Condition evaluateValue(Condition condition, Function<Condition, Object> computedValueEvaluator) {
    if (condition == null) return null;
    if (condition.computedType == null) return condition;
    Object val = computedValueEvaluator.apply(condition);
    val = condition.op.readRuleValue(new Condition(condition.name, val, condition.op, null, null));
    return new Condition(condition.name, val, condition.op, null, this);
  }

  public boolean doesOverride(Clause that) {
    return (collection.equals(that.collection) &&
        tag.name.equals(that.tag.name));

  }

  public boolean isPerCollectiontag() {
    return globalTag == null;
  }

  void parseCondition(String s, Object o) {
    if (IGNORE_TAGS.contains(s)) return;
    if (tag != null) {
      throw new IllegalArgumentException("Only one tag other than collection, shard, replica is possible");
    }
    tag = parse(s, singletonMap(s, o));
  }

  private int compareTypes(Replica.Type t1, Replica.Type t2) {
    if (t1 == null && t2 == null) return 0;
    if (t1 != null && t2 == null) return -1;
    if (t1 == null) return 1;
    return 0;
  }

  private boolean hasComputedValue() {
    if (replica != null && replica.computedType != null) return true;
    if (tag != null && tag.computedType != null) return true;
    if (globalTag != null && globalTag.computedType != null) return true;
    return false;

  }

  @Override
  public int compareTo(Clause that) {
    int v = Integer.compare(this.tag.op.priority, that.tag.op.priority);
    if (v != 0) return v;
    if (this.isPerCollectiontag() && that.isPerCollectiontag()) {
      v = Integer.compare(this.replica.op.priority, that.replica.op.priority);
      if (v == 0) {// higher the number of replicas , harder to satisfy
        Double thisVal = this.replica.val instanceof RangeVal ? ((RangeVal) this.replica.val).max.doubleValue() : (Double) this.replica.val;
        Double thatVal = that.replica.val instanceof RangeVal ? ((RangeVal) that.replica.val).max.doubleValue() : (Double) that.replica.val;
        v = Preference.compareWithTolerance(thisVal, thatVal, 1);
        v = this.replica.op == LESS_THAN ? v : v * -1;
      }
      if (v == 0) v = compareTypes(this.type, that.type);
      return v;
    } else {
      return 0;
    }
  }

  void addTags(Collection<String> params) {
    if (globalTag != null && !params.contains(globalTag.name)) params.add(globalTag.name);
    if (tag != null && !params.contains(tag.name)) params.add(tag.name);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof Clause)) return false;
    Clause that = (Clause)o;
    return  Objects.equals(this.original, that.original);
  }

  //replica value is zero
  boolean isReplicaZero() {
    return replica != null && replica.getOperand() == Operand.EQUAL && replica.val instanceof Double &&
        Preference.compareWithTolerance(0d, (Double) replica.val, 1) == 0;
  }

  public SealedClause getSealedClause(Function<Condition, Object> computedValueEvaluator) {
    return this instanceof SealedClause ?
        (SealedClause) this :
        new SealedClause(this, computedValueEvaluator);
  }

  Condition parse(String s, Map<String,Object> m) {

    Object expectedVal = null;
    ComputedType computedType = null;
    Object val = m.get(s);
    Type varType = VariableBase.getTagType(s);
    if (varType.meta.isHidden()) {
      throwExp(m,"''{0}'' is not allowed", varType.tagName);
    }
    try {
      String conditionName = s.trim();
      Operand operand = null;
      if (val == null) {
        operand = WILDCARD;
        expectedVal = Policy.ANY;
      } else if (val instanceof List) {
        if (!varType.meta.supportArrayVals()) {
          throwExp(m, "array values are not supported for {0}", conditionName);
        }
        expectedVal = readListVal(m, (List) val, varType, conditionName);
        operand = Operand.IN;
      } else if (val instanceof String) {
        String strVal = ((String) val).trim();
        val = strVal;
        operand = getOperand(strVal);
        strVal = strVal.substring(Operand.EQUAL == operand || WILDCARD == operand ? 0 : 1);
        for (ComputedType t : ComputedType.values()) {
          String changedVal = t.match(strVal);
          if (changedVal != null) {
            computedType = t;
            strVal = changedVal;
            if (varType == null || !varType.supportedComputedTypes.contains(computedType)) {
              throwExp(m,"''{0}'' is not allowed for variable :  ''{1}''",t,conditionName);
            }
          }
        }
        if (computedType == null && ((String) val).charAt(0) == '#' && !varType.wildCards.contains(val)) {
          throwExp(m, "''{0}'' is not an allowed value for ''{1}'', supported value is : {2} ", val, conditionName,  varType.wildCards );

        }
        operand = varType == null ? operand : varType.getOperand(operand, strVal, computedType);
        expectedVal = validate(s, new Condition(s, strVal, operand, computedType, null), true);

      } else if (val instanceof Number) {
        operand = Operand.EQUAL;
        operand = varType.getOperand(operand, val, null);
        expectedVal = validate(s, new Condition(s, val, operand, null, null), true);
      }
      return new Condition(conditionName, expectedVal, operand, computedType, this);

    } catch (IllegalArgumentException iae) {
      throw iae;
    } catch (Exception e) {
      throwExp(m, "Invalid tag : {0} ",s );
      return null;
    }
  }

  public void throwExp(Map<String, Object> clause, String msg, Object... args) {
    throw new IllegalArgumentException("syntax error in clause :"+ toJSONString(clause)+ " , msg:  "+  formatString(msg, args));
  }

  private List readListVal(Map m, List val, Type varType, String conditionName) {
    List list = val;
    list = (List) list.stream()
        .map(it -> varType.validate(conditionName, it, true))
        .map(it -> {
          if (it instanceof String) {
            String trim = ((String) it).trim();
            if (trim.isEmpty())
              throw new IllegalArgumentException(formatString("{0} cannot have an empty string value in clause : {1}",
                  conditionName, toJSONString(m)));
            return trim;
          } else return it;
        }).filter(it -> it == null ? false : true)
        .collect(Collectors.toList());
    if (list.isEmpty())
      throw new IllegalArgumentException(formatString("{0} cannot have an empty list value in clause : {1}",
          conditionName, toJSONString(m)));
    for (Object o : list) {
      if (o instanceof String) {
        if (getOperand((String) o) != EQUAL) {
          throw new IllegalArgumentException(formatString("No operators are supported in collection values in condition : {0} in clause : {1}",
              conditionName, toJSONString(m)));
        }
      }
    }
    if (list.size() < 2) {
      throw new IllegalArgumentException(formatString("Array should have more than one value in  condition : {0} in clause : {1}",
          conditionName, toJSONString(m)));

    }
    return list;
  }

  private Operand getOperand(String strVal) {
    Operand operand;
    if (Policy.ANY.equals(strVal) || Policy.EACH.equals(strVal)) operand = WILDCARD;
    else if (strVal.startsWith(NOT_EQUAL.operand)) operand = NOT_EQUAL;
    else if (strVal.startsWith(GREATER_THAN.operand)) operand = GREATER_THAN;
    else if (strVal.startsWith(LESS_THAN.operand)) operand = LESS_THAN;
    else operand = Operand.EQUAL;
    return operand;
  }

  List<Violation> testGroupNodes(Policy.Session session, double[] deviations) {
    //e.g:  {replica:'#EQUAL', shard:'#EACH',  sysprop.zone:'#EACH'}
    ComputedValueEvaluator eval = new ComputedValueEvaluator(session);
    eval.collName = (String) collection.getValue();
    Violation.Ctx ctx = new Violation.Ctx(this, session.matrix, eval);

    Set tags = new HashSet();
    for (Row row : session.matrix) {
      eval.node = row.node;
      Condition tag = this.tag;
      if (tag.computedType != null) tag = evaluateValue(tag, eval);
      Object val = row.getVal(tag.name);
      if (val != null && tag.isPass(val)) {
        if (tag.op == LESS_THAN || tag.op == GREATER_THAN) {
          tags.add(this.tag);
        } else {
          tags.add(val);
        }
      }
    }
    if (tags.isEmpty()) return Collections.emptyList();

    Set<String> shards = getShardNames(session, eval);

    for (String s : shards) {
      final ReplicaCount replicaCount = new ReplicaCount();
      eval.shardName = s;

      for (Object t : tags) {
        replicaCount.reset();
        for (Row row : session.matrix) {
          eval.node = row.node;
          if (t instanceof Condition) {
            Condition tag = (Condition) t;
            if (tag.computedType != null) tag = evaluateValue(tag, eval);
            if (!tag.isPass(row)) continue;
          } else {
            if (!t.equals(row.getVal(tag.name))) continue;
          }
          addReplicaCountsForNode(eval, replicaCount, row);
        }

        SealedClause sealedClause = this.getSealedClause(eval);
        if (!sealedClause.replica.isPass(replicaCount)) {
          ReplicaCount replicaCountCopy = replicaCount.copy();
          Violation violation = new Violation(sealedClause,
              eval.collName,
              eval.shardName,
              null,
              replicaCountCopy,
              sealedClause.getReplica().replicaCountDelta(replicaCountCopy),
              t);
          ctx.resetAndAddViolation(t, replicaCountCopy, violation);
          sealedClause.addViolatingReplicas(sealedClause.tag, eval, ctx, tag.name, t, violation, session);
        } else {
          computeDeviation(deviations, replicaCount, sealedClause);
        }
      }
    }
    return ctx.allViolations;
  }

  private void computeDeviation(double[] deviations, ReplicaCount replicaCount, SealedClause sealedClause) {
    if (deviations != null && sealedClause.replica.op == RANGE_EQUAL) {
      Number actualCount = replicaCount.getVal(type);
      Double realDelta = ((RangeVal) sealedClause.replica.val).realDelta(actualCount.doubleValue());
      realDelta = this.isReplicaZero() ? -1 * realDelta : realDelta;
      deviations[0] += Math.abs(realDelta);
    }
  }

  void addViolatingReplicas(Condition tag,
                            ComputedValueEvaluator eval,
                            Violation.Ctx ctx, String tagName, Object tagVal,
                            Violation violation,
                            Policy.Session session) {
    if (tag.varType.addViolatingReplicas(ctx)) return;
    for (Row row : session.matrix) {
      if (tagVal.equals(row.getVal(tagName))) {
        row.forEachReplica(eval.collName, ri -> {
          if (Policy.ANY.equals(eval.shardName)
              || eval.shardName.equals(ri.getShard()))
            violation.addReplica(new Violation.ReplicaInfoAndErr(ri).withDelta(tag.delta(row.getVal(tag.name))));
        });
      }
    }

  }

  private void addReplicaCountsForNode(ComputedValueEvaluator computedValueEvaluator, ReplicaCount replicaCount, Row node) {
    node.forEachReplica((String) collection.getValue(), ri -> {
      if (Policy.ANY.equals(computedValueEvaluator.shardName)
          || computedValueEvaluator.shardName.equals(ri.getShard()))
        replicaCount.increment(ri);
    });
  }

  List<Violation> testPerNode(Policy.Session session, double[] deviations) {
    ComputedValueEvaluator eval = new ComputedValueEvaluator(session);
    eval.collName = (String) collection.getValue();
    Violation.Ctx ctx = new Violation.Ctx(this, session.matrix, eval);
    Set<String> shards = getShardNames(session, eval);
    for (String s : shards) {
      final ReplicaCount replicaCount = new ReplicaCount();
      eval.shardName = s;
      for (Row row : session.matrix) {
        replicaCount.reset();
        eval.node = row.node;
        Condition tag = this.tag;
        if (tag.computedType != null) {
          tag = evaluateValue(tag, eval);
        }
        if (!tag.isPass(row)) continue;
        addReplicaCountsForNode(eval, replicaCount, row);
        SealedClause sealedClause = this.getSealedClause(eval);
        if (!sealedClause.replica.isPass(replicaCount)) {
          ReplicaCount replicaCountCopy = replicaCount.copy();
          Violation violation = new Violation(sealedClause,
              eval.collName,
              eval.shardName,
              eval.node,
              replicaCountCopy,
              sealedClause.getReplica().replicaCountDelta(replicaCountCopy),
              eval.node);
          ctx.resetAndAddViolation(row.node, replicaCountCopy, violation);
          sealedClause.addViolatingReplicas(sealedClause.tag, eval, ctx, NODE, row.node, violation, session);
        } else {
          computeDeviation(deviations, replicaCount, sealedClause);
        }
      }
    }
    return ctx.allViolations;
  }

  private Set<String> getShardNames(Policy.Session session,
                                    ComputedValueEvaluator eval) {
    Set<String> shards = new HashSet<>();
    if (isShardAbsent()) {
      shards.add(Policy.ANY); //consider the entire collection is a single shard
    } else {
      for (Row row : session.matrix) {
        row.forEachShard(eval.collName, (shard, r) -> {
          if (this.shard.isPass(shard)) shards.add(shard); // add relevant shards
        });
      }
    }
    return shards;
  }

  boolean isShardAbsent() {
    return Policy.ANY.equals(shard.val);
  }

  public List<Violation> test(Policy.Session session, double[] deviations) {
    if (isPerCollectiontag()) {
      return tag.varType == Type.NODE ||
          (tag.varType.meta.isNodeSpecificVal() && replica.computedType == null) ?
          testPerNode(session, deviations) :
          testGroupNodes(session, deviations);
    } else {
      ComputedValueEvaluator computedValueEvaluator = new ComputedValueEvaluator(session);
      Violation.Ctx ctx = new Violation.Ctx(this, session.matrix, computedValueEvaluator);
      for (Row r : session.matrix) {
        computedValueEvaluator.node = r.node;
        SealedClause sealedClause = getSealedClause(computedValueEvaluator);
        if (!sealedClause.getGlobalTag().isPass(r)) {
          ctx.resetAndAddViolation(r.node, null, new Violation(sealedClause, null, null, r.node, r.getVal(sealedClause.globalTag.name),
              sealedClause.globalTag.delta(r.getVal(globalTag.name)), r.node));
          addViolatingReplicas(sealedClause.globalTag, computedValueEvaluator, ctx, Type.CORES.tagName, r.node, ctx.currentViolation, session);

        }
      }
      return ctx.allViolations;

    }
  }


  public boolean isMatch(ReplicaInfo r, String collection, String shard) {
    if (type != null && r.getType() != type) return false;
    if (r.getCollection().equals(collection)) {
      if (this.shard == null || this.shard.val.equals(Policy.ANY)) return true;
      else if (this.shard.val.equals(Policy.EACH) && r.getShard().equals(shard)) return true;
      else return this.shard.val.equals(r.getShard()) && r.getShard().equals(shard);
    }
    return false;
  }

  boolean matchShard(String replicaShard, String shardInContext) {
    if (shard == null || shard.val.equals(ANY)) return true;
    if (shard.val.equals(Policy.EACH) && replicaShard.equals(shardInContext)) return true;
    if (shard.val.equals(replicaShard)) return true;
    return false;
  }

  public boolean isStrict() {
    return strict;
  }

  @Override
  public String toString() {
    return toJSONString(original);
  }

  @Override
  public void writeMap(EntryWriter ew) throws IOException {
    for (Map.Entry<String, Object> e : original.entrySet()) ew.put(e.getKey(), e.getValue());
  }

  enum TestStatus {
    NOT_APPLICABLE, FAIL, PASS
  }

  public static class ComputedValueEvaluator implements Function<Condition, Object> {
    final Policy.Session session;
    String collName = null;
    String shardName = null;
    String node = null;

    public ComputedValueEvaluator(Policy.Session session) {
      this.session = session;
    }

    @Override
    public Object apply(Condition computedCondition) {
      return computedCondition.varType.computeValue(session, computedCondition, collName, shardName, node);
    }

  }

  /**
   * @param name      name of the condition
   * @param val       value of the condition
   * @param isRuleVal is this provided in the rule
   * @return actual validated value
   */
  public static Object  validate(String name, Object val, boolean isRuleVal) {
    if (val == null) return null;
    Type info = VariableBase.getTagType(name);
    if (info == null) throw new RuntimeException("Unknown type :" + name);
    return info.validate(name, val, isRuleVal);
  }

  public static Long parseLong(String name, Object val) {
    if (val == null) return null;
    if (val instanceof Long) return (Long) val;
    Number num = null;
    if (val instanceof String) {
      try {
        num = Long.parseLong(((String) val).trim());
      } catch (NumberFormatException e) {
        try {
          num = Double.parseDouble((String) val);
        } catch (NumberFormatException e1) {
          throw new RuntimeException(name + ": " + val + "not a valid number", e);
        }
      }

    } else if (val instanceof Number) {
      num = (Number) val;
    }

    if (num != null) {
      return num.longValue();
    }
    throw new RuntimeException(name + ": " + val + "not a valid number");
  }


  public static Double parseDouble(String name, Object val) {
    if (val == null) return null;
    if (val instanceof RangeVal) val = ((RangeVal) val).actual;
    if (val instanceof Double) return (Double) val;
    Number num = null;
    if (val instanceof String) {
      try {
        num = Double.parseDouble((String) val);
      } catch (NumberFormatException e) {
        throw new RuntimeException(name + ": " + val + "not a valid number", e);
      }

    } else if (val instanceof Number) {
      num = (Number) val;
    }

    if (num != null) {
      return num.doubleValue();
    }
    throw new RuntimeException(name + ": " + val + "not a valid number");
  }

  public static final String METRICS_PREFIX = "metrics:";

}
