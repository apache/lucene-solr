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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import org.apache.solr.common.MapWriter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;

import static java.util.Collections.singletonMap;
import static org.apache.solr.client.solrj.cloud.autoscaling.Clause.TestStatus.PASS;
import static org.apache.solr.client.solrj.cloud.autoscaling.Operand.GREATER_THAN;
import static org.apache.solr.client.solrj.cloud.autoscaling.Operand.LESS_THAN;
import static org.apache.solr.client.solrj.cloud.autoscaling.Operand.NOT_EQUAL;
import static org.apache.solr.client.solrj.cloud.autoscaling.Operand.WILDCARD;
import static org.apache.solr.client.solrj.cloud.autoscaling.Policy.ANY;
import static org.apache.solr.common.params.CoreAdminParams.COLLECTION;
import static org.apache.solr.common.params.CoreAdminParams.REPLICA;
import static org.apache.solr.common.params.CoreAdminParams.SHARD;

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

  private Clause(Map<String, Object> m) {
    this.original = Utils.getDeepCopy(m, 10);
    String type = (String) m.get("type");
    this.type = type == null || ANY.equals(type) ? null : Replica.Type.valueOf(type.toUpperCase(Locale.ROOT));
    strict = Boolean.parseBoolean(String.valueOf(m.getOrDefault("strict", "true")));
    Optional<String> globalTagName = m.keySet().stream().filter(Policy.GLOBAL_ONLY_TAGS::contains).findFirst();
    if (globalTagName.isPresent()) {
      globalTag = parse(globalTagName.get(), m);
      if (m.size() > 2) {
        throw new RuntimeException("Only one extra tag supported for the tag " + globalTagName.get() + " in " + Utils.toJSONString(m));
      }
      tag = parse(m.keySet().stream()
          .filter(s -> (!globalTagName.get().equals(s) && !IGNORE_TAGS.contains(s)))
          .findFirst().get(), m);
    } else {
      collection = parse(COLLECTION, m);
      shard = parse(SHARD, m);
      if (m.get(REPLICA) == null) {
        throw new RuntimeException(StrUtils.formatString("'replica' is required in {0}", Utils.toJSONString(m)));
      }
      this.replica = parse(REPLICA, m);
      if (replica.op == WILDCARD) throw new RuntimeException("replica val cannot be null" + Utils.toJSONString(m));
      m.forEach(this::parseCondition);
    }
    if (tag == null)
      throw new RuntimeException("Invalid op, must have one and only one tag other than collection, shard,replica " + Utils.toJSONString(m));
    if (tag.name.startsWith(Clause.METRICS_PREFIX)) {
      List<String> ss = StrUtils.splitSmart(tag.name, ':');
      if (ss.size() < 3 || ss.size() > 4) {
        throw new RuntimeException("Invalid metrics: param in " + Utils.toJSONString(m) + " must have at 2 or 3 segments after 'metrics:' separated by ':'");
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
        throw new IllegalArgumentException(StrUtils.formatString("Error in clause : {0}, caused by : {1}", Utils.toJSONString(original), err));
      }
    }
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

  private Condition evaluateValue(Condition condition, Function<Condition, Object> computedValueEvaluator) {
    if (condition == null) return null;
    if (condition.computationType == null) return condition;
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
    if (replica != null && replica.computationType != null) return true;
    if (tag != null && tag.computationType != null) return true;
    if (globalTag != null && globalTag.computationType != null) return true;
    return false;

  }

  @Override
  public int compareTo(Clause that) {
    int v = Integer.compare(this.tag.op.priority, that.tag.op.priority);
    if (v != 0) return v;
    if (this.isPerCollectiontag() && that.isPerCollectiontag()) {
      v = Integer.compare(this.replica.op.priority, that.replica.op.priority);
      if (v == 0) {// higher the number of replicas , harder to satisfy
        v = Preference.compareWithTolerance((Double) this.replica.val, (Double) that.replica.val, 1);
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
    return replica != null && replica.getOperand() == Operand.EQUAL &&
        Preference.compareWithTolerance(0d, (Double) replica.val, 1) == 0;
  }

  public SealedClause getSealedClause(Function<Condition, Object> computedValueEvaluator) {
    return this instanceof SealedClause ?
        (SealedClause) this :
        new SealedClause(this, computedValueEvaluator);
  }

  Condition parse(String s, Map m) {
    Object expectedVal = null;
    ComputationType computationType = null;
    Object val = m.get(s);
    Suggestion.ConditionType varType = Suggestion.getTagType(s);
    try {
      String conditionName = s.trim();
      Operand operand = null;
      if (val == null) {
        operand = WILDCARD;
        expectedVal = Policy.ANY;
      } else if (val instanceof String) {
        String strVal = ((String) val).trim();
        if (Policy.ANY.equals(strVal) || Policy.EACH.equals(strVal)) operand = WILDCARD;
        else if (strVal.startsWith(NOT_EQUAL.operand)) operand = NOT_EQUAL;
        else if (strVal.startsWith(GREATER_THAN.operand)) operand = GREATER_THAN;
        else if (strVal.startsWith(LESS_THAN.operand)) operand = LESS_THAN;
        else operand = Operand.EQUAL;
        strVal = strVal.substring(Operand.EQUAL == operand || WILDCARD == operand ? 0 : 1);
        for (ComputationType t : ComputationType.values()) {
          String changedVal = t.match(strVal);
          if (changedVal != null) {
            computationType = t;
            strVal = changedVal;
            if (varType == null || !varType.supportComputed(computationType, this)) {
              throw new IllegalArgumentException(StrUtils.formatString("''{0}'' is not allowed for variable :  ''{1}'' , in condition : ''{2}'' ",
                  t, conditionName, Utils.toJSONString(m)));
            }
          }
        }
        operand = varType == null ? operand : varType.getOperand(operand, strVal, computationType);
        expectedVal = validate(s, new Condition(s, strVal, operand, computationType, null), true);

      } else if (val instanceof Number) {
        operand = Operand.EQUAL;
        operand = varType.getOperand(operand, val, null);
        expectedVal = validate(s, new Condition(s, val, operand, null, null), true);
      }
      return new Condition(conditionName, expectedVal, operand, computationType, this);

    } catch (IllegalArgumentException iae) {
      throw iae;
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid tag : " + s + ":" + val, e);
    }
  }

  public List<Violation> test(Policy.Session session) {
    ComputedValueEvaluator computedValueEvaluator = new ComputedValueEvaluator(session);
    Suggestion.ViolationCtx ctx = new Suggestion.ViolationCtx(this, session.matrix, computedValueEvaluator);
    if (isPerCollectiontag()) {
      Map<String, Map<String, Map<String, ReplicaCount>>> replicaCounts = computeReplicaCounts(session.matrix, computedValueEvaluator);
      for (Map.Entry<String, Map<String, Map<String, ReplicaCount>>> e : replicaCounts.entrySet()) {
        computedValueEvaluator.collName = e.getKey();
        if (!collection.isPass(computedValueEvaluator.collName)) continue;
        for (Map.Entry<String, Map<String, ReplicaCount>> shardVsCount : e.getValue().entrySet()) {
          computedValueEvaluator.shardName = shardVsCount.getKey();
          if (!shard.isPass(computedValueEvaluator.shardName)) continue;
          for (Map.Entry<String, ReplicaCount> counts : shardVsCount.getValue().entrySet()) {
            SealedClause sealedClause = getSealedClause(computedValueEvaluator);
            ReplicaCount replicas = counts.getValue();
            if (!sealedClause.replica.isPass(replicas)) {
              Violation violation = new Violation(sealedClause,
                  computedValueEvaluator.collName,
                  computedValueEvaluator.shardName,
                  tag.name.equals("node") ? counts.getKey() : null,
                  counts.getValue(),
                  sealedClause.getReplica().delta(replicas),
                  counts.getKey());
              tag.varType.addViolatingReplicas(ctx.reset(counts.getKey(), replicas, violation));
            }
          }
        }
      }
    } else {
      for (Row r : session.matrix) {
        SealedClause sealedClause = getSealedClause(computedValueEvaluator);
        if (!sealedClause.getGlobalTag().isPass(r)) {
          Suggestion.ConditionType.CORES.addViolatingReplicas(ctx.reset(null, null,
              new Violation(sealedClause, null, null, r.node, r.getVal(sealedClause.globalTag.name), sealedClause.globalTag.delta(r.getVal(globalTag.name)), null)));
        }
      }
    }
    return ctx.allViolations;

  }

  private Map<String, Map<String, Map<String, ReplicaCount>>> computeReplicaCounts(List<Row> allRows,
                                                                                   ComputedValueEvaluator computedValueEvaluator) {
    Map<String, Map<String, Map<String, ReplicaCount>>> collVsShardVsTagVsCount = new HashMap<>();
    for (Row row : allRows) {
      for (Map.Entry<String, Map<String, List<ReplicaInfo>>> colls : row.collectionVsShardVsReplicas.entrySet()) {
        String collectionName = colls.getKey();
        if (!collection.isPass(collectionName)) continue;
        Map<String, Map<String, ReplicaCount>> collMap = collVsShardVsTagVsCount.computeIfAbsent(collectionName, s -> new HashMap<>());
        for (Map.Entry<String, List<ReplicaInfo>> shards : colls.getValue().entrySet()) {
          String shardName = shards.getKey();
          if (ANY.equals(shard.val)) shardName = ANY;
          if (!shard.isPass(shardName)) break;
          Map<String, ReplicaCount> tagVsCount = collMap.computeIfAbsent(shardName, s -> new HashMap<>());
          Object tagVal = row.getVal(tag.name);
          computedValueEvaluator.collName = collectionName;
          computedValueEvaluator.shardName = shardName;
          SealedClause sealedClause = getSealedClause(computedValueEvaluator);
          boolean pass = sealedClause.getTag().isPass(tagVal);
          tagVsCount.computeIfAbsent(pass ? String.valueOf(tagVal) : "", s -> new ReplicaCount());
          if (pass) {
            tagVsCount.get(String.valueOf(tagVal)).increment(shards.getValue());
          }
        }
      }
    }
    return collVsShardVsTagVsCount;
  }

  enum ComputationType {
    EQUAL() {
      @Override
      public String wrap(String value) {
        return "#EQUAL";
      }

      @Override
      public String match(String val) {
        if ("#EQUAL".equals(val)) return "1";
        return null;
      }

    },


    PERCENT {
      @Override
      public String wrap(String value) {
        return value + "%";
      }

      @Override
      public String match(String val) {
        if (val != null && !val.isEmpty() && val.charAt(val.length() - 1) == '%') {
          String newVal = val.substring(0, val.length() - 1);
          double d;
          try {
            d = Double.parseDouble(newVal);
          } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid percentage value : " + val);
          }
          if (d < 0 || d > 100) {
            throw new IllegalArgumentException("Percentage value must lie between [1 -100] : provided value : " + val);
          }
          return newVal;
        } else {
          return null;
        }
      }

      @Override
      public String toString() {
        return "%";
      }
    };

    // return null if there is no match. return a modified string
    // if there is a match
    public String match(String val) {
      return null;
    }

    public String wrap(String value) {
      return value;
    }
  }

  public static class Condition implements MapWriter {
    final String name;
    final Object val;
    final Suggestion.ConditionType varType;
    final ComputationType computationType;
    final Operand op;
    private Clause clause;

    Condition(String name, Object val, Operand op, ComputationType computationType, Clause parent) {
      this.name = name;
      this.val = val;
      this.op = op;
      varType = Suggestion.getTagType(name);
      this.computationType = computationType;
      this.clause = parent;
    }

    @Override
    public void writeMap(EntryWriter ew) throws IOException {
      String value = op.wrap(val);
      if (computationType != null) value = computationType.wrap(value);
      ew.put(name, value);
    }

    @Override
    public String toString() {
      return jsonStr();
    }

    public Clause getClause() {
      return clause;
    }

    boolean isPass(Object inputVal) {
      if (computationType != null) {
        throw new IllegalStateException("This is supposed to be called only from a Condition with no computed value or a SealedCondition");

      }
      if (inputVal instanceof ReplicaCount) inputVal = ((ReplicaCount) inputVal).getVal(getClause().type);
      if (varType == Suggestion.ConditionType.LAZY) { // we don't know the type
        return op.match(parseString(val), parseString(inputVal)) == PASS;
      } else {
        return op.match(val, validate(name, inputVal, false)) == PASS;
      }
    }


    boolean isPass(Row row) {
      return isPass(row.getVal(name));
    }

    @Override
    public boolean equals(Object that) {
      if (that instanceof Condition) {
        Condition c = (Condition) that;
        return Objects.equals(c.name, name) && Objects.equals(c.val, val) && c.op == op;
      }
      return false;
    }

    public Double delta(Object val) {
      if (val instanceof ReplicaCount) val = ((ReplicaCount) val).getVal(getClause().type);
      if (this.val instanceof String) {
        if (op == LESS_THAN || op == GREATER_THAN) {
          return op
              .opposite(getClause().isReplicaZero() && this == getClause().tag)
              .delta(Clause.parseDouble(name, this.val), Clause.parseDouble(name, val));
        } else {
          return 0d;
        }
      } else return op
          .opposite(getClause().isReplicaZero() && this == getClause().getTag())
          .delta(this.val, val);
    }

    public String getName() {
      return name;
    }

    public Object getValue() {
      return val;
    }

    public Operand getOperand() {
      return op;
    }
  }

  public boolean isStrict() {
    return strict;
  }

  @Override
  public String toString() {
    return Utils.toJSONString(original);
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

    public ComputedValueEvaluator(Policy.Session session) {
      this.session = session;
    }

    @Override
    public Object apply(Condition computedValue) {
      return computedValue.varType.computeValue(session, computedValue, collName, shardName);
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
    Suggestion.ConditionType info = Suggestion.getTagType(name);
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

  static class RangeVal implements MapWriter {
    final Number min, max, actual;

    RangeVal(Number min, Number max, Number actual) {
      this.min = min;
      this.max = max;
      this.actual = actual;
    }

    public boolean match(Number testVal) {
      return Double.compare(testVal.doubleValue(), min.doubleValue()) >= 0 &&
          Double.compare(testVal.doubleValue(), max.doubleValue()) <= 0;
    }

    public Double delta(double v) {
      if (actual != null) return v - actual.doubleValue();
      if (v >= max.doubleValue()) return v - max.doubleValue();
      if (v <= min.doubleValue()) return v - min.doubleValue();
      return 0d;
    }

    @Override
    public String toString() {
      return jsonStr();
    }

    @Override
    public void writeMap(EntryWriter ew) throws IOException {
      ew.put("min", min).put("max", max).putIfNotNull("actual", actual);
    }
  }

}
