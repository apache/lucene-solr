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
import static org.apache.solr.client.solrj.cloud.autoscaling.Operand.WILDCARD;
import static org.apache.solr.client.solrj.cloud.autoscaling.Policy.ANY;
import static org.apache.solr.common.params.CoreAdminParams.COLLECTION;
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

  private Condition evaluateValue(Condition condition, Function<Condition, Object> computedValueEvaluator) {
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
    ComputedType computedType = null;
    Object val = m.get(s);
    Type varType = VariableBase.getTagType(s);
    if (varType.meta.isHidden()) {
      throw new IllegalArgumentException(formatString("''{0}'' is not allowed in a policy rule :  ''{1}''  ", varType.tagName, toJSONString(m)));
    }
    try {
      String conditionName = s.trim();
      Operand operand = null;
      if (val == null) {
        operand = WILDCARD;
        expectedVal = Policy.ANY;
      } else if (val instanceof List) {
        if (!varType.meta.supportArrayVals()) {
          throw new IllegalArgumentException(formatString("array values are not supported for {0} in clause {1}",
              conditionName, toJSONString(m)));
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
              throw new IllegalArgumentException(formatString("''{0}'' is not allowed for variable :  ''{1}'' , in clause : ''{2}'' ",
                  t, conditionName, toJSONString(m)));
            }
          }
        }
        if (computedType == null && ((String) val).charAt(0) == '#' && !varType.wildCards.contains(val)) {
          throw new IllegalArgumentException(formatString("''{0}'' is not an allowed value for ''{1}'' , in clause : ''{2}'' . Supported value is : {3}",
              val, conditionName, toJSONString(m), varType.wildCards));

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
      throw new IllegalArgumentException("Invalid tag : " + s + ":" + val, e);
    }
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

  public List<Violation> test(Policy.Session session) {
    ComputedValueEvaluator computedValueEvaluator = new ComputedValueEvaluator(session);
    Violation.Ctx ctx = new Violation.Ctx(this, session.matrix, computedValueEvaluator);
    if (isPerCollectiontag()) {
      Map<String, Map<String, Map<String, ReplicaCount>>> replicaCounts = computeReplicaCounts(session.matrix, computedValueEvaluator);
      for (Map.Entry<String, Map<String, Map<String, ReplicaCount>>> e : replicaCounts.entrySet()) {
        computedValueEvaluator.collName = e.getKey();
        if (!collection.isPass(computedValueEvaluator.collName)) continue;
        for (Map.Entry<String, Map<String, ReplicaCount>> shardVsCount : e.getValue().entrySet()) {
          computedValueEvaluator.shardName = shardVsCount.getKey();
          if (!shard.isPass(computedValueEvaluator.shardName)) continue;
          for (Map.Entry<String, ReplicaCount> counts : shardVsCount.getValue().entrySet()) {
            if (tag.varType.meta.isNodeSpecificVal()) computedValueEvaluator.node = counts.getKey();
            SealedClause sealedClause = getSealedClause(computedValueEvaluator);
            ReplicaCount replicas = counts.getValue();
            if (!sealedClause.replica.isPass(replicas)) {
              Violation violation = new Violation(sealedClause,
                  computedValueEvaluator.collName,
                  computedValueEvaluator.shardName,
                  tag.varType.meta.isNodeSpecificVal() ? computedValueEvaluator.node : null,
                  counts.getValue(),
                  sealedClause.getReplica().delta(replicas),
                  tag.varType.meta.isNodeSpecificVal() ? null : counts.getKey());
              tag.varType.addViolatingReplicas(ctx.reset(counts.getKey(), replicas, violation));
            }
          }
        }
      }
    } else {
      for (Row r : session.matrix) {
        computedValueEvaluator.node = r.node;
        SealedClause sealedClause = getSealedClause(computedValueEvaluator);
        if (!sealedClause.getGlobalTag().isPass(r)) {
          sealedClause.getGlobalTag().varType.addViolatingReplicas(ctx.reset(null, null,
              new Violation(sealedClause, null, null, r.node, r.getVal(sealedClause.globalTag.name),
                  sealedClause.globalTag.delta(r.getVal(globalTag.name)), null)));
        }
      }
    }
    return ctx.allViolations;

  }

  private Map<String, Map<String, Map<String, ReplicaCount>>> computeReplicaCounts(List<Row> allRows,
                                                                                   ComputedValueEvaluator computedValueEvaluator) {
    Map<String, Map<String, Map<String, ReplicaCount>>> collVsShardVsTagVsCount = new HashMap<>();
    for (Row row : allRows) {
      computedValueEvaluator.node = row.node;
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
          Condition t = sealedClause.getTag();
          if (t.varType.meta.isNodeSpecificVal()) {
            boolean pass = t.getOperand().match(t.val, tagVal) == TestStatus.PASS;
            tagVsCount.computeIfAbsent(row.node, s -> new ReplicaCount());
            if(pass) {
              tagVsCount.get(row.node).increment(shards.getValue());
            }
          } else {
            boolean pass = sealedClause.getTag().isPass(tagVal);
            tagVsCount.computeIfAbsent(pass ? String.valueOf(tagVal) : "", s -> new ReplicaCount());
            if (pass) {
              tagVsCount.get(String.valueOf(tagVal)).increment(shards.getValue());
            }
          }
        }
      }
    }

    if (this.getTag().op != LESS_THAN && this.getTag().varType == Type.NODE) {
      collVsShardVsTagVsCount.forEach((coll, shardVsNodeVsCount) ->
          shardVsNodeVsCount.forEach((shard, nodeVsCount) -> {
            for (Row row : allRows) {
              if (!nodeVsCount.containsKey(row.node)) {
                nodeVsCount.put(row.node, new ReplicaCount());
              }
            }
          }));
    }
    return collVsShardVsTagVsCount;
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

  public enum ComputedType {
    NULL(),
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
    ALL() {
      @Override
      public String wrap(String value) {
        return "#ALL";
      }

      @Override
      public String match(String val) {
        if ("#ALL".equals(val)) return "1";
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
      public Object compute(Object val, Condition c) {
        if (val == null || Clause.parseDouble(c.name, val) == 0) return 0d;
        return Clause.parseDouble(c.name, val) * Clause.parseDouble(c.name, c.val).doubleValue() / 100;
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

    public Object compute(Object val, Condition c) {
      return val;
    }

  }

  public static class Condition implements MapWriter {
    final String name;
    final Object val;
    final Type varType;
    final ComputedType computedType;
    final Operand op;
    private Clause clause;

    Condition(String name, Object val, Operand op, ComputedType computedType, Clause parent) {
      this.name = name;
      this.val = val;
      this.op = op;
      varType = VariableBase.getTagType(name);
      this.computedType = computedType;
      this.clause = parent;
    }

    @Override
    public void writeMap(EntryWriter ew) throws IOException {
      String value = op.wrap(val);
      if (computedType != null) value = computedType.wrap(value);
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
      return isPass(inputVal, null);
    }

    boolean isPass(Object inputVal, Row row) {
      if (computedType != null) {
        throw new IllegalStateException("This is supposed to be called only from a Condition with no computed value or a SealedCondition");

      }
      if (inputVal instanceof ReplicaCount) inputVal = ((ReplicaCount) inputVal).getVal(getClause().type);
      return varType.match(inputVal, op, val, name, row);
    }


    boolean isPass(Row row) {
      return isPass(row.getVal(name), row);
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
      } else {
        if (this == getClause().getReplica()) {
          Double delta = op.delta(this.val, val);
          return getClause().isReplicaZero() ? -1 * delta : delta;
        } else {
          return op
              .opposite(getClause().isReplicaZero() && this == getClause().getTag())
              .delta(this.val, val);
        }

      }
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
