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

import org.apache.solr.common.MapWriter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;

import static java.util.Collections.singletonMap;
import static org.apache.solr.client.solrj.cloud.autoscaling.Clause.TestStatus.PASS;
import static org.apache.solr.client.solrj.cloud.autoscaling.Operand.EQUAL;
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

  final Map<String, Object> original;
  Condition collection, shard, replica, tag, globalTag;
  final Replica.Type type;

  boolean strict;

  public Clause(Map<String, Object> m) {
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

  @Override
  public int compareTo(Clause that) {
    int v = Integer.compare(this.tag.op.priority, that.tag.op.priority);
    if (v != 0) return v;
    if (this.isPerCollectiontag() && that.isPerCollectiontag()) {
      v = Integer.compare(this.replica.op.priority, that.replica.op.priority);
      if (v == 0) {// higher the number of replicas , harder to satisfy
        v = Long.compare((Long) this.replica.val, (Long) that.replica.val);
        v = this.replica.op == LESS_THAN ? v : v * -1;
      }
      if (v == 0) v = compareTypes(this.type, that.type);
      return v;
    } else {
      return 0;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Clause that = (Clause)o;
    return compareTo(that) == 0;
  }

  void addTags(Collection<String> params) {
    if (globalTag != null && !params.contains(globalTag.name)) params.add(globalTag.name);
    if (tag != null && !params.contains(tag.name)) params.add(tag.name);
  }

  boolean isReplicaZero() {
    return replica != null && replica.getOperand() == EQUAL && 0L == (Long) replica.val;
  }

  class Condition {
    final String name;
    final Object val;
    final Suggestion.ConditionType varType;
    final Operand op;

    Condition(String name, Object val, Operand op) {
      this.name = name;
      this.val = val;
      this.op = op;
      varType = Suggestion.getTagType(name);
    }


    boolean isPass(Object inputVal) {
      if (inputVal instanceof ReplicaCount) inputVal = ((ReplicaCount) inputVal).getVal(type);
      if (varType == Suggestion.ConditionType.LAZY) { // we don't know the type
        return op.match(parseString(val), parseString(inputVal)) == PASS;
      } else {
        return op.match(val, validate(name, inputVal, false)) == PASS;
      }
    }


    boolean isPass(Row row) {
      return op.match(val, row.getVal(name)) == PASS;
    }

    @Override
    public boolean equals(Object that) {
      if (that instanceof Condition) {
        Condition c = (Condition) that;
        return Objects.equals(c.name, name) && Objects.equals(c.val, val) && c.op == op;
      }
      return false;
    }

    public Long delta(Object val) {
      if (val instanceof ReplicaCount) val = ((ReplicaCount) val).getVal(type);
      if (this.val instanceof String) {
        if (op == LESS_THAN || op == GREATER_THAN) {
          return op
              .opposite(isReplicaZero() && this == tag)
              .delta(Clause.parseDouble(name, this.val), Clause.parseDouble(name, val));
        } else {
          return 0L;
        }
      } else return op
          .opposite(isReplicaZero() && this == tag)
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

  Condition parse(String s, Map m) {
    Object expectedVal = null;
    Object val = m.get(s);
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
        else operand = EQUAL;
        expectedVal = validate(s, strVal.substring(EQUAL == operand || WILDCARD == operand ? 0 : 1), true);
      } else if (val instanceof Number) {
        operand = EQUAL;
        expectedVal = validate(s, val, true);
      }
      return new Condition(conditionName, expectedVal, operand);

    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid tag : " + s + ":" + val, e);
    }
  }


  public List<Violation> test(List<Row> allRows) {
    Suggestion.ViolationCtx ctx = new Suggestion.ViolationCtx(this, allRows);
    if (isPerCollectiontag()) {
      Map<String, Map<String, Map<String, ReplicaCount>>> replicaCount = computeReplicaCounts(allRows);
      for (Map.Entry<String, Map<String, Map<String, ReplicaCount>>> e : replicaCount.entrySet()) {
        if (!collection.isPass(e.getKey())) continue;
        for (Map.Entry<String, Map<String, ReplicaCount>> shardVsCount : e.getValue().entrySet()) {
          if (!shard.isPass(shardVsCount.getKey())) continue;
          for (Map.Entry<String, ReplicaCount> counts : shardVsCount.getValue().entrySet()) {
            if (!replica.isPass(counts.getValue())) {
              Violation violation = new Violation(this,
                  e.getKey(),
                  shardVsCount.getKey(),
                  tag.name.equals("node") ? counts.getKey() : null,
                  counts.getValue(),
                  replica.delta(counts.getValue()),
                  counts.getKey());
              Suggestion.getTagType(tag.name).addViolatingReplicas(ctx.reset(counts.getKey(), counts.getValue(), violation));
            }
          }
        }
      }
    } else {
      for (Row r : allRows) {
        if (!globalTag.isPass(r)) {
          Suggestion.ConditionType.CORES.addViolatingReplicas(ctx.reset(null, null,
              new Violation(this, null, null, r.node, r.getVal(globalTag.name), globalTag.delta(r.getVal(globalTag.name)), null)));
        }
      }
    }
    return ctx.allViolations;

  }


  private Map<String, Map<String, Map<String, ReplicaCount>>> computeReplicaCounts(List<Row> allRows) {
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
          boolean pass = tag.isPass(tagVal);
          tagVsCount.computeIfAbsent(pass ? String.valueOf(tagVal) : "", s -> new ReplicaCount());
          if (pass) {
            tagVsCount.get(String.valueOf(tagVal)).increment(shards.getValue());
          }
        }
      }
    }
    return collVsShardVsTagVsCount;
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

  public static String parseString(Object val) {
    return val == null ? null : String.valueOf(val);
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

}
