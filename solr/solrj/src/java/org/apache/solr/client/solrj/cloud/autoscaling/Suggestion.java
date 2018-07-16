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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.V2RequestSupport;
import org.apache.solr.client.solrj.cloud.autoscaling.Clause.ComputedType;
import org.apache.solr.client.solrj.cloud.autoscaling.Violation.ReplicaInfoAndErr;
import org.apache.solr.common.cloud.rule.ImplicitSnitch;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.StrUtils;

import static java.util.Collections.emptySet;
import static java.util.Collections.unmodifiableSet;
import static org.apache.solr.client.solrj.cloud.autoscaling.Policy.ANY;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.MOVEREPLICA;

public class Suggestion {
  public static final String coreidxsize = "INDEX.sizeInGB";

  static final Map<String, ConditionType> validatetypes = new HashMap<>();
  private static final String NULL = "";

  @Target(ElementType.FIELD)
  @Retention(RetentionPolicy.RUNTIME)
  @interface Meta {
    String name();

    Class type();

    String[] associatedPerNodeValue() default NULL;

    String associatedPerReplicaValue() default NULL;

    String[] enumVals() default NULL;

    String[] wildCards() default NULL;

    boolean isNodeSpecificVal() default false;

    boolean isHidden() default false;

    boolean isAdditive() default true;

    double min() default -1d;

    double max() default -1d;

    String metricsKey() default NULL;

    ComputedType[] computedValues() default ComputedType.NULL;
  }

  public static ConditionType getTagType(String name) {
    ConditionType info = validatetypes.get(name);
    if (info == null && name.startsWith(ImplicitSnitch.SYSPROP)) info = ConditionType.STRING;
    if (info == null && name.startsWith(Clause.METRICS_PREFIX)) info = ConditionType.LAZY;
    return info;
  }

  private static Object getOperandAdjustedValue(Object val, Object original) {
    if (original instanceof Clause.Condition) {
      Clause.Condition condition = (Clause.Condition) original;
      if (condition.computedType == null && isIntegerEquivalent(val)) {
        if (condition.op == Operand.LESS_THAN) {
          //replica : '<3'
          val = val instanceof Long ?
              (Long) val - 1 :
              (Double) val - 1;
        } else if (condition.op == Operand.GREATER_THAN) {
          //replica : '>4'
          val = val instanceof Long ?
              (Long) val + 1 :
              (Double) val + 1;
        }
      }
    }
    return val;
  }


  static class SuggestionCtx {
    public Policy.Session session;
    public Violation violation;
    private List<Suggester.SuggestionInfo> suggestions = new ArrayList<>();

    SolrRequest addSuggestion(Suggester suggester) {
      SolrRequest op = suggester.getSuggestion();
      if (op != null) {
        session = suggester.getSession();
        suggestions.add(new Suggester.SuggestionInfo(violation,
            ((V2RequestSupport) op.setUseV2(true)).getV2Request()));
      }
      return op;
    }


    public SuggestionCtx setViolation(Violation violation) {
      this.violation = violation;
      return this;
    }

    public List<Suggester.SuggestionInfo> getSuggestions() {
      return suggestions;
    }
  }

  static boolean isIntegerEquivalent(Object val) {
    if (val instanceof Number) {
      Number number = (Number) val;
      return Math.ceil(number.doubleValue()) == Math.floor(number.doubleValue());
    } else if (val instanceof String) {
      try {
        double dval = Double.parseDouble((String) val);
        return Math.ceil(dval) == Math.floor(dval);
      } catch (NumberFormatException e) {
        return false;
      }
    } else {
      return false;
    }

  }


  /**
   * Type details of each variable in policies
   */
  public enum ConditionType {

    @Meta(name = "collection",
        type = String.class)
    COLL(),
    @Meta(
        name = "shard",
        type = String.class,
        wildCards = {Policy.EACH, Policy.ANY})
    SHARD(),

    @Meta(name = "replica",
        type = Double.class,
        min = 0, max = -1,
        computedValues = {ComputedType.EQUAL, ComputedType.PERCENT})
    REPLICA() {
      @Override
      public Object validate(String name, Object val, boolean isRuleVal) {
        return getOperandAdjustedValue(super.validate(name, val, isRuleVal), val);
      }

      @Override
      public Operand getOperand(Operand expected, Object strVal, ComputedType computedType) {
        if (strVal instanceof String) {
          String s = ((String) strVal).trim();
          int hyphenIdx = s.indexOf('-');
          if (hyphenIdx > 0) {
            if (hyphenIdx == s.length() - 1) {
              throw new IllegalArgumentException("bad range input :" + expected);
            }
            if (expected == Operand.EQUAL) return Operand.RANGE_EQUAL;
            if (expected == Operand.NOT_EQUAL) return Operand.RANGE_NOT_EQUAL;
          }

        }

        if (expected == Operand.EQUAL && (computedType != null || !isIntegerEquivalent(strVal))) {
          return Operand.RANGE_EQUAL;
        }
        if (expected == Operand.NOT_EQUAL && (computedType != null || !isIntegerEquivalent(strVal)))
          return Operand.RANGE_NOT_EQUAL;

        return expected;
      }

      @Override
      public String postValidate(Clause.Condition condition) {
        if (condition.computedType == ComputedType.EQUAL) {
          if (condition.getClause().tag != null &&
              condition.getClause().tag.varType == NODE &&
              condition.getClause().tag.op == Operand.WILDCARD) {
            return null;
          } else {
            return "'replica': '#EQUAL` must be used with 'node':'#ANY'";
          }
        }
        return null;
      }

      @Override
      public Object computeValue(Policy.Session session, Clause.Condition cv, String collection, String shard, String node) {
        if (cv.computedType == ComputedType.EQUAL) {
          int relevantReplicasCount = getRelevantReplicasCount(session, cv, collection, shard);
          if (relevantReplicasCount == 0) return 0;
          return (double) session.matrix.size() / (double) relevantReplicasCount;
        } else if (cv.computedType == ComputedType.PERCENT) {
          return ComputedType.PERCENT.compute(getRelevantReplicasCount(session, cv, collection, shard), cv);
        } else {
          throw new IllegalArgumentException("Unsupported type " + cv.computedType);

        }
      }
    },
    @Meta(name = ImplicitSnitch.PORT,
        type = Long.class,
        min = 1,
        max = 65535)
    PORT(),
    @Meta(name = "ip_1",
        type = Long.class,
        min = 0,
        max = 255)
    IP_1(),
    @Meta(name = "ip_2",
        type = Long.class,
        min = 0,
        max = 255)
    IP_2(),
    @Meta(name = "ip_3",
        type = Long.class,
        min = 0,
        max = 255)
    IP_3(),
    @Meta(name = "ip_4",
        type = Long.class,
        min = 0,
        max = 255)
    IP_4(),

    @Meta(name = ImplicitSnitch.DISK,
        type = Double.class,
        min = 0,
        isNodeSpecificVal = true,
        associatedPerReplicaValue = coreidxsize,
        associatedPerNodeValue = "totaldisk",
        computedValues = ComputedType.PERCENT)
    FREEDISK() {
      @Override
      public Object convertVal(Object val) {
        Number value = (Number) super.validate(ImplicitSnitch.DISK, val, false);
        if (value != null) {
          value = value.doubleValue() / 1024.0d / 1024.0d / 1024.0d;
        }
        return value;
      }

      @Override
      public Object computeValue(Policy.Session session, Clause.Condition condition, String collection, String shard, String node) {
        if (condition.computedType == ComputedType.PERCENT) {
          Row r = session.getNode(node);
          if (r == null) return 0d;
          return ComputedType.PERCENT.compute(r.getVal(TOTALDISK.tagName), condition);
        }
        throw new IllegalArgumentException("Unsupported type " + condition.computedType);
      }



      @Override
      public int compareViolation(Violation v1, Violation v2) {
        //TODO use tolerance compare
        return Double.compare(
            v1.getViolatingReplicas().stream().mapToDouble(v -> v.delta == null ? 0 : v.delta).max().orElse(0d),
            v2.getViolatingReplicas().stream().mapToDouble(v3 -> v3.delta == null ? 0 : v3.delta).max().orElse(0d));
      }

      @Override
      public void getSuggestions(SuggestionCtx ctx) {
        if (ctx.violation == null) return;
        if (ctx.violation.replicaCountDelta < 0 && !ctx.violation.getViolatingReplicas().isEmpty()) {

          Comparator<Row> rowComparator = Comparator.comparing(r -> ((Double) r.getVal(ImplicitSnitch.DISK, 0d)));
          List<Row> matchingNodes = ctx.session.matrix.stream().filter(
              row -> ctx.violation.getViolatingReplicas()
                  .stream()
                  .anyMatch(p -> row.node.equals(p.replicaInfo.getNode())))
              .sorted(rowComparator)
              .collect(Collectors.toList());


          for (Row node : matchingNodes) {
            //lets try to start moving the smallest cores off of the node
            ArrayList<ReplicaInfo> replicas = new ArrayList<>();
            node.forEachReplica(replicas::add);
            replicas.sort((r1, r2) -> {
              Long s1 = Clause.parseLong(ConditionType.CORE_IDX.tagName, r1.getVariables().get(ConditionType.CORE_IDX.tagName));
              Long s2 = Clause.parseLong(ConditionType.CORE_IDX.tagName, r2.getVariables().get(ConditionType.CORE_IDX.tagName));
              if (s1 != null && s2 != null) return s1.compareTo(s2);
              return 0;
            });
            double currentDelta = ctx.violation.getClause().tag.delta(node.getVal(ImplicitSnitch.DISK));
            for (ReplicaInfo replica : replicas) {
              if (currentDelta < 1) break;
              if (replica.getVariables().get(ConditionType.CORE_IDX.tagName) == null) continue;
              Suggester suggester = ctx.session.getSuggester(MOVEREPLICA)
                  .hint(Suggester.Hint.COLL_SHARD, new Pair<>(replica.getCollection(), replica.getShard()))
                  .hint(Suggester.Hint.SRC_NODE, node.node)
                  .forceOperation(true);
              if (ctx.addSuggestion(suggester) == null) break;
              currentDelta -= Clause.parseLong(ConditionType.CORE_IDX.tagName, replica.getVariable(ConditionType.CORE_IDX.tagName));
            }
          }
        }
      }

      //When a replica is added, freedisk should be incremented
      @Override
      public void projectAddReplica(Cell cell, ReplicaInfo ri) {
        //go through other replicas of this shard and copy the index size value into this
        for (Row row : cell.getRow().session.matrix) {
          row.forEachReplica(replicaInfo -> {
            if (ri != replicaInfo &&
                ri.getCollection().equals(replicaInfo.getCollection()) &&
                ri.getShard().equals(replicaInfo.getShard()) &&
                ri.getVariable(CORE_IDX.tagName) == null &&
                replicaInfo.getVariable(CORE_IDX.tagName) != null) {
              ri.getVariables().put(CORE_IDX.tagName, validate(CORE_IDX.tagName, replicaInfo.getVariable(CORE_IDX.tagName), false));
            }
          });
        }
        Double idxSize = (Double) validate(CORE_IDX.tagName, ri.getVariable(CORE_IDX.tagName), false);
        if (idxSize == null) return;
        Double currFreeDisk = cell.val == null ? 0.0d : (Double) cell.val;
        cell.val = currFreeDisk - idxSize;
      }

      @Override
      public void projectRemoveReplica(Cell cell, ReplicaInfo ri) {
        Double idxSize = (Double) validate(CORE_IDX.tagName, ri.getVariable(CORE_IDX.tagName), false);
        if (idxSize == null) return;
        Double currFreeDisk = cell.val == null ? 0.0d : (Double) cell.val;
        cell.val = currFreeDisk + idxSize;
      }
    },

    @Meta(name = "totaldisk",
        type = Double.class,
        isHidden = true)
    TOTALDISK() {
      @Override
      public Object convertVal(Object val) {
        return FREEDISK.convertVal(val);
      }
    },

    @Meta(name = coreidxsize,
        type = Double.class,
        isNodeSpecificVal = true,
        isHidden = true,
        min = 0,
        metricsKey = "INDEX.sizeInBytes")
    CORE_IDX() {
      @Override
      public Object convertVal(Object val) {
        return FREEDISK.convertVal(val);
      }
    },
    @Meta(name = ImplicitSnitch.NODEROLE,
        type = String.class,
        enumVals = "overseer")
    NODE_ROLE(),

    @Meta(name = ImplicitSnitch.CORES,
        type = Long.class,
        min = 0)
    CORES() {
      @Override
      public Object validate(String name, Object val, boolean isRuleVal) {
        return getOperandAdjustedValue(super.validate(name, val, isRuleVal), val);
      }

      @Override
      public void addViolatingReplicas(ViolationCtx ctx) {
        for (Row r : ctx.allRows) {
          if (!ctx.clause.tag.isPass(r)) {
            r.forEachReplica(replicaInfo -> ctx.currentViolation
                .addReplica(new ReplicaInfoAndErr(replicaInfo)
                    .withDelta(ctx.clause.tag.delta(r.getVal(ImplicitSnitch.CORES)))));
          }
        }

      }

      @Override
      public void getSuggestions(SuggestionCtx ctx) {
        if (ctx.violation == null || ctx.violation.replicaCountDelta == 0) return;
        if (ctx.violation.replicaCountDelta > 0) {//there are more replicas than necessary
          for (int i = 0; i < Math.abs(ctx.violation.replicaCountDelta); i++) {
            Suggester suggester = ctx.session.getSuggester(MOVEREPLICA)
                .hint(Suggester.Hint.SRC_NODE, ctx.violation.node);
            ctx.addSuggestion(suggester);
          }
        }
      }

      @Override
      public void projectAddReplica(Cell cell, ReplicaInfo ri) {
        cell.val = cell.val == null ? 0 : ((Number) cell.val).longValue() + 1;
      }

      @Override
      public void projectRemoveReplica(Cell cell, ReplicaInfo ri) {
        cell.val = cell.val == null ? 0 : ((Number) cell.val).longValue() - 1;
      }
    },

    @Meta(name = ImplicitSnitch.SYSLOADAVG,
        type = Double.class,
        min = 0,
        max = 100,
        isNodeSpecificVal = true)
    SYSLOADAVG(),

    @Meta(name = ImplicitSnitch.HEAPUSAGE,
        type = Double.class,
        min = 0,
        isNodeSpecificVal = true)
    HEAPUSAGE(),
    @Meta(name = "NUMBER",
        type = Long.class,
        min = 0)
    NUMBER(),
    @Meta(name = "STRING",
        type = String.class,
        wildCards = Policy.EACH)
    STRING(),

    @Meta(name = "node",
        type = String.class,
        isNodeSpecificVal = true,
        wildCards = {Policy.ANY, Policy.EACH})
    NODE() {
      @Override
      public void getSuggestions(SuggestionCtx ctx) {
        if (ctx.violation == null || ctx.violation.replicaCountDelta == 0) return;
        if (ctx.violation.replicaCountDelta > 0) {//there are more replicas than necessary
          for (int i = 0; i < Math.abs(ctx.violation.replicaCountDelta); i++) {
            Suggester suggester = ctx.session.getSuggester(MOVEREPLICA)
                .hint(Suggester.Hint.SRC_NODE, ctx.violation.node)
                .hint(ctx.violation.shard.equals(ANY) ? Suggester.Hint.COLL : Suggester.Hint.COLL_SHARD,
                    ctx.violation.shard.equals(ANY) ? ctx.violation.coll : new Pair<>(ctx.violation.coll, ctx.violation.shard));
            ctx.addSuggestion(suggester);
          }
        }

      }

      /*@Override
      public void addViolatingReplicas(ViolationCtx ctx) {
        for (Row r : ctx.allRows) {
          if(r.node.equals(ctx.tagKey)) collectViolatingReplicas(ctx,r);
        }
      }*/
    },

    @Meta(name = "LAZY",
        type = void.class)
    LAZY() {
      @Override
      public Object validate(String name, Object val, boolean isRuleVal) {
        return Clause.parseString(val);
      }

      @Override
      public void getSuggestions(SuggestionCtx ctx) {
        perNodeSuggestions(ctx);
      }
    },

    @Meta(name = ImplicitSnitch.DISKTYPE,
        type = String.class,
        enumVals = {"ssd", "rotational"})
    DISKTYPE() {
      @Override
      public void getSuggestions(SuggestionCtx ctx) {
        perNodeSuggestions(ctx);
      }
    };

    public final String tagName;
    public final Class type;
    public Meta meta;

    public final Set<String> vals;
    public final Number min;
    public final Number max;
    public final Boolean additive;
    public final boolean isHidden;
    public final Set<String> wildCards;
    public final String perReplicaValue;
    public final Set<String> associatedPerNodeValues;
    public final String metricsAttribute;
    public final boolean isPerNodeValue;
    public final Set<ComputedType> supportedComputedTypes;


    ConditionType() {
      try {
        meta = ConditionType.class.getField(name()).getAnnotation(Meta.class);
        if (meta == null) {
          throw new RuntimeException("Invalid type, should have a @Meta annotation " + name());
        }
      } catch (NoSuchFieldException e) {
        //cannot happen
      }
      this.tagName = meta.name();
      this.type = meta.type();

      this.vals = readSet(meta.enumVals());
      this.max = readNum(meta.max());
      this.min = readNum(meta.min());
      this.perReplicaValue = readStr(meta.associatedPerReplicaValue());
      this.associatedPerNodeValues = readSet(meta.associatedPerNodeValue());
      this.additive = meta.isAdditive();
      this.metricsAttribute = readStr(meta.metricsKey());
      this.isPerNodeValue = meta.isNodeSpecificVal();
      this.supportedComputedTypes = meta.computedValues()[0] == ComputedType.NULL ?
          emptySet() :
          unmodifiableSet(new HashSet(Arrays.asList(meta.computedValues())));
      this.isHidden = meta.isHidden();
      this.wildCards = readSet(meta.wildCards());
    }

    private String readStr(String s) {
      return NULL.equals(s) ? null : s;
    }

    private Number readNum(double v) {
      return v == -1 ? null :
          (Number) validate(null, v, true);
    }

    Set<String> readSet(String[] vals) {
      if (NULL.equals(vals[0])) return emptySet();
      return unmodifiableSet(new HashSet<>(Arrays.asList(vals)));
    }

    public void getSuggestions(SuggestionCtx ctx) {
      perNodeSuggestions(ctx);
    }

    public void addViolatingReplicas(ViolationCtx ctx) {
      for (Row row : ctx.allRows) {
        if (ctx.clause.tag.varType.isPerNodeValue && !row.node.equals(ctx.tagKey)) continue;
        collectViolatingReplicas(ctx, row);
      }
    }

    public Operand getOperand(Operand expected, Object val, ComputedType computedType) {
      return expected;
    }


    public Object convertVal(Object val) {
      return val;
    }

    public String postValidate(Clause.Condition condition) {
      return null;
    }

    public Object validate(String name, Object val, boolean isRuleVal) {
      if (val instanceof Clause.Condition) {
        Clause.Condition condition = (Clause.Condition) val;
        val = condition.op.readRuleValue(condition);
        if (val != condition.val) return val;
      }
      if (name == null) name = this.tagName;
      if (type == Double.class) {
        Double num = Clause.parseDouble(name, val);
        if (isRuleVal) {
          if (min != null)
            if (Double.compare(num, min.doubleValue()) == -1)
              throw new RuntimeException(name + ": " + val + " must be greater than " + min);
          if (max != null)
            if (Double.compare(num, max.doubleValue()) == 1)
              throw new RuntimeException(name + ": " + val + " must be less than " + max);
        }
        return num;
      } else if (type == Long.class) {
        Long num = Clause.parseLong(name, val);
        if (isRuleVal) {
          if (min != null)
            if (num < min.longValue())
              throw new RuntimeException(name + ": " + val + " must be greater than " + min);
          if (max != null)
            if (num > max.longValue())
              throw new RuntimeException(name + ": " + val + " must be less than " + max);
        }
        return num;
      } else if (type == String.class) {
        if (isRuleVal && !vals.isEmpty() && !vals.contains(val))
          throw new RuntimeException(name + ": " + val + " must be one of " + StrUtils.join(vals, ','));
        return val;
      } else {
        throw new RuntimeException("Invalid type ");
      }

    }

    /**
     * Simulate a replica addition to a node in the cluster
     */
    public void projectAddReplica(Cell cell, ReplicaInfo ri) {
    }

    public void projectRemoveReplica(Cell cell, ReplicaInfo ri) {
    }

    public int compareViolation(Violation v1, Violation v2) {
      if (v2.replicaCountDelta == null || v1.replicaCountDelta == null) return 0;
      if (Math.abs(v1.replicaCountDelta) == Math.abs(v2.replicaCountDelta)) return 0;
      return Math.abs(v1.replicaCountDelta) < Math.abs(v2.replicaCountDelta) ? -1 : 1;
    }

    public Object computeValue(Policy.Session session, Clause.Condition condition, String collection, String shard, String node) {
      return condition.val;
    }
  }

  private static void collectViolatingReplicas(ViolationCtx ctx, Row row) {
    if (ctx.clause.tag.varType.isPerNodeValue) {
      row.forEachReplica(replica -> {
        if (ctx.clause.collection.isPass(replica.getCollection()) && ctx.clause.getShard().isPass(replica.getShard())) {
          ctx.currentViolation.addReplica(new ReplicaInfoAndErr(replica)
              .withDelta(ctx.clause.tag.delta(row.getVal(ctx.clause.tag.name))));
        }
      });
    } else {
      row.forEachReplica(replica -> {
        if (ctx.clause.replica.isPass(0) && !ctx.clause.tag.isPass(row)) return;
        if (!ctx.clause.replica.isPass(0) && ctx.clause.tag.isPass(row)) return;
        if (!ctx.currentViolation.matchShard(replica.getShard())) return;
        if (!ctx.clause.collection.isPass(ctx.currentViolation.coll) || !ctx.clause.shard.isPass(ctx.currentViolation.shard))
          return;
        ctx.currentViolation.addReplica(new ReplicaInfoAndErr(replica).withDelta(ctx.clause.tag.delta(row.getVal(ctx.clause.tag.name))));
      });

    }


  }

  private static int getRelevantReplicasCount(Policy.Session session, Clause.Condition cv, String collection, String shard) {
    AtomicInteger totalReplicasOfInterest = new AtomicInteger(0);
    Clause clause = cv.getClause();
    for (Row row : session.matrix) {
      row.forEachReplica(replicaInfo -> {
        if (replicaInfo.getCollection().equals(collection)) {
          if (clause.getShard() ==null || clause.getShard().op == Operand.WILDCARD || replicaInfo.getShard().equals(shard)) {
            if (cv.getClause().type == null || replicaInfo.getType() == cv.getClause().type)
              totalReplicasOfInterest.incrementAndGet();
          }
        }
      });
    }
    return totalReplicasOfInterest.get();
  }

  static class ViolationCtx {
    final Function<Clause.Condition, Object> evaluator;
    String tagKey;
    Clause clause;
    ReplicaCount count;
    Violation currentViolation;
    List<Row> allRows;
    List<Violation> allViolations = new ArrayList<>();

    public ViolationCtx(Clause clause, List<Row> allRows, Function<Clause.Condition, Object> evaluator) {
      this.allRows = allRows;
      this.clause = clause;
      this.evaluator = evaluator;
    }

    public ViolationCtx reset(String tagKey, ReplicaCount count, Violation currentViolation) {
      this.tagKey = tagKey;
      this.count = count;
      this.currentViolation = currentViolation;
      allViolations.add(currentViolation);
      this.clause = currentViolation.getClause();
      return this;
    }
  }

  private static void perNodeSuggestions(SuggestionCtx ctx) {
    if (ctx.violation == null) return;
    for (ReplicaInfoAndErr e : ctx.violation.getViolatingReplicas()) {
      Suggester suggester = ctx.session.getSuggester(MOVEREPLICA)
          .forceOperation(true)
          .hint(Suggester.Hint.COLL_SHARD, new Pair<>(e.replicaInfo.getCollection(), e.replicaInfo.getShard()))
          .hint(Suggester.Hint.SRC_NODE, e.replicaInfo.getNode());
      if (ctx.addSuggestion(suggester) == null) break;
    }
  }

  /*public static final Map<String, String> tagVsPerReplicaVal = Stream.of(ConditionType.values())
      .filter(tag -> tag.perReplicaValue != null)
      .collect(Collectors.toMap(tag -> tag.tagName, tag -> tag.perReplicaValue));*/
  static {
    for (Suggestion.ConditionType t : Suggestion.ConditionType.values()) Suggestion.validatetypes.put(t.tagName, t);
  }


}
