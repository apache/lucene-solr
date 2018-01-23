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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.V2RequestSupport;
import org.apache.solr.client.solrj.cloud.autoscaling.Violation.ReplicaInfoAndErr;
import org.apache.solr.common.cloud.rule.ImplicitSnitch;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.StrUtils;

import static java.util.Collections.unmodifiableSet;
import static org.apache.solr.client.solrj.cloud.autoscaling.Policy.ANY;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.MOVEREPLICA;

public class Suggestion {
  public static final String coreidxsize = "INDEX.sizeInBytes";
  static final Map<String, ConditionType> validatetypes = new HashMap<>();

  public static ConditionType getTagType(String name) {
    ConditionType info = validatetypes.get(name);
    if (info == null && name.startsWith(ImplicitSnitch.SYSPROP)) info = ConditionType.LAZY;
    if (info == null && name.startsWith(Clause.METRICS_PREFIX)) info = ConditionType.LAZY;
    return info;
  }

  static class ViolationCtx {
    String tagKey;
    Clause clause;
    ReplicaCount count;

    Violation currentViolation;
    List<Row> allRows;

    List<Violation> allViolations = new ArrayList<>();

    public ViolationCtx(Clause clause, List<Row> allRows) {
      this.allRows = allRows;
      this.clause = clause;
    }

    public ViolationCtx reset(String tagKey, ReplicaCount count, Violation currentViolation) {
      this.tagKey = tagKey;
      this.count = count;
      this.currentViolation = currentViolation;
      allViolations.add(currentViolation);
      return this;
    }
  }


  static class SuggestionCtx {
    public Policy.Session session;
    public Violation violation;
    private List<Suggester.SuggestionInfo> suggestions = new ArrayList<>();

    SolrRequest addSuggestion(Suggester suggester) {
      SolrRequest op = suggester.getSuggestion();
      if (op != null) {
        session = suggester.getSession();
        suggestions.add(new Suggester.SuggestionInfo( violation,
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


  public static final Map<String, String> tagVsPerReplicaVal = Stream.of(ConditionType.values())
      .filter(tag -> tag.perReplicaValue != null)
      .collect(Collectors.toMap(tag -> tag.tagName, tag -> tag.perReplicaValue));

  public enum ConditionType {

    COLL("collection", String.class, null, null, null),
    SHARD("shard", String.class, null, null, null),
    REPLICA("replica", Long.class, null, 0L, null),
    PORT(ImplicitSnitch.PORT, Long.class, null, 1L, 65535L) ,
    IP_1("ip_1", Long.class, null, 0L, 255L),
    IP_2("ip_2", Long.class, null, 0L, 255L),
    IP_3("ip_3", Long.class, null, 0L, 255L),
    IP_4("ip_4", Long.class, null, 0L, 255L),
    FREEDISK(ImplicitSnitch.DISK, Double.class, null, 0d, Double.MAX_VALUE, coreidxsize) {
      @Override
      public Object convertVal(Object val) {
        Number value = (Number) super.validate(ImplicitSnitch.DISK, val, false);
        if (value != null) {
          value = value.doubleValue() / 1024.0d / 1024.0d / 1024.0d;
        }
        return value;
      }

      @Override
      public void getSuggestions(SuggestionCtx ctx) {
        if (ctx.violation == null) return;
        if (ctx.violation.replicaCountDelta < 0 && !ctx.violation.getViolatingReplicas().isEmpty()) {

          Comparator<Row> rowComparator = Comparator.comparing(r -> ((Long) r.getVal(ImplicitSnitch.DISK, 0l)));
          List<Row> matchingNodes = ctx.session.matrix.stream().filter(
              row -> ctx.violation.getViolatingReplicas()
                  .stream()
                  .anyMatch(p -> row.node.equals(p.replicaInfo.getNode())))
              .sorted(rowComparator)
              .collect(Collectors.toList());


          for (Row r : matchingNodes) {
            //lets try to start moving the smallest cores off of the node
            ArrayList<ReplicaInfo> replicas = new ArrayList<>();
            r.forEachReplica(replicas::add);
            replicas.sort((r1, r2) -> {
              Long s1 = Clause.parseLong(ConditionType.CORE_IDX.tagName, r1.getVariables().get(ConditionType.CORE_IDX.tagName));
              Long s2 = Clause.parseLong(ConditionType.CORE_IDX.tagName, r2.getVariables().get(ConditionType.CORE_IDX.tagName));
              if (s1 != null && s2 != null) return s1.compareTo(s2);
              return 0;
            });
            long currentDelta = ctx.violation.getClause().tag.delta(r.getVal(ImplicitSnitch.DISK));
            for (ReplicaInfo replica : replicas) {
              if (currentDelta <= 0) break;
              if (replica.getVariables().get(ConditionType.CORE_IDX.tagName) == null) continue;
              Suggester suggester = ctx.session.getSuggester(MOVEREPLICA)
                  .hint(Suggester.Hint.COLL_SHARD, new Pair<>(replica.getCollection(), replica.getShard()))
                  .hint(Suggester.Hint.SRC_NODE, r.node);
              if (ctx.addSuggestion(suggester) == null) break;
              currentDelta -= Clause.parseLong(ConditionType.CORE_IDX.tagName, replica.getVariables().get(ConditionType.CORE_IDX.tagName));
            }
          }
        }
      }
    },
    CORE_IDX(coreidxsize, Double.class, null, 0d, Double.MAX_VALUE) {
      @Override
      public Object convertVal(Object val) {
        return FREEDISK.convertVal(val);
      }
    },
    NODE_ROLE(ImplicitSnitch.NODEROLE, String.class, Collections.singleton("overseer"), null, null),
    CORES(ImplicitSnitch.CORES, Long.class, null, 0L, Long.MAX_VALUE) {
      @Override
      public void addViolatingReplicas(ViolationCtx ctx) {
        for (Row r : ctx.allRows) {
          if (!ctx.clause.tag.isPass(r)) {
            r.forEachReplica(replicaInfo -> ctx.currentViolation
                .addReplica(new ReplicaInfoAndErr(replicaInfo).withDelta(ctx.clause.tag.delta(r.getVal(ImplicitSnitch.CORES)))));
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
    },
    SYSLOADAVG(ImplicitSnitch.SYSLOADAVG, Double.class, null, 0d, 100d),
    HEAPUSAGE(ImplicitSnitch.HEAPUSAGE, Double.class, null, 0d, null),
    NUMBER("NUMBER", Long.class, null, 0L, Long.MAX_VALUE),
    STRING("STRING", String.class, null, null, null),
    NODE("node", String.class, null, null, null) {
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
    },
    LAZY("LAZY", null, null, null, null) {
      @Override
      public Object validate(String name, Object val, boolean isRuleVal) {
        return Clause.parseString(val);
      }

      @Override
      public void getSuggestions(SuggestionCtx ctx) {
        perNodeSuggestions(ctx);
      }
    },
    DISKTYPE(ImplicitSnitch.DISKTYPE, String.class, unmodifiableSet(new HashSet(Arrays.asList("ssd", "rotational"))), null, null, null) {
      @Override
      public void getSuggestions(SuggestionCtx ctx) {
        perNodeSuggestions(ctx);
      }
    };

    final Class type;
    final Set<String> vals;
    final Number min;
    final Number max;
    public final String tagName;
    public final String perReplicaValue;

    ConditionType(String tagName, Class type, Set<String> vals, Number min, Number max) {
      this(tagName, type, vals, min, max, null);

    }

    ConditionType(String tagName, Class type, Set<String> vals, Number min, Number max, String perReplicaValue) {
      this.tagName = tagName;
      this.type = type;
      this.vals = vals;
      this.min = min;
      this.max = max;
      this.perReplicaValue = perReplicaValue;
    }

    public void getSuggestions(SuggestionCtx ctx) {
      perNodeSuggestions(ctx);
    }

    public void addViolatingReplicas(ViolationCtx ctx) {
      for (Row row : ctx.allRows) {
        row.forEachReplica(replica -> {
          if (ctx.clause.replica.isPass(0) && !ctx.clause.tag.isPass(row)) return;
          if (!ctx.clause.replica.isPass(0) && ctx.clause.tag.isPass(row)) return;
          if(!ctx.currentViolation.matchShard(replica.getShard())) return;
          if (!ctx.clause.collection.isPass(ctx.currentViolation.coll) || !ctx.clause.shard.isPass(ctx.currentViolation.shard))
            return;
          ctx.currentViolation.addReplica(new ReplicaInfoAndErr(replica).withDelta(ctx.clause.tag.delta(row.getVal(ctx.clause.tag.name))));
        });
      }
    }


    public Object convertVal(Object val) {
      return val;
    }

    public Object validate(String name, Object val, boolean isRuleVal) {
      if (name == null) name = this.tagName;
      if (type == Double.class) {
        Double num = Clause.parseDouble(name, val);
        if (isRuleVal) {
          if (min != null)
            if (Double.compare(num, (Double) min) == -1)
              throw new RuntimeException(name + ": " + val + " must be greater than " + min);
          if (max != null)
            if (Double.compare(num, (Double) max) == 1)
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
        if (isRuleVal && vals != null && !vals.contains(val))
          throw new RuntimeException(name + ": " + val + " must be one of " + StrUtils.join(vals, ','));
        return val;
      } else {
        throw new RuntimeException("Invalid type ");
      }

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

  static {
    for (Suggestion.ConditionType t : Suggestion.ConditionType.values()) Suggestion.validatetypes.put(t.tagName, t);
  }
}
