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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import org.apache.solr.common.IteratorWriter;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.util.Utils;

/**
 *
 * @deprecated to be removed in Solr 9.0 (see SOLR-14656)
 */
public class Violation implements MapWriter {
  final String shard, coll, node;
  final Object actualVal;
  Double replicaCountDelta;//how many extra replicas
  final Object tagKey;
  private final int hash;
  private final Clause clause;
  private List<ReplicaInfoAndErr> replicaInfoAndErrs = new ArrayList<>();

  Violation(SealedClause clause, String coll, String shard, String node, Object actualVal, Double replicaCountDelta, Object tagKey) {
    this.clause = clause;
    this.shard = shard;
    this.coll = coll;
    this.node = node;
    this.replicaCountDelta = replicaCountDelta;
    this.actualVal = actualVal;
    this.tagKey = tagKey;
    hash = ("" + coll + " " + shard + " " + node + " " + String.valueOf(tagKey) + " " + Utils.toJSONString(getClause().toMap(new HashMap<>()))).hashCode();
  }

  public Violation addReplica(ReplicaInfoAndErr r) {
    replicaInfoAndErrs.add(r);
    return this;
  }

  public List<ReplicaInfoAndErr> getViolatingReplicas() {
    return replicaInfoAndErrs;
  }

  public Clause getClause() {
    return clause;
  }

  public boolean matchShard(String shard) {
    if (getClause().getShard().op == Operand.WILDCARD) return true;
    return this.shard == null || this.shard.equals(shard);
  }

  //if the delta is lower , this violation is less serious
  public boolean isLessSerious(Violation that) {
    return this.getClause().getTag().varType.compareViolation(this, that) < 0;
  }

  @Override
  public int hashCode() {
    return hash;
  }

  public boolean isSimilarViolation(Violation that) {
    if (Objects.equals(this.shard, that.shard) &&
        Objects.equals(this.coll, that.coll) &&
        Objects.equals(this.node, that.node)) {
      if (this.clause.isPerCollectiontag() && that.clause.isPerCollectiontag()) {
        return Objects.equals(this.clause.tag.getName(), that.clause.tag.getName());
      } else if (!this.clause.isPerCollectiontag() && !that.clause.isPerCollectiontag()) {
        return Objects.equals(this.clause.globalTag.getName(), that.clause.globalTag.getName())
            && Objects.equals(this.node, that.node);
      } else {
        return false;
      }
    } else {
      return false;
    }

  }

  @Override
  public boolean equals(Object that) {
    if (that instanceof Violation) {
      Violation v = (Violation) that;
      return Objects.equals(this.shard, v.shard) &&
          Objects.equals(this.coll, v.coll) &&
//          Objects.equals(this.node, v.node) &&
          Objects.equals(this.clause, v.clause)
          ;
    }
    return false;
  }

  static class ReplicaInfoAndErr implements MapWriter{
    final ReplicaInfo replicaInfo;
    Double delta;

    ReplicaInfoAndErr(ReplicaInfo replicaInfo) {
      this.replicaInfo = replicaInfo;
    }

    public ReplicaInfoAndErr withDelta(Double delta) {
      this.delta = delta;
      return this;
    }

    @Override
    public void writeMap(EntryWriter ew) throws IOException {
      ew.put("replica", replicaInfo);
      ew.putIfNotNull("delta", delta);
    }
  }

  @Override
  public String toString() {
    return Utils.toJSONString(Utils.getDeepCopy(toMap(new LinkedHashMap<>()), 5));
  }

  @Override
  public void writeMap(EntryWriter ew) throws IOException {
    ew.putIfNotNull("collection", coll);
    if (!Policy.ANY.equals(shard)) ew.putIfNotNull("shard", shard);
    ew.putIfNotNull("node", node);
    ew.putIfNotNull("tagKey", tagKey);
    ew.putIfNotNull("violation", (MapWriter) ew1 -> {
      if (getClause().isPerCollectiontag()) ew1.put("replica", actualVal);
      else ew1.put(clause.tag.name, String.valueOf(actualVal));
      ew1.putIfNotNull("delta", replicaCountDelta);
    });
    ew.put("clause", getClause());
    if (!replicaInfoAndErrs.isEmpty()) {
      ew.put("violatingReplicas", (IteratorWriter) iw -> {
        for (ReplicaInfoAndErr replicaInfoAndErr : replicaInfoAndErrs) {
          iw.add(replicaInfoAndErr.replicaInfo);
        }
      });
    }
  }

  static class Ctx {
    final Function<Condition, Object> evaluator;
    Object tagKey;
    Clause clause;
    ReplicaCount count;
    Violation currentViolation;
    List<Row> allRows;
    List<Violation> allViolations = new ArrayList<>();

    public Ctx(Clause clause, List<Row> allRows, Function<Condition, Object> evaluator) {
      this.allRows = allRows;
      this.clause = clause;
      this.evaluator = evaluator;
    }

    public Ctx resetAndAddViolation(Object tagKey, ReplicaCount count, Violation currentViolation) {
      this.tagKey = tagKey;
      this.count = count;
      this.currentViolation = currentViolation;
      allViolations.add(currentViolation);
      this.clause = currentViolation.getClause();
      return this;
    }
  }
}
