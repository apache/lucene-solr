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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.util.ReflectMapWriter;
import org.apache.solr.common.util.Utils;

public class PerClauseData implements ReflectMapWriter, Cloneable {
  @JsonProperty
  public Map<String, CollectionDetails> collections = new HashMap<>();

  static Function<String, ReplicaCount> NEW_CLAUSEVAL_FUN = c -> new ReplicaCount();

  ReplicaCount getClauseValue(String coll, String shard, Clause clause, String key) {
    Map<String, ReplicaCount> countMap = getCountsForClause(coll, shard, clause);
    return countMap.computeIfAbsent(key,NEW_CLAUSEVAL_FUN);
  }

  private Map<String, ReplicaCount> getCountsForClause(String coll, String shard, Clause clause) {
    CollectionDetails cd = collections.get(coll);
    if (cd == null) collections.put(coll, cd = new CollectionDetails(coll));
    ShardDetails psd = null;
    if (shard != null && clause.dataGrouping == Clause.DataGrouping.SHARD) {
      psd = cd.shards.get(shard);
      if (psd == null) cd.shards.put(shard, psd = new ShardDetails(coll, shard));
    }

    Map<Clause, Map<String, ReplicaCount>> map = (psd == null ? cd.clauseValues : psd.values);

    return (Map<String, ReplicaCount>) map.computeIfAbsent(clause, Utils.NEW_HASHMAP_FUN);
  }

  PerClauseData copy() {
    PerClauseData result = new PerClauseData();
    collections.forEach((s, v) -> result.collections.put(s, v.copy()));
    return result;
  }



  ShardDetails getShardDetails(String c, String s) {
    CollectionDetails cd = collections.get(c);
    if (cd == null) collections.put(c, cd = new CollectionDetails(c));
    ShardDetails sd = cd.shards.get(s);
    if (sd == null) cd.shards.put(c, sd = new ShardDetails(c, s));
    return sd;
  }


  public static class CollectionDetails implements ReflectMapWriter, Cloneable {

    final String coll;
    Map<String, ShardDetails> shards = new HashMap<>();

    Map<Clause, Map<String, ReplicaCount>> clauseValues = new HashMap<>();

    CollectionDetails copy() {
      CollectionDetails result = new CollectionDetails(coll);
      shards.forEach((k, shardDetails) -> result.shards.put(k, shardDetails.copy()));
      return result;
    }

    CollectionDetails(String coll) {
      this.coll = coll;
    }
  }

  public static class ShardDetails implements ReflectMapWriter, Cloneable {
    final String coll;
    final String shard;
    Double indexSize;
    ReplicaCount replicas = new ReplicaCount();

    public Map<Clause, Map<String, ReplicaCount>> values = new HashMap<>();

    ShardDetails(String coll, String shard) {
      this.coll = coll;
      this.shard = shard;
    }


    ShardDetails copy() {
      ShardDetails result = new ShardDetails(coll, shard);
      values.forEach((clause, clauseVal) -> {
        HashMap<String,ReplicaCount> m = new HashMap(clauseVal);
        for (Map.Entry<String, ReplicaCount> e : m.entrySet()) e.setValue(e.getValue().copy());
        result.values.put(clause, m);
      });
      return result;
    }
  }

  static class LazyViolation extends Violation {
    private Policy.Session session;

    LazyViolation(SealedClause clause, String coll, String shard, String node, Object actualVal, Double replicaCountDelta, Object tagKey, Policy.Session session) {
      super(clause, coll, shard, node, actualVal, replicaCountDelta, tagKey);
      super.replicaInfoAndErrs = null;
      this.session = session;
    }

    @Override
    public List<ReplicaInfoAndErr> getViolatingReplicas() {
      if(replicaInfoAndErrs == null){
        populateReplicas();
      }
      return replicaInfoAndErrs;
    }

    private void populateReplicas() {
      replicaInfoAndErrs = new ArrayList<>();
      for (Row row : session.matrix) {
        if(getClause().getThirdTag().isPass(row)) {
            row.forEachReplica(coll, ri -> {
              if(shard == null || Objects.equals(shard, ri.getShard()))
              replicaInfoAndErrs.add(new ReplicaInfoAndErr(ri));
            });

        }
      }
    }
  }

  void getViolations( Map<Clause, Map<String, ReplicaCount>> vals ,
                      List<Violation> violations,
                      Clause.ComputedValueEvaluator evaluator,
                      Clause clause){

    Map<String, ReplicaCount> cv = vals.get(clause);
    if (cv == null || cv.isEmpty()) return;
    SealedClause sc = clause.getSealedClause(evaluator);
    cv.forEach((s, replicaCount) -> {
      if (!sc.replica.isPass(replicaCount)) {
        Violation v = new LazyViolation(
            sc,
            evaluator.collName,
            evaluator.shardName,
            null,
            replicaCount,
            sc.getReplica().replicaCountDelta(replicaCount),
            s,
            evaluator.session);
        violations.add(v);
      }
    });

  }

  List<Violation> computeViolations(Policy.Session session, Clause clause) {
    Clause.ComputedValueEvaluator evaluator = new Clause.ComputedValueEvaluator(session);
    List<Violation> result = new ArrayList<>();
    collections.forEach((coll, cd) -> {
      evaluator.collName = coll;
      evaluator.shardName = null;
      if (clause.dataGrouping == Clause.DataGrouping.COLL) {
        getViolations(cd.clauseValues, result, evaluator, clause);
      } else if (clause.dataGrouping == Clause.DataGrouping.SHARD) {
        cd.shards.forEach((shard, sd) -> {
          evaluator.shardName = shard;
          getViolations(sd.values, result, evaluator, clause);
        });
      }
    });
    return result;
  }}
