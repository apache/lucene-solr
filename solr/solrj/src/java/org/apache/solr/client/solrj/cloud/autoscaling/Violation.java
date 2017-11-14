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

import org.apache.solr.common.MapWriter;
import org.apache.solr.common.util.Utils;

public class Violation implements MapWriter {
  final String shard, coll, node;
  final Object actualVal;
  final Long replicaCountDelta;//how far is the actual value from the expected value
  final Object tagKey;
  private final int hash;
  private final Clause clause;
  private List<ReplicaInfoAndErr> replicaInfoAndErrs = new ArrayList<>();

  Violation(Clause clause, String coll, String shard, String node, Object actualVal, Long replicaCountDelta, Object tagKey) {
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
    if (getClause().shard.op == Operand.WILDCARD) return true;
    return this.shard == null || this.shard.equals(shard);
  }

  static class ReplicaInfoAndErr implements MapWriter{
    final ReplicaInfo replicaInfo;

    ReplicaInfoAndErr(ReplicaInfo replicaInfo) {
      this.replicaInfo = replicaInfo;
    }
    Long delta;

    public ReplicaInfoAndErr withDelta(Long delta) {
      this.delta = delta;
      return this;
    }

    @Override
    public void writeMap(EntryWriter ew) throws IOException {
      ew.put("replica", replicaInfo);
      ew.putIfNotNull("delta",delta );
    }
  }

  @Override
  public int hashCode() {
    return hash;
  }
  //if the delta is lower , this violation is less serious
  public boolean isLessSerious(Violation that) {
    return that.replicaCountDelta != null && replicaCountDelta != null &&
        Math.abs(replicaCountDelta) < Math.abs(that.replicaCountDelta);
  }

  @Override
  public boolean equals(Object that) {
    if (that instanceof Violation) {
      Violation v = (Violation) that;
      return Objects.equals(this.shard, v.shard) &&
          Objects.equals(this.coll, v.coll) &&
          Objects.equals(this.node, v.node) &&
          Objects.equals(this.tagKey, v.tagKey)
          ;
    }
    return false;
  }

  @Override
  public String toString() {
    return Utils.toJSONString(Utils.getDeepCopy(toMap(new LinkedHashMap<>()), 5));
  }

  @Override
  public void writeMap(EntryWriter ew) throws IOException {
    ew.putIfNotNull("collection", coll);
    ew.putIfNotNull("shard", shard);
    ew.putIfNotNull("node", node);
    ew.putIfNotNull("tagKey", String.valueOf(tagKey));
    ew.putIfNotNull("violation", (MapWriter) ew1 -> {
      if (getClause().isPerCollectiontag()) ew1.put("replica", actualVal);
      else ew1.put(clause.tag.name, String.valueOf(actualVal));
      ew1.putIfNotNull("delta", replicaCountDelta);
    });
    ew.put("clause", getClause());
  }
}
