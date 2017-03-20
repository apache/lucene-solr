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

package org.apache.solr.recipe;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.solr.common.MapWriter;
import org.apache.solr.common.util.Utils;
import org.apache.solr.recipe.RuleSorter.Row;

import static org.apache.solr.common.params.CoreAdminParams.COLLECTION;
import static org.apache.solr.common.params.CoreAdminParams.REPLICA;
import static org.apache.solr.common.params.CoreAdminParams.SHARD;
import static org.apache.solr.recipe.Operand.EQUAL;
import static org.apache.solr.recipe.Operand.GREATER_THAN;
import static org.apache.solr.recipe.Operand.LESS_THAN;
import static org.apache.solr.recipe.Operand.NOT_EQUAL;
import static org.apache.solr.recipe.RuleSorter.ANY;


class Clause implements MapWriter {
  Map<String, Object> original;
  Condition collection, shard, replica, tag;
  boolean strict = true;

  Clause(Map<String, Object> m) {
    this.original = m;
    collection = new Condition(COLLECTION, m.containsKey(COLLECTION) ? (String) m.get(COLLECTION) : ANY, EQUAL);
    shard = new Condition(SHARD, m.containsKey(SHARD) ? (String) m.get(SHARD) : ANY, EQUAL);
    String replica = m.containsKey(REPLICA) ? String.valueOf(m.get(REPLICA)) : ANY;
    this.replica = parse(REPLICA, replica);
    strict = Boolean.parseBoolean(String.valueOf(m.getOrDefault("strict", "true")));
    m.forEach(this::parseCondition);
    if (tag == null)
      throw new RuntimeException("Invalid op, must have one and only one tag other than collection, shard,replica " + Utils.toJSONString(m));
  }

  void parseCondition(String s, Object o) {
    if (IGNORE_TAGS.contains(s)) return;
    if (tag != null) {
      throw new IllegalArgumentException("Only one tag other than collection, shard, replica is possible");
    }
    tag = parse(s, o);
  }

  class Condition {
    final String name;
    final Object val;
    final Operand op;

    Condition(String name, Object val, Operand op) {
      this.name = name;
      this.val = val;
      this.op = op;
    }

    boolean isMatch(Object inputVal) {
      return op.canMatch(val, inputVal);
    }

  }

  Condition parse(String s, Object o) {
    Object expectedVal;
    String value = null;
    try {
      String conditionName = s.trim();
      value = String.valueOf(o).trim();
      Operand operand = null;
      if ((expectedVal = NOT_EQUAL.match(value)) != null) {
        operand = NOT_EQUAL;
      } else if ((expectedVal = GREATER_THAN.match(value)) != null) {
        operand = GREATER_THAN;
      } else if ((expectedVal = LESS_THAN.match(value)) != null) {
        operand = LESS_THAN;
      } else {
        operand = EQUAL;
        expectedVal = EQUAL.match(value);
      }

      return new Condition(conditionName, expectedVal, operand);

    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid tag : " + s + ":" + value, e);
    }
  }


  TestStatus test(Row row) {
    AtomicReference<TestStatus> result = new AtomicReference<>(TestStatus.NOT_APPLICABLE);
    Object val = row.getVal(tag.name);
    if (tag.isMatch(val)) {
      checkReplicaCount(row, result);
      if (result.get() == TestStatus.FAIL) row.violations.add(this);
    }
    return result.get();
  }

  TestStatus checkReplicaCount(Row row, AtomicReference<TestStatus> result) {
    row.replicaInfo.forEach((coll, e) -> {
      if (!collection.isMatch(coll)) return;
      AtomicInteger count = new AtomicInteger();
      e.forEach((sh, replicas) -> {
        if (!shard.isMatch(sh)) return;
        count.addAndGet(replicas.size());
      });
      result.set(replica.isMatch(count) ? TestStatus.PASS : TestStatus.FAIL);
      if (RuleSorter.EACH.equals(shard.val)) count.set(0);
    });
    return TestStatus.PASS;
  }


  @Override
  public void writeMap(EntryWriter ew) throws IOException {
    for (Map.Entry<String, Object> e : original.entrySet()) ew.put(e.getKey(), e.getValue());
  }

  enum TestStatus {
    NOT_APPLICABLE, FAIL, PASS
  }

  private static final Set<String> IGNORE_TAGS = new HashSet<>(Arrays.asList(REPLICA, COLLECTION, SHARD, "strict"));
}
