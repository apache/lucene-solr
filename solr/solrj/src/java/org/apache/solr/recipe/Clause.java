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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.solr.common.MapWriter;
import org.apache.solr.common.util.Utils;
import org.apache.solr.recipe.RuleSorter.Row;

import static java.util.Collections.singletonMap;
import static org.apache.solr.common.params.CoreAdminParams.COLLECTION;
import static org.apache.solr.common.params.CoreAdminParams.REPLICA;
import static org.apache.solr.common.params.CoreAdminParams.SHARD;
import static org.apache.solr.recipe.Operand.EQUAL;
import static org.apache.solr.recipe.Operand.GREATER_THAN;
import static org.apache.solr.recipe.Operand.LESS_THAN;
import static org.apache.solr.recipe.Operand.NOT_EQUAL;


public class Clause implements MapWriter {
  Map<String, Object> original;
  Condition collection, shard, replica, tag;
  boolean strict = true;

  Clause(Map<String, Object> m) {
    this.original = m;
    collection = parse(COLLECTION, m);
    shard = parse(SHARD, m);
    this.replica = parse(REPLICA, m);
    strict = Boolean.parseBoolean(String.valueOf(m.getOrDefault("strict", "true")));
    m.forEach((s, o) -> parseCondition(s, o));
    if (tag == null)
      throw new RuntimeException("Invalid op, must have one and only one tag other than collection, shard,replica " + Utils.toJSONString(m));
  }

  void parseCondition(String s, Object o) {
    if (IGNORE_TAGS.contains(s)) return;
    if (tag != null) {
      throw new IllegalArgumentException("Only one tag other than collection, shard, replica is possible");
    }
    tag = parse(s, singletonMap(s, o));
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

    boolean isMatch(Row row) {
      return op.canMatch(val, row.getVal(name));
    }
    boolean isMatch(Object inputVal) {
      return op.canMatch(val, inputVal);
    }
  }

  Condition parse(String s, Map m) {
    Object expectedVal = null;
    Object val = m.get(s);
    try {
      String conditionName = s.trim();
      String value = val == null ? null : String.valueOf(val).trim();
      Operand operand = null;
      if ((expectedVal = Operand.ANY.parse(value)) != null) {
        operand = Operand.ANY;
      } else if ((expectedVal = NOT_EQUAL.parse(value)) != null) {
        operand = NOT_EQUAL;
      } else if ((expectedVal = GREATER_THAN.parse(value)) != null) {
        operand = GREATER_THAN;
      } else if ((expectedVal = LESS_THAN.parse(value)) != null) {
        operand = LESS_THAN;
      } else {
        operand = EQUAL;
        expectedVal = EQUAL.parse(value);
      }

      return new Condition(conditionName, expectedVal, operand);

    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid tag : " + s + ":" + val, e);
    }
  }


  TestStatus test(Row row) {
    AtomicReference<TestStatus> result = new AtomicReference<>(TestStatus.NOT_APPLICABLE);
    outer:
    for (Map.Entry<String, Map<String, List<RuleSorter.ReplicaStat>>> e : row.replicaInfo.entrySet()) {
      if (!collection.isMatch(e.getKey())) break;
      int count = 0;
      for (Map.Entry<String, List<RuleSorter.ReplicaStat>> e1 : e.getValue().entrySet()) {
        if (!shard.isMatch(e1.getKey())) break;
        count += e1.getValue().size();
        if (shard.val.equals(RuleSorter.EACH) && count > 0 && replica.isMatch(count) && tag.isMatch(row)) {
          result.set(TestStatus.FAIL);
          continue outer;
        }
        if (RuleSorter.EACH.equals(shard.val)) count = 0;
      }
      if (shard.val.equals(RuleSorter.ANY) && count > 0 && replica.isMatch(count) && !tag.isMatch(row)) {
        result.set(TestStatus.FAIL);
      }
    }
    if (result.get() == TestStatus.FAIL) row.violations.add(this);
    return result.get();
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

  private static final Set<String> IGNORE_TAGS = new HashSet<>(Arrays.asList(REPLICA, COLLECTION, SHARD, "strict"));
}
