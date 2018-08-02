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

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.solr.common.util.StrUtils;

class ReplicaVariable extends VariableBase {

  public ReplicaVariable(Type type) {
    super(type);
  }

  static int getRelevantReplicasCount(Policy.Session session, Clause.Condition cv, String collection, String shard) {
    AtomicInteger totalReplicasOfInterest = new AtomicInteger(0);
    Clause clause = cv.getClause();
    for (Row row : session.matrix) {
      row.forEachReplica(replicaInfo -> {
        if (clause.isMatch(replicaInfo, collection, shard))
          totalReplicasOfInterest.incrementAndGet();
      });
    }
    return totalReplicasOfInterest.get();
  }

  @Override
  public Object validate(String name, Object val, boolean isRuleVal) {
    return getOperandAdjustedValue(super.validate(name, val, isRuleVal), val);
  }



  @Override
  public Operand getOperand(Operand expected, Object strVal, Clause.ComputedType computedType) {
    if (computedType == Clause.ComputedType.ALL) return expected;
    return checkForRangeOperand(expected, strVal, computedType);
  }

  static Operand checkForRangeOperand(Operand expected, Object strVal, Clause.ComputedType computedType) {
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
    if (condition.computedType == Clause.ComputedType.EQUAL) {
      if (condition.getClause().tag != null &&
//              condition.getClause().tag.varType == NODE &&
          (condition.getClause().tag.op == Operand.WILDCARD || condition.getClause().tag.op == Operand.IN)) {
        return null;
      } else {
        return "'replica': '#EQUAL` must be used with 'node':'#ANY'";
      }
    }
    if (condition.computedType == Clause.ComputedType.ALL) {
      if (condition.getClause().tag != null && (condition.getClause().getTag().op == Operand.IN ||
          condition.getClause().getTag().op == Operand.WILDCARD)) {
        return StrUtils.formatString("array value or wild card cannot be used for tag {0} with replica : '#ALL'",
            condition.getClause().tag.getName());
      }
    }
    return null;
  }

  @Override
  public Object computeValue(Policy.Session session, Clause.Condition cv, String collection, String shard, String node) {
    if (cv.computedType == Clause.ComputedType.ALL)
      return Double.valueOf(getRelevantReplicasCount(session, cv, collection, shard));
    if (cv.computedType == Clause.ComputedType.EQUAL) {
      int relevantReplicasCount = getRelevantReplicasCount(session, cv, collection, shard);
      double bucketsCount = getNumBuckets(session, cv.getClause());
      if (relevantReplicasCount == 0 || bucketsCount == 0) return 0;
      return (double) relevantReplicasCount / bucketsCount;
    } else if (cv.computedType == Clause.ComputedType.PERCENT) {
      return Clause.ComputedType.PERCENT.compute(getRelevantReplicasCount(session, cv, collection, shard), cv);
    } else {
      throw new IllegalArgumentException("Unsupported type " + cv.computedType);

    }
  }

  private int getNumBuckets(Policy.Session session, Clause clause) {
    if (clause.getTag().getOperand() == Operand.IN) {
      return ((Collection) clause.getTag().val).size();
    } else if (clause.getTag().getOperand() == Operand.WILDCARD) {
      if (clause.getTag().varType == Type.NODE) return session.matrix.size();
      Set uniqueVals = new HashSet();
      for (Row matrix : session.matrix) {
        Object val = matrix.getVal(clause.getTag().name);
        if (val != null) uniqueVals.add(val);
      }
      return uniqueVals.size();
    } else {
      throw new IllegalArgumentException("Invalid operand for the tag in  " + clause);
    }

  }
}