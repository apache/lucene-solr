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
import java.util.Objects;

import org.apache.solr.common.MapWriter;

import static org.apache.solr.client.solrj.cloud.autoscaling.Operand.GREATER_THAN;
import static org.apache.solr.client.solrj.cloud.autoscaling.Operand.LESS_THAN;

/**
 *
 * @deprecated to be removed in Solr 9.0 (see SOLR-14656)
 */
public class Condition implements MapWriter {
  final String name;
  final Object val;
  final Variable.Type varType;
  final ComputedType computedType;
  final Operand op;
  Clause clause;

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
  public int hashCode() {
    return Objects.hash(name, val, op);
  }

  @Override
  public boolean equals(Object that) {
    if (that instanceof Condition) {
      Condition c = (Condition) that;
      return Objects.equals(c.name, name) && Objects.equals(c.val, val) && c.op == op;
    }
    return false;
  }

  public Double replicaCountDelta(Object val) {
    if (val instanceof ReplicaCount) val = ((ReplicaCount) val).getVal(getClause().type);
    return op.delta(this.val, val);
  }

  public Double delta(Object val) {
    if (this.val instanceof String) {
      if (op == LESS_THAN || op == GREATER_THAN) {
        return op
            .opposite(getClause().isReplicaZero() && this == getClause().tag)
            .delta(Clause.parseDouble(name, this.val), Clause.parseDouble(name, val));
      } else {
        return 0d;
      }
    } else {
      return op
          .opposite(getClause().isReplicaZero() && this == getClause().getTag())
          .delta(this.val, val);
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
