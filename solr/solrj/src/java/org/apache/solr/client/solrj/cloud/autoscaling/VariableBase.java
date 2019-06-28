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

import org.apache.solr.common.cloud.rule.ImplicitSnitch;
import org.apache.solr.common.util.StrUtils;

import static org.apache.solr.client.solrj.cloud.autoscaling.Clause.parseString;
import static org.apache.solr.client.solrj.cloud.autoscaling.Suggestion.suggestNegativeViolations;
import static org.apache.solr.client.solrj.cloud.autoscaling.Suggestion.suggestPositiveViolations;
import static org.apache.solr.client.solrj.cloud.autoscaling.Variable.Type.FREEDISK;

public class VariableBase implements Variable {
  final Type varType;

  public VariableBase(Type type) {
    this.varType = type;
  }

  @Override
  public void getSuggestions(Suggestion.Ctx ctx) {
    if (ctx.violation == null) return;
    if (ctx.violation.replicaCountDelta > 0) {
      suggestPositiveViolations(ctx);
    } else {
      suggestNegativeViolations(ctx, ArrayList::new);
    }
  }

  @Override
  public String postValidate(Condition condition) {
    if(Clause.IGNORE_TAGS.contains(condition.getName())) return null;
    if(condition.getOperand() == Operand.WILDCARD && condition.clause.nodeSetPresent){
      return "#EACH not supported in tags in nodeset";
    }
    return null;
  }

  static Object getOperandAdjustedValue(Object val, Object original) {
    if (original instanceof Condition) {
      Condition condition = (Condition) original;
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

  public static Type getTagType(String name) {
    Type info = Type.get(name);
    if (info == null && name.startsWith(ImplicitSnitch.SYSPROP)) info = Type.SYSPROP;
    if (info == null && name.startsWith(Clause.METRICS_PREFIX)) info = Type.LAZY;
    return info;
  }

  static Variable loadImpl(Meta meta, Type t) {
    Class implementation = meta.implementation();
    if (implementation == void.class) implementation = VariableBase.class;
    try {
      return (Variable) implementation.getConstructor(Type.class).newInstance(t);
    } catch (Exception e) {
      throw new RuntimeException("Unable to instantiate: " + implementation.getName(), e);
    }
  }

  @Override
  public int compareViolation(Violation v1, Violation v2) {
    if (v2.replicaCountDelta == null || v1.replicaCountDelta == null) return 0;
    if (Math.abs(v1.replicaCountDelta) == Math.abs(v2.replicaCountDelta)) return 0;
    return Math.abs(v1.replicaCountDelta) < Math.abs(v2.replicaCountDelta) ? -1 : 1;
  }

  @Override
  public Object validate(String name, Object val, boolean isRuleVal) {
    if (val instanceof Condition) {
      Condition condition = (Condition) val;
      val = condition.op.readRuleValue(condition);
      if (val != condition.val) return val;
    }
    if (!isRuleVal && "".equals(val) && this.varType.type != String.class) val = -1;
    if (name == null) name = this.varType.tagName;
    if (varType.type == Double.class) {
      Double num = Clause.parseDouble(name, val);
      if (isRuleVal) {
        if (varType.min != null)
          if (Double.compare(num, varType.min.doubleValue()) == -1)
            throw new RuntimeException(name + ": " + val + " must be greater than " + varType.min);
        if (varType.max != null)
          if (Double.compare(num, varType.max.doubleValue()) == 1)
            throw new RuntimeException(name + ": " + val + " must be less than " + varType.max);
      }
      return num;
    } else if (varType.type == Long.class) {
      Long num = Clause.parseLong(name, val);
      if (isRuleVal) {
        if (varType.min != null)
          if (num < varType.min.longValue())
            throw new RuntimeException(name + ": " + val + " must be greater than " + varType.min);
        if (varType.max != null)
          if (num > varType.max.longValue())
            throw new RuntimeException(name + ": " + val + " must be less than " + varType.max);
      }
      return num;
    } else if (varType.type == String.class) {
      if (isRuleVal && !varType.vals.isEmpty() && !varType.vals.contains(val))
        throw new RuntimeException(name + ": " + val + " must be one of " + StrUtils.join(varType.vals, ','));
      return val;
    } else {
      throw new RuntimeException("Invalid type ");
    }
  }

  public static class TotalDiskVariable extends VariableBase {
    public TotalDiskVariable(Type type) {
      super(type);
    }

    @Override
    public Object convertVal(Object val) {
      return FREEDISK.convertVal(val);
    }
  }

  public static class CoreIndexSizeVariable extends VariableBase {
    public CoreIndexSizeVariable(Type type) {
      super(type);
    }

    @Override
    public Object convertVal(Object val) {
      return FREEDISK.convertVal(val);
    }
  }

  public static class LazyVariable extends VariableBase {
    public LazyVariable(Type type) {
      super(type);
    }

    @Override
    public Object validate(String name, Object val, boolean isRuleVal) {
      return parseString(val);
    }

    @Override
    public boolean match(Object inputVal, Operand op, Object val, String name, Row row) {
      return op.match(parseString(val), parseString(inputVal)) == Clause.TestStatus.PASS;
    }

    @Override
    public void getSuggestions(Suggestion.Ctx ctx) {
      suggestPositiveViolations(ctx);
    }
  }

  public static class DiskTypeVariable extends VariableBase {
    public DiskTypeVariable(Type type) {
      super(type);
    }

    @Override
    public void getSuggestions(Suggestion.Ctx ctx) {
      suggestPositiveViolations(ctx);
    }
  }
}
