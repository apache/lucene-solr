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

import java.util.List;
import java.util.Objects;

import org.apache.solr.client.solrj.cloud.autoscaling.Clause.TestStatus;

import static org.apache.solr.client.solrj.cloud.autoscaling.Clause.TestStatus.FAIL;
import static org.apache.solr.client.solrj.cloud.autoscaling.Clause.TestStatus.NOT_APPLICABLE;
import static org.apache.solr.client.solrj.cloud.autoscaling.Clause.TestStatus.PASS;
import static org.apache.solr.client.solrj.cloud.autoscaling.Policy.ANY;

/**
 *
 * @deprecated to be removed in Solr 9.0 (see SOLR-14656)
 */
public enum Operand {
  WILDCARD(ANY, Integer.MAX_VALUE) {
    @Override
    public TestStatus match(Object ruleVal, Object testVal) {
      return testVal == null ? NOT_APPLICABLE : PASS;
    }

  },

  RANGE_EQUAL("", 0) {
    @Override
    public TestStatus match(Object ruleVal, Object testVal) {
      return ((RangeVal) ruleVal).match((Number) testVal) ? PASS : FAIL;
    }

    @Override
    public Double delta(Object expected, Object actual) {
      return ((RangeVal) expected).delta(((Number) actual).doubleValue());
    }

    @Override
    public Object readRuleValue(Condition condition) {
      if (condition.val instanceof String) {
        String strVal = ((String) condition.val).trim();
        int hyphenIdx = strVal.indexOf('-');
        if (hyphenIdx > 0) {
          String minS = strVal.substring(0, hyphenIdx).trim();
          String maxS = strVal.substring(hyphenIdx + 1, strVal.length()).trim();
          return new RangeVal(
              (Number) condition.varType.validate(condition.name, minS, true),
              (Number) condition.varType.validate(condition.name, maxS, true),
              null
          );

        }

      }


      Number num = (Number) condition.varType.validate(condition.name, condition.val, true);
      return new RangeVal(Math.floor(num.doubleValue()), Math.ceil(num.doubleValue()), num);
    }
  },
  EQUAL("", 0) {
    @Override
    public double _delta(double expected, double actual) {
      return actual - expected;
    }
  },
  IN("", 0) {
    @Override
    public TestStatus match(Object ruleVal, Object testVal) {
      @SuppressWarnings({"rawtypes"})
      List l = (List) ruleVal;
      return (l.contains(testVal)) ?  PASS: FAIL;
    }
  },
  RANGE_NOT_EQUAL("", 2) {
    @Override
    public TestStatus match(Object ruleVal, Object testVal) {
      return ((RangeVal) ruleVal).match((Number) testVal) ? FAIL : PASS;
    }

    @Override
    public Object readRuleValue(Condition condition) {
      return RANGE_EQUAL.readRuleValue(condition);
    }

  },
  NOT_EQUAL("!", 2) {
    @Override
    public TestStatus match(Object ruleVal, Object testVal) {
      if(testVal == null) return PASS;
      return super.match(ruleVal, testVal) == PASS ? FAIL : PASS;
    }

    @Override
    public double _delta(double expected, double actual) {
      return expected - actual;
    }

  },
  GREATER_THAN(">", 1) {
    @Override
    public TestStatus match(Object ruleVal, Object testVal) {
      if (testVal == null) return NOT_APPLICABLE;
      if (ruleVal instanceof String) ruleVal = Clause.parseDouble("", ruleVal);
      if (ruleVal instanceof Double) {
        return Double.compare(Clause.parseDouble("", testVal), (Double) ruleVal) == -1 ? FAIL : PASS;
      }
     return getLong(testVal) > getLong(ruleVal) ? PASS: FAIL ;
    }

    @Override
    public String wrap(Object val) {
      return ">" + (((Number) val).doubleValue() - 1);
    }

    @Override
    public Operand opposite(boolean flag) {
      return flag ? LESS_THAN : GREATER_THAN;
    }

    @Override
    protected double _delta(double expected, double actual) {
      return actual > expected ? 0 : expected - actual;
    }
  },
  LESS_THAN("<", 2) {
    @Override
    public TestStatus match(Object ruleVal, Object testVal) {
      if (testVal == null) return NOT_APPLICABLE;
      if (ruleVal instanceof String) ruleVal = Clause.parseDouble("", ruleVal);
      if (ruleVal instanceof Double) {
        return Double.compare(Clause.parseDouble("", testVal), (Double) ruleVal) == 1 ? FAIL : PASS;
      }
      return getLong(testVal) < getLong(ruleVal) ? PASS: FAIL ;
    }

    @Override
    public String wrap(Object val) {
      return "<" + (((Number) val).doubleValue() + 1);
    }

    @Override
    protected double _delta(double expected, double actual) {
      return actual < expected ? 0 : actual - expected;
    }

    @Override
    public Operand opposite(boolean flag) {
      return flag ? GREATER_THAN : this;
    }
  };

  public Operand opposite(boolean flag) {
    return this;
  }
  public final String operand;
  final int priority;

  Operand(String val, int priority) {
    this.operand = val;
    this.priority = priority;
  }

  public TestStatus match(Object ruleVal, Object testVal) {
    return Objects.equals(ruleVal, testVal) ? PASS : FAIL;
  }

  Long getLong(Object o) {
    if (o instanceof Long) return (Long) o;
    if(o instanceof Number ) return ((Number) o).longValue();
    return Long.parseLong(String.valueOf(o));

  }

  public Double delta(Object expected, Object actual) {
    if (expected instanceof Number && actual instanceof Number) {
      Double expectedL = ((Number) expected).doubleValue();
      Double actualL = ((Number) actual).doubleValue();
      return _delta(expectedL, actualL);
    } else {
      return 0d;
    }

  }

  protected double _delta(double expected, double actual) {
    return 0;
  }

  public String wrap(Object val) {
    return operand + val.toString();
  }

  public Object readRuleValue(Condition condition) {
    return condition.val;
  }
}
