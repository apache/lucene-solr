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

import static org.apache.solr.client.solrj.cloud.autoscaling.Clause.TestStatus.FAIL;
import static org.apache.solr.client.solrj.cloud.autoscaling.Clause.TestStatus.NOT_APPLICABLE;
import static org.apache.solr.client.solrj.cloud.autoscaling.Clause.TestStatus.PASS;
import static org.apache.solr.client.solrj.cloud.autoscaling.Policy.ANY;

import java.util.Objects;

import org.apache.solr.client.solrj.cloud.autoscaling.Clause.TestStatus;


public enum Operand {
  WILDCARD(ANY, Integer.MAX_VALUE) {
    @Override
    public TestStatus match(Object ruleVal, Object testVal) {
      return testVal == null ? NOT_APPLICABLE : PASS;
    }

  },
  EQUAL("", 0) {
    @Override
    public int _delta(int expected, int actual) {
      return expected - actual;
    }
  },
  NOT_EQUAL("!", 2) {
    @Override
    public TestStatus match(Object ruleVal, Object testVal) {
      return super.match(ruleVal, testVal) == PASS ? FAIL : PASS;
    }

    @Override
    public int _delta(int expected, int actual) {
      return expected - actual;
    }

  },
  GREATER_THAN(">", 1) {
    @Override
    public TestStatus match(Object ruleVal, Object testVal) {
      if (testVal == null) return NOT_APPLICABLE;
      if (ruleVal instanceof Double) {
        return Double.compare(Clause.parseDouble("", testVal), (Double) ruleVal) == 1 ? PASS : FAIL;
      }
     return getLong(testVal) > getLong(ruleVal) ? PASS: FAIL ;
    }

    @Override
    protected int _delta(int expected, int actual) {
      return actual > expected ? 0 : (expected + 1) - actual;
    }
  },
  LESS_THAN("<", 2) {
    @Override
    public TestStatus match(Object ruleVal, Object testVal) {
      if (testVal == null) return NOT_APPLICABLE;
      if (ruleVal instanceof Double) {
        return Double.compare(Clause.parseDouble("", testVal), (Double) ruleVal) == -1 ? PASS : FAIL;
      }
      return getLong(testVal) < getLong(ruleVal) ? PASS: FAIL ;
    }

    @Override
    protected int _delta(int expected, int actual) {
      return actual < expected ? 0 : (expected ) - actual;
    }

  };
  public final String operand;
  final int priority;

  Operand(String val, int priority) {
    this.operand = val;
    this.priority = priority;
  }

  public String toStr(Object expectedVal) {
    return operand + expectedVal.toString();
  }

  public TestStatus match(Object ruleVal, Object testVal) {
    return Objects.equals(ruleVal, testVal) ? PASS : FAIL;
  }

  Long getLong(Object o) {
    if (o instanceof Long) return (Long) o;
    if(o instanceof Number ) return ((Number) o).longValue();
    return Long.parseLong(String.valueOf(o));

  }

  public Integer delta(Object expected, Object actual) {
    try {
      Integer expectedInt = Integer.parseInt(String.valueOf(expected));
      Integer actualInt = Integer.parseInt(String.valueOf(actual));
      return _delta(expectedInt, actualInt);
    } catch (Exception e) {
      return null;
    }
  }

  protected int _delta(int expected, int actual) {
    return 0;
  }
}
