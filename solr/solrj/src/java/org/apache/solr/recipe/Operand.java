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

import java.util.Objects;


public enum Operand {
  EQUAL(""),
  NOT_EQUAL("!") {
    @Override
    public boolean canMatch(Object ruleVal, Object testVal) {
      return !super.canMatch(ruleVal, testVal);
    }
  },
  GREATER_THAN(">") {
    @Override
    public Object match(String val) {
      return checkNumeric(super.match(val));
    }


    @Override
    public boolean canMatch(Object ruleVal, Object testVal) {
      return testVal != null && compareNum(ruleVal, testVal) == 1;
    }

  },
  LESS_THAN("<") {
    @Override
    public int compare(Object n1Val, Object n2Val) {
      return GREATER_THAN.compare(n1Val, n2Val) * -1;
    }

    @Override
    public boolean canMatch(Object ruleVal, Object testVal) {
      return testVal != null && compareNum(ruleVal, testVal) == -1;
    }

    @Override
    public Object match(String val) {
      return checkNumeric(super.match(val));
    }
  };
  public final String operand;

  Operand(String val) {
    this.operand = val;
  }

  public String toStr(Object expectedVal) {
    return operand + expectedVal.toString();
  }

  Object checkNumeric(Object val) {
    if (val == null) return null;
    try {
      return Integer.parseInt(val.toString());
    } catch (NumberFormatException e) {
      throw new RuntimeException("for operand " + operand + " the value must be numeric");
    }
  }

  public Object match(String val) {
    if (operand.isEmpty()) return val;
    return val.startsWith(operand) ? val.substring(1) : null;
  }

  public boolean canMatch(Object ruleVal, Object testVal) {
    return Objects.equals(String.valueOf(ruleVal), String.valueOf(testVal));
  }


  public int compare(Object n1Val, Object n2Val) {
    return 0;
  }

  public int compareNum(Object n1Val, Object n2Val) {
    Integer n1 = (Integer) parseObj(n1Val, Integer.class);
    Integer n2 = (Integer) parseObj(n2Val, Integer.class);
    return n1 > n2 ? -1 : Objects.equals(n1, n2) ? 0 : 1;
  }

  Object parseObj(Object o, Class typ) {
    if (o == null) return o;
    if (typ == String.class) return String.valueOf(o);
    if (typ == Integer.class) {
      return Integer.parseInt(String.valueOf(o));
    }
    return o;
  }
}
