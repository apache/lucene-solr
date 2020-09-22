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

/**
 *
 * @deprecated to be removed in Solr 9.0 (see SOLR-14656)
 */
public enum ComputedType {
  NULL(),
  EQUAL() {
    @Override
    public String wrap(String value) {
      return "#EQUAL";
    }

    @Override
    public String match(String val) {
      if ("#EQUAL".equals(val)) return "1";
      return null;
    }

  },
  ALL() {
    @Override
    public String wrap(String value) {
      return "#ALL";
    }

    @Override
    public String match(String val) {
      if ("#ALL".equals(val)) return "1";
      return null;
    }

  },
  PERCENT {
    @Override
    public String wrap(String value) {
      return value + "%";
    }

    @Override
    public String match(String val) {
      if (val != null && !val.isEmpty() && val.charAt(val.length() - 1) == '%') {
        String newVal = val.substring(0, val.length() - 1);
        double d;
        try {
          d = Double.parseDouble(newVal);
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException("Invalid percentage value : " + val);
        }
        if (d < 0 || d > 100) {
          throw new IllegalArgumentException("Percentage value must lie between [1 -100] : provided value : " + val);
        }
        return newVal;
      } else {
        return null;
      }
    }

    @Override
    public Object compute(Object val, Condition c) {
      if (val == null || Clause.parseDouble(c.name, val) == 0) return 0d;
      return Clause.parseDouble(c.name, val) * Clause.parseDouble(c.name, c.val).doubleValue() / 100;
    }

    @Override
    public String toString() {
      return "%";
    }
  };

  // return null if there is no match. return a modified string
  // if there is a match
  public String match(String val) {
    return null;
  }

  public String wrap(String value) {
    return value;
  }

  public Object compute(Object val, Condition c) {
    return val;
  }

}
