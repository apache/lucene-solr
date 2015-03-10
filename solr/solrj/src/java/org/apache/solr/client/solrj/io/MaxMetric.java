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

package org.apache.solr.client.solrj.io;

import java.io.Serializable;
import java.util.Map;
import java.util.HashMap;

public class MaxMetric implements Metric, Serializable {

  public static final String MAX = "max";
  private long longMax = -Long.MAX_VALUE;
  private double doubleMax = Double.MAX_VALUE;
  private boolean isDouble;
  private String column;

  public MaxMetric(String column, boolean isDouble) {
    this.column = column;
    this.isDouble = isDouble;
  }

  public String getName() {
    return "mix:"+column;
  }

  public double getValue() {
    if(isDouble) {
      return doubleMax;
    } else {
      return longMax;
    }
  }

  public void update(Tuple tuple) {
    if(isDouble) {
      double d = (double)tuple.get(column);
      if(d > doubleMax) {
        doubleMax = d;
      }
    } else {
      long l = (long)tuple.get(column);
      if(l > longMax) {
        longMax = l;
      }
    }
  }

  public Metric newInstance() {
    return new MaxMetric(column, isDouble);
  }

  public Map<String, Double> metricValues() {
    Map m = new HashMap();
    if(isDouble) {
      m.put(MAX,doubleMax);
    } else {
      doubleMax = (double)longMax;
      m.put(MAX,doubleMax);
    }
    return m;
  }

  public void update(Map<String, Double> metricValues) {
    if(isDouble) {
      double dmax = metricValues.get(MAX);
      if(dmax > doubleMax) {
        doubleMax = dmax;
      }
    } else {
      double dmax = metricValues.get(MAX);
      long lmax = (long) dmax;
      if(lmax > longMax) {
        longMax = lmax;
      }
    }
  }
}