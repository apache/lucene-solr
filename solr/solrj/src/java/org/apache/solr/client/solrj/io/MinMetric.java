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

public class MinMetric implements Metric, Serializable {

  public static final String MIN = "min";
  private long longMin = Long.MAX_VALUE;
  private double doubleMin = Double.MAX_VALUE;
  private boolean isDouble;
  private String column;

  public MinMetric(String column, boolean isDouble) {
    this.column = column;
    this.isDouble = isDouble;
  }

  public String getName() {
    return "min:"+column;
  }

  public double getValue() {
    if(isDouble) {
      return doubleMin;
    } else {
      return longMin;
    }
  }

  public void update(Tuple tuple) {
    if(isDouble) {
      double d = (double)tuple.get(column);
      if(d < doubleMin) {
        doubleMin = d;
      }
    } else {
      long l = (long)tuple.get(column);
      if(l < longMin) {
        longMin = l;
      }
    }
  }

  public Metric newInstance() {
    return new MinMetric(column, isDouble);
  }

  public Map<String, Double> metricValues() {
    Map m = new HashMap();
    if(isDouble) {
      m.put(MIN,doubleMin);
    } else {
      doubleMin = (double)longMin;
      m.put(MIN,doubleMin);
    }
    return m;
  }

  public void update(Map<String, Double> metricValues) {
    if(isDouble) {
      double dmin = metricValues.get(MIN);
      if(dmin < doubleMin) {
        doubleMin = dmin;
      }
    } else {
      double dmin = metricValues.get(MIN);
      long lmin = (long) dmin;
      if(lmin < longMin) {
        longMin = lmin;
      }
    }
  }
}