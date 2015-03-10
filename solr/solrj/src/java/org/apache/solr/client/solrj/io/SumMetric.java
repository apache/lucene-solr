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

public class SumMetric implements Metric, Serializable {

  private static final long serialVersionUID = 1;

  public static final String SUM = "sum";

  private String column;
  private boolean isDouble;
  private double doubleSum;
  private long longSum;

  public SumMetric(String column, boolean isDouble) {
    this.column = column;
    this.isDouble = isDouble;
  }

  public String getName() {
    return "sum:"+column;
  }

  public void update(Tuple tuple) {
    if(isDouble) {
      Double d = (Double)tuple.get(column);
      doubleSum += d.doubleValue();
    } else {
      Long l = (Long)tuple.get(column);
      longSum += l.doubleValue();
    }
  }

  public Metric newInstance() {
    return new SumMetric(column, isDouble);
  }

  public Map<String, Double> metricValues() {
    Map m = new HashMap();
    if(isDouble) {
      m.put(SUM,doubleSum);

    } else {
      doubleSum = (double)longSum;
      m.put(SUM,doubleSum);
    }

    return m;
  }

  public double getValue() {
    if(isDouble) {
      return doubleSum;
    } else {
      return (double)longSum;
    }
  }

  public void update(Map<String, Double> metricValues) {
    if(isDouble) {
      double dsum = metricValues.get(SUM);
      doubleSum+=dsum;
    } else {
      double dsum = metricValues.get(SUM);
      longSum+=(long)dsum;
    }
  }
}