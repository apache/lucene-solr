package org.apache.solr.client.solrj.io.stream.metrics;

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

import java.io.Serializable;

import org.apache.solr.client.solrj.io.Tuple;

public class MaxMetric implements Metric, Serializable {

  private static final long serialVersionUID = 1;

  public static final String MAX = "max";
  private long longMax = -Long.MIN_VALUE;
  private double doubleMax = -Double.MAX_VALUE;
  private String column;

  public MaxMetric(String column) {
    this.column = column;
  }

  public String getName() {
    return "max("+column+")";
  }

  public double getValue() {
    if(longMax == Long.MIN_VALUE) {
      return doubleMax;
    } else {
      return longMax;
    }
  }

  public void update(Tuple tuple) {
    Object o = tuple.get(column);
    if(o instanceof Double) {
      double d = (double)o;
      if(d > doubleMax) {
        doubleMax = d;
      }
    } else {
      long l = (long)o;
      if(l > longMax) {
        longMax = l;
      }
    }
  }

  public Metric newInstance() {
    return new MaxMetric(column);
  }
}