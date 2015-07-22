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
import java.util.Map;
import java.util.HashMap;

import org.apache.solr.client.solrj.io.Tuple;

public class MeanMetric implements Metric, Serializable {

  private static final long serialVersionUID = 1;

  private String column;
  private double doubleSum;
  private long longSum;
  private long count;

  public MeanMetric(String column) {
    this.column = column;
  }

  public String getName() {
    return "avg("+column+")";
  }

  public void update(Tuple tuple) {
    ++count;
    Object o = tuple.get(column);
    if(o instanceof Double) {
      Double d = (Double)tuple.get(column);
      doubleSum += d.doubleValue();
    } else {
      Long l = (Long)tuple.get(column);
      longSum += l.doubleValue();
    }
  }

  public Metric newInstance() {
    return new MeanMetric(column);
  }

  public double getValue() {
    double dcount = (double)count;
    if(longSum == 0) {
      double ave = doubleSum/dcount;
      return ave;

    } else {
      double ave = longSum/dcount;
      return ave;
    }
  }
}