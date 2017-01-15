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
package org.apache.solr.analytics.statistics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.solr.analytics.util.MedianCalculator;

/**
 * <code>MedianStatsCollector</code> computes the median.
 */
public class MedianStatsCollector extends AbstractDelegatingStatsCollector{

  private final List<Double> values = new ArrayList<>();
  protected double median;
  
  public MedianStatsCollector(StatsCollector delegate) {
    super(delegate);
  }

  public Double getMedian() {
    return new Double(MedianCalculator.getMedian(values));
  }

  @Override
  public Comparable getStat(String stat) {
    if (stat.equals("median")) {
      return new Double(median);
    }
    return delegate.getStat(stat);
  }
  
  public void compute(){
    delegate.compute();
    median = getMedian();
  }
  
  @Override
  public void collect(int doc) throws IOException {
    super.collect(doc);
    if (value.exists) {
      values.add(function.doubleVal(doc));
    }
  }
}
class DateMedianStatsCollector extends MedianStatsCollector{
  
  public DateMedianStatsCollector(StatsCollector delegate) {
    super(delegate);
  }

  @Override
  public Comparable getStat(String stat) {
    if (stat.equals("median")) {
      return new Date((long)median);
    }
    return delegate.getStat(stat);
  }
}
