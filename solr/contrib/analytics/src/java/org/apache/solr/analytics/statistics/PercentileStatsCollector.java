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
import java.util.List;
import java.util.regex.Pattern;

import org.apache.solr.analytics.util.PercentileCalculator;

import com.google.common.collect.Iterables;

/**
 * <code>PercentileStatsCollector</code> computes a given list of percentiles.
 */
@SuppressWarnings("rawtypes")
public class PercentileStatsCollector extends AbstractDelegatingStatsCollector{
  public final List<Comparable> values = new ArrayList<>();
  public static final Pattern PERCENTILE_PATTERN = Pattern.compile("perc(?:entile)?_(\\d+)",Pattern.CASE_INSENSITIVE);
  protected final double[] percentiles;
  protected final String[] percentileNames;
  protected Comparable[] results;
  
  public PercentileStatsCollector(StatsCollector delegate, double[] percentiles, String[] percentileNames) {
    super(delegate);
    this.percentiles = percentiles;
    this.percentileNames = percentileNames;
  }

  @Override
  public Comparable getStat(String stat) {
    for( int i=0; i < percentiles.length; i++ ){
      if (stat.equals(percentileNames[i])) {
        if (results!=null) {
          return results[i];
        } else {
          return null;
        }
      }
    }
    return delegate.getStat(stat);
  }

  public void compute(){
    delegate.compute();
    if (values.size()>0) {
      results = Iterables.toArray(getPercentiles(),Comparable.class);
    } else {
      results = null;
    }
  }

  @SuppressWarnings({ "unchecked"})
  protected List<Comparable> getPercentiles() {
    return PercentileCalculator.getPercentiles(values, percentiles);
  }
  
  public void collect(int doc) throws IOException {
    super.collect(doc);
    if (value.exists) {
      values.add((Comparable)value.toObject());
    }
  }

}
