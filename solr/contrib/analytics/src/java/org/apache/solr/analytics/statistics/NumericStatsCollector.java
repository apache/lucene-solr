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
import java.util.Set;

import org.apache.lucene.queries.function.ValueSource;

/**
 * <code>NumericStatsCollector</code> computes the sum, sum of squares, mean and standard deviation.
 */
public class NumericStatsCollector extends MinMaxStatsCollector {
  protected double sum = 0;
  protected double sumOfSquares = 0;
  protected double mean = 0;
  protected double stddev = 0;
  
  public NumericStatsCollector(ValueSource source, Set<String> statsList) {
    super(source, statsList);
  }
  
  public void collect(int doc) throws IOException {
    super.collect(doc);
    double value = function.doubleVal(doc);
    sum += value;
    sumOfSquares += (value * value);
  }
  
  @Override
  public Comparable getStat(String stat) {
    if (stat.equals("sum")) {
      return new Double(sum);
    }
    if (stat.equals("sumofsquares")) {
      return new Double(sumOfSquares);
    }
    if (stat.equals("mean")) {
      return new Double(mean);
    }
    if (stat.equals("stddev")) {
      return new Double(stddev);
    }
    return super.getStat(stat);
  }  
  
  @Override
  public void compute(){
    super.compute();
    mean = (valueCount==0)? 0:sum / valueCount;
    stddev = (valueCount <= 1) ? 0.0D : Math.sqrt((sumOfSquares/valueCount) - (mean*mean));
  }
  
}
