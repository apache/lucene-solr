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
import java.util.Locale;
import java.util.Set;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.FunctionValues.ValueFiller;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.util.mutable.MutableValue;

/**
 * <code>MinMaxStatsCollector</code> computes the min, max, number of values and number of missing values.
 */
public class MinMaxStatsCollector implements StatsCollector{
  protected long missingCount = 0;
  protected long valueCount = 0;
  protected MutableValue max;
  protected MutableValue min;
  protected MutableValue value;
  protected final Set<String> statsList;
  protected final ValueSource source;
  protected FunctionValues function;
  protected ValueFiller valueFiller;
  
  public MinMaxStatsCollector(ValueSource source, Set<String> statsList) {
    this.source = source;
    this.statsList = statsList;
  }
  
  public void setNextReader(LeafReaderContext context) throws IOException {
    function = source.getValues(null, context);
    valueFiller = function.getValueFiller();
    value = valueFiller.getValue();
  }
  
  public void collect(int doc) {
    valueFiller.fillValue(doc);
    if( value.exists ){
      valueCount += 1;
      if ( max==null ) max = value.duplicate();
      else if( !max.exists || value.compareTo(max) > 0 ) max.copy(value);
      if ( min==null ) min = value.duplicate();
      else if( !min.exists || value.compareTo(min) < 0 ) min.copy(value);
    } else {
      missingCount += 1;
    }
  }
 
  @Override
  public String toString() {
    return String.format(Locale.ROOT, "<min=%s max=%s c=%d m=%d>", min, max, valueCount, missingCount );
  }
  
  public Comparable getStat(String stat){
    if (stat.equals("min")&&min!=null) {
      return (Comparable)min.toObject();
    }
    if (stat.equals("max")&&max!=null) {
      return (Comparable)max.toObject();
    }
    if (stat.equals("count")) {
      return new Long(valueCount);
    }
    if (stat.equals("missing")) {
      return new Long(missingCount);
    }

    return null;
//    throw new IllegalArgumentException("No stat named '"+stat+"' in this collector " + this);
  }
  
  public Set<String> getStatsList() {
    return statsList;
  }

  @Override
  public void compute() {  }
  
  @Override 
  public MutableValue getValue() {
    return value;
  }
  
  @Override 
  public FunctionValues getFunction() {
    return function;
  }
  
  public String valueSourceString() {
    return source.toString();
  }
  
  public String statString(String stat) {
    return stat+"("+valueSourceString()+")";
  }
}
