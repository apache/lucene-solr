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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.util.mutable.MutableValue;

/**
 * <code>AbstractDelegationStatsCollector</code> objects wrap other StatsCollectors.
 * While they compute their own statistics they pass along all inputs and requests
 * to the delegates as well.
 */
public abstract class AbstractDelegatingStatsCollector implements StatsCollector{
  protected final StatsCollector delegate;
  protected final Set<String> statsList;
  MutableValue value;
  FunctionValues function;
  
  /**
   * @param delegate The delegate computing statistics on the same set of values.
   */
  public AbstractDelegatingStatsCollector(StatsCollector delegate) {
    this.delegate = delegate;
    this.statsList = delegate.getStatsList();
  }
  
  public void setNextReader(LeafReaderContext context) throws IOException {
    delegate.setNextReader(context);
    value = getValue();
    function = getFunction();
  }
  
  public StatsCollector delegate(){
    return delegate;
  }
  
  public Set<String> getStatsList(){
    return statsList;
  }
  
  public MutableValue getValue() {
    return delegate.getValue();
  }
  
  public FunctionValues getFunction() {
    return delegate.getFunction();
  }
  
  public void collect(int doc) {
    delegate.collect(doc);
  }
  
  public String valueSourceString() {
    return delegate.valueSourceString();
  }
}
