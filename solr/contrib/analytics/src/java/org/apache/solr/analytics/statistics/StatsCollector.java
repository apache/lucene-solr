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
 * <code>StatsCollector</code> implementations reduce a list of Objects to a single value.
 * Most implementations reduce a list to a statistic on that list.
 */
public interface StatsCollector {
  
  /**
   * Collect values from the value source and add to statistics.
   * @param doc Document to collect from
   */
  void collect(int doc);
  
  /**
   * @param context The context to read documents from.
   * @throws IOException if setting next reader fails
   */
  void setNextReader(LeafReaderContext context) throws IOException;
  
  MutableValue getValue();
  FunctionValues getFunction();
  
  /**
   * @return The set of statistics being computed by the stats collector.
   */
  Set<String> getStatsList();
  
  /**
   * Return the value of the given statistic.
   * @param stat the stat
   * @return a comparable
   */
  Comparable getStat(String stat);
  
  /**
   * After all documents have been collected, this method should be
   * called to finalize the calculations of each statistic.
   */
  void compute();
  
  /**
   * @return The string representation of the value source.
   */
  String valueSourceString();
}
