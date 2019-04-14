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

package org.apache.lucene.luwak;

import java.util.concurrent.TimeUnit;

/**
 * Encapsulates various configuration settings for a Monitor's query index
 */
public class QueryIndexConfiguration {

  private int queryUpdateBufferSize = 5000;
  private long purgeFrequency = 5;
  private TimeUnit purgeFrequencyUnits = TimeUnit.MINUTES;
  private QueryDecomposer queryDecomposer = new QueryDecomposer();
  private boolean storeQueries = true;

  /**
   * Set the QueryDecomposer to be used by the Monitor
   *
   * @param queryDecomposer the QueryDecomposer to be used by the Monitor
   * @return the current configuration
   */
  public QueryIndexConfiguration setQueryDecomposer(QueryDecomposer queryDecomposer) {
    this.queryDecomposer = queryDecomposer;
    return this;
  }

  /**
   * @return the QueryDecomposer used by the Monitor
   */
  public QueryDecomposer getQueryDecomposer() {
    return queryDecomposer;
  }

  /**
   * Set the frequency with with the Monitor's querycache will be garbage-collected
   *
   * @param frequency the frequency value
   * @param units     the frequency units
   * @return the current configuration
   */
  public QueryIndexConfiguration setPurgeFrequency(long frequency, TimeUnit units) {
    this.purgeFrequency = frequency;
    this.purgeFrequencyUnits = units;
    return this;
  }

  /**
   * @return the value of Monitor's querycache garbage-collection frequency
   */
  public long getPurgeFrequency() {
    return purgeFrequency;
  }

  /**
   * @return Get the units of the Monitor's querycache garbage-collection frequency
   */
  public TimeUnit getPurgeFrequencyUnits() {
    return purgeFrequencyUnits;
  }

  /**
   * Set how many queries will be buffered in memory before being committed to the queryindex
   *
   * @param size how many queries will be buffered in memory before being committed to the queryindex
   * @return the current configuration
   */
  public QueryIndexConfiguration setQueryUpdateBufferSize(int size) {
    this.queryUpdateBufferSize = size;
    return this;
  }

  /**
   * @return the size of the queryindex's in-memory buffer
   */
  public int getQueryUpdateBufferSize() {
    return queryUpdateBufferSize;
  }

  /**
   * Set whether or not the Monitor should store its MonitorQueries
   * <p>
   * If you don't need to call Monitor.getQuery() at all, you can save some memory
   * by setting this to {@code false}.
   *
   * @param storeQueries whether or not the Monitor should store its MonitorQueries
   * @return the current configuration
   */
  public QueryIndexConfiguration storeQueries(boolean storeQueries) {
    this.storeQueries = storeQueries;
    return this;
  }

  /**
   * @return whether or not the Monitor is storing its MonitorQueries
   */
  public boolean storeQueries() {
    return storeQueries;
  }

}
