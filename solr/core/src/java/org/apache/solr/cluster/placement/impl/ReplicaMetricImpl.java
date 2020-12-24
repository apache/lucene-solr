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
package org.apache.solr.cluster.placement.impl;

import org.apache.solr.cluster.placement.ReplicaMetric;

import java.util.function.Function;

/**
 * Replica metric identifier, corresponding to one of the
 * internal replica-level metric names (as reported in <code>solr.core.[collection].[replica]</code> registry)
 */
public class ReplicaMetricImpl<T> extends MetricImpl<T> implements ReplicaMetric<T> {

  public static final ReplicaMetricImpl<Double> INDEX_SIZE_GB = new ReplicaMetricImpl<>("sizeGB", "INDEX.sizeInBytes", BYTES_TO_GB_CONVERTER);

  public static final ReplicaMetricImpl<Double> QUERY_RATE_1MIN = new ReplicaMetricImpl<>("queryRate", "QUERY./select.requestTimes:1minRate");
  public static final ReplicaMetricImpl<Double> UPDATE_RATE_1MIN = new ReplicaMetricImpl<>("updateRate", "UPDATE./update.requestTimes:1minRate");

  public ReplicaMetricImpl(String name, String internalName) {
    super(name, internalName);
  }

  public ReplicaMetricImpl(String name, String internalName, Function<Object, T> converter) {
    super(name, internalName, converter);
  }
}
