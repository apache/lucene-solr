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
package org.apache.solr.cluster.placement;

import java.util.function.Function;

/**
 * Replica metric wrapper that defines a short symbolic name of the metric, the corresponding
 * internal metric name (as reported in <code>solr.core.[collection].[replica]</code> registry)
 * and the desired format/unit conversion.
 */
public class ReplicaMetric<T> extends MetricAttribute<T> {

  public static final ReplicaMetric<Double> INDEX_SIZE_GB = new ReplicaMetric<>("sizeGB", "INDEX.sizeInBytes",
      v -> {
        double sizeInBytes;
        if (!(v instanceof Number)) {
          if (v == null) {
            return null;
          }
          try {
            sizeInBytes = Double.valueOf(String.valueOf(v)).doubleValue();
          } catch (Exception nfe) {
            return null;
          }
        } else {
          sizeInBytes = ((Number) v).doubleValue();
        }
        return sizeInBytes / GB;
      });

  public static final ReplicaMetric<Double> QUERY_RATE_1MIN = new ReplicaMetric<>("queryRate", "QUERY./select.requestTimes:1minRate");
  public static final ReplicaMetric<Double> UPDATE_RATE_1MIN = new ReplicaMetric<>("updateRate", "UPDATE./update.requestTimes:1minRate");

  public ReplicaMetric(String name, String internalName) {
    super(name, internalName);
  }

  public ReplicaMetric(String name, String internalName, Function<Object, T> converter) {
    super(name, internalName, converter);
  }
}
