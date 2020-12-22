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

import org.apache.solr.cluster.placement.CollectionMetrics;
import org.apache.solr.cluster.placement.ReplicaMetric;
import org.apache.solr.cluster.placement.ReplicaMetrics;
import org.apache.solr.cluster.placement.ShardMetrics;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Builder class for constructing instances of {@link CollectionMetrics}.
 */
public class CollectionMetricsBuilder {

  final Map<String, ShardMetricsBuilder> shardMetricsBuilders = new HashMap<>();


  public Map<String, ShardMetricsBuilder> getShardMetricsBuilders() {
    return shardMetricsBuilders;
  }

  public CollectionMetrics build() {
    final Map<String, ShardMetrics> metricsMap = new HashMap<>();
    shardMetricsBuilders.forEach((shard, builder) -> metricsMap.put(shard, builder.build()));
    return shardName -> Optional.ofNullable(metricsMap.get(shardName));
  }

  public static class ShardMetricsBuilder {
    final Map<String, ReplicaMetricsBuilder> replicaMetricsBuilders = new HashMap<>();
    ReplicaMetricsBuilder leaderMetricsBuilder;

    public Map<String, ReplicaMetricsBuilder> getReplicaMetricsBuilders() {
      return replicaMetricsBuilders;
    }

    public ShardMetricsBuilder setLeaderMetrics(ReplicaMetricsBuilder replicaMetricsBuilder) {
      leaderMetricsBuilder = replicaMetricsBuilder;
      return this;
    }

    public ShardMetrics build() {
      final Map<String, ReplicaMetrics> metricsMap = new HashMap<>();
      replicaMetricsBuilders.forEach((name, replicaBuilder) -> {
        ReplicaMetrics metrics = replicaBuilder.build();
        metricsMap.put(name, metrics);
        // skip leader from map
        if (replicaBuilder.leader) {
          if (leaderMetricsBuilder == null) {
            leaderMetricsBuilder = replicaBuilder;
          }
          if (replicaBuilder != leaderMetricsBuilder) {
            throw new RuntimeException("inconsistent data for leader metrics: found " + replicaBuilder + " but expected " + leaderMetricsBuilder);
          }
        }
      });
      final ReplicaMetrics finalLeaderMetrics = leaderMetricsBuilder != null ? leaderMetricsBuilder.build() : null;
      return new ShardMetrics() {
        @Override
        public Optional<ReplicaMetrics> getLeaderMetrics() {
          return Optional.ofNullable(finalLeaderMetrics);
        }

        @Override
        public Optional<ReplicaMetrics> getReplicaMetrics(String replicaName) {
          return Optional.ofNullable(metricsMap.get(replicaName));
        }
      };
    }
  }

  public static class ReplicaMetricsBuilder {
    final Map<ReplicaMetric<?>, Object> metrics = new HashMap<>();
    boolean leader;

    public ReplicaMetricsBuilder setLeader(boolean leader) {
      this.leader = leader;
      return this;
    }

    /** Add unconverted (raw) values here, this method internally calls
     * {@link ReplicaMetric#convert(Object)}.
     * @param metric metric to add
     * @param value raw (unconverted) metric value
     */
    public ReplicaMetricsBuilder addMetric(ReplicaMetric<?> metric, Object value) {
      value = metric.convert(value);
      if (value != null) {
        metrics.put(metric, value);
      }
      return this;
    }

    public ReplicaMetrics build() {
      return new ReplicaMetrics() {
        @Override
        @SuppressWarnings("unchecked")
        public <T> Optional<T> getReplicaMetric(ReplicaMetric<T> metric) {
          return Optional.ofNullable((T) metrics.get(metric));
        }
      };
    }
  }
}
