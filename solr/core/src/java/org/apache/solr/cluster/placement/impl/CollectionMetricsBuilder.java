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
import org.apache.solr.cluster.placement.ReplicaMetrics;
import org.apache.solr.cluster.placement.ShardMetrics;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 *
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

    public Map<String, ReplicaMetricsBuilder> getReplicaMetricsBuilders() {
      return replicaMetricsBuilders;
    }

    public ShardMetricsBuilder setLeaderMetrics(ReplicaMetricsBuilder replicaMetricsBuilder) {
      replicaMetricsBuilders.put(LEADER, replicaMetricsBuilder);
      return this;
    }

    public static final String LEADER = "__leader__";

    public ShardMetrics build() {
      final Map<String, ReplicaMetrics> metricsMap = new HashMap<>();
      replicaMetricsBuilders.forEach((name, replicaBuilder) -> {
        ReplicaMetrics metrics = replicaBuilder.build();
        metricsMap.put(name, metrics);
        if (replicaBuilder.leader) {
          metricsMap.put(LEADER, metrics);
        }
      });
      return new ShardMetrics() {
        @Override
        public Optional<ReplicaMetrics> getLeaderMetrics() {
          return Optional.ofNullable(metricsMap.get(LEADER));
        }

        @Override
        public Optional<ReplicaMetrics> getReplicaMetrics(String replicaName) {
          return Optional.ofNullable(metricsMap.get(replicaName));
        }
      };
    }
  }

  public static class ReplicaMetricsBuilder {
    final Map<String, Object> metrics = new HashMap<>();
    Double sizeGB;
    boolean leader;

    public ReplicaMetricsBuilder setSizeGB(double size) {
      this.sizeGB = size;
      return this;
    }

    public ReplicaMetricsBuilder setLeader(boolean leader) {
      this.leader = leader;
      return this;
    }

    public ReplicaMetricsBuilder addMetric(String metricName, Object value) {
      metrics.put(metricName, value);
      return this;
    }

    public ReplicaMetrics build() {
      return new ReplicaMetrics() {
        @Override
        public Double getReplicaSizeGB() {
          return sizeGB;
        }

        @Override
        public Optional<Object> getReplicaMetric(String metricName) {
          return Optional.ofNullable(metrics.get(metricName));
        }
      };
    }
  }
}
