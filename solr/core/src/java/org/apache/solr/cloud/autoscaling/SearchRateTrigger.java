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
package org.apache.solr.cloud.autoscaling;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AtomicDouble;
import org.apache.solr.client.solrj.cloud.autoscaling.Policy;
import org.apache.solr.client.solrj.cloud.autoscaling.ReplicaInfo;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.Suggester;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventType;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.AutoScalingParams;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.metrics.SolrCoreMetricManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Trigger for the {@link org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventType#SEARCHRATE} event.
 */
public class SearchRateTrigger extends TriggerBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String COLLECTIONS_PROP = "collections";
  public static final String METRIC_PROP = "metric";
  public static final String MAX_OPS_PROP = "maxOps";
  public static final String MIN_REPLICAS_PROP = "minReplicas";
  public static final String ABOVE_RATE_PROP = "aboveRate";
  public static final String BELOW_RATE_PROP = "belowRate";
  public static final String ABOVE_OP_PROP = "aboveOp";
  public static final String BELOW_OP_PROP = "belowOp";
  public static final String ABOVE_NODE_OP_PROP = "aboveNodeOp";
  public static final String BELOW_NODE_OP_PROP = "belowNodeOp";

  // back-compat
  public static final String BC_COLLECTION_PROP = "collection";
  public static final String BC_RATE_PROP = "rate";


  public static final String HOT_NODES = "hotNodes";
  public static final String HOT_COLLECTIONS = "hotCollections";
  public static final String HOT_SHARDS = "hotShards";
  public static final String HOT_REPLICAS = "hotReplicas";
  public static final String COLD_NODES = "coldNodes";
  public static final String COLD_COLLECTIONS = "coldCollections";
  public static final String COLD_SHARDS = "coldShards";
  public static final String COLD_REPLICAS = "coldReplicas";

  public static final int DEFAULT_MAX_OPS = 3;
  public static final String DEFAULT_METRIC = "QUERY./select.requestTimes:1minRate";

  private String metric;
  private int maxOps;
  private Integer minReplicas = null;
  private final Set<String> collections = new HashSet<>();
  private String shard;
  private String node;
  private double aboveRate;
  private double belowRate;
  private CollectionParams.CollectionAction aboveOp, belowOp, aboveNodeOp, belowNodeOp;
  private final Map<String, Long> lastCollectionEvent = new ConcurrentHashMap<>();
  private final Map<String, Long> lastNodeEvent = new ConcurrentHashMap<>();
  private final Map<String, Long> lastShardEvent = new ConcurrentHashMap<>();
  private final Map<String, Long> lastReplicaEvent = new ConcurrentHashMap<>();
  private final Map<String, Object> state = new HashMap<>();

  public SearchRateTrigger(String name) {
    super(TriggerEventType.SEARCHRATE, name);
    this.state.put("lastCollectionEvent", lastCollectionEvent);
    this.state.put("lastNodeEvent", lastNodeEvent);
    this.state.put("lastShardEvent", lastShardEvent);
    this.state.put("lastReplicaEvent", lastReplicaEvent);
    TriggerUtils.validProperties(validProperties,
        COLLECTIONS_PROP, AutoScalingParams.SHARD, AutoScalingParams.NODE,
        METRIC_PROP,
        MAX_OPS_PROP,
        MIN_REPLICAS_PROP,
        ABOVE_OP_PROP,
        BELOW_OP_PROP,
        ABOVE_NODE_OP_PROP,
        BELOW_NODE_OP_PROP,
        ABOVE_RATE_PROP,
        BELOW_RATE_PROP,
        // back-compat props
        BC_COLLECTION_PROP,
        BC_RATE_PROP);
  }

  @Override
  public void configure(SolrResourceLoader loader, SolrCloudManager cloudManager, Map<String, Object> properties) throws TriggerValidationException {
    super.configure(loader, cloudManager, properties);
    // parse config options
    String collectionsStr = (String)properties.get(COLLECTIONS_PROP);
    if (collectionsStr != null) {
      collections.addAll(StrUtils.splitSmart(collectionsStr, ','));
    }
    // check back-compat collection prop
    collectionsStr = (String)properties.get(BC_COLLECTION_PROP);
    if (collectionsStr != null) {
      if (!collectionsStr.equals(Policy.ANY)) {
        collections.add(collectionsStr);
      }
    }
    shard = (String)properties.getOrDefault(AutoScalingParams.SHARD, Policy.ANY);
    if (!shard.equals(Policy.ANY) && (collections.isEmpty() || collections.size() > 1)) {
      throw new TriggerValidationException(name, AutoScalingParams.SHARD, "When 'shard' is other than #ANY then exactly one collection name must be set");
    }
    node = (String)properties.getOrDefault(AutoScalingParams.NODE, Policy.ANY);
    metric = (String)properties.getOrDefault(METRIC_PROP, DEFAULT_METRIC);

    String maxOpsStr = String.valueOf(properties.getOrDefault(MAX_OPS_PROP, DEFAULT_MAX_OPS));
    try {
      maxOps = Integer.parseInt(maxOpsStr);
    } catch (Exception e) {
      throw new TriggerValidationException(name, MAX_OPS_PROP, "invalid value '" + maxOpsStr + "': " + e.toString());
    }

    Object o = properties.get(MIN_REPLICAS_PROP);
    if (o != null) {
      try {
        minReplicas = Integer.parseInt(o.toString());
        if (minReplicas < 1) {
          throw new Exception("must be at least 1, or not set to use 'replicationFactor'");
        }
      } catch (Exception e) {
        throw new TriggerValidationException(name, MIN_REPLICAS_PROP, "invalid value '" + o + "': " + e.toString());
      }
    }

    Object above = properties.get(ABOVE_RATE_PROP);
    Object below = properties.get(BELOW_RATE_PROP);
    // back-compat rate prop
    if (properties.containsKey(BC_RATE_PROP)) {
      above = properties.get(BC_RATE_PROP);
    }
    if (above == null && below == null) {
      throw new TriggerValidationException(name, ABOVE_RATE_PROP, "at least one of '" +
      ABOVE_RATE_PROP + "' or '" + BELOW_RATE_PROP + "' must be set");
    }
    if (above != null) {
      try {
        aboveRate = Double.parseDouble(String.valueOf(above));
      } catch (Exception e) {
        throw new TriggerValidationException(name, ABOVE_RATE_PROP, "Invalid configuration value: '" + above + "': " + e.toString());
      }
    } else {
      aboveRate = Double.MAX_VALUE;
    }
    if (below != null) {
      try {
        belowRate = Double.parseDouble(String.valueOf(below));
      } catch (Exception e) {
        throw new TriggerValidationException(name, BELOW_RATE_PROP, "Invalid configuration value: '" + below + "': " + e.toString());
      }
    } else {
      belowRate = -1;
    }

    String aboveOpStr = String.valueOf(properties.getOrDefault(ABOVE_OP_PROP, CollectionParams.CollectionAction.ADDREPLICA.toLower()));
    String belowOpStr = String.valueOf(properties.getOrDefault(BELOW_OP_PROP, CollectionParams.CollectionAction.DELETEREPLICA.toLower()));
    aboveOp = CollectionParams.CollectionAction.get(aboveOpStr);
    if (aboveOp == null) {
      throw new TriggerValidationException(getName(), ABOVE_OP_PROP, "unrecognized value: '" + aboveOpStr + "'");
    }
    belowOp = CollectionParams.CollectionAction.get(belowOpStr);
    if (belowOp == null) {
      throw new TriggerValidationException(getName(), BELOW_OP_PROP, "unrecognized value: '" + belowOpStr + "'");
    }
    Object aboveNodeObj = properties.getOrDefault(ABOVE_NODE_OP_PROP, CollectionParams.CollectionAction.MOVEREPLICA.toLower());
    // do NOT set the default to DELETENODE
    Object belowNodeObj = properties.get(BELOW_NODE_OP_PROP);
    try {
      aboveNodeOp = CollectionParams.CollectionAction.get(String.valueOf(aboveNodeObj));
    } catch (Exception e) {
      throw new TriggerValidationException(getName(), ABOVE_NODE_OP_PROP, "unrecognized value: '" + aboveNodeObj + "'");
    }
    if (belowNodeObj != null) {
      try {
        belowNodeOp = CollectionParams.CollectionAction.get(String.valueOf(belowNodeObj));
      } catch (Exception e) {
        throw new TriggerValidationException(getName(), BELOW_NODE_OP_PROP, "unrecognized value: '" + belowNodeObj + "'");
      }
    }
  }

  @VisibleForTesting
  Map<String, Object> getConfig() {
    Map<String, Object> config = new HashMap<>();
    config.put("name", name);
    config.put(COLLECTIONS_PROP, collections);
    config.put(AutoScalingParams.SHARD, shard);
    config.put(AutoScalingParams.NODE, node);
    config.put(METRIC_PROP, metric);
    config.put(MAX_OPS_PROP, maxOps);
    config.put(MIN_REPLICAS_PROP, minReplicas);
    config.put(ABOVE_RATE_PROP, aboveRate);
    config.put(BELOW_RATE_PROP, belowRate);
    config.put(ABOVE_OP_PROP, aboveOp);
    config.put(ABOVE_NODE_OP_PROP, aboveNodeOp);
    config.put(BELOW_OP_PROP, belowOp);
    config.put(BELOW_NODE_OP_PROP, belowNodeOp);
    return config;
  }

  @Override
  protected Map<String, Object> getState() {
    return state;
  }

  @Override
  protected void setState(Map<String, Object> state) {
    lastCollectionEvent.clear();
    lastNodeEvent.clear();
    lastShardEvent.clear();
    lastReplicaEvent.clear();
    Map<String, Long> collTimes = (Map<String, Long>)state.get("lastCollectionEvent");
    if (collTimes != null) {
      lastCollectionEvent.putAll(collTimes);
    }
    Map<String, Long> nodeTimes = (Map<String, Long>)state.get("lastNodeEvent");
    if (nodeTimes != null) {
      lastNodeEvent.putAll(nodeTimes);
    }
    Map<String, Long> shardTimes = (Map<String, Long>)state.get("lastShardEvent");
    if (shardTimes != null) {
      lastShardEvent.putAll(shardTimes);
    }
    Map<String, Long> replicaTimes = (Map<String, Long>)state.get("lastReplicaEvent");
    if (replicaTimes != null) {
      lastReplicaEvent.putAll(replicaTimes);
    }
  }

  @Override
  public void restoreState(AutoScaling.Trigger old) {
    assert old.isClosed();
    if (old instanceof SearchRateTrigger) {
      SearchRateTrigger that = (SearchRateTrigger)old;
      assert this.name.equals(that.name);
      this.lastCollectionEvent.clear();
      this.lastNodeEvent.clear();
      this.lastShardEvent.clear();
      this.lastReplicaEvent.clear();
      this.lastCollectionEvent.putAll(that.lastCollectionEvent);
      this.lastNodeEvent.putAll(that.lastNodeEvent);
      this.lastShardEvent.putAll(that.lastShardEvent);
      this.lastReplicaEvent.putAll(that.lastReplicaEvent);
    } else {
      throw new SolrException(SolrException.ErrorCode.INVALID_STATE,
          "Unable to restore state from an unknown type of trigger");
    }

  }

  @Override
  public void run() {
    AutoScaling.TriggerEventProcessor processor = processorRef.get();
    if (processor == null) {
      return;
    }

    // collection, shard, list(replica + rate)
    Map<String, Map<String, List<ReplicaInfo>>> collectionRates = new HashMap<>();
    // node, rate
    Map<String, AtomicDouble> nodeRates = new HashMap<>();
    // this replication factor only considers replica types that are searchable
    // collection, shard, RF
    Map<String, Map<String, AtomicInteger>> searchableReplicationFactors = new HashMap<>();

    ClusterState clusterState = null;
    try {
      clusterState = cloudManager.getClusterStateProvider().getClusterState();
    } catch (IOException e) {
      log.warn("Error getting ClusterState", e);
      return;
    }
    for (String node : cloudManager.getClusterStateProvider().getLiveNodes()) {
      Map<String, ReplicaInfo> metricTags = new HashMap<>();
      // coll, shard, replica
      Map<String, Map<String, List<ReplicaInfo>>> infos = cloudManager.getNodeStateProvider().getReplicaInfo(node, Collections.emptyList());
      infos.forEach((coll, shards) -> {
        Map<String, AtomicInteger> replPerShard = searchableReplicationFactors.computeIfAbsent(coll, c -> new HashMap<>());
        shards.forEach((sh, replicas) -> {
          AtomicInteger repl = replPerShard.computeIfAbsent(sh, s -> new AtomicInteger());
          replicas.forEach(replica -> {
            // skip non-active replicas
            if (replica.getState() != Replica.State.ACTIVE) {
              return;
            }
            repl.incrementAndGet();
            // we have to translate to the metrics registry name, which uses "_replica_nN" as suffix
            String replicaName = Utils.parseMetricsReplicaName(coll, replica.getCore());
            if (replicaName == null) { // should never happen???
              replicaName = replica.getName(); // which is actually coreNode name...
            }
            String registry = SolrCoreMetricManager.createRegistryName(true, coll, sh, replicaName, null);
            String tag = "metrics:" + registry + ":" + metric;
            metricTags.put(tag, replica);
          });
        });
      });
      if (metricTags.isEmpty()) {
        continue;
      }
      Map<String, Object> rates = cloudManager.getNodeStateProvider().getNodeValues(node, metricTags.keySet());
      rates.forEach((tag, rate) -> {
        ReplicaInfo info = metricTags.get(tag);
        if (info == null) {
          log.warn("Missing replica info for response tag " + tag);
        } else {
          Map<String, List<ReplicaInfo>> perCollection = collectionRates.computeIfAbsent(info.getCollection(), s -> new HashMap<>());
          List<ReplicaInfo> perShard = perCollection.computeIfAbsent(info.getShard(), s -> new ArrayList<>());
          info = (ReplicaInfo)info.clone();
          info.getVariables().put(AutoScalingParams.RATE, ((Number)rate).doubleValue());
          perShard.add(info);
          AtomicDouble perNode = nodeRates.computeIfAbsent(node, s -> new AtomicDouble());
          perNode.addAndGet(((Number)rate).doubleValue());
        }
      });
    }

    long now = cloudManager.getTimeSource().getTimeNs();
    Map<String, Double> hotNodes = new HashMap<>();
    Map<String, Double> coldNodes = new HashMap<>();
    // check for exceeded rates and filter out those with less than waitFor from previous events
    nodeRates.entrySet().stream()
        .filter(entry -> node.equals(Policy.ANY) || node.equals(entry.getKey()))
        .forEach(entry -> {
          if (entry.getValue().get() > aboveRate) {
            if (waitForElapsed(entry.getKey(), now, lastNodeEvent)) {
              hotNodes.put(entry.getKey(), entry.getValue().get());
            }
          } else if (entry.getValue().get() < belowRate) {
            if (waitForElapsed(entry.getKey(), now, lastNodeEvent)) {
              coldNodes.put(entry.getKey(), entry.getValue().get());
            }
          } else {
            // no violation - clear waitForElapsed
            // (violation is only valid if it persists throughout waitFor)
            lastNodeEvent.remove(entry.getKey());
          }
        });

    Map<String, Map<String, Double>> hotShards = new HashMap<>();
    Map<String, Map<String, Double>> coldShards = new HashMap<>();
    List<ReplicaInfo> hotReplicas = new ArrayList<>();
    List<ReplicaInfo> coldReplicas = new ArrayList<>();
    collectionRates.forEach((coll, shardRates) -> {
      shardRates.forEach((sh, replicaRates) -> {
        double shardRate = replicaRates.stream()
            .map(r -> {
              String elapsedKey = r.getCollection() + "." + r.getCore();
              if ((Double)r.getVariable(AutoScalingParams.RATE) > aboveRate) {
                if (waitForElapsed(elapsedKey, now, lastReplicaEvent)) {
                  hotReplicas.add(r);
                }
              } else if ((Double)r.getVariable(AutoScalingParams.RATE) < belowRate) {
                if (waitForElapsed(elapsedKey, now, lastReplicaEvent)) {
                  coldReplicas.add(r);
                }
              } else {
                // no violation - clear waitForElapsed
                lastReplicaEvent.remove(elapsedKey);
              }
              return r;
            })
            .mapToDouble(r -> (Double)r.getVariable(AutoScalingParams.RATE)).sum();
        String elapsedKey = coll + "." + sh;
        if ((collections.isEmpty() || collections.contains(coll)) &&
            (shard.equals(Policy.ANY) || shard.equals(sh))) {
          if (shardRate > aboveRate) {
            if (waitForElapsed(elapsedKey, now, lastShardEvent)) {
              hotShards.computeIfAbsent(coll, s -> new HashMap<>()).put(sh, shardRate);
            }
          } else if (shardRate < belowRate) {
            if (waitForElapsed(elapsedKey, now, lastShardEvent)) {
              coldShards.computeIfAbsent(coll, s -> new HashMap<>()).put(sh, shardRate);
            }
          } else {
            // no violation - clear waitForElapsed
            lastShardEvent.remove(elapsedKey);
          }
        }
      });
    });

    Map<String, Double> hotCollections = new HashMap<>();
    Map<String, Double> coldCollections = new HashMap<>();
    collectionRates.forEach((coll, shardRates) -> {
      double total = shardRates.entrySet().stream()
          .mapToDouble(e -> e.getValue().stream()
              .mapToDouble(r -> (Double)r.getVariable(AutoScalingParams.RATE)).sum()).sum();
      if (collections.isEmpty() || collections.contains(coll)) {
        if (total > aboveRate) {
          if (waitForElapsed(coll, now, lastCollectionEvent)) {
            hotCollections.put(coll, total);
          }
        } else if (total < belowRate) {
          if (waitForElapsed(coll, now, lastCollectionEvent)) {
            coldCollections.put(coll, total);
          }
        } else {
          // no violation - clear waitForElapsed
          lastCollectionEvent.remove(coll);
        }
      }
    });

    if (hotCollections.isEmpty() &&
        hotShards.isEmpty() &&
        hotReplicas.isEmpty() &&
        hotNodes.isEmpty() &&
        coldCollections.isEmpty() &&
        coldShards.isEmpty() &&
        coldReplicas.isEmpty() &&
        coldNodes.isEmpty()) {
      return;
    }

    // generate event

    // find the earliest time when a condition was exceeded
    final AtomicLong eventTime = new AtomicLong(now);
    hotCollections.forEach((c, r) -> {
      long time = lastCollectionEvent.get(c);
      if (eventTime.get() > time) {
        eventTime.set(time);
      }
    });
    coldCollections.forEach((c, r) -> {
      long time = lastCollectionEvent.get(c);
      if (eventTime.get() > time) {
        eventTime.set(time);
      }
    });
    hotShards.forEach((c, shards) -> {
      shards.forEach((s, r) -> {
        long time = lastShardEvent.get(c + "." + s);
        if (eventTime.get() > time) {
          eventTime.set(time);
        }
      });
    });
    coldShards.forEach((c, shards) -> {
      shards.forEach((s, r) -> {
        long time = lastShardEvent.get(c + "." + s);
        if (eventTime.get() > time) {
          eventTime.set(time);
        }
      });
    });
    hotReplicas.forEach(r -> {
      long time = lastReplicaEvent.get(r.getCollection() + "." + r.getCore());
      if (eventTime.get() > time) {
        eventTime.set(time);
      }
    });
    coldReplicas.forEach(r -> {
      long time = lastReplicaEvent.get(r.getCollection() + "." + r.getCore());
      if (eventTime.get() > time) {
        eventTime.set(time);
      }
    });
    hotNodes.forEach((n, r) -> {
      long time = lastNodeEvent.get(n);
      if (eventTime.get() > time) {
        eventTime.set(time);
      }
    });
    coldNodes.forEach((n, r) -> {
      long time = lastNodeEvent.get(n);
      if (eventTime.get() > time) {
        eventTime.set(time);
      }
    });

    final List<TriggerEvent.Op> ops = new ArrayList<>();

    calculateHotOps(ops, searchableReplicationFactors, hotNodes, hotCollections, hotShards, hotReplicas);
    calculateColdOps(ops, clusterState, searchableReplicationFactors, coldNodes, coldCollections, coldShards, coldReplicas);

    if (ops.isEmpty()) {
      return;
    }

    if (processor.process(new SearchRateEvent(getName(), eventTime.get(), ops,
        hotNodes, hotCollections, hotShards, hotReplicas,
        coldNodes, coldCollections, coldShards, coldReplicas))) {
      // update lastEvent times
      hotNodes.keySet().forEach(node -> lastNodeEvent.put(node, now));
      coldNodes.keySet().forEach(node -> lastNodeEvent.put(node, now));
      hotCollections.keySet().forEach(coll -> lastCollectionEvent.put(coll, now));
      coldCollections.keySet().forEach(coll -> lastCollectionEvent.put(coll, now));
      hotShards.entrySet().forEach(e -> e.getValue()
          .forEach((sh, rate) -> lastShardEvent.put(e.getKey() + "." + sh, now)));
      coldShards.entrySet().forEach(e -> e.getValue()
          .forEach((sh, rate) -> lastShardEvent.put(e.getKey() + "." + sh, now)));
      hotReplicas.forEach(r -> lastReplicaEvent.put(r.getCollection() + "." + r.getCore(), now));
      coldReplicas.forEach(r -> lastReplicaEvent.put(r.getCollection() + "." + r.getCore(), now));
    }
  }

  private void calculateHotOps(List<TriggerEvent.Op> ops,
                               Map<String, Map<String, AtomicInteger>> searchableReplicationFactors,
                               Map<String, Double> hotNodes,
                               Map<String, Double> hotCollections,
                               Map<String, Map<String, Double>> hotShards,
                               List<ReplicaInfo> hotReplicas) {
    // calculate the number of replicas to add to each hot shard, based on how much the rate was
    // exceeded - but within limits.

    // first resolve a situation when only a node is hot but no collection / shard / replica is hot
    // TODO: eventually we may want to commission a new node
    if (!hotNodes.isEmpty() && hotShards.isEmpty() && hotCollections.isEmpty() && hotReplicas.isEmpty()) {
      // move replicas around
      if (aboveNodeOp != null) {
        hotNodes.forEach((n, r) -> {
          ops.add(new TriggerEvent.Op(aboveNodeOp, Suggester.Hint.SRC_NODE, n));
        });
      }
    } else {
      // add replicas
      Map<String, Map<String, List<Pair<String, String>>>> hints = new HashMap<>();

      hotShards.forEach((coll, shards) -> shards.forEach((s, r) -> {
        List<Pair<String, String>> perShard = hints
            .computeIfAbsent(coll, c -> new HashMap<>())
            .computeIfAbsent(s, sh -> new ArrayList<>());
        addReplicaHints(coll, s, r, searchableReplicationFactors.get(coll).get(s).get(), perShard);
      }));
      hotReplicas.forEach(ri -> {
        double r = (Double)ri.getVariable(AutoScalingParams.RATE);
        // add only if not already accounted for in hotShards
        List<Pair<String, String>> perShard = hints
            .computeIfAbsent(ri.getCollection(), c -> new HashMap<>())
            .computeIfAbsent(ri.getShard(), sh -> new ArrayList<>());
        if (perShard.isEmpty()) {
          addReplicaHints(ri.getCollection(), ri.getShard(), r, searchableReplicationFactors.get(ri.getCollection()).get(ri.getShard()).get(), perShard);
        }
      });

      hints.values().forEach(m -> m.values().forEach(lst -> lst.forEach(p -> {
        ops.add(new TriggerEvent.Op(aboveOp, Suggester.Hint.COLL_SHARD, p));
      })));
    }

  }

  /**
   * This method implements a primitive form of proportional controller with a limiter.
   */
  private void addReplicaHints(String collection, String shard, double r, int replicationFactor, List<Pair<String, String>> hints) {
    int numReplicas = (int)Math.round((r - aboveRate) / (double) replicationFactor);
    // in one event add at least 1 replica
    if (numReplicas < 1) {
      numReplicas = 1;
    }
    // ... and at most maxOps replicas
    if (numReplicas > maxOps) {
      numReplicas = maxOps;
    }
    for (int i = 0; i < numReplicas; i++) {
      hints.add(new Pair(collection, shard));
    }
  }

  private void calculateColdOps(List<TriggerEvent.Op> ops,
                                ClusterState clusterState,
                                Map<String, Map<String, AtomicInteger>> searchableReplicationFactors,
                                Map<String, Double> coldNodes,
                                Map<String, Double> coldCollections,
                                Map<String, Map<String, Double>> coldShards,
                                List<ReplicaInfo> coldReplicas) {
    // COLD COLLECTIONS
    // Probably can't do anything reasonable about whole cold collections
    // because they may be needed even if not used.

    // COLD SHARDS:
    // Cold shards mean that there are too many replicas per shard - but it also
    // means that all replicas in these shards are cold too, so we can simply
    // address this by deleting cold replicas

    // COLD REPLICAS:
    // Remove cold replicas but only when there's at least a minimum number of searchable
    // replicas still available (additional non-searchable replicas may exist, too)
    // NOTE: do this before adding ops for DELETENODE because we don't want to attempt
    // deleting replicas that have been already moved elsewhere
    Map<String, Map<String, List<ReplicaInfo>>> byCollectionByShard = new HashMap<>();
    coldReplicas.forEach(ri -> {
      byCollectionByShard.computeIfAbsent(ri.getCollection(), c -> new HashMap<>())
          .computeIfAbsent(ri.getShard(), s -> new ArrayList<>())
          .add(ri);
    });
    byCollectionByShard.forEach((coll, shards) -> {
      shards.forEach((shard, replicas) -> {
        // only delete if there's at least minRF searchable replicas left
        int rf = searchableReplicationFactors.get(coll).get(shard).get();
        // we only really need a leader and we may be allowed to remove other replicas
        int minRF = 1;
        // but check the official RF and don't go below that
        Integer RF = clusterState.getCollection(coll).getReplicationFactor();
        if (RF != null) {
          minRF = RF;
        }
        // unless minReplicas is set explicitly
        if (minReplicas != null) {
          minRF = minReplicas;
        }
        if (minRF < 1) {
          minRF = 1;
        }
        if (rf > minRF) {
          // delete at most maxOps replicas at a time
          AtomicInteger limit = new AtomicInteger(Math.min(maxOps, rf - minRF));
          replicas.forEach(ri -> {
            if (limit.get() == 0) {
              return;
            }
            // don't delete a leader
            if (ri.getBool(ZkStateReader.LEADER_PROP, false)) {
              return;
            }
            TriggerEvent.Op op = new TriggerEvent.Op(belowOp,
                Suggester.Hint.COLL_SHARD, new Pair<>(ri.getCollection(), ri.getShard()));
            op.addHint(Suggester.Hint.REPLICA, ri.getName());
            ops.add(op);
            limit.decrementAndGet();
          });
        }
      });
    });

    // COLD NODES:
    // Unlike the case of hot nodes, if a node is cold then any monitored
    // collections / shards / replicas located on that node are cold, too.
    // HOWEVER, we check only non-pull replicas and only from selected collections / shards,
    // so deleting a cold node is dangerous because it may interfere with these
    // non-monitored resources - this is the reason the default belowNodeOp is null / ignored.
    //
    // Also, note that due to the way activity is measured only nodes that contain any
    // monitored resources are considered - there may be cold nodes in the cluster that don't
    // belong to the monitored collections and they will be ignored.
    if (belowNodeOp != null) {
      coldNodes.forEach((node, rate) -> {
        ops.add(new TriggerEvent.Op(belowNodeOp, Suggester.Hint.SRC_NODE, node));
      });
    }


  }

  private boolean waitForElapsed(String name, long now, Map<String, Long> lastEventMap) {
    Long lastTime = lastEventMap.computeIfAbsent(name, s -> now);
    long elapsed = TimeUnit.SECONDS.convert(now - lastTime, TimeUnit.NANOSECONDS);
    log.trace("name={}, lastTime={}, elapsed={}", name, lastTime, elapsed);
    if (TimeUnit.SECONDS.convert(now - lastTime, TimeUnit.NANOSECONDS) < getWaitForSecond()) {
      return false;
    }
    return true;
  }

  public static class SearchRateEvent extends TriggerEvent {
    public SearchRateEvent(String source, long eventTime, List<Op> ops,
                           Map<String, Double> hotNodes,
                           Map<String, Double> hotCollections,
                           Map<String, Map<String, Double>> hotShards,
                           List<ReplicaInfo> hotReplicas,
                           Map<String, Double> coldNodes,
                           Map<String, Double> coldCollections,
                           Map<String, Map<String, Double>> coldShards,
                           List<ReplicaInfo> coldReplicas) {
      super(TriggerEventType.SEARCHRATE, source, eventTime, null);
      properties.put(TriggerEvent.REQUESTED_OPS, ops);
      properties.put(HOT_NODES, hotNodes);
      properties.put(HOT_COLLECTIONS, hotCollections);
      properties.put(HOT_SHARDS, hotShards);
      properties.put(HOT_REPLICAS, hotReplicas);
      properties.put(COLD_NODES, coldNodes);
      properties.put(COLD_COLLECTIONS, coldCollections);
      properties.put(COLD_SHARDS, coldShards);
      properties.put(COLD_REPLICAS, coldReplicas);
    }
  }
}