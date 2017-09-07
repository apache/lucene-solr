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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.util.concurrent.AtomicDouble;
import org.apache.solr.client.solrj.cloud.autoscaling.Policy;
import org.apache.solr.client.solrj.cloud.autoscaling.ReplicaInfo;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventType;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.SolrClientDataProvider;
import org.apache.solr.client.solrj.impl.ZkClientClusterStateProvider;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.AutoScalingParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.metrics.SolrCoreMetricManager;
import org.apache.solr.util.TimeSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Trigger for the {@link org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventType#SEARCHRATE} event.
 */
public class SearchRateTrigger extends TriggerBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final TimeSource timeSource;
  private final CoreContainer container;
  private final String handler;
  private final String collection;
  private final String shard;
  private final String node;
  private final double rate;
  private final Map<String, Long> lastCollectionEvent = new ConcurrentHashMap<>();
  private final Map<String, Long> lastNodeEvent = new ConcurrentHashMap<>();
  private final Map<String, Long> lastShardEvent = new ConcurrentHashMap<>();
  private final Map<String, Long> lastReplicaEvent = new ConcurrentHashMap<>();
  private final Map<String, Object> state = new HashMap<>();

  public SearchRateTrigger(String name, Map<String, Object> properties, CoreContainer container) {
    super(TriggerEventType.SEARCHRATE, name, properties, container.getResourceLoader(), container.getZkController().getZkClient());
    this.timeSource = TimeSource.CURRENT_TIME;
    this.container = container;
    this.state.put("lastCollectionEvent", lastCollectionEvent);
    this.state.put("lastNodeEvent", lastNodeEvent);
    this.state.put("lastShardEvent", lastShardEvent);
    this.state.put("lastReplicaEvent", lastReplicaEvent);

    // parse config options
    collection = (String)properties.getOrDefault(AutoScalingParams.COLLECTION, Policy.ANY);
    shard = (String)properties.getOrDefault(AutoScalingParams.SHARD, Policy.ANY);
    if (collection.equals(Policy.ANY) && !shard.equals(Policy.ANY)) {
      throw new IllegalArgumentException("When 'shard' is other than #ANY collection name must be also other than #ANY");
    }
    node = (String)properties.getOrDefault(AutoScalingParams.NODE, Policy.ANY);
    handler = (String)properties.getOrDefault(AutoScalingParams.HANDLER, "/select");

    if (properties.get("rate") == null) {
      throw new IllegalArgumentException("No 'rate' specified in configuration");
    }
    String rateString = String.valueOf(properties.get("rate"));
    try {
      rate = Double.parseDouble(rateString);
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid 'rate' configuration value: '" + rateString + "'", e);
    }
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

    Map<String, Map<String, List<ReplicaInfo>>> collectionRates = new HashMap<>();
    Map<String, AtomicDouble> nodeRates = new HashMap<>();

    try (CloudSolrClient cloudSolrClient = new CloudSolrClient.Builder()
        .withClusterStateProvider(new ZkClientClusterStateProvider(container.getZkController().getZkStateReader()))
        .build()) {

      SolrClientDataProvider dataProvider = new SolrClientDataProvider(cloudSolrClient);

      for (String node : dataProvider.getNodes()) {
        Map<String, ReplicaInfo> metricTags = new HashMap<>();
        // coll, shard, replica
        Map<String, Map<String, List<ReplicaInfo>>> infos = dataProvider.getReplicaInfo(node, Collections.emptyList());
        infos.forEach((coll, shards) -> {
          shards.forEach((sh, replicas) -> {
            replicas.forEach(replica -> {
              // we have to translate to the metrics registry name, which uses "_replica_nN" as suffix
              String replicaName = SolrCoreMetricManager.parseReplicaName(coll, replica.getCore());
              if (replicaName == null) { // should never happen???
                replicaName = replica.getName(); // which is actually coreNode name...
              }
              String registry = SolrCoreMetricManager.createRegistryName(true, coll, sh, replicaName, null);
              String tag = "metrics:" + registry
                  + ":QUERY." + handler + ".requestTimes:1minRate";
              metricTags.put(tag, replica);
            });
          });
        });
        Map<String, Object> rates = dataProvider.getNodeValues(node, metricTags.keySet());
        rates.forEach((tag, rate) -> {
          ReplicaInfo info = metricTags.get(tag);
          if (info == null) {
            log.warn("Missing replica info for response tag " + tag);
          } else {
            Map<String, List<ReplicaInfo>> perCollection = collectionRates.computeIfAbsent(info.getCollection(), s -> new HashMap<>());
            List<ReplicaInfo> perShard = perCollection.computeIfAbsent(info.getShard(), s -> new ArrayList<>());
            info.getVariables().put(AutoScalingParams.RATE, rate);
            perShard.add(info);
            AtomicDouble perNode = nodeRates.computeIfAbsent(node, s -> new AtomicDouble());
            perNode.addAndGet((Double)rate);
          }
        });
      }
    } catch (IOException e) {
      log.warn("Exception getting node values", e);
      return;
    }

    long now = timeSource.getTime();
    // check for exceeded rates and filter out those with less than waitFor from previous events
    Map<String, Double> hotNodes = nodeRates.entrySet().stream()
        .filter(entry -> node.equals(Policy.ANY) || node.equals(entry.getKey()))
        .filter(entry -> waitForElapsed(entry.getKey(), now, lastNodeEvent))
        .filter(entry -> entry.getValue().get() > rate)
        .collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue().get()));

    Map<String, Map<String, Double>> hotShards = new HashMap<>();
    Map<String, String> warmShards = new HashMap<>();
    List<ReplicaInfo> hotReplicas = new ArrayList<>();
    collectionRates.forEach((coll, shardRates) -> {
      final Object[] warmShard = new Object[2];
      shardRates.forEach((sh, replicaRates) -> {
        double shardRate = replicaRates.stream()
            .map(r -> {
              if (waitForElapsed(r.getCollection() + "." + r.getCore(), now, lastReplicaEvent) &&
                  ((Double)r.getVariable(AutoScalingParams.RATE) > rate)) {
                hotReplicas.add(r);
              }
              return r;
            })
            .mapToDouble(r -> (Double)r.getVariable(AutoScalingParams.RATE)).sum();
        if (warmShard[0] == null) {
          warmShard[0] = sh;
          warmShard[1] = shardRate;
        }
        if (shardRate > (double)warmShard[1]) {
          warmShard[0] = sh;
          warmShard[1] = shardRate;
        }
        if (waitForElapsed(coll + "." + sh, now, lastShardEvent) &&
            (shardRate > rate) &&
            (collection.equals(Policy.ANY) || collection.equals(coll)) &&
            (shard.equals(Policy.ANY) || shard.equals(sh))) {
          hotShards.computeIfAbsent(coll, s -> new HashMap<>()).put(sh, shardRate);
        }
      });
      warmShards.put(coll, (String)warmShard[0]);
    });

    Map<String, Double> hotCollections = new HashMap<>();
    collectionRates.forEach((coll, shRates) -> {
      double total = shRates.entrySet().stream()
          .mapToDouble(e -> e.getValue().stream()
              .mapToDouble(r -> (Double)r.getVariable(AutoScalingParams.RATE)).sum()).sum();
      if (waitForElapsed(coll, now, lastCollectionEvent) &&
          (total > rate) &&
          (collection.equals(Policy.ANY) || collection.equals(coll))) {
        hotCollections.put(coll, total);
      }
    });

    if (hotCollections.isEmpty() && hotShards.isEmpty() && hotReplicas.isEmpty() && hotNodes.isEmpty()) {
      return;
    }

    // generate event

    if (processor.process(new SearchRateEvent(getName(), now, hotNodes, hotCollections, hotShards, hotReplicas, warmShards))) {
      // update lastEvent times
      hotNodes.keySet().forEach(node -> lastNodeEvent.put(node, now));
      hotCollections.keySet().forEach(coll -> lastCollectionEvent.put(coll, now));
      hotShards.entrySet().forEach(e -> e.getValue()
          .forEach((sh, rate) -> lastShardEvent.put(e.getKey() + "." + sh, now)));
      hotReplicas.forEach(r -> lastReplicaEvent.put(r.getCollection() + "." + r.getCore(), now));
    }
  }

  private boolean waitForElapsed(String name, long now, Map<String, Long> lastEventMap) {
    Long lastTime = lastEventMap.computeIfAbsent(name, s -> now);
    long elapsed = TimeUnit.SECONDS.convert(now - lastTime, TimeUnit.NANOSECONDS);
    log.info("name=" + name + ", lastTime=" + lastTime + ", elapsed=" + elapsed);
    if (TimeUnit.SECONDS.convert(now - lastTime, TimeUnit.NANOSECONDS) < getWaitForSecond()) {
      return false;
    }
    return true;
  }

  public static class SearchRateEvent extends TriggerEvent {
    public SearchRateEvent(String source, long eventTime, Map<String, Double> hotNodes,
                           Map<String, Double> hotCollections,
                           Map<String, Map<String, Double>> hotShards, List<ReplicaInfo> hotReplicas,
                           Map<String, String> warmShards) {
      super(TriggerEventType.SEARCHRATE, source, eventTime, null);
      properties.put(AutoScalingParams.COLLECTION, hotCollections);
      properties.put(AutoScalingParams.SHARD, hotShards);
      properties.put(AutoScalingParams.REPLICA, hotReplicas);
      properties.put(AutoScalingParams.NODE, hotNodes);
      properties.put(AutoScalingParams.WARM_SHARD, warmShards);
    }
  }
}