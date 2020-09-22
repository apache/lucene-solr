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
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.cloud.autoscaling.Policy;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.Suggester;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventType;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.params.AutoScalingParams;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.util.Pair;
import org.apache.solr.core.SolrResourceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.AutoScalingParams.ABOVE;
import static org.apache.solr.common.params.AutoScalingParams.BELOW;
import static org.apache.solr.common.params.AutoScalingParams.METRIC;
import static org.apache.solr.common.params.AutoScalingParams.PREFERRED_OP;

/**
 *
 * @deprecated to be removed in Solr 9.0 (see SOLR-14656)
 */
public class MetricTrigger extends TriggerBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private String metric;
  private Number above, below;
  private String collection, shard, node, preferredOp;

  private final Map<String, Long> lastNodeEvent = new ConcurrentHashMap<>();

  public MetricTrigger(String name) {
    super(TriggerEventType.METRIC, name);
    TriggerUtils.requiredProperties(requiredProperties, validProperties, METRIC);
    TriggerUtils.validProperties(validProperties, ABOVE, BELOW, PREFERRED_OP,
        AutoScalingParams.COLLECTION,
        AutoScalingParams.SHARD,
        AutoScalingParams.NODE);
  }

  @Override
  public void configure(SolrResourceLoader loader, SolrCloudManager cloudManager, Map<String, Object> properties) throws TriggerValidationException {
    super.configure(loader, cloudManager, properties);
    this.metric = (String) properties.get(METRIC);
    this.above = (Number) properties.get(ABOVE);
    this.below = (Number) properties.get(BELOW);
    this.collection = (String) properties.getOrDefault(AutoScalingParams.COLLECTION, Policy.ANY);
    shard = (String) properties.getOrDefault(AutoScalingParams.SHARD, Policy.ANY);
    if (collection.equals(Policy.ANY) && !shard.equals(Policy.ANY)) {
      throw new TriggerValidationException("shard", "When 'shard' is other than #ANY then collection name must be also other than #ANY");
    }
    node = (String) properties.getOrDefault(AutoScalingParams.NODE, Policy.ANY);
    preferredOp = (String) properties.getOrDefault(PREFERRED_OP, CollectionParams.CollectionAction.MOVEREPLICA.toLower());
  }

  @Override
  protected Map<String, Object> getState() {
    return null;
  }

  @Override
  protected void setState(Map<String, Object> state) {
    lastNodeEvent.clear();
    @SuppressWarnings({"unchecked"})
    Map<String, Long> nodeTimes = (Map<String, Long>) state.get("lastNodeEvent");
    if (nodeTimes != null) {
      lastNodeEvent.putAll(nodeTimes);
    }
  }

  @Override
  public void restoreState(AutoScaling.Trigger old) {
    assert old.isClosed();
    if (old instanceof MetricTrigger) {
      MetricTrigger that = (MetricTrigger) old;
      assert this.name.equals(that.name);
      this.lastNodeEvent.clear();
      this.lastNodeEvent.putAll(that.lastNodeEvent);
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

    Set<String> liveNodes = null;
    if (node.equals(Policy.ANY)) {
      if (collection.equals(Policy.ANY)) {
        liveNodes = cloudManager.getClusterStateProvider().getLiveNodes();
      } else {
        final Set<String> nodes = new HashSet<>();
        ClusterState.CollectionRef ref = cloudManager.getClusterStateProvider().getState(collection);
        DocCollection docCollection;
        if (ref == null || (docCollection = ref.get()) == null) {
          log.warn("MetricTrigger could not find collection: {}", collection);
          return;
        }
        if (shard.equals(Policy.ANY)) {
          docCollection.getReplicas().forEach(replica -> {
            nodes.add(replica.getNodeName());
          });
        } else {
          Slice slice = docCollection.getSlice(shard);
          if (slice == null) {
            log.warn("MetricTrigger could not find collection: {} shard: {}", collection, shard);
            return;
          }
          slice.getReplicas().forEach(replica -> nodes.add(replica.getNodeName()));
        }
        liveNodes = nodes;
      }
    } else {
      liveNodes = Collections.singleton(node);
    }

    Map<String, Number> rates = new HashMap<>(liveNodes.size());
    for (String node : liveNodes) {
      Map<String, Object> values = cloudManager.getNodeStateProvider().getNodeValues(node, Collections.singletonList(metric));
      values.forEach((tag, rate) -> rates.computeIfAbsent(node, s -> (Number) rate));
    }

    long now = cloudManager.getTimeSource().getTimeNs();
    // check for exceeded rates and filter out those with less than waitFor from previous events
    Map<String, Number> hotNodes = rates.entrySet().stream()
        .filter(entry -> waitForElapsed(entry.getKey(), now, lastNodeEvent))
        .filter(entry -> (below != null && Double.compare(entry.getValue().doubleValue(), below.doubleValue()) < 0) || (above != null && Double.compare(entry.getValue().doubleValue(), above.doubleValue()) > 0))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    if (hotNodes.isEmpty()) return;

    final AtomicLong eventTime = new AtomicLong(now);
    hotNodes.forEach((n, r) -> {
      long time = lastNodeEvent.get(n);
      if (eventTime.get() > time) {
        eventTime.set(time);
      }
    });

    if (processor.process(new MetricBreachedEvent(getName(), collection, shard, preferredOp, eventTime.get(), metric, hotNodes))) {
      hotNodes.keySet().forEach(node -> lastNodeEvent.put(node, now));
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

  public static class MetricBreachedEvent extends TriggerEvent {
    public MetricBreachedEvent(String source, String collection, String shard, String preferredOp, long eventTime, String metric, Map<String, Number> hotNodes) {
      super(TriggerEventType.METRIC, source, eventTime, null);
      properties.put(METRIC, metric);
      properties.put(AutoScalingParams.NODE, hotNodes);
      if (!collection.equals(Policy.ANY)) {
        properties.put(AutoScalingParams.COLLECTION, collection);
      }
      if (!shard.equals(Policy.ANY))  {
        properties.put(AutoScalingParams.SHARD, shard);
      }
      properties.put(PREFERRED_OP, preferredOp);

      // specify requested ops
      List<Op> ops = new ArrayList<>(hotNodes.size());
      for (String n : hotNodes.keySet()) {
        Op op = new Op(CollectionParams.CollectionAction.get(preferredOp));
        op.addHint(Suggester.Hint.SRC_NODE, n);
        if (!collection.equals(Policy.ANY)) {
          if (!shard.equals(Policy.ANY)) {
            op.addHint(Suggester.Hint.COLL_SHARD, new Pair<>(collection, shard));
          } else {
            op.addHint(Suggester.Hint.COLL, collection);
          }
        }
        ops.add(op);
      }
      properties.put(TriggerEvent.REQUESTED_OPS, ops);
    }
  }
}
