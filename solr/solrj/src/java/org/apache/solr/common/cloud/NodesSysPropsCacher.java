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

package org.apache.solr.common.cloud;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.cloud.NodeStateProvider;
import org.apache.solr.client.solrj.routing.PreferenceRule;
import org.apache.solr.common.SolrCloseable;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.CommonTestInjection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.rule.ImplicitSnitch.SYSPROP;

/**
 * Caching other nodes system properties. The properties that will be cached based on the value define in
 * {@link org.apache.solr.common.cloud.ZkStateReader#DEFAULT_SHARD_PREFERENCES } of
 * {@link org.apache.solr.common.cloud.ZkStateReader#CLUSTER_PROPS }.
 * If that key does not present then this cacher will do nothing.
 *
 * The cache will be refresh whenever /live_nodes get changed.
 */
public class NodesSysPropsCacher implements SolrCloseable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final int NUM_RETRY = 5;

  private final AtomicBoolean isRunning = new AtomicBoolean(false);
  private final NodeStateProvider nodeStateProvider;
  private Map<String, String> additionalProps = CommonTestInjection.injectAdditionalProps();
  private final String currentNode;
  private final ConcurrentHashMap<String, Map<String, Object>> cache = new ConcurrentHashMap<>();
  private final AtomicInteger fetchCounting = new AtomicInteger(0);

  private volatile boolean isClosed;
  private volatile Collection<String> tags = new ArrayList<>();

  public NodesSysPropsCacher(NodeStateProvider nodeStateProvider,
                             String currentNode,
                             ZkStateReader stateReader) {
    this.nodeStateProvider = nodeStateProvider;
    this.currentNode = currentNode;

    stateReader.registerClusterPropertiesListener(properties -> {
      Collection<String> tags = new ArrayList<>();
      String shardPreferences = (String) properties.getOrDefault(ZkStateReader.DEFAULT_SHARD_PREFERENCES, "");
      if (shardPreferences.contains(ShardParams.SHARDS_PREFERENCE_NODE_WITH_SAME_SYSPROP)) {
        try {
          tags = PreferenceRule
              .from(shardPreferences)
              .stream()
              .filter(r -> ShardParams.SHARDS_PREFERENCE_NODE_WITH_SAME_SYSPROP.equals(r.name))
              .map(r -> r.value)
              .collect(Collectors.toSet());
        } catch (Exception e) {
          log.info("Error on parsing shards preference:{}", shardPreferences);
        }
      }

      if (tags.isEmpty()) {
        pause();
      } else {
        start(tags);
        // start fetching now
        fetchSysProps(stateReader.getClusterState().getLiveNodes());
      }
      return isClosed;
    });

    stateReader.registerLiveNodesListener((oldLiveNodes, newLiveNodes) -> {
      fetchSysProps(newLiveNodes);
      return isClosed;
    });
  }

  private void start(Collection<String> tags) {
    if (isClosed)
      return;
    this.tags = tags;
    isRunning.set(true);
  }

  private void fetchSysProps(Set<String> newLiveNodes) {
    if (isRunning.get()) {
      int fetchRound = fetchCounting.incrementAndGet();
      //TODO smarter keeping caching entries by relying on Stat.cversion
      cache.clear();
      for (String node: newLiveNodes) {
        // this might takes some times to finish, therefore if there are a latter change in listener
        // triggering this method, skipping the old runner
        if (isClosed && fetchRound != fetchCounting.get())
          return;

        if (currentNode.equals(node)) {
          Map<String, String> props = new HashMap<>();
          for (String tag : tags) {
            String propName = tag.substring(SYSPROP.length());
            if (additionalProps != null && additionalProps.containsKey(propName)) {
              props.put(tag, additionalProps.get(propName));
            } else {
              props.put(tag, System.getProperty(propName));
            }
          }
          cache.put(node, Collections.unmodifiableMap(props));
        } else {
          fetchRemoteProps(node, fetchRound);
        }

      }
    }
  }

  private void fetchRemoteProps(String node, int fetchRound) {
    for (int i = 0; i < NUM_RETRY; i++) {
      if (isClosed && fetchRound != fetchCounting.get())
        return;

      try {
        Map<String, Object> props = nodeStateProvider.getNodeValues(node, tags);
        cache.put(node, Collections.unmodifiableMap(props));
        break;
      } catch (Exception e) {
        try {
          // 1, 4, 9
          int backOffTime = 1000 * (i+1);
          backOffTime = backOffTime * backOffTime;
          backOffTime = Math.min(10000, backOffTime);
          Thread.sleep(backOffTime);
        } catch (InterruptedException e1) {
          Thread.currentThread().interrupt();
          log.info("Exception on caching node:{} system.properties:{}, retry {}/{}", node, tags, i+1, NUM_RETRY, e); // nowarn
          break;
        }
        log.info("Exception on caching node:{} system.properties:{}, retry {}/{}", node, tags, i+1, NUM_RETRY, e); // nowarn
      }
    }
  }

  public Map<String, Object> getSysProps(String node, Collection<String> tags) {
    Map<String, Object> props = cache.get(node);
    HashMap<String, Object> result = new HashMap<>();
    if (props != null) {
      for (String tag: tags) {
        if (props.containsKey(tag)) {
          result.put(tag, props.get(tag));
        }
      }
    }
    return result;
  }

  public int getCacheSize() {
    return cache.size();
  }

  public boolean isRunning() {
    return isRunning.get();
  }

  private void pause() {
    isRunning.set(false);
  }

  @Override
  public boolean isClosed() {
    return isClosed;
  }

  @Override
  public void close() {
    isClosed = true;
    pause();
  }
}
