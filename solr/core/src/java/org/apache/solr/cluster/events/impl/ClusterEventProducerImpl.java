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
package org.apache.solr.cluster.events.impl;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.solr.cloud.ZkController;
import org.apache.solr.cluster.events.ClusterEvent;
import org.apache.solr.cluster.events.ClusterEventListener;
import org.apache.solr.cluster.events.ClusterEventProducer;
import org.apache.solr.core.CoreContainer;

/**
 *
 */
public class ClusterEventProducerImpl implements ClusterEventProducer {

  private final Map<ClusterEvent.EventType, Set<ClusterEventListener>> listeners = new HashMap<>();
  private final CoreContainer cc;

  public ClusterEventProducerImpl(CoreContainer coreContainer) {
    this.cc = coreContainer;
    ZkController zkController = this.cc.getZkController();
    if (zkController == null) {
      return;
    }
    // XXX register liveNodesListener

    // XXX register collection state listener
  }

  @Override
  public void registerListener(ClusterEventListener listener) {
    listener.getEventTypes().forEach(type -> {
      Set<ClusterEventListener> perType = listeners.computeIfAbsent(type, t -> ConcurrentHashMap.newKeySet());
      perType.add(listener);
    });
  }

  @Override
  public void unregisterListener(ClusterEventListener listener) {
    listener.getEventTypes().forEach(type ->
      listeners.getOrDefault(type, Collections.emptySet()).remove(listener)
    );
  }

  @Override
  public Map<ClusterEvent.EventType, Set<ClusterEventListener>> getEventListeners() {
    return listeners;
  }
}
