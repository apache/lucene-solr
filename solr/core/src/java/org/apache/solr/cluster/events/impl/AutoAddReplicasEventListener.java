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

import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.cluster.events.ClusterEvent;
import org.apache.solr.cluster.events.ClusterEventListener;
import org.apache.solr.cloud.ClusterSingleton;
import org.apache.solr.core.CoreContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class AutoAddReplicasEventListener implements ClusterSingleton, ClusterEventListener {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final Set<ClusterEvent.EventType> EVENT_TYPES = new HashSet<>(
      Arrays.asList(
          ClusterEvent.EventType.NODE_DOWN,
          ClusterEvent.EventType.REPLICA_DOWN
      ));

  private final CoreContainer cc;
  private final SolrClientCache solrClientCache;

  private boolean running = false;

  public AutoAddReplicasEventListener(CoreContainer cc) {
    this.cc = cc;
    this.solrClientCache = cc.getSolrClientCache();
  }

  @Override
  public Set<ClusterEvent.EventType> getEventTypes() {
    return EVENT_TYPES;
  }

  @Override
  public void onEvent(ClusterEvent event) {
    if (!isRunning()) {
      // ignore the event
      return;
    }
    switch (event.getType()) {
      case NODE_DOWN:
        handleNodeDown(event);
        break;
      case NODE_UP:
        // ignore?
        break;
      case REPLICA_DOWN:
        handleReplicaDown(event);
        break;
      default:
        log.warn("Unsupported event {}, ignoring...", event);
    }
  }

  private void handleNodeDown(ClusterEvent event) {
    // send MOVEREPLICA admin requests for all replicas from that node
  }

  private void handleReplicaDown(ClusterEvent event) {
    // send ADDREPLICA admin request
  }

  @Override
  public void start() throws Exception {
    running = true;
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  @Override
  public void stop() {
    running = false;
  }
}
