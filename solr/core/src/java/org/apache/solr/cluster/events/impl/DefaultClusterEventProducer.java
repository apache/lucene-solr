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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.solr.cloud.ZkController;
import org.apache.solr.cluster.events.ClusterEventProducerBase;
import org.apache.solr.cluster.events.ClusterPropertiesChangedEvent;
import org.apache.solr.cluster.events.ClusterEvent;
import org.apache.solr.cluster.events.ClusterEventProducer;
import org.apache.solr.cluster.events.CollectionsAddedEvent;
import org.apache.solr.cluster.events.CollectionsRemovedEvent;
import org.apache.solr.cluster.events.NodesDownEvent;
import org.apache.solr.cluster.events.NodesUpEvent;
import org.apache.solr.common.cloud.CloudCollectionsListener;
import org.apache.solr.common.cloud.ClusterPropertiesListener;
import org.apache.solr.common.cloud.LiveNodesListener;
import org.apache.solr.core.CoreContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link ClusterEventProducer}.
 * <h2>Implementation notes</h2>
 * <p>For each cluster event relevant listeners are always invoked sequentially
 * (not in parallel) and in arbitrary order. This means that if any listener blocks the
 * processing other listeners may be invoked much later or not at all.</p>
 */
public class DefaultClusterEventProducer extends ClusterEventProducerBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private LiveNodesListener liveNodesListener;
  private CloudCollectionsListener cloudCollectionsListener;
  private ClusterPropertiesListener clusterPropertiesListener;
  private ZkController zkController;

  private final Set<ClusterEvent.EventType> supportedEvents =
      new HashSet<>(Arrays.asList(
          ClusterEvent.EventType.NODES_DOWN,
          ClusterEvent.EventType.NODES_UP,
          ClusterEvent.EventType.COLLECTIONS_ADDED,
          ClusterEvent.EventType.COLLECTIONS_REMOVED,
          ClusterEvent.EventType.CLUSTER_PROPERTIES_CHANGED
      ));

  public DefaultClusterEventProducer(CoreContainer cc) {
    super(cc);
  }

  // ClusterSingleton lifecycle methods
  @Override
  public synchronized void start() {
    if (cc == null) {
      liveNodesListener = null;
      cloudCollectionsListener = null;
      clusterPropertiesListener = null;
      state = State.STOPPED;
      return;
    }
    if (state == State.RUNNING) {
      log.warn("Double start() invoked on {}, ignoring", this);
      return;
    }
    state = State.STARTING;
    this.zkController = this.cc.getZkController();

    // clean up any previous instances
    doStop();

    // register liveNodesListener
    liveNodesListener = (oldNodes, newNodes) -> {
      // already closed but still registered
      if (state == State.STOPPING || state == State.STOPPED) {
        // remove the listener
        return true;
      }
      // spurious event, ignore but keep listening
      if (oldNodes.equals(newNodes)) {
        return false;
      }
      final Instant now = Instant.now();
      final Set<String> downNodes = new HashSet<>(oldNodes);
      downNodes.removeAll(newNodes);
      if (!downNodes.isEmpty()) {
        fireEvent(new NodesDownEvent() {
          @Override
          public Iterator<String> getNodeNames() {
            return downNodes.iterator();
          }

          @Override
          public Instant getTimestamp() {
            return now;
          }
        });
      }
      final Set<String> upNodes = new HashSet<>(newNodes);
      upNodes.removeAll(oldNodes);
      if (!upNodes.isEmpty()) {
        fireEvent(new NodesUpEvent() {
          @Override
          public Iterator<String> getNodeNames() {
            return upNodes.iterator();
          }

          @Override
          public Instant getTimestamp() {
            return now;
          }
        });
      }
      return false;
    };
    zkController.zkStateReader.registerLiveNodesListener(liveNodesListener);

    cloudCollectionsListener = ((oldCollections, newCollections) -> {
      if (oldCollections.equals(newCollections)) {
        return;
      }
      final Instant now = Instant.now();
      final Set<String> removed = new HashSet<>(oldCollections);
      removed.removeAll(newCollections);
      if (!removed.isEmpty()) {
        fireEvent(new CollectionsRemovedEvent() {
          @Override
          public Iterator<String> getCollectionNames() {
            return removed.iterator();
          }

          @Override
          public Instant getTimestamp() {
            return now;
          }
        });
      }
      final Set<String> added = new HashSet<>(newCollections);
      added.removeAll(oldCollections);
      if (!added.isEmpty()) {
        fireEvent(new CollectionsAddedEvent() {
          @Override
          public Iterator<String> getCollectionNames() {
            return added.iterator();
          }

          @Override
          public Instant getTimestamp() {
            return now;
          }
        });
      }
    });
    zkController.zkStateReader.registerCloudCollectionsListener(cloudCollectionsListener);

    clusterPropertiesListener = (newProperties) -> {
      fireEvent(new ClusterPropertiesChangedEvent() {
        final Instant now = Instant.now();
        @Override
        public Map<String, Object> getNewClusterProperties() {
          return newProperties;
        }

        @Override
        public Instant getTimestamp() {
          return now;
        }
      });
      return false;
    };
    zkController.zkStateReader.registerClusterPropertiesListener(clusterPropertiesListener);

    // XXX register collection state listener?
    // XXX not sure how to efficiently monitor for REPLICA_DOWN events

    state = State.RUNNING;
  }

  @Override
  public Set<ClusterEvent.EventType> getSupportedEventTypes() {
    return supportedEvents;
  }

  @Override
  public synchronized void stop() {
    state = State.STOPPING;
    doStop();
    state = State.STOPPED;
  }

  private void doStop() {
    if (liveNodesListener != null) {
      zkController.zkStateReader.removeLiveNodesListener(liveNodesListener);
    }
    if (cloudCollectionsListener != null) {
      zkController.zkStateReader.removeCloudCollectionsListener(cloudCollectionsListener);
    }
    if (clusterPropertiesListener != null) {
      zkController.zkStateReader.removeClusterPropertiesListener(clusterPropertiesListener);
    }
    liveNodesListener = null;
    cloudCollectionsListener = null;
    clusterPropertiesListener = null;
  }

  @Override
  public void close() throws IOException {
    stop();
    super.close();
  }
}
