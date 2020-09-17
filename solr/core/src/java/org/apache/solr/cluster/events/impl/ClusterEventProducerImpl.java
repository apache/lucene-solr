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

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.solr.cloud.ZkController;
import org.apache.solr.cluster.events.ClusterPropertiesChangedEvent;
import org.apache.solr.cluster.events.ClusterEvent;
import org.apache.solr.cluster.events.ClusterEventListener;
import org.apache.solr.cluster.events.ClusterEventProducer;
import org.apache.solr.cloud.ClusterSingleton;
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
 * <h3>Implementation notes</h3>
 * <p>For each cluster event relevant listeners are always invoked sequentially
 * (not in parallel) and in arbitrary order. This means that if any listener blocks the
 * processing other listeners may be invoked much later or not at all.</p>
 */
public class ClusterEventProducerImpl implements ClusterEventProducer, ClusterSingleton, Closeable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Map<ClusterEvent.EventType, Set<ClusterEventListener>> listeners = new HashMap<>();
  private CoreContainer coreContainer;
  private LiveNodesListener liveNodesListener;
  private CloudCollectionsListener cloudCollectionsListener;
  private ClusterPropertiesListener clusterPropertiesListener;
  private Map<String, Object> lastClusterProperties;
  private ZkController zkController;
  private boolean running;

  private final Set<ClusterEvent.EventType> supportedEvents =
      new HashSet<>(Arrays.asList(
          ClusterEvent.EventType.NODES_DOWN,
          ClusterEvent.EventType.NODES_UP,
          ClusterEvent.EventType.COLLECTIONS_ADDED,
          ClusterEvent.EventType.COLLECTIONS_REMOVED,
          ClusterEvent.EventType.CLUSTER_PROPERTIES_CHANGED
      ));

  private volatile boolean isClosed = false;

  public ClusterEventProducerImpl(CoreContainer coreContainer) {
    this.coreContainer = coreContainer;
  }

  // ClusterSingleton lifecycle methods
  @Override
  public void start() {
    if (coreContainer == null) {
      liveNodesListener = null;
      cloudCollectionsListener = null;
      clusterPropertiesListener = null;
      return;
    }
    this.zkController = this.coreContainer.getZkController();

    // clean up any previous instances
    doStop();

    // register liveNodesListener
    liveNodesListener = (oldNodes, newNodes) -> {
      // already closed but still registered
      if (isClosed) {
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
          public Collection<String> getNodeNames() {
            return downNodes;
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
          public Collection<String> getNodeNames() {
            return upNodes;
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
          public Collection<String> getCollectionNames() {
            return removed;
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
          public Collection<String> getCollectionNames() {
            return added;
          }

          @Override
          public Instant getTimestamp() {
            return now;
          }
        });
      }
    });
    zkController.zkStateReader.registerCloudCollectionsListener(cloudCollectionsListener);

    lastClusterProperties = new LinkedHashMap<>(zkController.zkStateReader.getClusterProperties());
    clusterPropertiesListener = (newProperties) -> {
      if (newProperties.equals(lastClusterProperties)) {
        return false;
      }
      fireEvent(new ClusterPropertiesChangedEvent() {
        final Map<String, Object> oldProps = lastClusterProperties;
        @Override
        public Map<String, Object> getOldClusterProperties() {
          return oldProps;
        }

        @Override
        public Map<String, Object> getNewClusterProperties() {
          return newProperties;
        }

        @Override
        public Instant getTimestamp() {
          return Instant.now();
        }
      });
      lastClusterProperties = new LinkedHashMap<>(newProperties);
      return false;
    };
    zkController.zkStateReader.registerClusterPropertiesListener(clusterPropertiesListener);

    // XXX register collection state listener?
    // XXX not sure how to efficiently monitor for REPLICA_DOWN events
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  @Override
  public void stop() {
    doStop();
    running = false;
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
  public void registerListener(ClusterEventListener listener) throws Exception {
    try {
      listener.getEventTypes().forEach(type -> {
        if (!supportedEvents.contains(type)) {
          log.warn("event type {} not supported yet.", type);
        }
      });
    } catch (Throwable e) {
      throw new Exception(e);
    }
    ClusterEventProducer.super.registerListener(listener);
  }

  @Override
  public void close() throws IOException {
    stop();
    isClosed = true;
    listeners.clear();
  }

  @Override
  public Map<ClusterEvent.EventType, Set<ClusterEventListener>> getEventListeners() {
    return listeners;
  }
}
