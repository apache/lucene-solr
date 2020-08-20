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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.solr.cloud.ZkController;
import org.apache.solr.cluster.events.ClusterEvent;
import org.apache.solr.cluster.events.ClusterEventListener;
import org.apache.solr.cluster.events.ClusterEventProducer;
import org.apache.solr.cluster.events.ClusterSingleton;
import org.apache.solr.cluster.events.NodeDownEvent;
import org.apache.solr.cluster.events.NodeUpEvent;
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
  private final CoreContainer cc;
  private LiveNodesListener liveNodesListener;
  private ZkController zkController;
  private boolean running;

  private final Set<ClusterEvent.EventType> supportedEvents =
      new HashSet<>() {{
        add(ClusterEvent.EventType.NODE_DOWN);
        add(ClusterEvent.EventType.NODE_UP);
      }};

  private volatile boolean isClosed = false;

  public ClusterEventProducerImpl(CoreContainer coreContainer) {
    this.cc = coreContainer;
    this.zkController = this.cc.getZkController();
  }

  // ClusterSingleton lifecycle methods
  @Override
  public void start() {
    if (zkController == null) {
      liveNodesListener = null;
      return;
    }

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
      oldNodes.forEach(oldNode -> {
        if (!newNodes.contains(oldNode)) {
          fireEvent(new NodeDownEvent() {
            final Instant timestamp = Instant.now();
            @Override
            public Instant getTimestamp() {
              return timestamp;
            }

            @Override
            public String getNodeName() {
              return oldNode;
            }
          });
        }
      });
      newNodes.forEach(newNode -> {
        if (!oldNodes.contains(newNode)) {
          fireEvent(new NodeUpEvent() {
            final Instant timestamp = Instant.now();
            @Override
            public Instant getTimestamp() {
              return timestamp;
            }
            @Override
            public String getNodeName() {
              return newNode;
            }
          });
        }
      });
      return false;
    };

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
    liveNodesListener = null;
  }

  private void ensureRunning() {
    if (isClosed || !running) {
      throw new RuntimeException("ClusterEventProducerImpl is not running.");
    }
  }

  @Override
  public void registerListener(ClusterEventListener listener) throws Exception {
    ensureRunning();
    try {
      listener.getEventTypes().forEach(type -> {
        if (!supportedEvents.contains(type)) {
          throw new RuntimeException("event type " + type + " not supported yet");
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
    ensureRunning();
    return listeners;
  }
}
