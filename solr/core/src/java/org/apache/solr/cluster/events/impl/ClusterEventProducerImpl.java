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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.solr.cloud.ZkController;
import org.apache.solr.cluster.events.ClusterEvent;
import org.apache.solr.cluster.events.ClusterEventListener;
import org.apache.solr.cluster.events.ClusterEventProducer;
import org.apache.solr.cluster.events.ClusterSingleton;
import org.apache.solr.cluster.events.NodeDownEvent;
import org.apache.solr.cluster.events.NodeUpEvent;
import org.apache.solr.cluster.events.Schedule;
import org.apache.solr.cluster.events.ScheduledEvent;
import org.apache.solr.cluster.events.ScheduledEventListener;
import org.apache.solr.common.cloud.LiveNodesListener;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.core.CoreContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link ClusterEventProducer}.
 * <h3>Implementation notes</h3>
 * <p>For each cluster event relevant listeners are always invoked sequentially
 * (not in parallel) and in arbitrary order. This means that if any listener blocks the
 * processing other listeners may be invoked much later or not at all.</p>
 * <p>Scheduled events are triggered at most with {@link #SCHEDULE_INTERVAL_SEC} interval. See also above note
 * on the sequential processing. If the total time of execution exceeds any schedule
 * interval such events will be silently missed and they will be invoked only when the
 * next event will be generated.</p>
 */
public class ClusterEventProducerImpl implements ClusterEventProducer, ClusterSingleton, Closeable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final int SCHEDULE_INTERVAL_SEC = 10;

  private final Map<ClusterEvent.EventType, Set<ClusterEventListener>> listeners = new HashMap<>();
  private final Map<String, CompiledSchedule> schedules = new ConcurrentHashMap<>();
  private final CoreContainer cc;
  private LiveNodesListener liveNodesListener;
  private ZkController zkController;
  private ScheduledExecutorService scheduler;
  private boolean running;

  private final Set<ClusterEvent.EventType> supportedEvents =
      new HashSet<>() {{
        add(ClusterEvent.EventType.NODE_DOWN);
        add(ClusterEvent.EventType.NODE_UP);
        add(ClusterEvent.EventType.SCHEDULED);
      }};

  private volatile boolean isClosed = false;

  public ClusterEventProducerImpl(CoreContainer coreContainer) {
    this.cc = coreContainer;
    this.zkController = this.cc.getZkController();
  }

  // ClusterSingleton lifecycle methods
  @Override
  public void start() throws Exception {
    if (zkController == null) {
      liveNodesListener = null;
      scheduler = null;
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

    // create scheduler
    scheduler = Executors.newSingleThreadScheduledExecutor(new SolrNamedThreadFactory("cluster-event-scheduler"));
    scheduler.schedule(() -> maybeFireScheduledEvent(), SCHEDULE_INTERVAL_SEC, TimeUnit.SECONDS);
    running = true;
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
    if (scheduler != null) {
      scheduler.shutdownNow();
      try {
        scheduler.awaitTermination(60, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        log.warn("Interrupted while waiting for the scheduler to shut down, ignoring");
      }
    }
    liveNodesListener = null;
    scheduler = null;
  }

  private void ensureRunning() {
    if (isClosed || !running) {
      throw new RuntimeException("ClusterEventProducerImpl is not running.");
    }
  }

  private void maybeFireScheduledEvent() {
    ensureRunning();
    Set<ClusterEventListener> scheduledListeners = listeners.getOrDefault(ClusterEvent.EventType.SCHEDULED, Collections.emptySet());
    if (scheduledListeners.isEmpty()) {
      return;
    }
    scheduledListeners.forEach(listener -> {
      ScheduledEventListener scheduledEventListener = (ScheduledEventListener) listener;
      Schedule schedule = scheduledEventListener.getSchedule();
      CompiledSchedule compiledSchedule = schedules.get(schedule.getName());
      if (compiledSchedule == null) { // ???
        return;
      }
      if (compiledSchedule.shouldRun()) {
        Instant now = Instant.now();
        ClusterEvent event = new ScheduledEvent() {
          @Override
          public Schedule getSchedule() {
            return schedule;
          }

          @Override
          public Instant getTimestamp() {
            return now;
          }
        };
        listener.onEvent(event);
        compiledSchedule.setLastRunAt(now);
      }
    });
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
    CompiledSchedule compiledSchedule = null;
    if (listener.getEventTypes().contains(ClusterEvent.EventType.SCHEDULED)) {
      if (!(listener instanceof ScheduledEventListener)) {
        throw new Exception("listener " + listener + " wants to process " +
            ClusterEvent.EventType.SCHEDULED + " events but is not a " +
            ScheduledEventListener.class.getSimpleName());
      } else {
        compiledSchedule = new CompiledSchedule(((ScheduledEventListener) listener).getSchedule());
      }
    }
    ClusterEventProducer.super.registerListener(listener);
    if (compiledSchedule != null) {
      schedules.put(compiledSchedule.name, compiledSchedule);
    }
  }

  @Override
  public void unregisterListener(ClusterEventListener listener) {
    if (listener.getEventTypes().contains(ClusterEvent.EventType.SCHEDULED)) {
      schedules.remove(((ScheduledEventListener) listener).getSchedule().getName());
    }
    ClusterEventProducer.super.unregisterListener(listener);
  }

  @Override
  public void close() throws IOException {
    stop();
    isClosed = true;
    schedules.clear();
    listeners.clear();
  }

  @Override
  public Map<ClusterEvent.EventType, Set<ClusterEventListener>> getEventListeners() {
    ensureRunning();
    return listeners;
  }
}
