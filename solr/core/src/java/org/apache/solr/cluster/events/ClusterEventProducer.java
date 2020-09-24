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
package org.apache.solr.cluster.events;

import org.apache.solr.cloud.ClusterSingleton;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Component that produces {@link ClusterEvent} instances.
 */
public interface ClusterEventProducer extends ClusterSingleton {

  String PLUGIN_NAME = "clusterEventProducer";

  default String getName() {
    return PLUGIN_NAME;
  }

  /**
   * Returns a modifiable map of event types and listeners to process events
   * of a given type.
   */
  Map<ClusterEvent.EventType, Set<ClusterEventListener>> getEventListeners();

  /**
   * Register an event listener for processing the specified event types.
   * @param listener non-null listener. If the same instance of the listener is
   *                 already registered it will be ignored.
   * @param eventTypes non-empty array of event types that this listener
   *                   is being registered for. If this is null or empty then all types will be used.
   */
  default void registerListener(ClusterEventListener listener, ClusterEvent.EventType... eventTypes) throws Exception {
    Objects.requireNonNull(listener);
    if (eventTypes == null || eventTypes.length == 0) {
      eventTypes = ClusterEvent.EventType.values();
    }
    for (ClusterEvent.EventType type : eventTypes) {
      Set<ClusterEventListener> perType = getEventListeners().computeIfAbsent(type, t -> ConcurrentHashMap.newKeySet());
      perType.add(listener);
    }
  }

  /**
   * Unregister an event listener.
   * @param listener non-null listener.
   */
  default void unregisterListener(ClusterEventListener listener) {
    Objects.requireNonNull(listener);
    getEventListeners().forEach((type, listeners) -> {
      listeners.remove(listener);
    });
  }

  /**
   * Unregister an event listener for specified event types.
   * @param listener non-null listener.
   * @param eventTypes event types from which the listener will be unregistered. If this
   *                   is null or empty then all event types will be used
   */
  default void unregisterListener(ClusterEventListener listener, ClusterEvent.EventType... eventTypes) {
    Objects.requireNonNull(listener);
    if (eventTypes == null || eventTypes.length == 0) {
      eventTypes = ClusterEvent.EventType.values();
    }
    for (ClusterEvent.EventType type : eventTypes) {
      getEventListeners()
          .getOrDefault(type, Collections.emptySet())
          .remove(listener);
    }
  }

  /**
   * Fire an event. This method will call registered listeners that subscribed to the
   * type of event being passed.
   * @param event cluster event
   */
  default void fireEvent(ClusterEvent event) {
    getEventListeners().getOrDefault(event.getType(), Collections.emptySet())
        .forEach(listener -> listener.onEvent(event));
  }
}
