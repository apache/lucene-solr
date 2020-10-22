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

import java.time.Instant;

/**
 * Cluster-level event.
 */
public interface ClusterEvent {

  enum EventType {
    /** One or more nodes went down. */
    NODES_DOWN,
    /** One or more nodes went up. */
    NODES_UP,
    /** One or more collections have been added. */
    COLLECTIONS_ADDED,
    /** One or more collections have been removed. */
    COLLECTIONS_REMOVED,
    /** Cluster properties have changed. */
    CLUSTER_PROPERTIES_CHANGED
    // other types? eg. Overseer leader change, shard leader change,
    // node overload (eg. CPU / MEM circuit breakers tripped)?
  }

  /** Get event type. */
  EventType getType();

  /** Get event timestamp. This is the instant when the event was generated (not necessarily when
   * the underlying condition first occurred). */
  Instant getTimestamp();
}
