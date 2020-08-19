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

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Component that produces {@link ClusterEvent} instances.
 */
public interface ClusterEventProducer {

  // XXX should we have an option to register only for particular types of
  // events (the producer would have to filter them per listener)?
  // if not then each listener will get all event types and will have to
  // filter itself, which is cumbersome
  // See also ClusterEventListener.getEventTypes()
  void registerListener(ClusterEventListener listener);

  void unregisterListener(ClusterEventListener listener);

  Map<ClusterEvent.EventType, Set<ClusterEventListener>> getEventListeners();

  default void fireEvent(ClusterEvent event) {
    // XXX filter here by acceptable event types per listener?
    getEventListeners().getOrDefault(event.getType(), Collections.emptySet())
        .forEach(listener -> listener.onEvent(event));
  }
}
