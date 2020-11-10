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

import org.apache.solr.core.CoreContainer;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * No-op implementation of {@link ClusterEventProducer}. This implementation doesn't
 * generate any events.
 */
public final class NoOpProducer extends ClusterEventProducerBase {

  public static final Set<ClusterEvent.EventType> ALL_EVENT_TYPES = new HashSet<>(Arrays.asList(ClusterEvent.EventType.values()));

  public NoOpProducer(CoreContainer cc) {
    super(cc);
  }

  @Override
  public Set<ClusterEvent.EventType> getSupportedEventTypes() {
    return ALL_EVENT_TYPES;
  }

  @Override
  public void start() throws Exception {
    state = State.RUNNING;
  }

  @Override
  public void stop() {
    state = State.STOPPED;
  }
}
