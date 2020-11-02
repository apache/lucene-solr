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

/**
 * No-op implementation of {@link ClusterEventProducer}. This implementation is always in
 * RUNNING state.
 */
public final class NoOpProducer implements ClusterEventProducer {

  @Override
  public void registerListener(ClusterEventListener listener, ClusterEvent.EventType... eventTypes) {
    // no-op
  }

  @Override
  public void unregisterListener(ClusterEventListener listener, ClusterEvent.EventType... eventTypes) {
    // no-op
  }

  @Override
  public String getName() {
    return ClusterEventProducer.PLUGIN_NAME;
  }

  @Override
  public void start() throws Exception {
    // no-op
  }

  @Override
  public State getState() {
    return State.RUNNING;
  }

  @Override
  public void stop() {
    // no-op
  }
}
