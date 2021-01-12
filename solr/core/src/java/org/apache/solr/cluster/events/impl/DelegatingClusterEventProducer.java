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

import org.apache.solr.cluster.events.ClusterEvent;
import org.apache.solr.cluster.events.ClusterEventListener;
import org.apache.solr.cluster.events.ClusterEventProducer;
import org.apache.solr.cluster.events.NoOpProducer;
import org.apache.solr.cluster.events.ClusterEventProducerBase;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.core.CoreContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Set;

/**
 * This implementation allows Solr to dynamically change the underlying implementation
 * of {@link ClusterEventProducer} in response to the changed plugin configuration.
 */
public final class DelegatingClusterEventProducer extends ClusterEventProducerBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private ClusterEventProducer delegate;

  public DelegatingClusterEventProducer(CoreContainer cc) {
    super(cc);
    delegate = new NoOpProducer(cc);
  }

  @Override
  public void close() throws IOException {
    if (log.isDebugEnabled()) {
      log.debug("--closing delegate for CC-{}: {}", Integer.toHexString(cc.hashCode()), delegate);
    }
    IOUtils.closeQuietly(delegate);
    super.close();
  }

  public void setDelegate(ClusterEventProducer newDelegate) {
    if (log.isDebugEnabled()) {
      log.debug("--setting new delegate for CC-{}: {}", Integer.toHexString(cc.hashCode()), newDelegate);
    }
    this.delegate = newDelegate;
    // transfer all listeners to the new delegate
    listeners.forEach((type, listenerSet) -> {
      listenerSet.forEach(listener -> {
        try {
          delegate.registerListener(listener, type);
        } catch (Exception e) {
          log.warn("Exception registering listener with the new event producer", e);
          // make sure it's not registered
          delegate.unregisterListener(listener, type);
          // unregister it here, too
          super.unregisterListener(listener, type);
        }
      });
    });
    if ((state == State.RUNNING || state == State.STARTING) &&
        !(delegate.getState() == State.RUNNING || delegate.getState() == State.STARTING)) {
      try {
        delegate.start();
        if (log.isDebugEnabled()) {
          log.debug("--- started delegate {}", delegate);
        }
      } catch (Exception e) {
        log.warn("Unable to start the new delegate {}: {}", delegate.getClass().getName(), e);
      }
    } else {
      if (log.isDebugEnabled()) {
        log.debug("--- delegate {} already in state {}", delegate, delegate.getState());
      }
    }
  }

  @Override
  public void registerListener(ClusterEventListener listener, ClusterEvent.EventType... eventTypes) {
    super.registerListener(listener, eventTypes);
    delegate.registerListener(listener, eventTypes);
  }

  @Override
  public void unregisterListener(ClusterEventListener listener, ClusterEvent.EventType... eventTypes) {
    super.unregisterListener(listener, eventTypes);
    delegate.unregisterListener(listener, eventTypes);
  }

  @Override
  public synchronized void start() throws Exception {
    if (log.isDebugEnabled()) {
      log.debug("-- starting CC-{}, Delegating {}, delegate {}",
          Integer.toHexString(cc.hashCode()), Integer.toHexString(hashCode()), delegate);
    }
    state = State.STARTING;
    if (!(delegate.getState() == State.RUNNING || delegate.getState() == State.STARTING)) {
      try {
        delegate.start();
        if (log.isDebugEnabled()) {
          log.debug("--- started delegate {}", delegate);
        }
      } finally {
        state = delegate.getState();
      }
    } else {
      if (log.isDebugEnabled()) {
        log.debug("--- delegate {} already in state {}", delegate, delegate.getState());
      }
    }
  }

  @Override
  public Set<ClusterEvent.EventType> getSupportedEventTypes() {
    return NoOpProducer.ALL_EVENT_TYPES;
  }

  @Override
  public synchronized void stop() {
    if (log.isDebugEnabled()) {
      log.debug("-- stopping Delegating {}, delegate {}", Integer.toHexString(hashCode()), delegate);
    }
    state = State.STOPPING;
    delegate.stop();
    state = delegate.getState();
  }
}
