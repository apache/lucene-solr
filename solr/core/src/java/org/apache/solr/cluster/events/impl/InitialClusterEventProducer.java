package org.apache.solr.cluster.events.impl;

import org.apache.solr.cluster.events.ClusterEvent;
import org.apache.solr.cluster.events.ClusterEventListener;
import org.apache.solr.cluster.events.ClusterEventProducer;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * This class helps in handling the initial registration of plugin-based listeners,
 * when both the final {@link ClusterEventProducer} implementation and listeners
 * are configured using plugins.
 */
public class InitialClusterEventProducer implements ClusterEventProducer {

  Map<ClusterEvent.EventType, Set<ClusterEventListener>> initialListeners = new HashMap<>();

  public void transferListeners(ClusterEventProducer target) {
    initialListeners.forEach((type, listeners) -> {
      listeners.forEach(listener -> target.registerListener(listener, type));
    });
  }

  @Override
  public void registerListener(ClusterEventListener listener, ClusterEvent.EventType... eventTypes) {
    if (eventTypes == null || eventTypes.length == 0) {
      eventTypes = ClusterEvent.EventType.values();
    }
    for (ClusterEvent.EventType type : eventTypes) {
      initialListeners.computeIfAbsent(type, t -> new HashSet<>())
          .add(listener);
    }
  }

  @Override
  public void unregisterListener(ClusterEventListener listener, ClusterEvent.EventType... eventTypes) {
    throw new UnsupportedOperationException("unregister listener not implemented");
  }

  @Override
  public String getName() {
    return ClusterEventProducer.PLUGIN_NAME;
  }

  @Override
  public void start() throws Exception {
    throw new UnsupportedOperationException("start not implemented");
  }

  @Override
  public State getState() {
    return State.STOPPED;
  }

  @Override
  public void stop() {
    throw new UnsupportedOperationException("stop not implemented");
  }
}
