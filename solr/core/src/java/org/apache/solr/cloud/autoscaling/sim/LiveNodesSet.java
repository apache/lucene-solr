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
package org.apache.solr.cloud.autoscaling.sim;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.solr.common.cloud.LiveNodesListener;

/**
 * This class represents a set of live nodes and allows adding listeners to track their state.
 *
 * @deprecated to be removed in Solr 9.0 (see SOLR-14656)
 */
public class LiveNodesSet implements Iterable<String> {

  private final Set<String> set = ConcurrentHashMap.newKeySet();
  private final Set<LiveNodesListener> listeners = ConcurrentHashMap.newKeySet();

  public Set<String> get() {
    return Collections.unmodifiableSet(set);
  }

  public int size() {
    return set.size();
  }

  public void registerLiveNodesListener(LiveNodesListener listener) {
    listeners.add(listener);
  }

  public void removeLiveNodesListener(LiveNodesListener listener) {
    listeners.remove(listener);
  }
  
  public void removeAllLiveNodesListeners() {
    listeners.clear();
  }

  private void fireListeners(SortedSet<String> oldNodes, SortedSet<String> newNodes) {
    for (LiveNodesListener listener : listeners) {
      listener.onChange(oldNodes, newNodes);
    }
  }

  public boolean isEmpty() {
    return set.isEmpty();
  }

  public boolean contains(String id) {
    return set.contains(id);
  }

  public synchronized boolean add(String id) {
    if (set.contains(id)) {
      return false;
    }
    TreeSet<String> oldNodes = new TreeSet<>(set);
    set.add(id);
    TreeSet<String> newNodes = new TreeSet<>(set);
    fireListeners(oldNodes, newNodes);
    return true;
  }

  public synchronized boolean addAll(Collection<String> nodes) {
    TreeSet<String> oldNodes = new TreeSet<>(set);
    boolean changed = set.addAll(nodes);
    TreeSet<String> newNodes = new TreeSet<>(set);
    if (changed) {
      fireListeners(oldNodes, newNodes);
    }
    return changed;
  }

  public synchronized boolean remove(String id) {
    if (!set.contains(id)) {
      return false;
    }
    TreeSet<String> oldNodes = new TreeSet<>(set);
    set.remove(id);
    TreeSet<String> newNodes = new TreeSet<>(set);
    fireListeners(oldNodes, newNodes);
    return true;
  }

  public synchronized void clear() {
    TreeSet<String> oldNodes = new TreeSet<>(set);
    set.clear();
    fireListeners(oldNodes, Collections.emptySortedSet());
  }

  @Override
  public Iterator<String> iterator() {
    return set.iterator();
  }
}