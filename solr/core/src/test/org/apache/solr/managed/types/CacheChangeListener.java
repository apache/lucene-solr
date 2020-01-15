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

package org.apache.solr.managed.types;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import org.apache.solr.common.util.Utils;
import org.apache.solr.managed.ChangeListener;
import org.apache.solr.managed.ManagedComponent;

/**
 *
 */
public class CacheChangeListener implements ChangeListener {
  public Map<String, List<Map<String, Object>>> changedValues = new ConcurrentHashMap<>();
  public Map<String, List<Map<String, Object>>> errors = new ConcurrentHashMap<>();

  /** Wait on this latch to make sure at least this number events has been captured. */
  public CountDownLatch atLeastEvents;
  private int atLeast;

  /**
   * Test listener that captures events and provides support for waiting for at least N events.
   * @param atLeast the number of events
   */
  public CacheChangeListener(int atLeast) {
    this.atLeast = atLeast;
    if (atLeast > 0) {
      atLeastEvents = new CountDownLatch(atLeast);
    }
  }

  public void setAtLeast(int atLeast) {
    this.atLeast = atLeast;
    clear();
  }

  @Override
  public void changedLimit(String poolName, ManagedComponent component, String limitName, Object newRequestedVal, Object newActualVal, Reason reason) {
    List<Map<String, Object>> perComponent = changedValues.computeIfAbsent(component.getManagedComponentId().toString(), Utils.NEW_ARRAYLIST_FUN);
    Map<String, Object> entry = new HashMap<>();
    entry.put(limitName, newActualVal);
    entry.put("reason", reason);
    perComponent.add(entry);
    if (atLeastEvents != null) {
      atLeastEvents.countDown();
    }
  }

  @Override
  public void onError(String poolName, ManagedComponent component, String limitName, Object newRequestedVal, Reason reason, Throwable error) {
    List<Map<String, Object>> perComponent = changedValues.computeIfAbsent(component.getManagedComponentId().toString(), Utils.NEW_ARRAYLIST_FUN);
    Map<String, Object> entry = new HashMap<>();
    entry.put(limitName, newRequestedVal);
    entry.put("reason", reason);
    entry.put("error", error);
    perComponent.add(entry);
  }

  public void clear() {
    changedValues.clear();
    errors.clear();
    if (atLeast > 0) {
      atLeastEvents = new CountDownLatch(atLeast);
    }
  }
}
