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
package org.apache.solr.cloud.autoscaling;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.solr.common.params.AutoScalingParams;
import org.apache.solr.common.util.Utils;

/**
 * Simple bean representation of <code>autoscaling.json</code>, which parses data
 * lazily.
 */
public class AutoScalingConfig {

  private final Map<String, Object> jsonMap;

  private Policy policy;
  private Map<String, TriggerConfig> triggers;
  private Map<String, ListenerConfig> listeners;

  /**
   * Bean representation of {@link org.apache.solr.cloud.autoscaling.AutoScaling.TriggerListener} config.
   */
  public static class ListenerConfig {
    public String trigger;
    public List<String> stages;
    public String listenerClass;
    public List<Map<String, String>> beforeActions;
    public List<Map<String, String>> afterActions;

    public ListenerConfig(Map<String, Object> properties) {
      trigger = (String)properties.get(AutoScalingParams.TRIGGER);
      stages = (List<String>)properties.getOrDefault(AutoScalingParams.STAGE, Collections.emptyList());
      listenerClass = (String)properties.get(AutoScalingParams.CLASS);
      beforeActions = (List<Map<String, String>>)properties.getOrDefault(AutoScalingParams.BEFORE_ACTION, Collections.emptyList());
      afterActions = (List<Map<String, String>>)properties.getOrDefault(AutoScalingParams.AFTER_ACTION, Collections.emptyList());
    }
  }

  /**
   * Bean representation of {@link org.apache.solr.cloud.autoscaling.AutoScaling.Trigger} config.
   */
  public static class TriggerConfig {
    public final AutoScaling.EventType eventType;
    public final Map<String, Object> properties = new HashMap<>();

    public TriggerConfig(Map<String, Object> properties) {
      String event = (String) properties.get(AutoScalingParams.EVENT);
      this.eventType = AutoScaling.EventType.valueOf(event.toUpperCase(Locale.ROOT));
      this.properties.putAll(properties);
    }
  }

  /**
   * Construct from a JSON map representation.
   * @param jsonMap
   */
  public AutoScalingConfig(Map<String, Object> jsonMap) {
    this.jsonMap = Utils.getDeepCopy(jsonMap, 10);
  }

  /**
   * Get {@link Policy} configuration.
   */
  public Policy getPolicy() {
    if (policy == null) {
      policy = new Policy(jsonMap);
    }
    return policy;
  }

  /**
   * Get trigger configurations.
   */
  public Map<String, TriggerConfig> getTriggerConfigs() {
    if (triggers == null) {
      Map<String, Object> trigMap = (Map<String, Object>)jsonMap.get("triggers");
      if (trigMap == null) {
        triggers = Collections.emptyMap();
      } else {
        triggers = new HashMap<>(trigMap.size());
        for (Map.Entry<String, Object> entry : trigMap.entrySet()) {
          triggers.put(entry.getKey(), new TriggerConfig((Map<String, Object>)entry.getValue()));
        }
      }
    }
    return triggers;
  }

  /**
   * Get listener configurations.
   */
  public Map<String, ListenerConfig> getListenerConfigs() {
    if (listeners == null) {
      Map<String, Object> map = (Map<String, Object>)jsonMap.get("listeners");
      if (map == null) {
        listeners = Collections.emptyMap();
      } else {
        listeners = new HashMap<>(map.size());
        for (Map.Entry<String, Object> entry : map.entrySet()) {
          listeners.put(entry.getKey(), new ListenerConfig((Map<String, Object>)entry.getValue()));
        }
      }
    }
    return listeners;
  }

  /**
   * Check whether triggers for specific event type exist.
   * @param types list of event types
   * @return true if there's at least one trigger matching at least one event type,
   * false otherwise,
   */
  public boolean hasTriggerForEvents(AutoScaling.EventType... types) {
    if (types == null || types.length == 0) {
      return false;
    }
    for (TriggerConfig config : getTriggerConfigs().values()) {
      for (AutoScaling.EventType type : types) {
        if (config.eventType.equals(type)) {
          return true;
        }
      }
    }
    return false;
  }
}
