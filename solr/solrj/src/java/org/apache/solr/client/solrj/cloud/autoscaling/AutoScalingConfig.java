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
package org.apache.solr.client.solrj.cloud.autoscaling;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.Objects;

import org.apache.solr.common.MapWriter;
import org.apache.solr.common.params.AutoScalingParams;
import org.apache.solr.common.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toList;

/**
 * Bean representation of <code>autoscaling.json</code>, which parses data
 * lazily.
 *
 * @deprecated to be removed in Solr 9.0 (see SOLR-14656)
 */
public class AutoScalingConfig implements MapWriter {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Map<String, Object> jsonMap;
  private final boolean empty;

  private Policy policy;
  private Map<String, TriggerConfig> triggers;
  private Map<String, TriggerListenerConfig> listeners;
  private Map<String, Object> properties;

  private final int zkVersion;

  /**
   * Bean representation of trigger listener config.
   */
  public static class TriggerListenerConfig implements MapWriter {
    public final String name;
    public final String trigger;
    public final EnumSet<TriggerEventProcessorStage> stages = EnumSet.noneOf(TriggerEventProcessorStage.class);
    public final String listenerClass;
    public final Set<String> beforeActions;
    public final Set<String> afterActions;
    public final Map<String, Object> properties;

    public TriggerListenerConfig(String name, Map<String, Object> properties) {
      this.name = name;
      if (properties == null) {
        this.properties = Collections.emptyMap();
      } else {
        this.properties = Collections.unmodifiableMap(new LinkedHashMap<>(properties));
      }
      trigger = (String)this.properties.get(AutoScalingParams.TRIGGER);
      List<Object> stageNames = getList(AutoScalingParams.STAGE, this.properties);
      for (Object stageName : stageNames) {
        try {
          TriggerEventProcessorStage stage = TriggerEventProcessorStage.valueOf(String.valueOf(stageName).toUpperCase(Locale.ROOT));
          stages.add(stage);
        } catch (Exception e) {
          log.warn("Invalid stage name '{}' for '{}' in listener config, skipping it in: {}",
              stageName, name, properties);
        }
      }
      listenerClass = (String)this.properties.get(AutoScalingParams.CLASS);
      Set<String> bActions = new LinkedHashSet<>();
      getList(AutoScalingParams.BEFORE_ACTION, this.properties).forEach(o -> bActions.add(String.valueOf(o)));
      beforeActions = Collections.unmodifiableSet(bActions);
      Set<String> aActions = new LinkedHashSet<>();
      getList(AutoScalingParams.AFTER_ACTION, this.properties).forEach(o -> aActions.add(String.valueOf(o)));
      afterActions = Collections.unmodifiableSet(aActions);
    }

    @Override
    public void writeMap(EntryWriter ew) throws IOException {
      // don't write duplicate entries - skip explicit fields if their values
      // are already contained in properties
//      if (!properties.containsKey(AutoScalingParams.NAME)) {
//        ew.put(AutoScalingParams.NAME, name);
//      }
      if (!properties.containsKey(AutoScalingParams.CLASS)) {
        ew.put(AutoScalingParams.CLASS, listenerClass);
      }
      if (!properties.containsKey(AutoScalingParams.TRIGGER)) {
        ew.put(AutoScalingParams.TRIGGER, trigger);
      }
      if (!properties.containsKey(AutoScalingParams.STAGE)) {
        ew.put(AutoScalingParams.STAGE, stages);
      }
      if (!properties.containsKey(AutoScalingParams.BEFORE_ACTION)) {
        ew.put(AutoScalingParams.BEFORE_ACTION, beforeActions);
      }
      if (!properties.containsKey(AutoScalingParams.AFTER_ACTION)) {
        ew.put(AutoScalingParams.AFTER_ACTION, afterActions);
      }
      // forEach doesn't allow throwing exceptions...
      for (Map.Entry<String, Object> entry : properties.entrySet()) {
        ew.put(entry.getKey(), entry.getValue());
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      TriggerListenerConfig that = (TriggerListenerConfig) o;

      if (name != null ? !name.equals(that.name) : that.name != null) return false;
      if (trigger != null ? !trigger.equals(that.trigger) : that.trigger != null) return false;
      if (!stages.equals(that.stages)) return false;
      if (listenerClass != null ? !listenerClass.equals(that.listenerClass) : that.listenerClass != null) return false;
      if (!beforeActions.equals(that.beforeActions)) return false;
      if (!afterActions.equals(that.afterActions)) return false;
      return properties.equals(that.properties);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, trigger, listenerClass);
    }

    @Override
    public String toString() {
      return Utils.toJSONString(this);
    }
  }

  /**
   * Bean representation of trigger config.
   */
  public static class TriggerConfig implements MapWriter {
    /** Trigger name. */
    public final String name;
    /** Trigger event type. */
    public final TriggerEventType event;
    /** Enabled flag. */
    public final boolean enabled;
    /** List of configured actions, never null. */
    public final List<ActionConfig> actions;
    /** Map of additional trigger properties, never null. */
    public final Map<String, Object> properties;

    public TriggerConfig(String name, Map<String, Object> properties) {
      this.name = name;
      if (properties != null) {
        this.properties = Collections.unmodifiableMap(new LinkedHashMap<>(properties));
      } else {
        this.properties = Collections.emptyMap();
      }
      String event = (String) this.properties.get(AutoScalingParams.EVENT);
      if (event != null) {
        TriggerEventType type = null;
        try {
          type = TriggerEventType.valueOf(event.toUpperCase(Locale.ROOT));
        } catch (Exception e) {
        }
        if (type == null) {
          this.event = TriggerEventType.INVALID;
        } else {
          this.event = type;
        }
      } else {
        this.event = TriggerEventType.INVALID;
      }
      enabled = Boolean.parseBoolean(String.valueOf(this.properties.getOrDefault("enabled", "true")));

      @SuppressWarnings({"unchecked"})
      List<Map<String, Object>> newActions = (List<Map<String, Object>>)this.properties.get("actions");
      if (newActions != null) {
        this.actions = newActions.stream().map(ActionConfig::new).collect(collectingAndThen(toList(), Collections::unmodifiableList));
      } else {
        this.actions = Collections.emptyList();
      }
    }

    /**
     * Create a copy of this config with specified enabled flag.
     * @param enabled true when enabled, false otherwise.
     * @return modified copy of the configuration
     */
    public TriggerConfig withEnabled(boolean enabled) {
      Map<String, Object> props = new LinkedHashMap<>(properties);
      props.put(AutoScalingParams.ENABLED, String.valueOf(enabled));
      return new TriggerConfig(name, props);
    }

    /**
     * Create a copy of this config with specified property.
     * @param key property name
     * @param value property value
     * @return modified copy of the configuration
     */
    public TriggerConfig withProperty(String key, Object value) {
      Map<String, Object> props = new LinkedHashMap<>(properties);
      props.put(key, String.valueOf(value));
      return new TriggerConfig(name, props);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      TriggerConfig that = (TriggerConfig) o;

      if (name != null ? !name.equals(that.name) : that.name != null) return false;
      if (event != that.event) return false;
      return properties.equals(that.properties);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name);
    }

    @Override
    public void writeMap(EntryWriter ew) throws IOException {
//      if (!properties.containsKey(AutoScalingParams.NAME)) {
//        ew.put(AutoScalingParams.NAME, name);
//      }
      if (!properties.containsKey(AutoScalingParams.EVENT)) {
        ew.put(AutoScalingParams.EVENT, event.toString());
      }
      // forEach doesn't allow throwing exceptions...
      for (Map.Entry<String, Object> entry : properties.entrySet()) {
        ew.put(entry.getKey(), entry.getValue());
      }
    }

    @Override
    public String toString() {
      return Utils.toJSONString(this);
    }
  }

  /**
   * Bean representation of trigger action configuration.
   */
  public static class ActionConfig implements MapWriter {
    /** Action name. */
    public final String name;
    /** Class name of action implementation. */
    public final String actionClass;
    /** Additional action properties. */
    public final Map<String, Object> properties;

    /**
     * Construct from a JSON map.
     * @param properties JSON map with properties - selected properties will be
     *                   used for setting the values of <code>name</code> and
     *                   <code>actionClass</code>.
     */
    public ActionConfig(Map<String, Object> properties) {
      if (properties != null) {
        this.properties = Collections.unmodifiableMap(new LinkedHashMap<>(properties));
      } else {
        this.properties = Collections.emptyMap();
      }
      this.name = (String)this.properties.get(AutoScalingParams.NAME);
      this.actionClass = (String)this.properties.get(AutoScalingParams.CLASS);
    }

    @Override
    public void writeMap(EntryWriter ew) throws IOException {
      // forEach doesn't allow throwing exceptions...
      for (Map.Entry<String, Object> entry : properties.entrySet()) {
        ew.put(entry.getKey(), entry.getValue());
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      ActionConfig that = (ActionConfig) o;

      return properties.equals(that.properties);
    }

    @Override
    public int hashCode() {
      return Objects.hash(properties);
    }

    @Override
    public String toString() {
      return Utils.toJSONString(this);
    }
  }

  /**
   * Construct from bytes that represent a UTF-8 JSON string.
   * @param utf8 config data
   */
  @SuppressWarnings({"unchecked"})
  public AutoScalingConfig(byte[] utf8) {
    this(utf8 != null && utf8.length > 0 ? (Map<String, Object>)Utils.fromJSON(utf8) : Collections.emptyMap());
  }

  /**
   * Construct from a JSON map representation.
   * @param jsonMap JSON map representation of the config. Note that this map is evaluated lazily, and
   *                outside modifications may cause unpredictable behavior.
   */
  public AutoScalingConfig(Map<String, Object> jsonMap) {
    this.jsonMap = jsonMap;
    int version = 0;
    if (jsonMap.containsKey(AutoScalingParams.ZK_VERSION)) {
      try {
        version = (Integer)jsonMap.get(AutoScalingParams.ZK_VERSION);
      } catch (Exception e) {
        // ignore
      }
    }
    zkVersion = version;
    jsonMap.remove(AutoScalingParams.ZK_VERSION);
    empty = jsonMap.isEmpty();
  }

  public AutoScalingConfig(Policy policy, Map<String, TriggerConfig> triggerConfigs, Map<String,
      TriggerListenerConfig> listenerConfigs, Map<String, Object> properties, int zkVersion) {
    this.policy = policy;
    this.triggers = triggerConfigs != null ? Collections.unmodifiableMap(new LinkedHashMap<>(triggerConfigs)) : null;
    this.listeners = listenerConfigs != null ? Collections.unmodifiableMap(new LinkedHashMap<>(listenerConfigs)) : null;
    this.jsonMap = null;
    this.properties = properties != null ? Collections.unmodifiableMap(new LinkedHashMap<>(properties)) : null;
    this.zkVersion = zkVersion;
    this.empty = policy == null &&
        (triggerConfigs == null || triggerConfigs.isEmpty()) &&
        (listenerConfigs == null || listenerConfigs.isEmpty());
  }

  /**
   * Return true if the source <code>autoscaling.json</code> was empty, false otherwise.
   */
  public boolean isEmpty() {
    return empty;
  }

  /**
   * Get {@link Policy} configuration.
   */
  public Policy getPolicy() {
    if (policy == null) {
      if (jsonMap != null) {
        policy = new Policy(jsonMap, zkVersion);
      } else {
        policy = new Policy();
      }
    }
    return policy;
  }

  /**
   * Get trigger configurations.
   */
  @SuppressWarnings({"unchecked"})
  public Map<String, TriggerConfig> getTriggerConfigs() {
    if (triggers == null) {
      if (jsonMap != null) {
        Map<String, Object> trigMap = (Map<String, Object>)jsonMap.get("triggers");
        if (trigMap == null) {
          triggers = Collections.emptyMap();
        } else {
          Map<String, TriggerConfig> newTriggers = new LinkedHashMap<>(trigMap.size());
          for (Map.Entry<String, Object> entry : trigMap.entrySet()) {
            newTriggers.put(entry.getKey(), new TriggerConfig(entry.getKey(), (Map<String, Object>)entry.getValue()));
          }
          triggers = Collections.unmodifiableMap(newTriggers);
        }
      } else {
        triggers = Collections.emptyMap();
      }
    }
    return triggers;
  }

  /**
   * Check whether triggers for specific event type exist.
   * @param types list of event types
   * @return true if there's at least one trigger matching at least one event type,
   * false otherwise,
   */
  public boolean hasTriggerForEvents(TriggerEventType... types) {
    if (types == null || types.length == 0) {
      return false;
    }
    for (TriggerConfig config : getTriggerConfigs().values()) {
      for (TriggerEventType type : types) {
        if (config.event.equals(type)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Get listener configurations.
   */
  @SuppressWarnings({"unchecked"})
  public Map<String, TriggerListenerConfig> getTriggerListenerConfigs() {
    if (listeners == null) {
      if (jsonMap != null) {
        Map<String, Object> map = (Map<String, Object>)jsonMap.get("listeners");
        if (map == null) {
          listeners = Collections.emptyMap();
        } else {
          Map<String, TriggerListenerConfig> newListeners = new LinkedHashMap<>(map.size());
          for (Map.Entry<String, Object> entry : map.entrySet()) {
            newListeners.put(entry.getKey(), new TriggerListenerConfig(entry.getKey(), (Map<String, Object>)entry.getValue()));
          }
          this.listeners = Collections.unmodifiableMap(newListeners);
        }
      } else {
        listeners = Collections.emptyMap();
      }
    }
    return listeners;
  }

  public Map<String, Object> getProperties()  {
    if (properties == null) {
      if (jsonMap != null)  {
        @SuppressWarnings({"unchecked"})
        Map<String, Object> map = (Map<String, Object>) jsonMap.get("properties");
        if (map == null) {
          this.properties = Collections.emptyMap();
        } else  {
          this.properties = new LinkedHashMap<>(map);
        }
      } else  {
        this.properties = Collections.emptyMap();
      }
    }
    return properties;
  }

  /**
   * Create a copy of the config with replaced properties.
   * @param properties the new properties map
   * @return modified copy of the configuration
   */
  public AutoScalingConfig withProperties(Map<String, Object> properties) {
    return new AutoScalingConfig(policy, getTriggerConfigs(), getTriggerListenerConfigs(), properties, zkVersion);
  }

  /**
   * Create a copy of the config with replaced policy.
   * @param policy new policy
   * @return modified copy of the configuration
   */
  public AutoScalingConfig withPolicy(Policy policy) {
    return new AutoScalingConfig(policy, getTriggerConfigs(), getTriggerListenerConfigs(), getProperties(), zkVersion);
  }

  /**
   * Create a copy of the config with replaced trigger configurations.
   * @param configs new trigger configurations
   * @return modified copy of the configuration
   */
  public AutoScalingConfig withTriggerConfigs(Map<String, TriggerConfig> configs) {
    return new AutoScalingConfig(getPolicy(), configs, getTriggerListenerConfigs(), getProperties(), zkVersion);
  }

  /**
   * Create a copy of the config with replaced trigger configuration
   * @param config new trigger configuration
   * @return modified copy of the configuration
   */
  public AutoScalingConfig withTriggerConfig(TriggerConfig config) {
    Map<String, TriggerConfig> configs = new LinkedHashMap<>(getTriggerConfigs());
    configs.put(config.name, config);
    return withTriggerConfigs(configs);
  }

  /**
   * Create a copy of the config without a trigger configuration.
   * @param name trigger configuration name
   * @return modified copy of the configuration, even if the specified config name didn't exist.
   */
  public AutoScalingConfig withoutTriggerConfig(String name) {
    Map<String, TriggerConfig> configs = new LinkedHashMap<>(getTriggerConfigs());
    configs.remove(name);
    return withTriggerConfigs(configs);
  }

  /**
   * Create a copy of the config with replaced trigger listener configurations.
   * @param configs new trigger listener configurations
   * @return modified copy of the configuration
   */
  public AutoScalingConfig withTriggerListenerConfigs(Map<String, TriggerListenerConfig> configs) {
    return new AutoScalingConfig(getPolicy(), getTriggerConfigs(), configs, getProperties(), zkVersion);
  }

  /**
   * Create a copy of the config with replaced trigger listener configuration.
   * @param config new trigger listener configuration
   * @return modified copy of the configuration
   */
  public AutoScalingConfig withTriggerListenerConfig(TriggerListenerConfig config) {
    Map<String, TriggerListenerConfig> configs = new LinkedHashMap<>(getTriggerListenerConfigs());
    configs.put(config.name, config);
    return withTriggerListenerConfigs(configs);
  }

  /**
   * Create a copy of the config without a trigger listener configuration.
   * @param name trigger listener configuration name
   * @return modified copy of the configuration, even if the specified config name didn't exist.
   */
  public AutoScalingConfig withoutTriggerListenerConfig(String name) {
    Map<String, TriggerListenerConfig> configs = new LinkedHashMap<>(getTriggerListenerConfigs());
    configs.remove(name);
    return withTriggerListenerConfigs(configs);
  }

  @Override
  public Object clone() {
    if (jsonMap != null) {
      return new AutoScalingConfig(jsonMap);
    } else {
      return new AutoScalingConfig(getPolicy(), getTriggerConfigs(), getTriggerListenerConfigs(), getProperties(), zkVersion);
    }
  }

  /**
   * Return the znode version that was used to create this configuration.
   */
  public int getZkVersion() {
    return zkVersion;
  }

  @Override
  public void writeMap(EntryWriter ew) throws IOException {
    Policy policy = getPolicy();
    // properties of Policy are expected at top level
    policy.writeMap(ew);

    ew.put("triggers", getTriggerConfigs());
    ew.put("listeners", getTriggerListenerConfigs());
    ew.put("properties", getProperties());
  }
//  @Override
//  public int hashCode() {
//    throw new UnsupportedOperationException("TODO unimplemented");
//  }


  public String toString() {
    return Utils.toJSONString(this);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    AutoScalingConfig that = (AutoScalingConfig) o;

    if (!getPolicy().equals(that.getPolicy())) return false;
    if (!getTriggerConfigs().equals(that.getTriggerConfigs())) return false;
    if (!getTriggerListenerConfigs().equals(that.getTriggerListenerConfigs())) return false;
    return getProperties().equals(that.getProperties());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getPolicy());
  }

  private static List<Object> getList(String key, Map<String, Object> properties) {
    return getList(key, properties, null);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static List<Object> getList(String key, Map<String, Object> properties, List<Object> defaultList) {
    if (defaultList == null) {
      defaultList = Collections.emptyList();
    }
    Object o = properties.get(key);
    if (o == null) {
      return defaultList;
    }
    if (o instanceof List) {
      return (List)o;
    } else if (o instanceof Collection) {
      return new ArrayList<>((Collection) o);
    } else {
      return Collections.singletonList(String.valueOf(o));
    }
  }

}
