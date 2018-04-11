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

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.solr.client.solrj.cloud.autoscaling.Suggester;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventType;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.IdUtils;

/**
 * Trigger event.
 */
public class TriggerEvent implements MapWriter {
  public static final String IGNORED = "ignored";
  public static final String COOLDOWN = "cooldown";
  public static final String REPLAYING = "replaying";
  public static final String NODE_NAMES = "nodeNames";
  public static final String EVENT_TIMES = "eventTimes";
  public static final String REQUESTED_OPS = "requestedOps";

  public static final class Op {
    private final CollectionParams.CollectionAction action;
    private final EnumMap<Suggester.Hint, Object> hints = new EnumMap<>(Suggester.Hint.class);

    public Op(CollectionParams.CollectionAction action) {
      this.action = action;
    }

    public Op(CollectionParams.CollectionAction action, Suggester.Hint hint, Object hintValue) {
      this.action = action;
      addHint(hint, hintValue);
    }

    public void addHint(Suggester.Hint hint, Object value) {
      hint.validator.accept(value);
      if (hint.multiValued) {
        Collection<?> values = value instanceof Collection ? (Collection) value : Collections.singletonList(value);
        ((Set) hints.computeIfAbsent(hint, h -> new HashSet<>())).addAll(values);
      } else {
        hints.put(hint, value == null ? null : String.valueOf(value));
      }
    }

    public CollectionParams.CollectionAction getAction() {
      return action;
    }

    public EnumMap<Suggester.Hint, Object> getHints() {
      return hints;
    }

    @Override
    public String toString() {
      return "Op{" +
          "action=" + action +
          ", hints=" + hints +
          '}';
    }
  }

  protected final String id;
  protected final String source;
  protected final long eventTime;
  protected final TriggerEventType eventType;
  protected final Map<String, Object> properties = new HashMap<>();
  protected final boolean ignored;

  public TriggerEvent(TriggerEventType eventType, String source, long eventTime,
                      Map<String, Object> properties) {
    this(IdUtils.timeRandomId(eventTime), eventType, source, eventTime, properties, false);
  }

  public TriggerEvent(TriggerEventType eventType, String source, long eventTime,
                      Map<String, Object> properties, boolean ignored) {
    this(IdUtils.timeRandomId(eventTime), eventType, source, eventTime, properties, ignored);
  }

  public TriggerEvent(String id, TriggerEventType eventType, String source, long eventTime,
                      Map<String, Object> properties) {
    this(id, eventType, source, eventTime, properties, false);
  }

  public TriggerEvent(String id, TriggerEventType eventType, String source, long eventTime,
                      Map<String, Object> properties, boolean ignored) {
    this.id = id;
    this.eventType = eventType;
    this.source = source;
    this.eventTime = eventTime;
    if (properties != null) {
      this.properties.putAll(properties);
    }
    this.ignored = ignored;
  }

  /**
   * Unique event id.
   */
  public String getId() {
    return id;
  }

  /**
   * Name of the trigger that fired the event.
   */
  public String getSource() {
    return source;
  }

  /**
   * Timestamp of the actual event, in nanoseconds.
   * NOTE: this is NOT the timestamp when the event was fired - events may be fired
   * much later than the actual condition that generated the event, due to the "waitFor" limit.
   */
  public long getEventTime() {
    return eventTime;
  }

  /**
   * Get event properties (modifiable).
   */
  public Map<String, Object> getProperties() {
    return properties;
  }

  /**
   * Get a named event property or null if missing.
   */
  public Object getProperty(String name) {
    return properties.get(name);
  }

  /**
   * Get a named event property or default value if missing.
   */
  public Object getProperty(String name, Object defaultValue) {
    Object v = properties.get(name);
    if (v == null) {
      return defaultValue;
    } else {
      return v;
    }
  }

  /**
   * Event type.
   */
  public TriggerEventType getEventType() {
    return eventType;
  }

  public boolean isIgnored() {
    return ignored;
  }

  /**
   * Set event properties.
   *
   * @param properties may be null. A shallow copy of this parameter is used.
   */
  public void setProperties(Map<String, Object> properties) {
    this.properties.clear();
    if (properties != null) {
      this.properties.putAll(properties);
    }
  }

  @Override
  public void writeMap(EntryWriter ew) throws IOException {
    ew.put("id", id);
    ew.put("source", source);
    ew.put("eventTime", eventTime);
    ew.put("eventType", eventType.toString());
    ew.put("properties", properties);
    if (ignored)  {
      ew.put("ignored", true);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    TriggerEvent that = (TriggerEvent) o;

    if (eventTime != that.eventTime) return false;
    if (!id.equals(that.id)) return false;
    if (!source.equals(that.source)) return false;
    if (eventType != that.eventType) return false;
    if (ignored != that.ignored)  return false;
    return properties.equals(that.properties);
  }

  @Override
  public int hashCode() {
    int result = id.hashCode();
    result = 31 * result + source.hashCode();
    result = 31 * result + (int) (eventTime ^ (eventTime >>> 32));
    result = 31 * result + eventType.hashCode();
    result = 31 * result + properties.hashCode();
    result = 31 * result + Boolean.hashCode(ignored);
    return result;
  }

  @Override
  public String toString() {
    return Utils.toJSONString(this);
  }
}
