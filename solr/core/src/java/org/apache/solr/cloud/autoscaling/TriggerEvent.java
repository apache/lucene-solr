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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.client.solrj.cloud.autoscaling.Suggester;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventType;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.IdUtils;

/**
 * Trigger event.
 *
 * @deprecated to be removed in Solr 9.0 (see SOLR-14656)
 */
public class TriggerEvent implements MapWriter {
  public static final String IGNORED = "ignored";
  public static final String COOLDOWN = "cooldown";
  public static final String REPLAYING = "replaying";
  public static final String NODE_NAMES = "nodeNames";
  public static final String EVENT_TIMES = "eventTimes";
  public static final String REQUESTED_OPS = "requestedOps";
  public static final String UNSUPPORTED_OPS = "unsupportedOps";

  public static final class Op implements MapWriter {
    private final CollectionParams.CollectionAction action;
    private final EnumMap<Suggester.Hint, Object> hints = new EnumMap<>(Suggester.Hint.class);

    public Op(CollectionParams.CollectionAction action) {
      this.action = action;
    }

    public Op(CollectionParams.CollectionAction action, Suggester.Hint hint, Object hintValue) {
      this.action = action;
      addHint(hint, hintValue);
    }

    @SuppressWarnings({"unchecked"})
    public void addHint(Suggester.Hint hint, Object value) {
      hint.validator.accept(value);
      if (hint.multiValued) {
        Collection<?> values = value instanceof Collection ? (Collection) value : Collections.singletonList(value);
        ((Set) hints.computeIfAbsent(hint, h -> new LinkedHashSet<>())).addAll(values);
      } else if (value instanceof Map) {
        hints.put(hint, value);
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
    public void writeMap(EntryWriter ew) throws IOException {
      ew.put("action", action);
      ew.put("hints", hints);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static Op fromMap(Map<String, Object> map) {
      if (!map.containsKey("action")) {
        return null;
      }
      CollectionParams.CollectionAction action = CollectionParams.CollectionAction.get(String.valueOf(map.get("action")));
      if (action == null) {
        return null;
      }
      Op op = new Op(action);
      Map<Object, Object> hints = (Map<Object, Object>)map.get("hints");
      if (hints != null && !hints.isEmpty()) {
        hints.forEach((k, v) ->  {
          Suggester.Hint h = Suggester.Hint.get(k.toString());
          if (h == null) {
            return;
          }
          if (!(v instanceof Collection)) {
            v = Collections.singletonList(v);
          }
          ((Collection)v).forEach(vv -> {
            if (vv instanceof Map) {
              // maybe it's a Pair?
              Map<String, Object> m = (Map<String, Object>)vv;
              if (m.containsKey("first") && m.containsKey("second")) {
                Pair p = Pair.parse(m);
                if (p != null) {
                  op.addHint(h, p);
                  return;
                }
              }
            }
            op.addHint(h, vv);
          });
        });
      }
      return op;
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

  @SuppressWarnings({"unchecked"})
  public static TriggerEvent fromMap(Map<String, Object> map) {
    String id = (String)map.get("id");
    String source = (String)map.get("source");
    long eventTime = ((Number)map.get("eventTime")).longValue();
    TriggerEventType eventType = TriggerEventType.valueOf((String)map.get("eventType"));
    Map<String, Object> properties = (Map<String, Object>)map.get("properties");
    // properly deserialize some well-known complex properties
    fixOps(TriggerEvent.REQUESTED_OPS, properties);
    fixOps(TriggerEvent.UNSUPPORTED_OPS, properties);
    TriggerEvent res = new TriggerEvent(id, eventType, source, eventTime, properties);
    return res;
  }

  @SuppressWarnings({"unchecked"})
  public static void fixOps(String type, Map<String, Object> properties) {
    List<Object> ops = (List<Object>)properties.get(type);
    if (ops != null && !ops.isEmpty()) {
      for (int i = 0; i < ops.size(); i++) {
        Object o = ops.get(i);
        if (o instanceof Map) {
          TriggerEvent.Op op = TriggerEvent.Op.fromMap((Map)o);
          if (op != null) {
            ops.set(i, op);
          }
        }
      }
    }
  }
}
