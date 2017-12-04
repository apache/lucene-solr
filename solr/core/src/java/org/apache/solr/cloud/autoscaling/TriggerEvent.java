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
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventType;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.IdUtils;

/**
 * Trigger event.
 */
public class TriggerEvent implements MapWriter {
  public static final String COOLDOWN = "cooldown";
  public static final String REPLAYING = "replaying";
  public static final String NODE_NAMES = "nodeNames";
  public static final String EVENT_TIMES = "eventTimes";

  protected final String id;
  protected final String source;
  protected final long eventTime;
  protected final TriggerEventType eventType;
  protected final Map<String, Object> properties = new HashMap<>();

  public TriggerEvent(TriggerEventType eventType, String source, long eventTime,
                      Map<String, Object> properties) {
    this(IdUtils.timeRandomId(eventTime), eventType, source, eventTime, properties);
  }

  public TriggerEvent(String id, TriggerEventType eventType, String source, long eventTime,
                      Map<String, Object> properties) {
    this.id = id;
    this.eventType = eventType;
    this.source = source;
    this.eventTime = eventTime;
    if (properties != null) {
      this.properties.putAll(properties);
    }
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
   * Event type.
   */
  public TriggerEventType getEventType() {
    return eventType;
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
    return properties.equals(that.properties);
  }

  @Override
  public int hashCode() {
    int result = id.hashCode();
    result = 31 * result + source.hashCode();
    result = 31 * result + (int) (eventTime ^ (eventTime >>> 32));
    result = 31 * result + eventType.hashCode();
    result = 31 * result + properties.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return Utils.toJSONString(this);
  }
}
