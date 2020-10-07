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
package org.apache.solr.metrics;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.InvalidAttributeValueException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.ReflectionException;
import javax.management.openmbean.OpenMBeanAttributeInfoSupport;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.SolrException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dynamically constructed map of metrics, intentionally different from {@link com.codahale.metrics.MetricSet}
 * where each metric had to be known in advance and registered separately in {@link com.codahale.metrics.MetricRegistry}.
 * <p>Note: this awkwardly extends {@link Gauge} and not {@link Metric} because awkwardly {@link Metric} instances
 * are not supported by {@link com.codahale.metrics.MetricRegistryListener} :(</p>
 * <p>Note 2: values added to this metric map should belong to the list of types supported by JMX:
 * {@link javax.management.openmbean.OpenType#ALLOWED_CLASSNAMES_LIST}, otherwise only their toString()
 * representation will be shown in JConsole.</p>
 */
public class MetricsMap implements Gauge<Map<String,Object>>, MapWriter, DynamicMBean {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  // set to true to use cached statistics between getMBeanInfo calls to work
  // around over calling getStatistics on MBeanInfos when iterating over all attributes (SOLR-6586)
  private final boolean useCachedStatsBetweenGetMBeanInfoCalls = Boolean.getBoolean("useCachedStatsBetweenGetMBeanInfoCalls");

  private BiConsumer<Boolean, Map<String, Object>> mapInitializer;
  private MapWriter initializer;
  private Map<String, String> jmxAttributes;
  private volatile Map<String,Object> cachedValue;

  /**
   * Create an instance that reports values to a Map.
   * @param mapInitializer function to populate the Map result.
   * @deprecated use {@link #MetricsMap(MapWriter)} instead.
   */
  @Deprecated()
  public MetricsMap(BiConsumer<Boolean, Map<String,Object>> mapInitializer) {
    this.mapInitializer = mapInitializer;
  }

  /**
   * Create an instance that reports values to a MapWriter.
   * @param initializer function to populate the MapWriter result.
   */
  public MetricsMap(MapWriter initializer) {
    this.initializer = initializer;
  }

  @Override
  public Map<String,Object> getValue() {
    return getValue(true);
  }

  public Map<String,Object> getValue(boolean detailed) {
    Map<String,Object> map = new HashMap<>();
    if (mapInitializer != null) {
      mapInitializer.accept(detailed, map);
    } else {
      initializer.toMap(map);
    }
    return map;
  }

  public String toString() {
    return getValue().toString();
  }

  // lazy init
  private synchronized void initJmxAttributes() {
    if (jmxAttributes == null) {
      jmxAttributes = new HashMap<>();
    }
  }

  @Override
  public Object getAttribute(String attribute) throws AttributeNotFoundException, MBeanException, ReflectionException {
    Object val;
    // jmxAttributes override any real values
    if (jmxAttributes != null) {
      val = jmxAttributes.get(attribute);
      if (val != null) {
        return val;
      }
    }
    Map<String,Object> stats = null;
    if (useCachedStatsBetweenGetMBeanInfoCalls) {
      Map<String,Object> cachedStats = this.cachedValue;
      if (cachedStats != null) {
        stats = cachedStats;
      }
    }
    if (stats == null) {
      stats = getValue(true);
    }
    val = stats.get(attribute);

    if (val != null) {
      // It's String or one of the simple types, just return it as JMX suggests direct support for such types
      for (String simpleTypeName : SimpleType.ALLOWED_CLASSNAMES_LIST) {
        if (val.getClass().getName().equals(simpleTypeName)) {
          return val;
        }
      }
      // It's an arbitrary object which could be something complex and odd, return its toString, assuming that is
      // a workable representation of the object
      return val.toString();
    }
    return null;
  }

  @Override
  public void setAttribute(Attribute attribute) throws AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException {
    initJmxAttributes();
    jmxAttributes.put(attribute.getName(), String.valueOf(attribute.getValue()));
  }

  @Override
  public AttributeList getAttributes(String[] attributes) {
    AttributeList list = new AttributeList();
    for (String attribute : attributes) {
      try {
        list.add(new Attribute(attribute, getAttribute(attribute)));
      } catch (Exception e) {
        log.warn("Could not get attribute {}", attribute);
      }
    }
    return list;
  }

  @Override
  public AttributeList setAttributes(AttributeList attributes) {
    throw new UnsupportedOperationException("Operation not Supported");
  }

  @Override
  public Object invoke(String actionName, Object[] params, String[] signature) throws MBeanException, ReflectionException {
    throw new UnsupportedOperationException("Operation not Supported");
  }

  @Override
  public MBeanInfo getMBeanInfo() {
    ArrayList<MBeanAttributeInfo> attrInfoList = new ArrayList<>();
    Map<String,Object> stats = getValue(true);
    if (useCachedStatsBetweenGetMBeanInfoCalls) {
      cachedValue = stats;
    }
    if (jmxAttributes != null) {
      jmxAttributes.forEach((k, v) -> {
        attrInfoList.add(new MBeanAttributeInfo(k, String.class.getName(),
            null, true, false, false));
      });
    }
    try {
      stats.forEach((k, v) -> {
        if (jmxAttributes != null && jmxAttributes.containsKey(k)) {
          return;
        }
        @SuppressWarnings({"rawtypes"})
        Class type = v.getClass();
        @SuppressWarnings({"rawtypes"})
        OpenType typeBox = determineType(type);
        if (type.equals(String.class) || typeBox == null) {
          attrInfoList.add(new MBeanAttributeInfo(k, String.class.getName(),
              null, true, false, false));
        } else {
          attrInfoList.add(new OpenMBeanAttributeInfoSupport(
              k, k, typeBox, true, false, false));
        }
      });
    } catch (Exception e) {
      // don't log issue if the core is closing
      if (!(SolrException.getRootCause(e) instanceof AlreadyClosedException))
        log.warn("Could not get attributes of MetricsMap: {}", this, e);
    }
    MBeanAttributeInfo[] attrInfoArr = attrInfoList
        .toArray(new MBeanAttributeInfo[attrInfoList.size()]);
    return new MBeanInfo(getClass().getName(), "MetricsMap", attrInfoArr, null, null, null);
  }

  @SuppressWarnings({"rawtypes"})
  private OpenType determineType(Class type) {
    try {
      for (Field field : SimpleType.class.getFields()) {
        if (field.getType().equals(SimpleType.class)) {
          SimpleType candidate = (SimpleType) field.get(SimpleType.class);
          if (candidate.getTypeName().equals(type.getName())) {
            return candidate;
          }
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return null;
  }

  @Override
  public void writeMap(EntryWriter ew) throws IOException {
    if (mapInitializer != null) {
      Map<String, Object> value = getValue();
      value.forEach((k, v) -> ew.putNoEx(k, v));
    } else {
      initializer.writeMap(ew);
    }
  }
}