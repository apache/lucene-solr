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
package org.apache.solr.cluster.placement;

import java.util.Objects;
import java.util.function.Function;

/**
 * Metric-related attribute of a node or replica. It defines a short symbolic name of the metric, the corresponding
 * internal metric name and the desired format/unit conversion. Generic type
 * defines the type of converted values of this attribute.
 */
public class MetricAttribute<T> {

  public static final double GB = 1024 * 1024 * 1024;

  /**
   * Identity converter. It returns the raw value unchanged IFF
   * the value's type can be cast to the generic type of this attribute,
   * otherwise it returns null.
   */
  @SuppressWarnings("unchecked")
  public final Function<Object, T> IDENTITY_CONVERTER = v -> {
    try {
      return (T) v;
    } catch (ClassCastException cce) {
      return null;
    }
  };

  /**
   * Bytes to gigabytes converter. Supports converting number or string
   * representations of raw values expressed in bytes.
   */
  @SuppressWarnings("unchecked")
  public static final Function<Object, Double> BYTES_TO_GB_CONVERTER = v -> {
    double sizeInBytes;
    if (!(v instanceof Number)) {
      if (v == null) {
        return null;
      }
      try {
        sizeInBytes = Double.valueOf(String.valueOf(v)).doubleValue();
      } catch (Exception nfe) {
        return null;
      }
    } else {
      sizeInBytes = ((Number) v).doubleValue();
    }
    return sizeInBytes / GB;
  };

  protected final String name;
  protected final String internalName;
  protected final Function<Object, T> converter;

  /**
   * Create a metric attribute.
   * @param name short-hand name that identifies this attribute.
   * @param internalName internal name of a Solr metric.
   */
  public MetricAttribute(String name, String internalName) {
    this(name, internalName, null);
  }

  /**
   * Create a metric attribute.
   * @param name short-hand name that identifies this attribute.
   * @param internalName internal name of a Solr metric.
   * @param converter optional raw value converter. If null then
   *                  {@link #IDENTITY_CONVERTER} will be used.
   */
  public MetricAttribute(String name, String internalName, Function<Object, T> converter) {
    Objects.requireNonNull(name);
    Objects.requireNonNull(internalName);
    this.name = name;
    this.internalName = internalName;
    if (converter == null) {
      this.converter = IDENTITY_CONVERTER;
    } else {
      this.converter = converter;
    }
  }

  /**
   * Return the short-hand name that identifies this attribute.
   */
  public String getName() {
    return name;
  }

  /**
   * Return the internal name of a Solr metric associated with this attribute.
   */
  public String getInternalName() {
    return internalName;
  }

  /**
   * Convert raw value. This may involve changing the type or units.
   * @param value raw value
   * @return converted value
   */
  public T convert(Object value) {
    return converter.apply(value);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MetricAttribute<?> that = (MetricAttribute<?>) o;
    return name.equals(that.name) && internalName.equals(that.internalName) && converter.equals(that.converter);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, internalName, converter);
  }

  @Override
  public String toString() {
    return name + "(" + internalName + ")";
  }
}
