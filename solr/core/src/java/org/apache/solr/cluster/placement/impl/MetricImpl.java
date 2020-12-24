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
package org.apache.solr.cluster.placement.impl;

import org.apache.solr.cluster.placement.Metric;

import java.util.Objects;
import java.util.function.Function;

/**
 * Base class for {@link Metric} implementations.
 */
public abstract class MetricImpl<T> implements Metric<T> {

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
        sizeInBytes = Double.parseDouble(String.valueOf(v));
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
  public MetricImpl(String name, String internalName) {
    this(name, internalName, null);
  }

  /**
   * Create a metric attribute.
   * @param name short-hand name that identifies this attribute.
   * @param internalName internal name of a Solr metric.
   * @param converter optional raw value converter. If null then
   *                  {@link #IDENTITY_CONVERTER} will be used.
   */
  public MetricImpl(String name, String internalName, Function<Object, T> converter) {
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

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getInternalName() {
    return internalName;
  }

  @Override
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
    MetricImpl<?> that = (MetricImpl<?>) o;
    return name.equals(that.getName()) && internalName.equals(that.getInternalName()) && converter.equals(that.converter);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, internalName, converter);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "{" +
        "name=" + name +
        ", internalName=" + internalName +
        "}";
  }
}
