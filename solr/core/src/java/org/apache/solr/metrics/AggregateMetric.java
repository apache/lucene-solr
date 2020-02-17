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

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.codahale.metrics.Metric;

import java.lang.invoke.MethodHandles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used for keeping several partial named values and providing useful statistics over them.
 */
public class AggregateMetric implements Metric {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Simple class to represent current value and how many times it was set.
   * <p>Note: values should belong to the list of types supported by JMX:
   * {@link javax.management.openmbean.OpenType#ALLOWED_CLASSNAMES_LIST}</p>
   */
  public static class Update {
  
    // use of Object is deliberate - see note above
    public Object value;
    public final AtomicInteger updateCount = new AtomicInteger();

    public Update(Object value) {
      update(value);
    }

    public void update(Object value) {
      this.value = value;
      updateCount.incrementAndGet();
    }

    @Override
    public String toString() {
      return "Update{" +
          "value=" + value +
          ", updateCount=" + updateCount +
          '}';
    }

    private double toDouble() {
      if (value instanceof Boolean) {
        return 0;
      } 
      if (!(value instanceof Number)) {
        log.debug("not a Number: " + value);
        return 0;
      }
      Number n = (Number)value;
      double d = n.doubleValue();
      if (d<0) {
        log.warn("negative metric value: " + d);
      }
      return d;
    }
  }

  private final Map<String, Update> values = new ConcurrentHashMap<>();

  public void set(String name, Object value) {
    final Update existing = values.get(name);
    if (existing == null) {
      final Update created = new Update(value);
      final Update raced = values.putIfAbsent(name, created);
      if (raced != null) {
        raced.update(value);
      }
    } else {
      existing.update(value);
    }
  }

  public void clear(String name) {
    values.remove(name);
  }

  public void clear() {
    values.clear();
  }

  public int size() {
    return values.size();
  }

  public boolean isEmpty() {
    return values.isEmpty();
  }

  public Map<String, Update> getValues() {
    return Collections.unmodifiableMap(values);
  }

  // --------- stats ---------
  public double getMax() {
    if (values.isEmpty()) {
      return 0;
    }
    double res = 0;
    for (Update u : values.values()) {
      double d = u.toDouble();
      if (d > res) {
        res = d;
      }
    }
    return res;
  }

  public double getMin() {
    if (values.isEmpty()) {
      return 0;
    }
    double res = 0;
    for (Update u : values.values()) {
      double d = u.toDouble();
      if (d < res) {
        res = d;
      }
    }
    return res;
  }

  public double getMean() {
    if (values.isEmpty()) {
      return 0;
    }
    double total = 0;
    for (Update u : values.values()) {
      total += u.toDouble();
    }
    return total / values.size();
  }

  public double getStdDev() {
    int size = values.size();
    if (size < 2) {
      return 0;
    }
    final double mean = getMean();
    double sum = 0;
    int count = 0;
    for (Update u : values.values()) {
      double d = u.toDouble();
      count++;
      final double diff = d - mean;
      sum += diff * diff;
    }
    if (count < 2) {
      return 0;
    }
    final double variance = sum / (count - 1);
    return Math.sqrt(variance);
  }

  public double getSum() {
    if (values.isEmpty()) {
      return 0;
    }
    double res = 0;
    for (Update u : values.values()) {
      res += u.toDouble();
    }
    return res;
  }

  @Override
  public String toString() {
    return "AggregateMetric{" +
        "size=" + size() +
        ", max=" + getMax() +
        ", min=" + getMin() +
        ", mean=" + getMean() +
        ", stddev=" + getStdDev() +
        ", sum=" + getSum() +
        ", values=" + values +
        '}';
  }
}
