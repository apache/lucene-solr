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

import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;

/**
 * This is an alternative implementation of {@link com.codahale.metrics.jvm.BufferPoolMetricSet} that
 * doesn't need an MBean server.
 */
public class AltBufferPoolMetricSet implements MetricSet {

  @Override
  public Map<String, Metric> getMetrics() {
    final Map<String, Metric> metrics = new HashMap<>();
    List<BufferPoolMXBean> pools = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);
    for (final BufferPoolMXBean pool : pools) {
      String name = pool.getName();
      metrics.put(name + ".Count", (Gauge<Long>)() -> pool.getCount());
      metrics.put(name + ".MemoryUsed", (Gauge<Long>)() -> pool.getMemoryUsed());
      metrics.put(name + ".TotalCapacity", (Gauge<Long>)() -> pool.getTotalCapacity());
    }
    return metrics;
  }
}
