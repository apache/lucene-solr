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

import javax.management.JMException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.codahale.metrics.JmxAttributeGauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is an extended replacement for {@link com.codahale.metrics.jvm.FileDescriptorRatioGauge}
 * - that class uses reflection and doesn't work under Java 9. We can also get much more
 * information about OS environment once we have to go through MBeanServer anyway.
 */
public class OperatingSystemMetricSet implements MetricSet {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /** Metric names - these correspond to known numeric MBean attributes. Depending on the OS and
   * Java implementation only some of them may be actually present.
   */
  public static final String[] METRICS = {
      "AvailableProcessors",
      "CommittedVirtualMemorySize",
      "FreePhysicalMemorySize",
      "FreeSwapSpaceSize",
      "MaxFileDescriptorCount",
      "OpenFileDescriptorCount",
      "ProcessCpuLoad",
      "ProcessCpuTime",
      "SystemLoadAverage",
      "TotalPhysicalMemorySize",
      "TotalSwapSpaceSize"
  };

  private final MBeanServer mBeanServer;

  public OperatingSystemMetricSet(MBeanServer mBeanServer) {
    this.mBeanServer = mBeanServer;
  }

  @Override
  public Map<String, Metric> getMetrics() {
    final Map<String, Metric> metrics = new HashMap<>();

    try {
      final ObjectName on = new ObjectName("java.lang:type=OperatingSystem");
      // verify that it exists
      MBeanInfo info = mBeanServer.getMBeanInfo(on);
      // collect valid attributes
      Set<String> attributes = new HashSet<>();
      for (MBeanAttributeInfo ai : info.getAttributes()) {
        attributes.add(ai.getName());
      }
      for (String metric : METRICS) {
        // verify that an attribute exists before attempting to add it
        if (attributes.contains(metric)) {
          metrics.put(metric, new JmxAttributeGauge(mBeanServer, on, metric));
        }
      }
    } catch (JMException ignored) {
      log.debug("Unable to load OperatingSystem MBean", ignored);
    }

    return metrics;
  }
}
