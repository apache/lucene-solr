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

import org.apache.solr.cluster.placement.PropertyKey;
import org.apache.solr.cluster.placement.PropertyValue;

/**
 * This class and static subclasses implement all subtypes of {@link PropertyValue}
 */
public abstract class AbstractPropertyValue implements PropertyValue {
  private final PropertyKey propertyKey;

  AbstractPropertyValue(PropertyKey propertyKey) {
    this.propertyKey = propertyKey;
  }

  @Override
  public PropertyKey getKey() {
    return propertyKey;
  }

  static class CoresCountImpl extends AbstractPropertyValue implements CoresCount {
    private final int coresCount;

    public CoresCountImpl(PropertyKey key, Object nodeValue) {
      super(key);
      this.coresCount = ((Number) nodeValue).intValue();
    }

    @Override
    public int getCoresCount() {
      return coresCount;
    }
  }

  static class DiskTypeImpl extends AbstractPropertyValue implements PropertyValue.DiskType {
    private final HardwareType diskType;

    public DiskTypeImpl(PropertyKey key, Object nodeValue) {
      super(key);
      if ("rotational".equals(nodeValue)) {
        diskType = HardwareType.ROTATIONAL;
      } else if ("ssd".equals(nodeValue)) {
        diskType = HardwareType.SSD;
      } else {
        diskType = HardwareType.UNKNOWN;
      }
    }

    @Override
    public HardwareType getHardwareType() {
      return diskType;
    }
  }

  static class FreeDiskImpl extends AbstractPropertyValue implements PropertyValue.FreeDisk {
    private final long freeDiskGB;

    public FreeDiskImpl(PropertyKey key, Object nodeValue) {
      super(key);
      this.freeDiskGB = ((Number) nodeValue).longValue();
    }

    @Override
    public long getFreeSizeGB() {
      return freeDiskGB;
    }
  }

  static class HeapUsageImpl extends AbstractPropertyValue implements PropertyValue.HeapUsage {
    private final double heapUsage;

    public HeapUsageImpl(PropertyKey key, Object nodeValue) {
      super(key);
      this.heapUsage = ((Number) nodeValue).doubleValue();
    }

    @Override
    public double getUsedHeapMemoryUsage() {
      return heapUsage;
    }
  }

  static class MetricImpl extends AbstractPropertyValue implements PropertyValue.Metric {
    private final double metricValue;

    public MetricImpl(PropertyKey key, Object nodeValue) {
      super(key);
      this.metricValue = ((Number) nodeValue).doubleValue();
    }

    @Override
    public Double getNumberValue() {
      return metricValue;
    }
  }

  static class SyspropImpl extends AbstractPropertyValue implements PropertyValue.Sysprop {
    private final String systemProperty;

    public SyspropImpl(PropertyKey key, Object nodeValue) {
      super(key);
      this.systemProperty = (String) nodeValue;
    }

    @Override
    public String getSystemPropertyValue() {
      return systemProperty;
    }
  }

  static class SystemLoadImpl extends AbstractPropertyValue implements PropertyValue.SystemLoad {
    private final double systemLoadAverage;

    public SystemLoadImpl(PropertyKey key, Object nodeValue) {
      super(key);
      this.systemLoadAverage = ((Number) nodeValue).doubleValue();
    }

    @Override
    public double getSystemLoadAverage() {
      return systemLoadAverage;
    }
  }

  static class TotalDiskImpl extends AbstractPropertyValue implements PropertyValue.TotalDisk {
    private final long totalDiskGB;

    public TotalDiskImpl(PropertyKey key, Object nodeValue) {
      super(key);
      this.totalDiskGB = ((Number) nodeValue).longValue();
    }

    @Override
    public long getTotalSizeGB() {
      return totalDiskGB;
    }
  }
}
