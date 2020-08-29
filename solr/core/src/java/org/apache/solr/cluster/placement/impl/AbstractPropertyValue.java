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

import org.apache.solr.cluster.placement.CoresCountPropertyValue;
import org.apache.solr.cluster.placement.DiskTypePropertyValue;
import org.apache.solr.cluster.placement.FreeDiskPropertyValue;
import org.apache.solr.cluster.placement.HeapUsagePropertyValue;
import org.apache.solr.cluster.placement.MetricPropertyValue;
import org.apache.solr.cluster.placement.PropertyKey;
import org.apache.solr.cluster.placement.PropertyValue;
import org.apache.solr.cluster.placement.SyspropPropertyValue;
import org.apache.solr.cluster.placement.SystemLoadPropertyValue;
import org.apache.solr.cluster.placement.TotalDiskPropertyValue;

public abstract class AbstractPropertyValue implements PropertyValue {
  private final PropertyKey propertyKey;

  AbstractPropertyValue(PropertyKey propertyKey) {
    this.propertyKey = propertyKey;
  }

  @Override
  public PropertyKey getKey() {
    return propertyKey;
  }
}

class CoresCountPropertyValueImpl extends AbstractPropertyValue implements CoresCountPropertyValue {
  private final int coresCount;

  public CoresCountPropertyValueImpl(PropertyKey key, Object nodeValue) {
    super(key);
    this.coresCount = ((Number) nodeValue).intValue();
  }

  @Override
  public int getCoresCount() {
    return coresCount;
  }
}

class DiskTypePropertyValueImpl extends AbstractPropertyValue implements DiskTypePropertyValue {
  private final DiskType diskType;

  public DiskTypePropertyValueImpl(PropertyKey key, Object nodeValue) {
    super(key);
    if ("rotational".equals(nodeValue)) {
      diskType = DiskType.ROTATIONAL;
    } else if ("ssd".equals(nodeValue)) {
      diskType = DiskType.SSD;
    } else {
      diskType = DiskType.UNKNOWN;
    }
  }

  @Override
  public DiskType getDiskType() {
    return diskType;
  }
}

class FreeDiskPropertyValueImpl extends AbstractPropertyValue implements FreeDiskPropertyValue {
  private final long freeDiskGB;

  public FreeDiskPropertyValueImpl(PropertyKey key, Object nodeValue) {
    super(key);
    this.freeDiskGB = ((Number) nodeValue).longValue();
  }

  @Override
  public long getFreeSizeGB() {
    return freeDiskGB;
  }
}

class HeapUsagePropertyValueImpl extends AbstractPropertyValue implements HeapUsagePropertyValue {
  private final double heapUsage;

  public HeapUsagePropertyValueImpl(PropertyKey key, Object nodeValue) {
    super(key);
    this.heapUsage = ((Number) nodeValue).doubleValue();
  }

  @Override
  public double getUsedHeapMemoryUsage() {
    return heapUsage;
  }
}

class MetricPropertyValueImpl extends AbstractPropertyValue implements MetricPropertyValue {
  private final double metricValue;

  public MetricPropertyValueImpl(PropertyKey key, Object nodeValue) {
    super(key);
    this.metricValue = ((Number) nodeValue).doubleValue();
  }

  @Override
  public Double getNumberValue() {
    return metricValue;
  }
}

class SyspropPropertyValueImpl extends AbstractPropertyValue implements SyspropPropertyValue {
  private final String systemProperty;

  public SyspropPropertyValueImpl(PropertyKey key, Object nodeValue) {
    super(key);
    this.systemProperty = (String) nodeValue;
  }

  @Override
  public String getSystemPropertyValue() {
    return systemProperty;
  }
}

class SystemLoadPropertyValueImpl extends AbstractPropertyValue implements SystemLoadPropertyValue {
  private final double systemLoadAverage;

  public SystemLoadPropertyValueImpl(PropertyKey key, Object nodeValue) {
    super(key);
    this.systemLoadAverage = ((Number) nodeValue).doubleValue();
  }

  @Override
  public double getSystemLoadAverage() {
    return systemLoadAverage;
  }
}

class TotalDiskPropertyValueImpl extends AbstractPropertyValue implements TotalDiskPropertyValue {
  private final long totalDiskGB;

  public TotalDiskPropertyValueImpl(PropertyKey key, Object nodeValue) {
    super(key);
    this.totalDiskGB = ((Number) nodeValue).longValue();
  }

  @Override
  public long getTotalSizeGB() {
    return totalDiskGB;
  }
}
