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

/**
 *  <p>The value corresponding to a specific {@link PropertyKey}, in a specific context (e.g. property of a specific
 *  {@link Node} instance). The context is tracked in the {@link PropertyKey} using a {@link PropertyValueSource}.
 *
 *  <p>Instances are obtained by first getting a key using {@link PropertyKeyFactory} then getting the corresponding
 *  {@link PropertyValue} using {@link PropertyValueFetcher}.
 */
public interface PropertyValue {
  /**
   * The property key used for retrieving this property value.
   */
  PropertyKey getKey();

  /**
   *  Instances are obtained by first getting a key using {@link PropertyKeyFactory#createCoreCountKey} then calling
   *  {@link PropertyValueFetcher#fetchProperties}, retrieving the appropriate {@link PropertyValue} from the returned map
   *  using the {@link PropertyKey} as key and finally casting it to {@link PropertyValue.CoresCount}.
   */
  interface CoresCount extends PropertyValue {
    /**
     * Returns the number of cores on the {@link Node}) this instance was obtained from (i.e. instance
     * passed to {@link PropertyKeyFactory#createCoreCountKey(Node)}).
     */
    int getCoresCount();
  }

  /**
   *  Instances are obtained by first getting a key using {@link PropertyKeyFactory#createDiskTypeKey} then getting the
   *  {@link org.apache.solr.cluster.placement.PropertyValue.DiskType} using {@link PropertyValueFetcher#fetchProperties} and retrieving (then casting) the
   *  appropriate {@link PropertyValue} from the returned map using the {@link PropertyKey} as key.
   */
  interface DiskType extends PropertyValue {
    /**
     * Type of storage hardware used for the partition on which cores are stored on the {@link Node}) from which this instance
     * was obtained (i.e. instance passed to {@link PropertyKeyFactory#createDiskTypeKey(Node)}).
     */
    HardwareType getHardwareType();

    enum HardwareType {
      SSD, ROTATIONAL, UNKNOWN
    }
  }

  /**
   *  Instances are obtained by first getting a key using {@link PropertyKeyFactory#createTotalDiskKey(Node)} (Node)} then getting the
   *  {@link org.apache.solr.cluster.placement.PropertyValue.TotalDisk} using {@link PropertyValueFetcher#fetchProperties} and retrieving (then casting) the
   *  appropriate {@link PropertyValue} fetched from the returned map using the {@link PropertyKey} as key.
   */
  interface TotalDisk extends PropertyValue {
    /**
     * Total disk size of the partition on which cores are stored on the {@link Node}) from which this instance was obtained
     * (i.e. instance passed to {@link PropertyKeyFactory#createTotalDiskKey(Node)}).
     */
    long getTotalSizeGB();
  }

  /**
   *  Instances are obtained by first getting a key using {@link PropertyKeyFactory#createFreeDiskKey(Node)} then getting the
   *  {@link org.apache.solr.cluster.placement.PropertyValue.FreeDisk} using {@link PropertyValueFetcher#fetchProperties} and retrieving (then casting) the
   *  appropriate {@link PropertyValue} fetched from the returned map using the {@link PropertyKey} as key.
   */
  interface FreeDisk extends PropertyValue {
    /**
     * Free disk size of the partition on which cores are stored on the {@link Node}) from which this instance was obtained
     *  (i.e. instance passed to {@link PropertyKeyFactory#createDiskTypeKey(Node)}).
     */
    long getFreeSizeGB();
  }


  /**
   *  Instances are obtained by first getting a key using {@link PropertyKeyFactory#createHeapUsageKey(Node)} then calling
   *  {@link PropertyValueFetcher#fetchProperties}, retrieving the appropriate {@link PropertyValue} from the returned map
   *  using the {@link PropertyKey} as key and finally casting it to {@link org.apache.solr.cluster.placement.PropertyValue.HeapUsage}.
   */
  interface HeapUsage extends PropertyValue {

    /**
     * Percentage between 0 and 100 of used heap over max heap.
     */
    double getUsedHeapMemoryUsage();
  }

  /**
   * A {@link PropertyValue} representing a metric on the target {@link PropertyValueSource}.
   *
   * <p>Instances are obtained by first getting a key using {@link PropertyKeyFactory#createMetricKey(PropertyValueSource, String)}
   * or {@link PropertyKeyFactory#createMetricKey(Node, String, PropertyKeyFactory.NodeMetricRegistry)} then calling
   * {@link PropertyValueFetcher#fetchProperties}, retrieving the appropriate {@link PropertyValue} from the returned map
   * using the {@link PropertyKey} as key and finally casting it to {@link org.apache.solr.cluster.placement.PropertyValue.Metric}.
   */
  interface Metric extends PropertyValue {
    /**
     * Returns the metric value from the {@link PropertyValueSource} from which it was retrieved.
     */
    Double getNumberValue();
  }

  /**
   * A {@link PropertyValue} representing a sysprop (or System property) on the target {@link Node}.
   *
   *  Instances are obtained by first getting a key using {@link PropertyKeyFactory#createSyspropKey} then calling
   *  {@link PropertyValueFetcher#fetchProperties}, retrieving the appropriate {@link PropertyValue} from the returned map
   *  using the {@link PropertyKey} as key and finally casting it to {@link org.apache.solr.cluster.placement.PropertyValue.Sysprop}.
   */
  interface Sysprop extends PropertyValue {
    /**
     * Returns the system property value from the target {@link Node} from which it was retrieved.
     */
    String getSystemPropertyValue();
  }

  /**
   *  Instances are obtained by first getting a key using {@link PropertyKeyFactory#createSystemLoadKey} then calling
   *  {@link PropertyValueFetcher#fetchProperties}, retrieving the appropriate {@link PropertyValue} from the returned map
   *  using the {@link PropertyKey} as key and finally casting it to {@link org.apache.solr.cluster.placement.PropertyValue.SystemLoad}.
   */
  interface SystemLoad extends PropertyValue {
    /**
     * Matches {@link java.lang.management.OperatingSystemMXBean#getSystemLoadAverage()}
     */
    double getSystemLoadAverage();
  }

}
