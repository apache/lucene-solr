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

import java.util.Optional;
import java.util.Set;

/**
 * <p>A property key used by plugins to request values from Solr. Instances must be obtained using {@link PropertyKeyFactory}.
 *
 * <p>Once the properties are fetched using {@link PropertyValueFetcher#fetchProperties(Set)}, the specific getter of
 * each key allows retrieving the corresponding value if available.
 */
public interface PropertyKey {
    /**
     *  Instances are obtained by first getting a key using {@link PropertyKeyFactory#createCoreCountKey} then calling
     *  {@link PropertyValueFetcher#fetchProperties} and only then accessing {@link #getCoresCount()}.
     */
    interface CoresCount extends PropertyKey {
        /**
         * Returns the number of cores on the {@link Node}) passed to {@link PropertyKeyFactory#createCoreCountKey(Node)}
         * once properties got fetched using {@link PropertyValueFetcher#fetchProperties(Set)}.
         * @return an optional containing the number of cores on the node or {@link Optional#empty()} if either key not
         * yet fetched or if value was not available.
         */
        Optional<Integer> getCoresCount();
    }

    interface DiskType extends PropertyKey {
        /**
         * Type of storage hardware used for the partition on which cores are stored on the {@link Node}) from which this instance
         * was obtained (i.e. instance passed to {@link PropertyKeyFactory#createDiskTypeKey(Node)}).
         */
        Optional<DiskType.HardwareType> getHardwareType();

        enum HardwareType {
            SSD, ROTATIONAL
        }
    }

    interface TotalDisk extends PropertyKey {
        /**
         * Total disk size of the partition on which cores are stored on the {@link Node}) from which this instance was obtained
         * (i.e. instance passed to {@link PropertyKeyFactory#createTotalDiskKey(Node)}).
         */
        Optional<Long> getTotalSizeGB();
    }

    interface FreeDisk extends PropertyKey {
        /**
         * Free disk size of the partition on which cores are stored on the {@link Node}) from which this instance was obtained
         *  (i.e. instance passed to {@link PropertyKeyFactory#createDiskTypeKey(Node)}).
         */
        Optional<Long> getFreeSizeGB();
    }

    interface HeapUsage extends PropertyKey {
        /**
         * Percentage between 0 and 100 of used heap over max heap.
         */
        Optional<Double> getUsedHeapMemoryUsage();
    }

    interface Metric extends PropertyKey {
        /**
         * Returns the metric value from the {@link PropertyValueSource} from which it was retrieved.
         */
        Optional<Double> getNumberValue();
    }

    interface Sysprop extends PropertyKey {
        /**
         * Returns the system property from the target {@link Node} from which it was retrieved.
         */
        Optional<String> getSystemPropertyValue();
    }

    interface SystemLoad extends PropertyKey {
        /**
         * Matches {@link java.lang.management.OperatingSystemMXBean#getSystemLoadAverage()}
         */
        Optional<Double> getSystemLoadAverage();
    }
}
