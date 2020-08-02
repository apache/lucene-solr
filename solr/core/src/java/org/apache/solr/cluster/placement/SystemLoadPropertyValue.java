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

import java.lang.management.OperatingSystemMXBean;

public interface SystemLoadPropertyValue extends PropertyValue {
  /**
   * Matches {@link OperatingSystemMXBean#getSystemLoadAverage()}
   */
  double getSystemLoadAverage();

  /**
   * This is the value of metric <code>os.processCpuLoad</code>
   * @return the "recent cpu usage" for the Java Virtual Machine process. This value is a double in the [0.0,1.0]
   * interval. A value of 0.0 means that none of the CPUs were running threads from the JVM process during the recent
   * period of time observed, while a value of 1.0 means that all CPUs were actively running threads from the JVM 100%
   * of the time during the recent period being observed. Threads from the JVM include the application threads as well
   * as the JVM internal threads. All values betweens 0.0 and 1.0 are possible depending of the activities going on in
   * the JVM process and the whole system. If the Java Virtual Machine recent CPU usage is not available, the method
   * returns a negative value.
   */
  double getProcessCpuLoad();

  /**
   * TODO: Shall we return a {@link java.lang.management.MemoryUsage} instance instead, like {@link java.lang.management.MemoryMXBean#getHeapMemoryUsage}? (and rename the method)
   */
  long getUsedHeapMemoryUsage();
}