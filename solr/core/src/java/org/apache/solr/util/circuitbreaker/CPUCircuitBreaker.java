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

package org.apache.solr.util.circuitbreaker;

import java.lang.invoke.MethodHandles;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;

import org.apache.solr.core.SolrConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * Tracks current CPU usage and triggers if the specified threshold is breached.
 *
 * This circuit breaker gets the average CPU load over the last minute and uses
 * that data to take a decision. Ideally, we should be able to cache the value
 * locally and only query once the minute has elapsed. However, that will introduce
 * more complexity than the current structure and might not get us major performance
 * wins. If this ever becomes a performance bottleneck, that can be considered.
 * </p>
 *
 * <p>
 * The configuration to define which mode to use and the trigger threshold are defined in
 * solrconfig.xml
 * </p>
 */
public class CPUCircuitBreaker extends CircuitBreaker {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final OperatingSystemMXBean operatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean();

  private final boolean enabled;
  private final double cpuUsageThreshold;

  // Assumption -- the value of these parameters will be set correctly before invoking getDebugInfo()
  private static final ThreadLocal<Double> seenCPUUsage = ThreadLocal.withInitial(() -> 0.0);

  private static final ThreadLocal<Double> allowedCPUUsage = ThreadLocal.withInitial(() -> 0.0);

  public CPUCircuitBreaker(SolrConfig solrConfig) {
    super(solrConfig);

    this.enabled = solrConfig.cpuCircuitBreakerEnabled;
    this.cpuUsageThreshold = solrConfig.cpuCircuitBreakerThreshold;
  }

  @Override
  public boolean isTripped() {
    if (!isEnabled()) {
      return false;
    }

    if (!enabled) {
      return false;
    }

    double localAllowedCPUUsage = getCpuUsageThreshold();
    double localSeenCPUUsage = calculateLiveCPUUsage();

    if (localSeenCPUUsage < 0) {
      if (log.isWarnEnabled()) {
        String msg = "Unable to get CPU usage";

        log.warn(msg);
      }

      return false;
    }

    allowedCPUUsage.set(localAllowedCPUUsage);

    seenCPUUsage.set(localSeenCPUUsage);

    return (localSeenCPUUsage >= localAllowedCPUUsage);
  }

  @Override
  public String getDebugInfo() {

    if (seenCPUUsage.get() == 0.0 || seenCPUUsage.get() == 0.0) {
      log.warn("CPUCircuitBreaker's monitored values (seenCPUUSage, allowedCPUUsage) not set");
    }

    return "seenCPUUSage=" + seenCPUUsage.get() + " allowedCPUUsage=" + allowedCPUUsage.get();
  }

  @Override
  public String getErrorMessage() {
    return "CPU Circuit Breaker Triggered. Seen CPU usage " + seenCPUUsage.get() + " and allocated threshold " +
        allowedCPUUsage.get();
  }

  public double getCpuUsageThreshold() {
    return cpuUsageThreshold;
  }

  protected double calculateLiveCPUUsage() {
    return operatingSystemMXBean.getSystemLoadAverage();
  }
}
