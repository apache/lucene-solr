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
import java.lang.management.MemoryMXBean;

import org.apache.solr.core.SolrConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * Tracks the current JVM heap usage and triggers if it exceeds the defined percentage of the maximum
 * heap size allocated to the JVM. This circuit breaker is a part of the default CircuitBreakerManager
 * so is checked for every request -- hence it is realtime. Once the memory usage goes below the threshold,
 * it will start allowing queries again.
 * </p>
 *
 * <p>
 * The memory threshold is defined as a percentage of the maximum memory allocated -- see memoryCircuitBreakerThresholdPct
 * in solrconfig.xml.
 * </p>
 */

public class MemoryCircuitBreaker extends CircuitBreaker {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final MemoryMXBean MEMORY_MX_BEAN = ManagementFactory.getMemoryMXBean();

  private boolean isMemoryCircuitBreakerEnabled;
  private final long heapMemoryThreshold;

  // Assumption -- the value of these parameters will be set correctly before invoking getDebugInfo()
  private final ThreadLocal<Long> seenMemory = new ThreadLocal<>();
  private final ThreadLocal<Long> allowedMemory = new ThreadLocal<>();

  public MemoryCircuitBreaker(SolrConfig solrConfig) {
    super(solrConfig);

    long currentMaxHeap = MEMORY_MX_BEAN.getHeapMemoryUsage().getMax();

    if (currentMaxHeap <= 0) {
      throw new IllegalArgumentException("Invalid JVM state for the max heap usage");
    }

    int thresholdValueInPercentage = solrConfig.memoryCircuitBreakerThresholdPct;
    double thresholdInFraction = thresholdValueInPercentage / (double) 100;
    heapMemoryThreshold = (long) (currentMaxHeap * thresholdInFraction);

    if (heapMemoryThreshold <= 0) {
      throw new IllegalStateException("Memory limit cannot be less than or equal to zero");
    }
  }

  // TODO: An optimization can be to trip the circuit breaker for a duration of time
  // after the circuit breaker condition is matched. This will optimize for per call
  // overhead of calculating the condition parameters but can result in false positives.
  @Override
  public boolean isTripped() {
    if (!isEnabled()) {
      return false;
    }

    if (!isMemoryCircuitBreakerEnabled) {
      System.out.println("Memory circuit breaker is disabled");
      return false;
    }

    long localAllowedMemory = getCurrentMemoryThreshold();
    long localSeenMemory = calculateLiveMemoryUsage();

    allowedMemory.set(localAllowedMemory);

    seenMemory.set(localSeenMemory);

    return (localSeenMemory >= localAllowedMemory);
  }

  @Override
  public String getDebugInfo() {
    if (seenMemory.get() == 0L || allowedMemory.get() == 0L) {
      log.warn("MemoryCircuitBreaker's monitored values (seenMemory, allowedMemory) not set");
    }

    return "seenMemory=" + seenMemory.get() + " allowedMemory=" + allowedMemory.get();
  }

  private long getCurrentMemoryThreshold() {
    return heapMemoryThreshold;
  }

  /**
   * Calculate the live memory usage for the system. This method has package visibility
   * to allow using for testing.
   * @return Memory usage in bytes.
   */
  protected long calculateLiveMemoryUsage() {
    // NOTE: MemoryUsageGaugeSet provides memory usage statistics but we do not use them
    // here since it will require extra allocations and incur cost, hence it is cheaper to use
    // MemoryMXBean directly. Ideally, this call should not add noticeable
    // latency to a query -- but if it does, please signify on SOLR-14588
    return MEMORY_MX_BEAN.getHeapMemoryUsage().getUsed();
  }
}
