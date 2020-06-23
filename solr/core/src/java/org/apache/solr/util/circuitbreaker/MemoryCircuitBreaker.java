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

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;

import org.apache.solr.core.SolrCore;

public class MemoryCircuitBreaker extends CircuitBreaker {
  private static final MemoryMXBean MEMORY_MX_BEAN = ManagementFactory.getMemoryMXBean();

  private final long currentMaxHeap = MEMORY_MX_BEAN.getHeapMemoryUsage().getMax();

  // Assumption -- the value of these parameters will be set correctly before invoking printDebugInfo()
  private long seenMemory;
  private long allowedMemory;

  public MemoryCircuitBreaker(SolrCore solrCore) {
    super(solrCore);
  }

  // TODO: An optimization can be to trip the circuit breaker for a duration of time
  // after the circuit breaker condition is matched. This will optimize for per call
  // overhead of calculating the condition parameters but can result in false positives.
  @Override
  public boolean isCircuitBreakerGauntletTripped() {
    if (!isCircuitBreakerEnabled()) {
      return false;
    }

    allowedMemory = getCurrentMemoryThreshold();

    seenMemory = calculateLiveMemoryUsage();

    return (seenMemory >= allowedMemory);
  }

  @Override
  public String printDebugInfo() {
    return "seen memory=" + seenMemory + " allowed memory=" + allowedMemory;
  }

  private long getCurrentMemoryThreshold() {
    int thresholdValueInPercentage = solrCore.getSolrConfig().memoryCircuitBreakerThreshold;

    if (currentMaxHeap <= 0) {
      return Long.MIN_VALUE;
    }

    long thresholdInFraction = thresholdValueInPercentage / 100;
    long actualLimit = currentMaxHeap * thresholdInFraction;

    if (actualLimit <= 0) {
      throw new IllegalStateException("Memory limit cannot be less than or equal to zero");
    }

    return actualLimit;
  }

  /**
   * Calculate the live memory usage for the system. This method has package visibility
   * to allow using for testing
   * @return Memory usage in bytes
   */
  protected long calculateLiveMemoryUsage() {
    return MEMORY_MX_BEAN.getHeapMemoryUsage().getUsed();
  }
}
