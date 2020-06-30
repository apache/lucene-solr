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

import java.util.HashMap;
import java.util.Map;

import org.apache.solr.core.SolrConfig;

/**
 * Manages all registered circuit breaker instances. Responsible for a holistic view
 * of whether a circuit breaker has tripped or not.
 *
 * There are two typical ways of using this class's instance:
 * 1. Check if any circuit breaker has triggered -- and know which circuit breaker has triggered.
 * 2. Get an instance of a specific circuit breaker and perform checks.
 *
 * It is a good practice to register new circuit breakers here if you want them checked for every
 * request.
 *
 * NOTE: The current way of registering new default circuit breakers is minimal and not a long term
 * solution. There will be a follow up with a SIP for a schema API design.
 */
public class CircuitBreakerManager {
  private final Map<CircuitBreaker.CircuitBreakerType, CircuitBreaker> circuitBreakerMap = new HashMap<>();

  // Allows replacing of existing circuit breaker
  public void register(CircuitBreaker.CircuitBreakerType circuitBreakerType, CircuitBreaker circuitBreaker) {
    circuitBreakerMap.put(circuitBreakerType, circuitBreaker);
  }

  public CircuitBreaker getCircuitBreaker(CircuitBreaker.CircuitBreakerType circuitBreakerType) {
    assert circuitBreakerType != null;

    return circuitBreakerMap.get(circuitBreakerType);
  }

  /**
   * Check if any circuit breaker has triggered.
   * @return CircuitBreakers which have triggered, null otherwise
   */
  public Map<CircuitBreaker.CircuitBreakerType, CircuitBreaker> checkedTripped() {
    Map<CircuitBreaker.CircuitBreakerType, CircuitBreaker> triggeredCircuitBreakers = null;

    for (Map.Entry<CircuitBreaker.CircuitBreakerType, CircuitBreaker> entry : circuitBreakerMap.entrySet()) {
      CircuitBreaker circuitBreaker = entry.getValue();

      if (circuitBreaker.isEnabled() &&
          circuitBreaker.isTripped()) {
        if (triggeredCircuitBreakers == null) {
          triggeredCircuitBreakers = new HashMap<>();
        }

        triggeredCircuitBreakers.putIfAbsent(entry.getKey(), circuitBreaker);
      }
    }

    return triggeredCircuitBreakers;
  }

  /**
   * Returns true if *any* circuit breaker has triggered, false if none have triggered.
   *
   * <p>
   * NOTE: This method short circuits the checking of circuit breakers -- the method will
   * return as soon as it finds a circuit breaker that is enabled and has triggered.
   * </p>
   */
  public boolean checkAnyTripped() {
    for (Map.Entry<CircuitBreaker.CircuitBreakerType, CircuitBreaker> entry : circuitBreakerMap.entrySet()) {
      CircuitBreaker circuitBreaker = entry.getValue();

      if (circuitBreaker.isEnabled() &&
          circuitBreaker.isTripped()) {
        return true;
      }
    }

    return false;
  }

  /**
   * Construct the final error message to be printed when circuit breakers trip.
   *
   * @param circuitBreakerMap Input list for circuit breakers
   * @return Constructed error message
   */
  public static String toErrorMessage(Map<CircuitBreaker.CircuitBreakerType, CircuitBreaker> circuitBreakerMap) {
    assert circuitBreakerMap != null;

    StringBuilder sb = new StringBuilder();

    for (CircuitBreaker.CircuitBreakerType circuitBreakerType : circuitBreakerMap.keySet()) {
      sb.append(circuitBreakerType.toString() + " " + circuitBreakerMap.get(circuitBreakerType).getDebugInfo() + "\n");
    }

    return sb.toString();
  }

  /**
   * Register default circuit breakers and return a constructed CircuitBreakerManager
   * instance which serves the given circuit breakers.
   *
   * Any default circuit breakers should be registered here
   */
  public static CircuitBreakerManager build(SolrConfig solrConfig) {
    CircuitBreakerManager circuitBreakerManager = new CircuitBreakerManager();

    // Install the default circuit breakers
    CircuitBreaker memoryCircuitBreaker = new MemoryCircuitBreaker(solrConfig);
    circuitBreakerManager.register(CircuitBreaker.CircuitBreakerType.MEMORY, memoryCircuitBreaker);

    return circuitBreakerManager;
  }
}
