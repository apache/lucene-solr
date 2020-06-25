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

import org.apache.solr.core.SolrCore;

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

  private final Map<CircuitBreakerType, CircuitBreaker> circuitBreakerMap = new HashMap<>();

  // Allows replacing of existing circuit breaker
  public void registerCircuitBreaker(CircuitBreakerType circuitBreakerType, CircuitBreaker circuitBreaker) {
    circuitBreakerMap.put(circuitBreakerType, circuitBreaker);
  }

  public CircuitBreaker getCircuitBreaker(CircuitBreakerType circuitBreakerType) {
    assert circuitBreakerType != null;

    return circuitBreakerMap.get(circuitBreakerType);
  }

  /**
   * Check if any circuit breaker has triggered.
   * @return CircuitBreakers which have triggered, null otherwise
   */
  public Map<CircuitBreakerType, CircuitBreaker> checkAllCircuitBreakers() {
    Map<CircuitBreakerType, CircuitBreaker> triggeredCircuitBreakers = new HashMap<>();

    for (Map.Entry<CircuitBreakerType, CircuitBreaker> entry : circuitBreakerMap.entrySet()) {
      CircuitBreaker circuitBreaker = entry.getValue();

      if (circuitBreaker.isCircuitBreakerEnabled() &&
          circuitBreaker.isCircuitBreakerGauntletTripped()) {
        triggeredCircuitBreakers.put(entry.getKey(), circuitBreaker);
      }
    }

    return triggeredCircuitBreakers.size() > 0 ? triggeredCircuitBreakers : null;
  }

  /**
   * Construct the final error message to be printed when circuit breakers trip
   * @param circuitBreakerMap Input list for circuit breakers
   * @return Constructed error message
   */
  public static String constructFinalErrorMessageString(Map<CircuitBreakerType, CircuitBreaker> circuitBreakerMap) {
    assert circuitBreakerMap != null;

    StringBuilder sb = new StringBuilder();

    for (CircuitBreakerType circuitBreakerType : circuitBreakerMap.keySet()) {
      sb.append(circuitBreakerType.toString() + " " + circuitBreakerMap.get(circuitBreakerType).printDebugInfo());
    }

    return sb.toString();
  }

  /**
   * Register default circuit breakers and return a constructed CircuitBreakerManager
   * instance which serves the given circuit breakers.
   *
   * Any default circuit breakers should be registered here
   */
  public static CircuitBreakerManager buildDefaultCircuitBreakerManager(SolrCore solrCore) {
    CircuitBreakerManager circuitBreakerManager = new CircuitBreakerManager();

    // Install the default circuit breakers
    CircuitBreaker memoryCircuitBreaker = new MemoryCircuitBreaker(solrCore);
    circuitBreakerManager.registerCircuitBreaker(CircuitBreakerType.MEMORY, memoryCircuitBreaker);

    return circuitBreakerManager;
  }
}
