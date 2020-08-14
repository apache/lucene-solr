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

import org.apache.solr.core.SolrConfig;

/**
 * Default class to define circuit breakers for Solr.
 * <p>
 *  There are two (typical) ways to use circuit breakers:
 *  1. Have them checked at admission control by default (use CircuitBreakerManager for the same).
 *  2. Use the circuit breaker in a specific code path(s).
 *
 * TODO: This class should be grown as the scope of circuit breakers grow.
 * </p>
 */
public abstract class CircuitBreaker {
  public static final String NAME = "circuitbreaker";

  protected final SolrConfig solrConfig;

  public CircuitBreaker(SolrConfig solrConfig) {
    this.solrConfig = solrConfig;
  }

  // Global config for all circuit breakers. For specific circuit breaker configs, define
  // your own config.
  protected boolean isEnabled() {
    return solrConfig.useCircuitBreakers;
  }

  /**
   * Check if circuit breaker is tripped.
   */
  public abstract boolean isTripped();

  /**
   * Get debug useful info.
   */
  public abstract String getDebugInfo();
}