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

import org.apache.solr.core.SolrCore;

/**
 * Default class to define circuit breakers for Solr.
 *
 * TODO: This class should be grown as the scope of circuit breakers grow.
 */
public abstract class CircuitBreaker {
  public static final String NAME = "circuitbreaker";

  protected final SolrCore solrCore;

  public CircuitBreaker(SolrCore solrCore) {
    this.solrCore = solrCore;
  }

  // Global config for all circuit breakers. For specific circuit breaker configs, define
  // your own config
  protected boolean isCircuitBreakerEnabled() {
    return solrCore.getSolrConfig().useCircuitBreakers;
  }

  /**
   * Check if this allocation will trigger circuit breaker.
   */
  public abstract boolean isCircuitBreakerGauntletTripped();

  /**
   * Print debug useful info
   */
  public abstract String printDebugInfo();
}
