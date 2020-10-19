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

/**
 * Default class to define circuit breakers for Solr.
 * <p>
 *  There are two (typical) ways to use circuit breakers:
 *  1. Have them checked at admission control by default (use CircuitBreakerManager for the same).
 *  2. Use the circuit breaker in a specific code path(s).
 *
 * TODO: This class should be grown as the scope of circuit breakers grow.
 *
 * The class and its derivatives raise a standard exception when a circuit breaker is triggered.
 * We should make it into a dedicated exception (https://issues.apache.org/jira/browse/SOLR-14755)
 * </p>
 */
public abstract class CircuitBreaker {
  public static final String NAME = "circuitbreaker";

  protected final CircuitBreakerConfig config;

  public CircuitBreaker(CircuitBreakerConfig config) {
    this.config = config;
  }

  /**
   * Global config for all circuit breakers. For specific circuit breaker configs, define
   * your own config.
   */
  protected boolean isEnabled() {
    return config.isEnabled();
  }

  /**
   * Check if circuit breaker is tripped.
   */
  public abstract boolean isTripped();

  /**
   * Get debug useful info.
   */
  public abstract String getDebugInfo();

  /**
   * Get error message when the circuit breaker triggers
   */
  public abstract String getErrorMessage();

  /**
   * Represents the configuration for a circuit breaker
   */
  public static class CircuitBreakerConfig {
    private final boolean enabled;
    private final boolean memCBEnabled;
    private final int memCBThreshold;
    private final boolean cpuCBEnabled;
    private final int cpuCBThreshold;

    public CircuitBreakerConfig(final boolean enabled, final boolean memCBEnabled, final int memCBThreshold,
                                  final boolean cpuCBEnabled, final int cpuCBThreshold) {
      this.enabled = enabled;
      this.memCBEnabled = memCBEnabled;
      this.memCBThreshold = memCBThreshold;
      this.cpuCBEnabled = cpuCBEnabled;
      this.cpuCBThreshold = cpuCBThreshold;
    }

    public boolean isEnabled() {
      return enabled;
    }

    public boolean getMemCBEnabled() {
      return memCBEnabled;
    }

    public int getMemCBThreshold() {
      return memCBThreshold;
    }

    public boolean getCpuCBEnabled() {
      return cpuCBEnabled;
    }

    public int getCpuCBThreshold() {
      return cpuCBThreshold;
    }
  }
}