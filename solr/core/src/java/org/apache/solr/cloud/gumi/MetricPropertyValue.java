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

package org.apache.solr.cloud.gumi;

/**
 * A {@link PropertyValue} representing a metric on the target {@link PropertyKeyTarget}.
 * Note there might be overlap with {@link SystemLoadPropertyValue} (only applicable to {@link Node}'s), may need to clarify.
 */
public interface MetricPropertyValue extends PropertyValue {

  /**
   * Strongly typed property key retrieval corresponding to that property value.
   */
  @Override
  MetricPropertyKey getKey();

  /**
   * Returns the metric value from the {@link PropertyKeyTarget} on which it was retrieved.
   * TODO: what type should the metric be? Maybe offer multiple getters for different java types and have each metric implement the right one and throw from the wrong ones? This avoids casting...
   */
  Double getMetricValue();
}
