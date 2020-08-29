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

/**
 * A {@link PropertyValue} representing a metric on the target {@link PropertyValueSource}.
 *
 * <p>Instances are obtained by first getting a key using {@link PropertyKeyFactory#createMetricKey(PropertyValueSource, String)}
 * or {@link PropertyKeyFactory#createMetricKey(Node, String, PropertyKeyFactory.NodeMetricRegistry)} then calling
 * {@link PropertyValueFetcher#fetchProperties}, retrieving the appropriate {@link PropertyValue} from the returned map
 * using the {@link PropertyKey} as key and finally casting it to the appropriate subinterface of {@link MetricPropertyValue}.
 */
public interface MetricPropertyValue extends PropertyValue {
  /**
   * Returns the metric value from the {@link PropertyValueSource} from which it was retrieved.
   */
  Double getNumberValue();
}
