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

import java.util.Map;
import java.util.Set;

public interface PropertyKeyFetcher {
  /**
   * Retrieves the properties from their configured {@link PropertyKeyTarget}'s and returns those that are defined.
   * @param props the properties to retrieve
   * @return a map whose keys are the requested property keys and the values are the requested property values, when such
   *    values are defined on the {@link PropertyKeyTarget}. If there's not value for a given key, it will not appear in
   *    the returned map. The returned value will never be {@code null} but may be an empty map.
   */
  Map<PropertyKey, PropertyValue> fetchProperties(Set<PropertyKey> props);
}
