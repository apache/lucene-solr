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
 * <p>A property key used by plugins to request values from Solr. Instances must be obtained using {@link PropertyKeyFactory}.
 *
 * <p>Instances of classes implementing this interface will be used as keys in a map (see {@link PropertyValueFetcher#fetchProperties}
 * but given the usage pattern (pass {@link PropertyKey} instances to the method then look up {@link PropertyValue}'s in
 * the returned map), the default implementation of {@link Object#equals} is sufficient.
 */
public interface PropertyKey {
  /**
   * @return the target of this {@link PropertyKey}, i.e. from where the corresponding {@link PropertyValue}'s should be
   * (or were) obtained. The target of a {@link PropertyKey} is the source of its {@link PropertyValue}...
   */
  PropertyValueSource getPropertyValueSource();
}
