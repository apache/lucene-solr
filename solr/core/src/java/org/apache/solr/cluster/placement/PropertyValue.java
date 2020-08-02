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
 *  The value corresponding to a specific {@link PropertyKey}, in a specific context (e.g. property of a specific
 *  {@link Node} instance). The context is not tracked in the {@link PropertyKey} nor in the {@code PropertyValue}.
 */
public interface PropertyValue {
  /**
   * The property key used for retrieving this property value.
   */
  PropertyKey getKey();
}
