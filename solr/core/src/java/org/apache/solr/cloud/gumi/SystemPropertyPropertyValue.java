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
 * A {@link PropertyValue} representing a System property on the target {@link Node}.
 */
public interface SystemPropertyPropertyValue extends PropertyValue {

  /**
   * Strongly typed property key retrieval corresponding to that property value.
   */
  @Override
  SystemPropertyPropertyKey getKey();

  /**
   * Returns the system property value from the target {@link Node} on which it was retrieved.
   */
  String getSystemPropertyValue();
}
