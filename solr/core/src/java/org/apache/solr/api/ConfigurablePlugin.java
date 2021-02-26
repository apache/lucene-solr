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

package org.apache.solr.api;

import org.apache.solr.common.MapWriter;

/**Implement this interface if your plugin needs to accept some configuration
 * 
 * @param <T> the configuration Object type
 */
public interface ConfigurablePlugin<T extends MapWriter> {

  /**
   * This is invoked soon after the Object is initialized.
   * @param cfg value deserialized from JSON
   */
  void configure(T cfg);
}
