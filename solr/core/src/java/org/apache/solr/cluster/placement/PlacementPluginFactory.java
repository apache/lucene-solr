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
 * Factory implemented by client code and configured in {@code solr.xml} allowing the creation of instances of
 * {@link PlacementPlugin} to be used for replica placement computation.
 */
public interface PlacementPluginFactory {
  /**
   * Returns an instance of the plugin that will be repeatedly (and concurrently) be called to compute placement. Multiple
   * instances of a plugin can be used in parallel (for example if configuration has to change, but plugin instances with
   * the previous configuration are still being used).
   */
  PlacementPlugin createPluginInstance(PlacementPluginConfig config);
}
