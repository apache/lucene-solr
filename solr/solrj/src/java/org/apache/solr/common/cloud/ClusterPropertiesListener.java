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

package org.apache.solr.common.cloud;

import java.util.Map;

/**
 * Listener that can be used with {@link ZkStateReader#registerClusterPropertiesListener(ClusterPropertiesListener)}
 * and called whenever the cluster properties changes.
 */
public interface ClusterPropertiesListener {

  /**
   * Called when a change in the cluster properties occurs.
   *
   * Note that, due to the way Zookeeper watchers are implemented, a single call may be
   * the result of several state changes
   *
   * @param properties current cluster properties
   *
   * @return true if the listener should be removed
   */
  boolean onChange(Map<String, Object> properties);
}
