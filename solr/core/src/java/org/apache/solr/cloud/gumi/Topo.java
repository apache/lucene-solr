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

import java.util.Optional;
import java.util.Set;

/**
 * A (topographical) map of the cluster state, providing information on which nodes are part of the cluster and a mean
 * to get to more detailed info.
 */
public interface Topo {
  /**
   * @return current set of live nodes. Never <code>null</code>, never empty (Solr wouldn't call the plugin if empty
   * since no useful could then be done).
   */
  Set<Node> getLiveNodes();

  /**
   * Returns info about the given collection if one exists. Because it is not expected for plugins to request info about
   * a large number of collections, requests can only be made one by one.
   */
  Optional<SolrCollection> getCollection(String collectionName);
}
