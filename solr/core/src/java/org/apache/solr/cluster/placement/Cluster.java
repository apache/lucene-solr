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

import java.io.IOException;
import java.util.Optional;
import java.util.Set;

/**
 * <p>A representation of the (initial) cluster state, providing information on which nodes are part of the cluster and a way
 * to get to more detailed info.
 *
 * <p>This instance can also be used as a {@link PropertyValueSource} if {@link PropertyKey}'s need to be specified with
 * a global cluster target.
 */
public interface Cluster extends PropertyValueSource {
  /**
   * @return current set of live nodes. Never <code>null</code>, never empty (Solr wouldn't call the plugin if empty
   * since no useful work could then be done).
   */
  Set<Node> getLiveNodes();

  /**
   * <p>Returns info about the given collection if one exists. Because it is not expected for plugins to request info about
   * a large number of collections, requests can only be made one by one.
   *
   * <p>This is also the reason we do not return a {@link java.util.Map} or {@link Set} of {@link SolrCollection}'s here: it would be
   * wasteful to fetch all data and fill such a map when plugin code likely needs info about at most one or two collections.
   */
  Optional<SolrCollection> getCollection(String collectionName) throws IOException;

  /**
   * <p>Allows getting names of all {@link SolrCollection}'s present in the cluster.
   *
   * <p><b>WARNING:</b> this call will be extremely inefficient on large clusters. Usage is discouraged.
   */
  Set<String> getAllCollectionNames();
}
