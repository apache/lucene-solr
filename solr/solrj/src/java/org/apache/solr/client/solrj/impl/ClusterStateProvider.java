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
package org.apache.solr.client.solrj.impl;

import java.io.Closeable;
import java.util.Set;

import org.apache.solr.common.cloud.ClusterState;

public interface ClusterStateProvider extends Closeable {

  /**
   * Obtain the state of the collection (cluster status).
   * @return the collection state, or null is collection doesn't exist
   */
  ClusterState.CollectionRef getState(String collection);

  /**
   * Obtain set of live_nodes for the cluster.
   */
  Set<String> liveNodes();

  /**
   * Given an alias, returns the collection name that this alias points to
   */
  String getAlias(String alias);

  /**
   * Given a name, returns the collection name if an alias by that name exists, or
   * returns the name itself, if no alias exists.
   */
  String getCollectionName(String name);

  /**
   * Obtain a cluster property, or null if it doesn't exist.
   */
  Object getClusterProperty(String propertyName);

  /**
   * Obtain a cluster property, or the default value if it doesn't exist.
   */
  Object getClusterProperty(String propertyName, String def);

  void connect();
}