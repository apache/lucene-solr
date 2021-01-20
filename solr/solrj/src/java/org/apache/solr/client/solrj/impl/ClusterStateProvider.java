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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrCloseable;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.params.CollectionAdminParams;

public interface ClusterStateProvider extends SolrCloseable {

  /**
   * Obtain the state of the collection (cluster status).
   * @return the collection state, or null is collection doesn't exist
   */
  ClusterState.CollectionRef getState(String collection);

  /**
   * Obtain set of live_nodes for the cluster.
   */
  Set<String> getLiveNodes();

  /**
   * Given a collection alias, returns a list of collections it points to, or returns a singleton list of the input if
   * it's not an alias.
   */
  List<String> resolveAlias(String alias);

  /**
   * Return alias properties, or an empty map if the alias has no properties.
   */
  Map<String, String> getAliasProperties(String alias);

  /**
   * Given a collection alias, return a single collection it points to, or the original name if it's not an
   * alias.
   * @throws IllegalArgumentException if an alias points to more than 1 collection, either directly or indirectly.
   */
  default String resolveSimpleAlias(String alias) throws IllegalArgumentException {
    List<String> aliases = resolveAlias(alias);
    if (aliases.size() > 1) {
      throw new IllegalArgumentException("Simple alias '" + alias + "' points to more than 1 collection: " + aliases);
    }
    return aliases.get(0);
  }

  /**
   * Returns true if an alias exists and is a routed alias, false otherwise.
   */
  default boolean isRoutedAlias(String alias) {
    return getAliasProperties(alias).entrySet().stream().anyMatch(e -> e.getKey().startsWith(CollectionAdminParams.ROUTER_PREFIX));
  }

  /**
   * Obtain the current cluster state.
   */
  ClusterState getClusterState() throws IOException;

  default DocCollection getCollection(String name) throws IOException{
   return getClusterState().getCollectionOrNull(name);
  }

  /**
   * Obtain cluster properties.
   * @return configured cluster properties, or an empty map, never null.
   */
  Map<String, Object> getClusterProperties();

  /**
   * Obtain a cluster property, or the default value if it doesn't exist.
   */
  default <T> T getClusterProperty(String key, T defaultValue) {
    @SuppressWarnings({"unchecked"})
    T value = (T) getClusterProperties().get(key);
    if (value == null)
      return defaultValue;
    return value;
  }

  /**
   * Obtain a cluster property, or null if it doesn't exist.
   */
  @SuppressWarnings({"unchecked"})
  default <T> T getClusterProperty(String propertyName) {
    return (T) getClusterProperties().get(propertyName);
  }

  /**
   * Get the collection-specific policy
   */
  String getPolicyNameByCollection(String coll);

  void connect();
}