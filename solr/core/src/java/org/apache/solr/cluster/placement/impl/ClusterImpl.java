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

package org.apache.solr.cluster.placement.impl;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.cluster.placement.Cluster;
import org.apache.solr.cluster.placement.Node;
import org.apache.solr.cluster.placement.SolrCollection;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;

class ClusterImpl implements Cluster {
  private final Set<Node> liveNodes;
  private final ClusterState clusterState;

  ClusterImpl(SolrCloudManager solrCloudManager) throws IOException {
    liveNodes = NodeImpl.getNodes(solrCloudManager.getClusterStateProvider().getLiveNodes());
    clusterState = solrCloudManager.getClusterStateProvider().getClusterState();
  }

  @Override
  public Set<Node> getLiveNodes() {
    return liveNodes;
  }

  @Override
  public Optional<SolrCollection> getCollection(String collectionName) {
    return SolrCollectionImpl.createCollectionFacade(clusterState, collectionName);
  }

  /**
   * <p>Returns the set of names of all collections in the cluster. This is a costly method as it potentially builds a
   * large set in memory. Usage is discouraged.
   *
   * <p>Eventually, a similar method allowing efficiently filtering the set of returned collections is desirable. Efficiently
   * implies filter does not have to be applied to each collection but a regular expression or similar is passed in,
   * allowing direct access to qualifying collections.
   */
  public Set<String> getAllCollectionNames() {
    return clusterState.getCollectionsMap().values().stream().map(DocCollection::getName).collect(Collectors.toSet());
  }

  // TODO implement hashCode() and equals() (just in case we end up supporting multiple Cluster instances at some point)
}
