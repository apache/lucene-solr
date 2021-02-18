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

import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.cluster.Cluster;
import org.apache.solr.cluster.placement.AttributeFetcher;
import org.apache.solr.cluster.placement.PlacementContext;
import org.apache.solr.cluster.placement.PlacementPlanFactory;

import java.io.IOException;

/**
 * Implementation of {@link PlacementContext} that uses {@link SimpleClusterAbstractionsImpl}
 * to create components necessary for the placement plugins to use.
 */
public class SimplePlacementContextImpl implements PlacementContext {

  private final Cluster cluster;
  private final AttributeFetcher attributeFetcher;
  private final PlacementPlanFactory placementPlanFactory = new PlacementPlanFactoryImpl();

  public SimplePlacementContextImpl(SolrCloudManager solrCloudManager) throws IOException {
    cluster = new SimpleClusterAbstractionsImpl.ClusterImpl(solrCloudManager);
    attributeFetcher = new AttributeFetcherImpl(solrCloudManager);
  }

  @Override
  public Cluster getCluster() {
    return cluster;
  }

  @Override
  public AttributeFetcher getAttributeFetcher() {
    return attributeFetcher;
  }

  @Override
  public PlacementPlanFactory getPlacementPlanFactory() {
    return placementPlanFactory;
  }
}
