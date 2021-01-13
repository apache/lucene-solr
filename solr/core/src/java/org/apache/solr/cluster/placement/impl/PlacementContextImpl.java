package org.apache.solr.cluster.placement.impl;

import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.cluster.Cluster;
import org.apache.solr.cluster.placement.AttributeFetcher;
import org.apache.solr.cluster.placement.PlacementContext;
import org.apache.solr.cluster.placement.PlacementPlanFactory;

import java.io.IOException;

/**
 *
 */
public class PlacementContextImpl implements PlacementContext {

  private final Cluster cluster;
  private final AttributeFetcher attributeFetcher;
  private final PlacementPlanFactory placementPlanFactory = new PlacementPlanFactoryImpl();

  public PlacementContextImpl(SolrCloudManager solrCloudManager) throws IOException {
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
