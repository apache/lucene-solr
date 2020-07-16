package org.apache.solr.cloud.api.collections.assign.policy8x;

import java.io.IOException;
import java.util.Map;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.cloud.DistribStateManager;
import org.apache.solr.client.solrj.cloud.DistributedQueueFactory;
import org.apache.solr.client.solrj.cloud.NodeStateProvider;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.impl.ClusterStateProvider;
import org.apache.solr.cloud.api.collections.assign.AssignerClusterState;
import org.apache.solr.common.util.ObjectCache;
import org.apache.solr.common.util.TimeSource;

/**
 *
 */
public class AssignerCloudManager implements SolrCloudManager {
  private final ObjectCache objectCache = new ObjectCache();
  private final TimeSource timeSource;
  private final AssignerClusterState assignerClusterState;
  private final AssignerClusterStateProvider clusterStateProvider;
  private final AssignerNodeStateProvider nodeStateProvider;
  private final AssignerDistribStateManager distribStateManager;

  public AssignerCloudManager(AssignerClusterState assignerClusterState,
                              TimeSource timeSource) {
    this.assignerClusterState = assignerClusterState;
    this.timeSource = timeSource;
    clusterStateProvider = new AssignerClusterStateProvider(assignerClusterState);
    nodeStateProvider = new AssignerNodeStateProvider(assignerClusterState);
    distribStateManager = new AssignerDistribStateManager(assignerClusterState);
  }

  @Override
  public ClusterStateProvider getClusterStateProvider() {
    return clusterStateProvider;
  }

  @Override
  public NodeStateProvider getNodeStateProvider() {
    return nodeStateProvider;
  }

  @Override
  public DistribStateManager getDistribStateManager() {
    return distribStateManager;
  }

  @Override
  public DistributedQueueFactory getDistributedQueueFactory() {
    throw new UnsupportedOperationException("getDistributedQueueFactory");
  }

  @Override
  public ObjectCache getObjectCache() {
    return objectCache;
  }

  @Override
  public TimeSource getTimeSource() {
    return timeSource;
  }

  @Override
  public SolrResponse request(SolrRequest req) throws IOException {
    throw new UnsupportedOperationException("request");
  }

  @Override
  public byte[] httpRequest(String url, SolrRequest.METHOD method, Map<String, String> headers, String payload, int timeout, boolean followRedirects) throws IOException {
    throw new UnsupportedOperationException("httpRequest");
  }

  @Override
  public void close() throws IOException {

  }
}
