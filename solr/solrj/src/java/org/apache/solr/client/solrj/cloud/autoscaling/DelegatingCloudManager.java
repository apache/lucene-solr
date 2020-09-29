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
package org.apache.solr.client.solrj.cloud.autoscaling;

import java.io.IOException;
import java.util.Map;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.cloud.DistribStateManager;
import org.apache.solr.client.solrj.cloud.DistributedQueueFactory;
import org.apache.solr.client.solrj.cloud.NodeStateProvider;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.impl.ClusterStateProvider;
import org.apache.solr.common.util.ObjectCache;
import org.apache.solr.common.util.TimeSource;

/**
 * Base class for overriding some behavior of {@link SolrCloudManager}.
 *
 * @deprecated to be removed in Solr 9.0 (see SOLR-14656)
 */
public class DelegatingCloudManager implements SolrCloudManager {
  protected final SolrCloudManager delegate;
  private ObjectCache objectCache = new ObjectCache();
  private TimeSource timeSource = TimeSource.NANO_TIME;

  public DelegatingCloudManager(SolrCloudManager delegate) {
    this.delegate = delegate;
  }

  @Override
  public ClusterStateProvider getClusterStateProvider() {
    return delegate.getClusterStateProvider();
  }

  @Override
  public NodeStateProvider getNodeStateProvider() {
    return delegate.getNodeStateProvider();
  }

  @Override
  public DistribStateManager getDistribStateManager() {
    return delegate.getDistribStateManager();
  }

  @Override
  public DistributedQueueFactory getDistributedQueueFactory() {
    return delegate.getDistributedQueueFactory();
  }

  @Override
  public ObjectCache getObjectCache() {
    return delegate == null ? objectCache : delegate.getObjectCache();
  }

  @Override
  public boolean isClosed() {
    return delegate.isClosed();
  }

  @Override
  public TimeSource getTimeSource() {
    return delegate == null ? timeSource : delegate.getTimeSource();
  }

  @Override
  public SolrResponse request(@SuppressWarnings({"rawtypes"})SolrRequest req) throws IOException {
    return delegate.request(req);
  }

  @Override
  public byte[] httpRequest(String url, SolrRequest.METHOD method, Map<String, String> headers, String payload, int timeout, boolean followRedirects) throws IOException {
    return delegate.httpRequest(url, method, headers, payload, timeout, followRedirects);
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }
}
