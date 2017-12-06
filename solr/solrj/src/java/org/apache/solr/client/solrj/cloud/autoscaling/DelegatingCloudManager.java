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
import org.apache.solr.client.solrj.cloud.DistributedQueueFactory;
import org.apache.solr.client.solrj.impl.ClusterStateProvider;
import org.apache.solr.common.util.ObjectCache;

/**
 * Base class for overriding some behavior of {@link SolrCloudManager}.
 */
public class DelegatingCloudManager implements SolrCloudManager {
  private final SolrCloudManager delegate;
  private ObjectCache objectCache = new ObjectCache();

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
  public SolrResponse request(SolrRequest req) throws IOException {
    return delegate.request(req);
  }

  @Override
  public byte[] httpRequest(String url, SolrRequest.METHOD method, Map<String, String> headers, String payload, int timeout, boolean followRedirects) throws IOException {
    return delegate.httpRequest(url, method, headers, payload, timeout, followRedirects);
  }
}
