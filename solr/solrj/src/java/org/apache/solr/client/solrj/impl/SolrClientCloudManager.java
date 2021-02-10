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

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.cloud.DistribStateManager;
import org.apache.solr.client.solrj.cloud.DistributedQueueFactory;
import org.apache.solr.client.solrj.cloud.NodeStateProvider;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.ObjectCache;
import org.apache.solr.common.util.TimeSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;

/**
 * Class that implements {@link SolrCloudManager} using a SolrClient
 */
public class SolrClientCloudManager implements SolrCloudManager {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected final BaseCloudSolrClient solrClient;
  private final ZkDistribStateManager stateManager;
  private final DistributedQueueFactory queueFactory;
  private final ZkStateReader zkStateReader;
  private final SolrZkClient zkClient;
  private final ObjectCache objectCache;
  private final boolean closeObjectCache;
  private final Http2SolrClient httpClient;
  private volatile boolean isClosed;

  public SolrClientCloudManager(DistributedQueueFactory queueFactory, BaseCloudSolrClient solrClient) {
    this(queueFactory, solrClient, null, null);
  }

  public SolrClientCloudManager(DistributedQueueFactory queueFactory, BaseCloudSolrClient solrClient, Http2SolrClient httpClient) {
    this(queueFactory, solrClient, null, httpClient);
  }

  public SolrClientCloudManager(DistributedQueueFactory queueFactory, BaseCloudSolrClient solrClient,
                                ObjectCache objectCache) {
    this(queueFactory, solrClient, objectCache, null);
  }

  public SolrClientCloudManager(DistributedQueueFactory queueFactory, BaseCloudSolrClient solrClient,
                                ObjectCache objectCache, Http2SolrClient httpClient) {
    this.queueFactory = queueFactory;
    this.solrClient = solrClient;

    if (httpClient == null && solrClient instanceof  CloudSolrClient) {
      this.httpClient = ((CloudHttp2SolrClient) solrClient).getHttpClient();
    } else if (httpClient == null) {
      throw new IllegalArgumentException("Must specify apache httpclient with non CloudSolrServer impls");
    } else {
      this.httpClient = httpClient;
    }

    this.zkStateReader = solrClient.getZkStateReader();
    this.zkClient = zkStateReader.getZkClient();
    this.stateManager = new ZkDistribStateManager(zkClient);
    this.isClosed = false;
    if (objectCache == null) {
      this.objectCache = new ObjectCache();
      closeObjectCache = true;
    } else {
      this.objectCache = objectCache;
      this.closeObjectCache = false;
    }
  }

  @Override
  public void close() {
    isClosed = true;
    if (closeObjectCache) {
      IOUtils.closeQuietly(objectCache);
    }
  }

  @Override
  public boolean isClosed() {
    return isClosed;
  }

  @Override
  public ObjectCache getObjectCache() {
    return objectCache;
  }

  @Override
  public TimeSource getTimeSource() {
    return TimeSource.NANO_TIME;
  }

  @Override
  public ClusterStateProvider getClusterStateProvider() {
    return solrClient.getClusterStateProvider();
  }

  @Override
  public NodeStateProvider getNodeStateProvider() {
    return new SolrClientNodeStateProvider(solrClient, httpClient);
  }

  @Override
  public DistribStateManager getDistribStateManager() {
    return stateManager;
  }

  @Override
  public SolrResponse request(SolrRequest req) throws IOException {
    try {
      return req.process(solrClient);
    } catch (SolrServerException e) {
      throw new IOException(e);
    }
  }

  private static final byte[] EMPTY = new byte[0];

  @Override
  public DistributedQueueFactory getDistributedQueueFactory() {
    return queueFactory;
  }

}
