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
package org.apache.solr.client.solrj.io;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 *  The SolrClientCache caches SolrClients so they can be reused by different TupleStreams.
 **/

public class SolrClientCache implements Serializable, Closeable {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private ZkStateReader zkStateReader;

  private final Map<String, SolrClient> solrClients = new HashMap<>();
  private final Http2SolrClient httpClient;
  private boolean closeClient;
  private boolean closeZKStateReader = false;

  public SolrClientCache(String zkHost) {
    this.httpClient = new Http2SolrClient.Builder().markInternalRequest().build();
    zkStateReader = new ZkStateReader(zkHost, 10000, 30000);
    zkStateReader.createClusterStateWatchersAndUpdate();
    closeZKStateReader = true;
    closeClient = true;
    assert ObjectReleaseTracker.track(this);
  }

  public SolrClientCache(ZkStateReader reader) {
    this(reader, new Http2SolrClient.Builder().markInternalRequest().build());
    closeClient = true;
  }

  public SolrClientCache(ZkStateReader reader, Http2SolrClient httpClient) {
    this.httpClient = httpClient;
    this.zkStateReader = reader;
    closeZKStateReader = false;
    closeClient = false;
    assert ObjectReleaseTracker.track(this);
  }

  public synchronized CloudHttp2SolrClient getCloudSolrClient() {
    if (zkStateReader == null) throw new UnsupportedOperationException();
    CloudHttp2SolrClient client;
    SolrZkClient zkClient = zkStateReader.getZkClient();
    if (solrClients.containsKey(zkClient.getZkServerAddress())) {
      client = (CloudHttp2SolrClient) solrClients.get(zkClient.getZkServerAddress());
    } else {
      final List<String> hosts = new ArrayList<String>();
      hosts.add(zkClient.getZkServerAddress());
      CloudHttp2SolrClient.Builder builder = new CloudHttp2SolrClient.Builder(zkStateReader);
      if (httpClient != null) {
        builder = builder.withHttpClient(httpClient);
      }
      client = builder.markInternalRequest().build();
      client.connect();
      solrClients.put(zkClient.getZkServerAddress(), client);
    }

    return client;
  }

  public synchronized Http2SolrClient getHttpSolrClient(String host) {
    Http2SolrClient client;
    if (solrClients.containsKey(host)) {
      client = (Http2SolrClient) solrClients.get(host);
    } else {
      Http2SolrClient.Builder builder = new Http2SolrClient.Builder(host);
      if (httpClient != null) {
        builder = builder.withHttpClient(httpClient);
      }
      client = builder.markInternalRequest().build();
      solrClients.put(host, client);
    }
    return client;
  }

  public void close() {
    synchronized (this) {
      try (ParWork closer = new ParWork(this, false, false)) {
        for (Map.Entry<String,SolrClient> entry : solrClients.entrySet()) {
          closer.collect("solrClient", entry.getValue());
        }
      }
      solrClients.clear();
    }
    if (closeZKStateReader && zkStateReader != null) zkStateReader.close();
    if (closeClient) {
      httpClient.close();
    }
    assert ObjectReleaseTracker.release(this);
  }
}
