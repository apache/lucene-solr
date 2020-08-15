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

import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.ParWork;
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
import java.util.Optional;


/**
 *  The SolrClientCache caches SolrClients so they can be reused by different TupleStreams.
 **/

public class SolrClientCache implements Serializable, Closeable {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Map<String, SolrClient> solrClients = new HashMap<>();
  private final Http2SolrClient httpClient;
  private boolean closeClient;

  public SolrClientCache() {
    this(new Http2SolrClient.Builder().markInternalRequest().build());
    closeClient = true;
  }

  public SolrClientCache(Http2SolrClient httpClient) {
    this.httpClient = httpClient;
    assert ObjectReleaseTracker.track(this);
  }

  public synchronized CloudHttp2SolrClient getCloudSolrClient(String zkHost) {
    CloudHttp2SolrClient client;
    if (solrClients.containsKey(zkHost)) {
      client = (CloudHttp2SolrClient) solrClients.get(zkHost);
    } else {
      final List<String> hosts = new ArrayList<String>();
      hosts.add(zkHost);
      CloudHttp2SolrClient.Builder builder = new CloudHttp2SolrClient.Builder(hosts, Optional.empty());
      if (httpClient != null) {
        builder = builder.withHttpClient(httpClient);
      }
      client = builder.markInternalRequest().build();
      client.connect();
      solrClients.put(zkHost, client);
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

  public synchronized void close() {
    try (ParWork closer = new ParWork(this, true)) {
      for (Map.Entry<String, SolrClient> entry : solrClients.entrySet()) {
        closer.collect("solrClient", entry.getValue());
      }
    }
    if (closeClient) {
      httpClient.close();
    }
    solrClients.clear();
    assert ObjectReleaseTracker.release(this);
  }
}
