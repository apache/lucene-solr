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

import java.io.IOException;
import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *  The SolrClientCache caches SolrClients so they can be reused by different TupleStreams.
 **/

public class SolrClientCache implements Serializable {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Map<String, SolrClient> solrClients = new HashMap<>();
  private final HttpClient httpClient;

  public SolrClientCache() {
    httpClient = null;
  }

  public SolrClientCache(HttpClient httpClient) {
    this.httpClient = httpClient;
  }

  public synchronized CloudSolrClient getCloudSolrClient(String zkHost) {
    Objects.requireNonNull(zkHost, "ZooKeeper host cannot be null!");
    CloudSolrClient client;
    if (solrClients.containsKey(zkHost)) {
      client = (CloudSolrClient) solrClients.get(zkHost);
    } else {
      final List<String> hosts = new ArrayList<String>();
      hosts.add(zkHost);
      CloudSolrClient.Builder builder = new CloudSolrClient.Builder(hosts, Optional.empty()).withSocketTimeout(30000).withConnectionTimeout(15000);
      if (httpClient != null) {
        builder = builder.withHttpClient(httpClient);
      }
      client = builder.build();
      client.connect();
      solrClients.put(zkHost, client);
    }

    return client;
  }

  public synchronized HttpSolrClient getHttpSolrClient(String host) {
    HttpSolrClient client;
    if (solrClients.containsKey(host)) {
      client = (HttpSolrClient) solrClients.get(host);
    } else {
      HttpSolrClient.Builder builder = new HttpSolrClient.Builder(host);
      if (httpClient != null) {
        builder = builder.withHttpClient(httpClient);
      }
      client = builder.build();
      solrClients.put(host, client);
    }
    return client;
  }

  public synchronized void close() {
    for(Map.Entry<String, SolrClient> entry : solrClients.entrySet()) {
      try {
        entry.getValue().close();
      } catch (IOException e) {
        log.error("Error closing SolrClient for {}", entry.getKey(), e);
      }
    }
    solrClients.clear();
  }
}
