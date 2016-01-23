package org.apache.solr.client.solrj.io;

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

import java.io.IOException;
import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
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

  private Map<String, SolrClient> solrClients = new HashMap();

  public synchronized CloudSolrClient getCloudSolrClient(String zkHost) {
    CloudSolrClient client = null;
    if (solrClients.containsKey(zkHost)) {
      client = (CloudSolrClient) solrClients.get(zkHost);
    } else {
      client = new CloudSolrClient(zkHost);
      client.connect();
      solrClients.put(zkHost, client);
    }

    return client;
  }

  public synchronized HttpSolrClient getHttpSolrClient(String host) {
    HttpSolrClient client = null;
    if (solrClients.containsKey(host)) {
      client = (HttpSolrClient) solrClients.get(host);
    } else {
      client = new HttpSolrClient(host);
      solrClients.put(host, client);
    }
    return client;
  }

  public void close() {
    Iterator<SolrClient> it = solrClients.values().iterator();
    while(it.hasNext()) {
      try {
        it.next().close();
      } catch (IOException e) {
        log.error(e.getMessage(), e);
      }
    }
  }
}
