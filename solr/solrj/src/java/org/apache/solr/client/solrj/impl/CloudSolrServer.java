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

import org.apache.http.client.HttpClient;

import java.util.Collection;

/**
 * @deprecated Use {@link org.apache.solr.client.solrj.impl.CloudSolrClient}
 */
@Deprecated
public class CloudSolrServer extends CloudSolrClient {

  public CloudSolrServer(String zkHost) {
    super(zkHost);
  }

  public CloudSolrServer(String zkHost, HttpClient httpClient) {
    super(zkHost, httpClient);
  }

  public CloudSolrServer(Collection<String> zkHosts, String chroot) {
    super(zkHosts, chroot);
  }

  public CloudSolrServer(Collection<String> zkHosts, String chroot, HttpClient httpClient) {
    super(zkHosts, chroot, httpClient);
  }

  public CloudSolrServer(String zkHost, boolean updatesToLeaders) {
    super(zkHost, updatesToLeaders);
  }

  public CloudSolrServer(String zkHost, boolean updatesToLeaders, HttpClient httpClient) {
    super(zkHost, updatesToLeaders, httpClient);
  }

  public CloudSolrServer(String zkHost, LBHttpSolrClient lbClient) {
    super(zkHost, lbClient);
  }

  public CloudSolrServer(String zkHost, LBHttpSolrClient lbClient, boolean updatesToLeaders) {
    super(zkHost, lbClient, updatesToLeaders);
  }
}
