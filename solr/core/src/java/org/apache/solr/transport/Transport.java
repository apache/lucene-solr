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

package org.apache.solr.transport;

import java.io.IOException;
import java.util.List;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.LBHttp2SolrClient;
import org.apache.solr.client.solrj.impl.LBSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.util.NamedList;

public class Transport {
  protected volatile Http2SolrClient defaultClient;
  private LBHttp2SolrClient loadbalancer;
  int   permittedLoadBalancerRequestsMinimumAbsolute = 0;
  float permittedLoadBalancerRequestsMaximumFraction = 1.0f;

  public NamedList<Object> request(final String url, SolrRequest solrRequest) throws IOException, SolrServerException {
    //TODO changes this into a method of http2solrclient
    solrRequest.setBasePath(url);
    return defaultClient.request(solrRequest);
  }

  public LBSolrClient.Rsp request(QueryRequest solrRequest, final List<String> urls) throws IOException, SolrServerException {
    return loadbalancer.request(newLBHttpSolrClientReq(solrRequest, urls));
  }



  protected LBSolrClient.Req newLBHttpSolrClientReq(final QueryRequest req, List<String> urls) {
    int numServersToTry = (int)Math.floor(urls.size() * this.permittedLoadBalancerRequestsMaximumFraction);
    if (numServersToTry < this.permittedLoadBalancerRequestsMinimumAbsolute) {
      numServersToTry = this.permittedLoadBalancerRequestsMinimumAbsolute;
    }
    return new LBSolrClient.Req(req, urls, numServersToTry);
  }
}
